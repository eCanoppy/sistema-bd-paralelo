#define _POSIX_C_SOURCE 200809L

#include "banco.h"
#include "protocolo.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <errno.h>
#include <time.h>

#define NUM_THREADS_PADRAO 4
#define TAM_FILA 64
#define CAMINHO_REQUISICOES "bd_requisicoes.fifo"
#define CAMINHO_BANCO "banco.txt"
/* Bem maior que MAX_LINHA: com muitos clientes concorrentes escrevendo quase
   ao mesmo tempo, um unico read() pode trazer varias mensagens coladas, e o
   buffer de reconstrucao de linha precisa de folga pra isso -- nao eh o
   tamanho de UMA linha, eh o acumulo de VARIAS ainda nao processadas. */
#define TAM_BUFFER_ENTRADA 8192

static volatile sig_atomic_t g_encerrando = 0;

static void handler_sinal(int sig) {
    (void)sig;
    g_encerrando = 1;
}

typedef struct {
    Requisicao itens[TAM_FILA];
    int inicio, fim, count;
    pthread_mutex_t mutex;
    pthread_cond_t nao_vazia;
    pthread_cond_t nao_cheia;
    int encerrando;
} Fila;

static void fila_inicializar(Fila *fila) {
    fila->inicio = fila->fim = fila->count = 0;
    fila->encerrando = 0;
    pthread_mutex_init(&fila->mutex, NULL);
    pthread_cond_init(&fila->nao_vazia, NULL);
    pthread_cond_init(&fila->nao_cheia, NULL);
}

static void fila_enfileirar(Fila *fila, const Requisicao *req) {
    pthread_mutex_lock(&fila->mutex);
    while (fila->count == TAM_FILA && !fila->encerrando) {
        pthread_cond_wait(&fila->nao_cheia, &fila->mutex);
    }
    if (!fila->encerrando) {
        fila->itens[fila->fim] = *req;
        fila->fim = (fila->fim + 1) % TAM_FILA;
        fila->count++;
        pthread_cond_signal(&fila->nao_vazia);
    }
    pthread_mutex_unlock(&fila->mutex);
}

/* Retorna 0 e preenche *req se conseguiu tirar da fila; -1 se encerrando e vazia (worker deve sair). */
static int fila_desenfileirar(Fila *fila, Requisicao *req) {
    pthread_mutex_lock(&fila->mutex);
    while (fila->count == 0 && !fila->encerrando) {
        pthread_cond_wait(&fila->nao_vazia, &fila->mutex);
    }
    int resultado = -1;
    if (fila->count > 0) {
        *req = fila->itens[fila->inicio];
        fila->inicio = (fila->inicio + 1) % TAM_FILA;
        fila->count--;
        pthread_cond_signal(&fila->nao_cheia);
        resultado = 0;
    }
    pthread_mutex_unlock(&fila->mutex);
    return resultado;
}

static void fila_encerrar(Fila *fila) {
    pthread_mutex_lock(&fila->mutex);
    fila->encerrando = 1;
    pthread_cond_broadcast(&fila->nao_vazia);
    pthread_cond_broadcast(&fila->nao_cheia);
    pthread_mutex_unlock(&fila->mutex);
}

typedef struct {
    Fila *fila;
    Banco *banco;
} ContextoWorker;

static void processar_requisicao(Banco *banco, const Requisicao *req, Resposta *resp) {
    resp->seq = req->seq;
    resp->tem_payload = 0;
    resp->id = req->id;
    resp->nome[0] = '\0';

    switch (req->op) {
        case OP_INSERT:
            resp->status = (banco_inserir(banco, req->id, req->nome) == 0) ? STATUS_OK : STATUS_ERROR;
            break;
        case OP_SELECT: {
            char nome[MAX_NOME];
            if (banco_buscar(banco, req->id, nome) == 0) {
                resp->status = STATUS_OK;
                resp->tem_payload = 1;
                strncpy(resp->nome, nome, MAX_NOME - 1);
                resp->nome[MAX_NOME - 1] = '\0';
            } else {
                resp->status = STATUS_NOT_FOUND;
            }
            break;
        }
        case OP_UPDATE:
            resp->status = (banco_atualizar(banco, req->id, req->nome) == 0) ? STATUS_OK : STATUS_NOT_FOUND;
            break;
        case OP_DELETE:
            resp->status = (banco_remover(banco, req->id) == 0) ? STATUS_OK : STATUS_NOT_FOUND;
            break;
        default:
            resp->status = STATUS_ERROR;
    }
}

static void enviar_resposta(pid_t pid_cliente, const Resposta *resp) {
    char path[128];
    snprintf(path, sizeof(path), "bd_resposta_%d.fifo", (int)pid_cliente);

    int fd = open(path, O_WRONLY | O_NONBLOCK);
    if (fd == -1) {
        fprintf(stderr, "[servidor] aviso: cliente %d nao esta mais escutando (%s), resposta descartada\n",
                (int)pid_cliente, strerror(errno));
        return;
    }
    char buf[MAX_LINHA];
    formatar_resposta(resp, buf, sizeof(buf));
    if (write(fd, buf, strlen(buf)) == -1) {
        fprintf(stderr, "[servidor] falha ao escrever resposta para %d: %s\n", (int)pid_cliente, strerror(errno));
    }
    close(fd);
}

static void *worker_thread(void *arg) {
    ContextoWorker *ctx = (ContextoWorker *)arg;
    Requisicao req;
    while (fila_desenfileirar(ctx->fila, &req) == 0) {
        Resposta resp;
        processar_requisicao(ctx->banco, &req, &resp);
        if (req.op != OP_SELECT) {
            banco_persistir(ctx->banco, CAMINHO_BANCO);
        }
        enviar_resposta(req.pid_cliente, &resp);
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    int num_threads = NUM_THREADS_PADRAO;
    if (argc >= 2) {
        int informado = atoi(argv[1]);
        if (informado > 0) num_threads = informado;
    }

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = handler_sinal;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    Banco banco;
    banco_inicializar(&banco);
    banco_carregar(&banco, CAMINHO_BANCO);

    if (mkfifo(CAMINHO_REQUISICOES, 0600) == -1 && errno != EEXIST) {
        perror("mkfifo");
        return 1;
    }

    Fila fila;
    fila_inicializar(&fila);

    ContextoWorker ctx;
    ctx.fila = &fila;
    ctx.banco = &banco;

    pthread_t *threads = malloc(sizeof(pthread_t) * (size_t)num_threads);
    if (!threads) {
        fprintf(stderr, "[servidor] falha ao alocar threads\n");
        return 1;
    }
    for (int i = 0; i < num_threads; i++) {
        pthread_create(&threads[i], NULL, worker_thread, &ctx);
    }

    printf("[servidor] pronto, %d threads no pool, escutando em %s (PID %d)\n",
           num_threads, CAMINHO_REQUISICOES, (int)getpid());
    fflush(stdout);

    /* O_RDWR (nao so O_RDONLY) mantem o proprio servidor como "escritor" do FIFO,
       evitando EOF quando nenhum cliente esta conectado no momento. */
    int fd_req = open(CAMINHO_REQUISICOES, O_RDWR);
    if (fd_req == -1) {
        perror("open requisicoes");
        free(threads);
        return 1;
    }

    char sobra[TAM_BUFFER_ENTRADA] = "";
    while (!g_encerrando) {
        char temp[TAM_BUFFER_ENTRADA];
        ssize_t n = read(fd_req, temp, sizeof(temp) - 1);
        if (n <= 0) {
            if (n == -1 && errno == EINTR) continue;
            struct timespec espera = { .tv_sec = 0, .tv_nsec = 10000000 };
            nanosleep(&espera, NULL);
            continue;
        }
        temp[n] = '\0';

        if (strlen(sobra) + (size_t)n < sizeof(sobra)) {
            strncat(sobra, temp, sizeof(sobra) - strlen(sobra) - 1);
        } else {
            /* So acontece se uma unica linha (sem '\n') sozinha ja excedesse
               8KB, o que nao ocorre com nosso formato de mensagem fixo e curto. */
            sobra[0] = '\0';
            fprintf(stderr, "[servidor] linha excedeu o buffer, descartada\n");
            continue;
        }

        char *inicio = sobra;
        char *quebra;
        while ((quebra = strchr(inicio, '\n')) != NULL) {
            *quebra = '\0';
            Requisicao req;
            if (parse_requisicao(inicio, &req) == 0) {
                fila_enfileirar(&fila, &req);
            } else {
                fprintf(stderr, "[servidor] linha malformada descartada: %s\n", inicio);
            }
            inicio = quebra + 1;
        }
        memmove(sobra, inicio, strlen(inicio) + 1);
    }

    printf("[servidor] encerrando...\n");
    fila_encerrar(&fila);
    for (int i = 0; i < num_threads; i++) {
        pthread_join(threads[i], NULL);
    }
    free(threads);
    banco_persistir(&banco, CAMINHO_BANCO);
    close(fd_req);
    unlink(CAMINHO_REQUISICOES);
    banco_destruir(&banco);
    return 0;
}
