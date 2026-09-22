#include "protocolo.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <errno.h>
#include <sys/types.h>

#define CAMINHO_REQUISICOES "bd_requisicoes.fifo"

static void limpar_flag_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL);
    if (flags != -1) {
        fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
    }
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Uso: %s <INSERT|SELECT|UPDATE|DELETE> <id> [nome]\n", argv[0]);
        return 1;
    }

    TipoOperacao op = texto_para_op(argv[1]);
    if (op == OP_INVALIDA) {
        fprintf(stderr, "Operacao invalida: %s\n", argv[1]);
        return 1;
    }
    int id = atoi(argv[2]);
    const char *nome = (argc >= 4) ? argv[3] : "";

    pid_t meu_pid = getpid();
    char caminho_resposta[128];
    snprintf(caminho_resposta, sizeof(caminho_resposta), "bd_resposta_%d.fifo", (int)meu_pid);

    if (mkfifo(caminho_resposta, 0600) == -1 && errno != EEXIST) {
        perror("mkfifo resposta");
        return 1;
    }

    /* O_RDWR pra contar como meu proprio escritor -- senao o read() la embaixo
       retorna EOF na hora, antes do servidor conseguir responder */
    int fd_resp = open(caminho_resposta, O_RDWR);
    if (fd_resp == -1) {
        perror("open resposta");
        unlink(caminho_resposta);
        return 1;
    }

    int fd_req = open(CAMINHO_REQUISICOES, O_WRONLY | O_NONBLOCK);
    if (fd_req == -1) {
        if (errno == ENXIO) {
            fprintf(stderr, "Erro: servidor nao esta rodando (FIFO %s sem leitor)\n", CAMINHO_REQUISICOES);
        } else {
            perror("open requisicoes");
        }
        close(fd_resp);
        unlink(caminho_resposta);
        return 1;
    }
    limpar_flag_nonblock(fd_req);

    Requisicao req;
    memset(&req, 0, sizeof(req));
    req.pid_cliente = meu_pid;
    req.seq = 1;
    req.op = op;
    req.id = id;
    strncpy(req.nome, nome, MAX_NOME - 1);
    req.nome[MAX_NOME - 1] = '\0';

    char buf[MAX_LINHA];
    formatar_requisicao(&req, buf, sizeof(buf));
    if (write(fd_req, buf, strlen(buf)) == -1) {
        perror("write requisicao");
        close(fd_req);
        close(fd_resp);
        unlink(caminho_resposta);
        return 1;
    }
    close(fd_req);

    char resp_buf[MAX_LINHA];
    ssize_t n = read(fd_resp, resp_buf, sizeof(resp_buf) - 1);
    close(fd_resp);
    unlink(caminho_resposta);

    if (n <= 0) {
        fprintf(stderr, "Erro: sem resposta do servidor\n");
        return 1;
    }
    resp_buf[n] = '\0';

    Resposta resp;
    if (parse_resposta(resp_buf, &resp) != 0) {
        fprintf(stderr, "Resposta malformada: %s\n", resp_buf);
        return 1;
    }

    switch (resp.status) {
        case STATUS_OK:
            if (resp.tem_payload) {
                printf("OK: id=%d nome=%s\n", resp.id, resp.nome);
            } else {
                printf("OK\n");
            }
            break;
        case STATUS_NOT_FOUND:
            printf("NOT_FOUND: id=%d\n", id);
            break;
        default:
            printf("ERROR\n");
    }

    return (resp.status == STATUS_OK) ? 0 : 2;
}
