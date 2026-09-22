# Sistema de Processamento Paralelo de Requisições a um Banco de Dados — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a two-process (cliente/servidor) system that exchanges DB
requests over a real POSIX FIFO, where the server processes requests
concurrently with a fixed thread pool, guarded by a real
`pthread_mutex_t` over a shared in-memory vector persisted to a text
file.

**Architecture:** Shared `protocolo.h/.c` defines the wire format.
`banco.h/.c` is a self-contained, independently-testable module (fixed
array + mutex + persistence). `servidor.c` owns a bounded producer/
consumer queue (mutex+cond) feeding a fixed pool of worker threads that
call into `banco.c` and reply over a per-client response FIFO.
`cliente.c` is a one-shot CLI process: one invocation = one request =
one response = exit. Integration/concurrency proof is a shell script
that launches N clients in parallel and verifies no data loss.

**Tech Stack:** C11, POSIX (pthreads, FIFOs), gcc under WSL2 Ubuntu, no
external dependencies, Make for the build.

**Spec:** `docs/design.md`

## Global Constraints

- Cliente e servidor são binários separados (processos de SO distintos).
- IPC via FIFO POSIX (`mkfifo`), nunca variável global/arquivo com polling ingênuo/chamada de função direta.
- Servidor usa pool de múltiplas threads (`pthread_create`), não uma thread só.
- Acesso à tabela compartilhada protegido por `pthread_mutex_t` real.
- Roda em WSL2 Ubuntu (gcc), não em Windows nativo.
- Sem placeholders: todo arquivo criado abaixo é código completo e compilável.

---

### Task 1: Protocolo de mensagens (`protocolo.h` / `protocolo.c`)

**Files:**
- Create: `src/protocolo.h`
- Create: `src/protocolo.c`
- Test: `testes/test_protocolo.c`

**Interfaces:**
- Produces: `MAX_NOME` (50), `MAX_LINHA` (256), `TipoOperacao {OP_INSERT, OP_SELECT, OP_UPDATE, OP_DELETE, OP_INVALIDA}`, `StatusResposta {STATUS_OK, STATUS_NOT_FOUND, STATUS_ERROR}`, `struct Requisicao {pid_t pid_cliente; long seq; TipoOperacao op; int id; char nome[MAX_NOME];}`, `struct Resposta {long seq; StatusResposta status; int id; char nome[MAX_NOME]; int tem_payload;}`, `int parse_requisicao(const char*, Requisicao*)`, `void formatar_requisicao(const Requisicao*, char*, size_t)`, `int parse_resposta(const char*, Resposta*)`, `void formatar_resposta(const Resposta*, char*, size_t)`, `const char* op_para_texto(TipoOperacao)`, `TipoOperacao texto_para_op(const char*)`, `const char* status_para_texto(StatusResposta)`, `StatusResposta texto_para_status(const char*)`.

- [ ] **Step 1: Create `src/protocolo.h`**

```c
#ifndef PROTOCOLO_H
#define PROTOCOLO_H

#include <sys/types.h>
#include <stddef.h>

#define MAX_NOME 50
#define MAX_LINHA 256

typedef enum { OP_INSERT, OP_SELECT, OP_UPDATE, OP_DELETE, OP_INVALIDA } TipoOperacao;
typedef enum { STATUS_OK, STATUS_NOT_FOUND, STATUS_ERROR } StatusResposta;

typedef struct {
    pid_t pid_cliente;
    long seq;
    TipoOperacao op;
    int id;
    char nome[MAX_NOME];
} Requisicao;

typedef struct {
    long seq;
    StatusResposta status;
    int id;
    char nome[MAX_NOME];
    int tem_payload;
} Resposta;

/* Parses "pid|seq|OP|id|nome" into *req. Returns 0 on success, -1 on malformed input. */
int parse_requisicao(const char *linha, Requisicao *req);

/* Formats *req into buf (>= MAX_LINHA bytes), with trailing '\n'. */
void formatar_requisicao(const Requisicao *req, char *buf, size_t bufsz);

/* Parses "seq|STATUS[|id|nome]" into *resp. Returns 0 on success, -1 on malformed input. */
int parse_resposta(const char *linha, Resposta *resp);

void formatar_resposta(const Resposta *resp, char *buf, size_t bufsz);

const char *op_para_texto(TipoOperacao op);
TipoOperacao texto_para_op(const char *s);

const char *status_para_texto(StatusResposta s);
StatusResposta texto_para_status(const char *s);

#endif
```

- [ ] **Step 2: Create `src/protocolo.c`**

```c
#include "protocolo.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

const char *op_para_texto(TipoOperacao op) {
    switch (op) {
        case OP_INSERT: return "INSERT";
        case OP_SELECT: return "SELECT";
        case OP_UPDATE: return "UPDATE";
        case OP_DELETE: return "DELETE";
        default: return "INVALIDA";
    }
}

TipoOperacao texto_para_op(const char *s) {
    if (strcmp(s, "INSERT") == 0) return OP_INSERT;
    if (strcmp(s, "SELECT") == 0) return OP_SELECT;
    if (strcmp(s, "UPDATE") == 0) return OP_UPDATE;
    if (strcmp(s, "DELETE") == 0) return OP_DELETE;
    return OP_INVALIDA;
}

const char *status_para_texto(StatusResposta s) {
    switch (s) {
        case STATUS_OK: return "OK";
        case STATUS_NOT_FOUND: return "NOT_FOUND";
        default: return "ERROR";
    }
}

StatusResposta texto_para_status(const char *s) {
    if (strcmp(s, "OK") == 0) return STATUS_OK;
    if (strcmp(s, "NOT_FOUND") == 0) return STATUS_NOT_FOUND;
    return STATUS_ERROR;
}

int parse_requisicao(const char *linha, Requisicao *req) {
    char buf[MAX_LINHA];
    strncpy(buf, linha, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
    buf[strcspn(buf, "\r\n")] = '\0';

    char *campos[5];
    int n = 0;
    char *tok = strtok(buf, "|");
    while (tok != NULL && n < 5) {
        campos[n++] = tok;
        tok = strtok(NULL, "|");
    }
    if (n < 4) return -1;

    req->pid_cliente = (pid_t)atoi(campos[0]);
    req->seq = atol(campos[1]);
    req->op = texto_para_op(campos[2]);
    if (req->op == OP_INVALIDA) return -1;
    req->id = atoi(campos[3]);
    req->nome[0] = '\0';
    if (n >= 5) {
        strncpy(req->nome, campos[4], MAX_NOME - 1);
        req->nome[MAX_NOME - 1] = '\0';
    }
    return 0;
}

void formatar_requisicao(const Requisicao *req, char *buf, size_t bufsz) {
    snprintf(buf, bufsz, "%d|%ld|%s|%d|%s\n",
             (int)req->pid_cliente, req->seq, op_para_texto(req->op), req->id, req->nome);
}

int parse_resposta(const char *linha, Resposta *resp) {
    char buf[MAX_LINHA];
    strncpy(buf, linha, sizeof(buf) - 1);
    buf[sizeof(buf) - 1] = '\0';
    buf[strcspn(buf, "\r\n")] = '\0';

    char *campos[4];
    int n = 0;
    char *tok = strtok(buf, "|");
    while (tok != NULL && n < 4) {
        campos[n++] = tok;
        tok = strtok(NULL, "|");
    }
    if (n < 2) return -1;

    resp->seq = atol(campos[0]);
    resp->status = texto_para_status(campos[1]);
    resp->tem_payload = 0;
    resp->id = 0;
    resp->nome[0] = '\0';
    if (n >= 3) {
        resp->id = atoi(campos[2]);
        resp->tem_payload = 1;
    }
    if (n >= 4) {
        strncpy(resp->nome, campos[3], MAX_NOME - 1);
        resp->nome[MAX_NOME - 1] = '\0';
    }
    return 0;
}

void formatar_resposta(const Resposta *resp, char *buf, size_t bufsz) {
    if (resp->tem_payload) {
        snprintf(buf, bufsz, "%ld|%s|%d|%s\n", resp->seq, status_para_texto(resp->status), resp->id, resp->nome);
    } else {
        snprintf(buf, bufsz, "%ld|%s\n", resp->seq, status_para_texto(resp->status));
    }
}
```

- [ ] **Step 3: Create `testes/test_protocolo.c`**

```c
#include "../src/protocolo.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>

static void test_round_trip_insert(void) {
    Requisicao req = { .pid_cliente = 1234, .seq = 7, .op = OP_INSERT, .id = 5 };
    strcpy(req.nome, "Joao");

    char buf[MAX_LINHA];
    formatar_requisicao(&req, buf, sizeof(buf));

    Requisicao lida;
    assert(parse_requisicao(buf, &lida) == 0);
    assert(lida.pid_cliente == 1234);
    assert(lida.seq == 7);
    assert(lida.op == OP_INSERT);
    assert(lida.id == 5);
    assert(strcmp(lida.nome, "Joao") == 0);
}

static void test_round_trip_select_sem_nome(void) {
    Requisicao req = { .pid_cliente = 99, .seq = 1, .op = OP_SELECT, .id = 3 };
    req.nome[0] = '\0';

    char buf[MAX_LINHA];
    formatar_requisicao(&req, buf, sizeof(buf));

    Requisicao lida;
    assert(parse_requisicao(buf, &lida) == 0);
    assert(lida.op == OP_SELECT);
    assert(lida.id == 3);
}

static void test_requisicao_malformada(void) {
    Requisicao lida;
    assert(parse_requisicao("lixo sem pipes", &lida) == -1);
    assert(parse_requisicao("1|2|OPERACAO_INVALIDA|3|x", &lida) == -1);
}

static void test_round_trip_resposta_com_payload(void) {
    Resposta resp = { .seq = 42, .status = STATUS_OK, .id = 5, .tem_payload = 1 };
    strcpy(resp.nome, "Maria");

    char buf[MAX_LINHA];
    formatar_resposta(&resp, buf, sizeof(buf));

    Resposta lida;
    assert(parse_resposta(buf, &lida) == 0);
    assert(lida.seq == 42);
    assert(lida.status == STATUS_OK);
    assert(lida.tem_payload == 1);
    assert(lida.id == 5);
    assert(strcmp(lida.nome, "Maria") == 0);
}

static void test_round_trip_resposta_sem_payload(void) {
    Resposta resp = { .seq = 1, .status = STATUS_NOT_FOUND, .tem_payload = 0 };

    char buf[MAX_LINHA];
    formatar_resposta(&resp, buf, sizeof(buf));

    Resposta lida;
    assert(parse_resposta(buf, &lida) == 0);
    assert(lida.status == STATUS_NOT_FOUND);
    assert(lida.tem_payload == 0);
}

int main(void) {
    test_round_trip_insert();
    test_round_trip_select_sem_nome();
    test_requisicao_malformada();
    test_round_trip_resposta_com_payload();
    test_round_trip_resposta_sem_payload();
    printf("test_protocolo: todos os testes passaram\n");
    return 0;
}
```

- [ ] **Step 4: Compile and run the test (will fail to link until Task 3's Makefile exists — compile directly for now)**

Run (from project root, inside WSL):
```bash
gcc -Wall -Wextra -std=c11 -o /tmp/test_protocolo testes/test_protocolo.c src/protocolo.c && /tmp/test_protocolo
```
Expected: `test_protocolo: todos os testes passaram`

- [ ] **Step 5: Commit**

```bash
git add src/protocolo.h src/protocolo.c testes/test_protocolo.c
git commit -m "feat: add message protocol parser/formatter with tests"
```

---

### Task 2: Módulo de banco em memória (`banco.h` / `banco.c`)

**Files:**
- Create: `src/banco.h`
- Create: `src/banco.c`
- Test: `testes/test_banco.c`

**Interfaces:**
- Consumes: `MAX_NOME` from `protocolo.h` is NOT reused here — `banco.h` defines its own `MAX_NOME` independently so this module has zero dependency on `protocolo.h` (kept as a self-contained, independently testable unit per the design's isolation goal).
- Produces: `MAX_REGISTROS` (1024), `struct Registro {int id; char nome[MAX_NOME]; int ocupado;}`, `struct Banco {Registro registros[MAX_REGISTROS]; int total_ocupados; pthread_mutex_t mutex;}`, `void banco_inicializar(Banco*)`, `void banco_destruir(Banco*)`, `int banco_inserir(Banco*, int id, const char *nome)`, `int banco_buscar(Banco*, int id, char *nome_saida)`, `int banco_atualizar(Banco*, int id, const char *nome)`, `int banco_remover(Banco*, int id)`, `int banco_persistir(Banco*, const char *path)`, `int banco_carregar(Banco*, const char *path)`.

- [ ] **Step 1: Create `src/banco.h`**

```c
#ifndef BANCO_H
#define BANCO_H

#include <pthread.h>

#define MAX_NOME 50
#define MAX_REGISTROS 1024

typedef struct {
    int id;
    char nome[MAX_NOME];
    int ocupado;
} Registro;

typedef struct {
    Registro registros[MAX_REGISTROS];
    int total_ocupados;
    pthread_mutex_t mutex;
} Banco;

void banco_inicializar(Banco *banco);
void banco_destruir(Banco *banco);

/* Retorna 0 em sucesso, -1 se id já existe ou banco cheio. */
int banco_inserir(Banco *banco, int id, const char *nome);

/* Retorna 0 e preenche nome_saida (>= MAX_NOME bytes) se encontrado, -1 se não encontrado. */
int banco_buscar(Banco *banco, int id, char *nome_saida);

/* Retorna 0 se atualizado, -1 se id não existe. */
int banco_atualizar(Banco *banco, int id, const char *nome);

/* Retorna 0 se removido, -1 se id não existe. */
int banco_remover(Banco *banco, int id);

/* Persiste todos os registros ocupados em "id|nome" por linha. Retorna 0 ou -1 em erro de I/O. */
int banco_persistir(Banco *banco, const char *path);

/* Carrega registros no formato de banco_persistir. Retorna 0 (inclusive se o arquivo não existir ainda). */
int banco_carregar(Banco *banco, const char *path);

#endif
```

- [ ] **Step 2: Create `src/banco.c`**

```c
#include "banco.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void banco_inicializar(Banco *banco) {
    memset(banco->registros, 0, sizeof(banco->registros));
    banco->total_ocupados = 0;
    pthread_mutex_init(&banco->mutex, NULL);
}

void banco_destruir(Banco *banco) {
    pthread_mutex_destroy(&banco->mutex);
}

static int indice_do_id(Banco *banco, int id) {
    for (int i = 0; i < MAX_REGISTROS; i++) {
        if (banco->registros[i].ocupado && banco->registros[i].id == id) return i;
    }
    return -1;
}

static int primeiro_slot_livre(Banco *banco) {
    for (int i = 0; i < MAX_REGISTROS; i++) {
        if (!banco->registros[i].ocupado) return i;
    }
    return -1;
}

int banco_inserir(Banco *banco, int id, const char *nome) {
    pthread_mutex_lock(&banco->mutex);
    int resultado = -1;
    if (indice_do_id(banco, id) == -1) {
        int slot = primeiro_slot_livre(banco);
        if (slot != -1) {
            banco->registros[slot].id = id;
            strncpy(banco->registros[slot].nome, nome, MAX_NOME - 1);
            banco->registros[slot].nome[MAX_NOME - 1] = '\0';
            banco->registros[slot].ocupado = 1;
            banco->total_ocupados++;
            resultado = 0;
        }
    }
    pthread_mutex_unlock(&banco->mutex);
    return resultado;
}

int banco_buscar(Banco *banco, int id, char *nome_saida) {
    pthread_mutex_lock(&banco->mutex);
    int idx = indice_do_id(banco, id);
    int resultado = -1;
    if (idx != -1) {
        strncpy(nome_saida, banco->registros[idx].nome, MAX_NOME - 1);
        nome_saida[MAX_NOME - 1] = '\0';
        resultado = 0;
    }
    pthread_mutex_unlock(&banco->mutex);
    return resultado;
}

int banco_atualizar(Banco *banco, int id, const char *nome) {
    pthread_mutex_lock(&banco->mutex);
    int idx = indice_do_id(banco, id);
    int resultado = -1;
    if (idx != -1) {
        strncpy(banco->registros[idx].nome, nome, MAX_NOME - 1);
        banco->registros[idx].nome[MAX_NOME - 1] = '\0';
        resultado = 0;
    }
    pthread_mutex_unlock(&banco->mutex);
    return resultado;
}

int banco_remover(Banco *banco, int id) {
    pthread_mutex_lock(&banco->mutex);
    int idx = indice_do_id(banco, id);
    int resultado = -1;
    if (idx != -1) {
        banco->registros[idx].ocupado = 0;
        banco->total_ocupados--;
        resultado = 0;
    }
    pthread_mutex_unlock(&banco->mutex);
    return resultado;
}

int banco_persistir(Banco *banco, const char *path) {
    pthread_mutex_lock(&banco->mutex);
    FILE *f = fopen(path, "w");
    int resultado = 0;
    if (!f) {
        resultado = -1;
    } else {
        for (int i = 0; i < MAX_REGISTROS; i++) {
            if (banco->registros[i].ocupado) {
                fprintf(f, "%d|%s\n", banco->registros[i].id, banco->registros[i].nome);
            }
        }
        fclose(f);
    }
    pthread_mutex_unlock(&banco->mutex);
    return resultado;
}

int banco_carregar(Banco *banco, const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;

    pthread_mutex_lock(&banco->mutex);
    char linha[128];
    while (fgets(linha, sizeof(linha), f)) {
        linha[strcspn(linha, "\r\n")] = '\0';
        char *sep = strchr(linha, '|');
        if (!sep) continue;
        *sep = '\0';
        int id = atoi(linha);
        int slot = primeiro_slot_livre(banco);
        if (slot == -1) break;
        banco->registros[slot].id = id;
        strncpy(banco->registros[slot].nome, sep + 1, MAX_NOME - 1);
        banco->registros[slot].nome[MAX_NOME - 1] = '\0';
        banco->registros[slot].ocupado = 1;
        banco->total_ocupados++;
    }
    pthread_mutex_unlock(&banco->mutex);
    fclose(f);
    return 0;
}
```

- [ ] **Step 3: Create `testes/test_banco.c`**

```c
#include "../src/banco.h"
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h>

static void test_insert_e_buscar(void) {
    Banco banco;
    banco_inicializar(&banco);

    assert(banco_inserir(&banco, 1, "Joao") == 0);
    char nome[MAX_NOME];
    assert(banco_buscar(&banco, 1, nome) == 0);
    assert(strcmp(nome, "Joao") == 0);

    banco_destruir(&banco);
}

static void test_insert_duplicado_falha(void) {
    Banco banco;
    banco_inicializar(&banco);

    assert(banco_inserir(&banco, 1, "Joao") == 0);
    assert(banco_inserir(&banco, 1, "Outro") == -1);

    banco_destruir(&banco);
}

static void test_atualizar_e_remover(void) {
    Banco banco;
    banco_inicializar(&banco);

    assert(banco_inserir(&banco, 2, "Ana") == 0);
    assert(banco_atualizar(&banco, 2, "Ana Paula") == 0);

    char nome[MAX_NOME];
    assert(banco_buscar(&banco, 2, nome) == 0);
    assert(strcmp(nome, "Ana Paula") == 0);

    assert(banco_remover(&banco, 2) == 0);
    assert(banco_buscar(&banco, 2, nome) == -1);
    assert(banco_remover(&banco, 2) == -1);

    banco_destruir(&banco);
}

static void test_persistir_e_carregar(void) {
    Banco banco;
    banco_inicializar(&banco);
    banco_inserir(&banco, 10, "Carlos");
    banco_inserir(&banco, 20, "Beatriz");
    assert(banco_persistir(&banco, "test_banco_tmp.txt") == 0);
    banco_destruir(&banco);

    Banco banco2;
    banco_inicializar(&banco2);
    assert(banco_carregar(&banco2, "test_banco_tmp.txt") == 0);
    char nome[MAX_NOME];
    assert(banco_buscar(&banco2, 10, nome) == 0);
    assert(strcmp(nome, "Carlos") == 0);
    assert(banco_buscar(&banco2, 20, nome) == 0);
    assert(strcmp(nome, "Beatriz") == 0);
    banco_destruir(&banco2);

    remove("test_banco_tmp.txt");
}

#define NUM_THREADS_TESTE 8
#define INSERTS_POR_THREAD 100

typedef struct {
    Banco *banco;
    int indice_thread;
} ArgThread;

static void *inserir_em_lote(void *arg) {
    ArgThread *a = (ArgThread *)arg;
    int base = a->indice_thread * INSERTS_POR_THREAD;
    for (int i = 0; i < INSERTS_POR_THREAD; i++) {
        char nome[MAX_NOME];
        snprintf(nome, sizeof(nome), "T%d-%d", a->indice_thread, i);
        int r = banco_inserir(a->banco, base + i, nome);
        assert(r == 0);
    }
    return NULL;
}

static void test_concorrencia_sem_corrupcao(void) {
    Banco banco;
    banco_inicializar(&banco);

    pthread_t threads[NUM_THREADS_TESTE];
    ArgThread args[NUM_THREADS_TESTE];

    for (int i = 0; i < NUM_THREADS_TESTE; i++) {
        args[i].banco = &banco;
        args[i].indice_thread = i;
        pthread_create(&threads[i], NULL, inserir_em_lote, &args[i]);
    }
    for (int i = 0; i < NUM_THREADS_TESTE; i++) {
        pthread_join(threads[i], NULL);
    }

    assert(banco.total_ocupados == NUM_THREADS_TESTE * INSERTS_POR_THREAD);

    for (int t = 0; t < NUM_THREADS_TESTE; t++) {
        for (int i = 0; i < INSERTS_POR_THREAD; i += 25) {
            char nome_esperado[MAX_NOME];
            snprintf(nome_esperado, sizeof(nome_esperado), "T%d-%d", t, i);
            char nome[MAX_NOME];
            assert(banco_buscar(&banco, t * INSERTS_POR_THREAD + i, nome) == 0);
            assert(strcmp(nome, nome_esperado) == 0);
        }
    }

    banco_destruir(&banco);
}

int main(void) {
    test_insert_e_buscar();
    test_insert_duplicado_falha();
    test_atualizar_e_remover();
    test_persistir_e_carregar();
    test_concorrencia_sem_corrupcao();
    printf("test_banco: todos os testes passaram (incluindo stress de %d threads x %d inserts)\n",
           NUM_THREADS_TESTE, INSERTS_POR_THREAD);
    return 0;
}
```

- [ ] **Step 4: Compile and run**

```bash
gcc -Wall -Wextra -std=c11 -pthread -o /tmp/test_banco testes/test_banco.c src/banco.c && /tmp/test_banco
```
Expected: `test_banco: todos os testes passaram (incluindo stress de 8 threads x 100 inserts)`

- [ ] **Step 5: Commit**

```bash
git add src/banco.h src/banco.c testes/test_banco.c
git commit -m "feat: add in-memory shared database with mutex and persistence, with concurrency stress test"
```

---

### Task 3: Makefile

**Files:**
- Create: `Makefile`

**Interfaces:**
- Consumes: `src/protocolo.c`, `src/banco.c` (Tasks 1-2), `src/servidor.c`, `src/cliente.c` (Tasks 4-5, not yet created — Makefile references them but `all` target isn't run until Task 5 completes).

- [ ] **Step 1: Create `Makefile`**

```makefile
CC = gcc
CFLAGS = -Wall -Wextra -std=c11 -pthread -Isrc
SRC = src
BIN = bin

all: $(BIN)/servidor $(BIN)/cliente

$(BIN):
	mkdir -p $(BIN)

$(BIN)/servidor: $(SRC)/servidor.c $(SRC)/banco.c $(SRC)/protocolo.c $(SRC)/banco.h $(SRC)/protocolo.h | $(BIN)
	$(CC) $(CFLAGS) -o $@ $(SRC)/servidor.c $(SRC)/banco.c $(SRC)/protocolo.c

$(BIN)/cliente: $(SRC)/cliente.c $(SRC)/protocolo.c $(SRC)/protocolo.h | $(BIN)
	$(CC) $(CFLAGS) -o $@ $(SRC)/cliente.c $(SRC)/protocolo.c

test: $(BIN)/test_protocolo $(BIN)/test_banco
	$(BIN)/test_protocolo
	$(BIN)/test_banco

$(BIN)/test_protocolo: testes/test_protocolo.c $(SRC)/protocolo.c $(SRC)/protocolo.h | $(BIN)
	$(CC) $(CFLAGS) -o $@ testes/test_protocolo.c $(SRC)/protocolo.c

$(BIN)/test_banco: testes/test_banco.c $(SRC)/banco.c $(SRC)/banco.h | $(BIN)
	$(CC) $(CFLAGS) -o $@ testes/test_banco.c $(SRC)/banco.c

clean:
	rm -rf $(BIN) *.fifo banco.txt servidor.log

.PHONY: all test clean
```

- [ ] **Step 2: Run the test target to confirm it wires up Tasks 1-2 correctly**

```bash
make test
```
Expected: both `test_protocolo` and `test_banco` success lines printed, exit code 0.

- [ ] **Step 3: Commit**

```bash
git add Makefile
git commit -m "build: add Makefile for binaries and test targets"
```

---

### Task 4: Processo servidor (`servidor.c`)

**Files:**
- Create: `src/servidor.c`

**Interfaces:**
- Consumes: everything from Task 1 (`protocolo.h`) and Task 2 (`banco.h`, including `banco_inicializar`, `banco_carregar`, `banco_inserir`, `banco_buscar`, `banco_atualizar`, `banco_remover`, `banco_persistir`, `banco_destruir`).
- Produces: the FIFO `bd_requisicoes.fifo` (fixed name, created in the current working directory) that Task 5's cliente writes to, and the per-client FIFOs `bd_resposta_<pid>.fifo` that Task 5's cliente reads from.

- [ ] **Step 1: Create `src/servidor.c`**

```c
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

#define NUM_THREADS_PADRAO 4
#define TAM_FILA 64
#define CAMINHO_REQUISICOES "bd_requisicoes.fifo"
#define CAMINHO_BANCO "banco.txt"

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

    char sobra[MAX_LINHA] = "";
    while (!g_encerrando) {
        char temp[MAX_LINHA];
        ssize_t n = read(fd_req, temp, sizeof(temp) - 1);
        if (n <= 0) {
            if (n == -1 && errno == EINTR) continue;
            usleep(10000);
            continue;
        }
        temp[n] = '\0';

        if (strlen(sobra) + (size_t)n < sizeof(sobra)) {
            strncat(sobra, temp, sizeof(sobra) - strlen(sobra) - 1);
        } else {
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
```

- [ ] **Step 2: Build it**

```bash
gcc -Wall -Wextra -std=c11 -pthread -Isrc -o bin/servidor src/servidor.c src/banco.c src/protocolo.c
```
Expected: no warnings, `bin/servidor` produced.

- [ ] **Step 3: Manual smoke test — start it and confirm it creates the FIFO and blocks waiting**

```bash
./bin/servidor 4 &
sleep 0.3
ls -la bd_requisicoes.fifo
kill -TERM %1
wait
```
Expected: `bd_requisicoes.fifo` exists as a `p` (pipe) type file, and the server prints `[servidor] encerrando...` and exits cleanly after the signal.

- [ ] **Step 4: Commit**

```bash
git add src/servidor.c
git commit -m "feat: add servidor process with bounded queue, thread pool, and FIFO IPC"
```

---

### Task 5: Processo cliente (`cliente.c`)

**Files:**
- Create: `src/cliente.c`

**Interfaces:**
- Consumes: `protocolo.h` (Task 1). Talks to the `bd_requisicoes.fifo` and `bd_resposta_<pid>.fifo` FIFOs that Task 4's servidor creates/reads.

- [ ] **Step 1: Create `src/cliente.c`**

```c
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

    /* Abre em modo leitura NAO-BLOQUEANTE so para se registrar como leitor do FIFO
       antes de mandar a requisicao — evita o deadlock de esperar por si mesmo. */
    int fd_resp = open(caminho_resposta, O_RDONLY | O_NONBLOCK);
    if (fd_resp == -1) {
        perror("open resposta");
        unlink(caminho_resposta);
        return 1;
    }
    limpar_flag_nonblock(fd_resp);

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
```

- [ ] **Step 2: Build both binaries via Make now that all sources exist**

```bash
make all
```
Expected: `bin/servidor` and `bin/cliente` built with no warnings.

- [ ] **Step 3: Manual end-to-end smoke test**

```bash
rm -f banco.txt bd_requisicoes.fifo bd_resposta_*.fifo
./bin/servidor 4 > servidor.log 2>&1 &
sleep 0.3
./bin/cliente INSERT 1 Joao
./bin/cliente SELECT 1
./bin/cliente UPDATE 1 "Joao Silva"
./bin/cliente SELECT 1
./bin/cliente DELETE 1
./bin/cliente SELECT 1
kill -TERM %1
wait
cat banco.txt 2>/dev/null || echo "(banco.txt vazio/removido, esperado apos o DELETE)"
```
Expected output sequence: `OK`, `OK: id=1 nome=Joao`, `OK`, `OK: id=1 nome=Joao Silva`, `OK`, `NOT_FOUND: id=1`.

- [ ] **Step 4: Commit**

```bash
git add src/cliente.c
git commit -m "feat: add cliente process for one-shot DB requests over FIFO"
```

---

### Task 6: Teste de integração e concorrência (`testes/teste_concorrencia.sh`)

**Files:**
- Create: `testes/teste_concorrencia.sh`

**Interfaces:**
- Consumes: `bin/servidor`, `bin/cliente` (Tasks 4-5).
- Produces: `servidor.log`, console timing output consumed by Task 7 (report data collection).

- [ ] **Step 1: Create `testes/teste_concorrencia.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

BIN=bin
K=${1:-16}
THREADS=${2:-4}

rm -f banco.txt bd_requisicoes.fifo bd_resposta_*.fifo

echo "== Subindo servidor com $THREADS threads =="
./$BIN/servidor "$THREADS" > servidor.log 2>&1 &
SERVIDOR_PID=$!
sleep 0.3

cleanup() {
  kill -TERM "$SERVIDOR_PID" 2>/dev/null || true
  wait "$SERVIDOR_PID" 2>/dev/null || true
}
trap cleanup EXIT

echo "== Disparando $K clientes em paralelo (INSERT) =="
INICIO=$(date +%s.%N)
for i in $(seq 1 "$K"); do
  ./$BIN/cliente INSERT "$i" "Cliente$i" &
done
wait
FIM=$(date +%s.%N)
DURACAO=$(echo "$FIM - $INICIO" | bc)
echo "Tempo para $K INSERTs concorrentes com $THREADS threads: ${DURACAO}s"

echo "== Verificando integridade: SELECT de cada ID =="
FALHAS=0
for i in $(seq 1 "$K"); do
  SAIDA=$(./$BIN/cliente SELECT "$i")
  echo "$SAIDA" | grep -q "nome=Cliente$i" || { echo "FALHOU id=$i: $SAIDA"; FALHAS=$((FALHAS+1)); }
done

if [ "$FALHAS" -eq 0 ]; then
  echo "OK: todos os $K registros integros, nenhuma corrupcao sob concorrencia."
else
  echo "FALHA: $FALHAS registros corrompidos/perdidos."
  exit 1
fi
```

- [ ] **Step 2: Make it executable and run it**

```bash
chmod +x testes/teste_concorrencia.sh
./testes/teste_concorrencia.sh 16 4
```
Expected: ends with `OK: todos os 16 registros integros, nenhuma corrupcao sob concorrencia.` and prints the timing line.

- [ ] **Step 3: Commit**

```bash
git add testes/teste_concorrencia.sh
git commit -m "test: add end-to-end concurrency integration test"
```

---

### Task 7: Coleta de dados de desempenho para o relatório

**Files:**
- Create: `docs/resultados.md` (raw timing data table, source for the report's results section)

**Interfaces:**
- Consumes: `testes/teste_concorrencia.sh` (Task 6).

- [ ] **Step 1: Run the concurrency test varying K and thread pool size, capturing timings**

```bash
for threads in 1 2 4 8; do
  for k in 10 50 100; do
    echo "--- threads=$threads k=$k ---"
    ./testes/teste_concorrencia.sh "$k" "$threads" | grep -E "Tempo para|OK:|FALHA"
  done
done
```

- [ ] **Step 2: Write `docs/resultados.md` with the real numbers captured above**

Format as a markdown table: columns `threads`, `k (clientes)`, `tempo (s)`, `resultado (íntegro/corrompido)`. This file is raw data — the actual prose analysis goes in the report (Task 8), which must reference these real numbers, never invented ones.

- [ ] **Step 3: Commit**

```bash
git add docs/resultados.md
git commit -m "docs: record measured throughput/integrity results across thread pool sizes"
```

---

### Task 8: Relatório em formato artigo científico (PDF)

**Files:**
- Create: `relatorio/relatorio.md`
- Create: `relatorio/relatorio.pdf` (generated from the `.md`, not hand-written)

**Interfaces:**
- Consumes: `docs/design.md` (architecture description), `docs/resultados.md` (Task 7's real measurements), the final `src/*.c` files (for the "códigos importantes" section — excerpt the mutex-protected functions from `banco.c` and the FIFO handshake from `cliente.c`/`servidor.c`, not entire files).

- [ ] **Step 1: Write `relatorio/relatorio.md`** covering, in order: identificação dos autores e do trabalho (nomes reais fornecidos pelo usuário, ou `[NOME COMPLETO]` placeholder explícito se ainda não informados), enunciado do projeto, explicação e contexto da aplicação, resultados obtidos com as simulações, trechos de código relevantes (banco.c mutex functions, the FIFO open/read/write sequence), resultados da implementação (a tabela de `docs/resultados.md`), análise e discussão dos resultados (o que o throughput por nº de threads mostra sobre concorrência real vs. contenção do mutex).

- [ ] **Step 2: Convert to PDF**

```bash
pandoc relatorio/relatorio.md -o relatorio/relatorio.pdf --pdf-engine=xelatex -V geometry:margin=2.5cm
```
If `pandoc`/`xelatex` are not available in WSL, install first: `sudo apt-get install -y pandoc texlive-xetex`.

- [ ] **Step 3: Commit**

```bash
git add relatorio/relatorio.md relatorio/relatorio.pdf
git commit -m "docs: add scientific-article-format report with real measured results"
```

---

## Self-Review Notes

- **Spec coverage:** processo cliente/servidor separados (Task 4/5), IPC real via FIFO (Tasks 4/5), pool de threads (Task 4), mutex real (Task 2), persistência em arquivo (Task 2), teste de concorrência provando ausência de corrupção (Tasks 2 and 6), relatório em formato artigo (Task 8), repositório git (initialized already, each task commits). All spec sections have a task.
- **Type consistency:** `Requisicao`/`Resposta` fields match across `protocolo.h` (Task 1), `servidor.c` (Task 4), and `cliente.c` (Task 5) — checked `pid_cliente`, `seq`, `op`, `id`, `nome`, `status`, `tem_payload` are spelled identically everywhere they're used.
- **No placeholders:** every task above contains complete, compilable source — the only deliberate placeholder is the author names in Task 8, explicitly called out as pending user input.
