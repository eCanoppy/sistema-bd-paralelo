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

/* le "pid|seq|OP|id|nome" e preenche *req. retorna 0 ok, -1 se invalido */
int parse_requisicao(const char *linha, Requisicao *req);

void formatar_requisicao(const Requisicao *req, char *buf, size_t bufsz);

/* le "seq|STATUS[|id|nome]" e preenche *resp. retorna 0 ok, -1 se invalido */
int parse_resposta(const char *linha, Resposta *resp);

void formatar_resposta(const Resposta *resp, char *buf, size_t bufsz);

const char *op_para_texto(TipoOperacao op);
TipoOperacao texto_para_op(const char *s);

const char *status_para_texto(StatusResposta s);
StatusResposta texto_para_status(const char *s);

#endif
