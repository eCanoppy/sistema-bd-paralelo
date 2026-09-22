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
