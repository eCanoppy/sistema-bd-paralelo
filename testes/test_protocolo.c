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
