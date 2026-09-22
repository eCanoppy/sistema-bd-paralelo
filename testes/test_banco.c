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
