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

/* Retorna 0 em sucesso, -1 se id ja existe ou banco cheio. */
int banco_inserir(Banco *banco, int id, const char *nome);

/* Retorna 0 e preenche nome_saida (>= MAX_NOME bytes) se encontrado, -1 se nao encontrado. */
int banco_buscar(Banco *banco, int id, char *nome_saida);

/* Retorna 0 se atualizado, -1 se id nao existe. */
int banco_atualizar(Banco *banco, int id, const char *nome);

/* Retorna 0 se removido, -1 se id nao existe. */
int banco_remover(Banco *banco, int id);

/* Persiste todos os registros ocupados em "id|nome" por linha. Retorna 0 ou -1 em erro de I/O. */
int banco_persistir(Banco *banco, const char *path);

/* Carrega registros no formato de banco_persistir. Retorna 0 (inclusive se o arquivo nao existir ainda). */
int banco_carregar(Banco *banco, const char *path);

#endif
