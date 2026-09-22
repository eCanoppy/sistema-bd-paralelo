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
