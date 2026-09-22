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
