#!/usr/bin/env bash
set -e
PROJ="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$HOME/execucao-sbdp"
mkdir -p "$RUN"
cd "$RUN"
rm -f bd_requisicoes.fifo banco.txt bd_resposta_*.fifo servidor.log

"$PROJ/bin/servidor" 4 > servidor.log 2>&1 &
SERVIDOR_PID=$!
sleep 0.3

CLI="$PROJ/bin/cliente"
"$CLI" INSERT 1 Joao
"$CLI" SELECT 1
"$CLI" UPDATE 1 "Joao Silva"
"$CLI" SELECT 1
"$CLI" DELETE 1
"$CLI" SELECT 1 || true

kill -TERM "$SERVIDOR_PID"
wait "$SERVIDOR_PID" 2>/dev/null || true

echo "--- banco.txt apos o fluxo (deve estar vazio/ausente pois o unico registro foi deletado) ---"
cat banco.txt 2>/dev/null || echo "(vazio/ausente, esperado)"
echo "--- servidor.log ---"
cat servidor.log
