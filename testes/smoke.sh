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
ls -la bd_requisicoes.fifo
kill -TERM "$SERVIDOR_PID"
wait "$SERVIDOR_PID" 2>/dev/null || true
echo "--- servidor.log ---"
cat servidor.log
echo DONE
