#!/usr/bin/env bash
set -uo pipefail

# FIFOs (mkfifo) nao funcionam no DrvFs do WSL (/mnt/c) -- so em filesystem
# nativo do Linux. Por isso a execucao (nao a compilacao) roda num diretorio
# nativo em $HOME, mesmo com o codigo-fonte vivendo em /mnt/c.
PROJ="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$HOME/execucao-sbdp"
mkdir -p "$RUN"
cd "$RUN"

K=${1:-16}
THREADS=${2:-4}

rm -f banco.txt bd_requisicoes.fifo bd_resposta_*.fifo servidor.log

echo "== Subindo servidor com $THREADS threads =="
"$PROJ/bin/servidor" "$THREADS" > servidor.log 2>&1 &
SERVIDOR_PID=$!
sleep 0.3

cleanup() {
  kill -TERM "$SERVIDOR_PID" 2>/dev/null || true
  wait "$SERVIDOR_PID" 2>/dev/null || true
}
trap cleanup EXIT

CLI="$PROJ/bin/cliente"

echo "== Disparando $K clientes em paralelo (INSERT) =="
INICIO=$(date +%s.%N)
PIDS_CLIENTES=()
for i in $(seq 1 "$K"); do
  "$CLI" INSERT "$i" "Cliente$i" &
  PIDS_CLIENTES+=("$!")
done
# wait so nos PIDs dos clientes -- um "wait" sem argumentos esperaria TAMBEM
# o processo do servidor (job em background nesta mesma shell), que nunca
# termina sozinho e travaria o script.
for pid in "${PIDS_CLIENTES[@]}"; do
  wait "$pid"
done
FIM=$(date +%s.%N)
DURACAO=$(echo "$FIM - $INICIO" | bc)
echo "Tempo para $K INSERTs concorrentes com $THREADS threads: ${DURACAO}s"

echo "== Verificando integridade: SELECT de cada ID =="
FALHAS=0
for i in $(seq 1 "$K"); do
  SAIDA=$("$CLI" SELECT "$i")
  echo "$SAIDA" | grep -q "nome=Cliente$i" || { echo "FALHOU id=$i: $SAIDA"; FALHAS=$((FALHAS+1)); }
done

if [ "$FALHAS" -eq 0 ]; then
  echo "OK: todos os $K registros integros, nenhuma corrupcao sob concorrencia."
else
  echo "FALHA: $FALHAS registros corrompidos/perdidos."
  exit 1
fi
