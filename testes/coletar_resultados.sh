#!/usr/bin/env bash
set -uo pipefail
PROJ="$(cd "$(dirname "$0")/.." && pwd)"
SCRIPT="$PROJ/testes/teste_concorrencia.sh"

for threads in 1 2 4 8; do
  for k in 10 50 100; do
    echo "--- threads=$threads k=$k ---"
    "$SCRIPT" "$k" "$threads" 2>&1 | grep -E "Tempo para|OK:|FALHA"
  done
done
