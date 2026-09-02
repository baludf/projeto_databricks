#!/usr/bin/env bash
# scripts/rodar-tarefa.sh
# Roda uma tarefa isolada do pipeline (evita rodar o job inteiro ~3min30).
# Uso: bash scripts/rodar-tarefa.sh <perfil> <task_key>
#
# Exemplos:
#   bash scripts/rodar-tarefa.sh projeto-dados-ia ml_features
#   bash scripts/rodar-tarefa.sh projeto-dados-ia ml_modelo

set -euo pipefail

if [ $# -ne 2 ]; then
  echo "Uso: bash $0 <perfil> <task_key>"
  echo "Exemplo: bash $0 projeto-dados-ia ml_features"
  exit 1
fi

PERFIL="$1"
TASK_KEY="$2"

echo "=== Rodando tarefa '${TASK_KEY}' com perfil '${PERFIL}' ==="
databricks bundle run rotaperfume_pipeline \
  --profile "$PERFIL" \
  --task "$TASK_KEY"
echo "=== Tarefa '${TASK_KEY}' concluída ==="
