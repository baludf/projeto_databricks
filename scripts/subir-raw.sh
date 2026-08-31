#!/usr/bin/env bash
# =============================================================================
# subir-raw.sh
# Sobe os 10 CSVs (erp + crm) para o Volume /Volumes/{catalog}/bronze/raw/.
# =============================================================================
# Uso: bash scripts/subir-raw.sh <profile>
#
# Exemplo: bash scripts/subir-raw.sh projeto-dados-ia
# =============================================================================

set -euo pipefail

PROFILE="${1:-}"
CATALOG="${CATALOG:-lakehouse_rotaperfume}"

# dados/ está na raiz do repositório (../), não dentro do bundle
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# dados/ está na raiz do repo: scripts/../.. = repo root
DADOS_DIR="${DADOS_DIR:-${SCRIPT_DIR}/../../dados}"

if [[ -z "$PROFILE" ]]; then
  echo "Erro: passe o profile como primeiro argumento."
  echo "Uso: bash scripts/subir-raw.sh <profile>"
  exit 1
fi

# -----------------------------------------------------------------------------
# Se dados/ não existir, gera antes com o material de aula.
# -----------------------------------------------------------------------------
if [[ ! -d "$DADOS_DIR" ]]; then
  echo "Pasta $DADOS_DIR/ não encontrada."
  echo "Gere o dataset com: python3 material/gerar_dataset.py --saida ./dados --seed 42"
  exit 1
fi

# Volume alvo (precisa existir — criado pelo bundle)
VOLUME_BASE="dbfs:/Volumes/${CATALOG}/bronze/raw"

echo ""
echo "=== Subindo CSVs do ERP para ${VOLUME_BASE}/erp ==="
databricks fs cp --recursive --overwrite \
  "${DADOS_DIR}/erp" \
  "${VOLUME_BASE}/erp" \
  --profile "$PROFILE"

echo ""
echo "=== Subindo CSVs do CRM para ${VOLUME_BASE}/crm ==="
databricks fs cp --recursive --overwrite \
  "${DADOS_DIR}/crm" \
  "${VOLUME_BASE}/crm" \
  --profile "$PROFILE"

echo ""
echo "=== Upload concluído ==="
echo "Listando arquivos no Volume:"
databricks fs ls "${VOLUME_BASE}/erp" --profile "$PROFILE"
databricks fs ls "${VOLUME_BASE}/crm" --profile "$PROFILE"
