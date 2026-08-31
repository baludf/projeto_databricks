#!/usr/bin/env bash
# =============================================================================
# criar-catalogo.sh
# Cria o catálogo lakehouse_rotaperfume no workspace.
# =============================================================================
# Uso: bash scripts/criar-catalogo.sh <profile>
#
# Exemplo: bash scripts/criar-catalogo.sh projeto-dados-ia
# =============================================================================

set -euo pipefail

PROFILE="${1:-}"

if [[ -z "$PROFILE" ]]; then
  echo "Erro: passe o profile como primeiro argumento."
  echo "Uso: bash scripts/criar-catalogo.sh <profile>"
  exit 1
fi

echo "=== Criando catálogo lakehouse_rotaperfume (via SQL) ==="

# -----------------------------------------------------------------------------
# POR QUE NÃO ESTÁ NO BUNDLE (databricks.yml → resources/catalogo.yml)?
#
# Em Databricks Free Edition o Default Storage está LIGADO. Nessa configuração,
# a API REST do Unity Catalog RECUSA criar um catálogo via bundle porque exige
# um MANAGED LOCATION explícito — que a conta gratuita não permite configurar.
#
# Erro que a API devolveria:
#   Error: Metastore storage root URL does not exist.
#          Default Storage is enabled in your account. (400 INVALID_STATE)
#
# O comando SQL CREATE CATALOG IF NOT EXISTS funciona perfeitamente,
# pois o Default Storage é usado automaticamente. Por isso o catálogo
# é criado aqui por script, e os schemas/volume ficam no bundle.
# -----------------------------------------------------------------------------

databricks experimental aitools tools query \
  --profile "$PROFILE" \
  "CREATE CATALOG IF NOT EXISTS lakehouse_rotaperfume COMMENT 'Catálogo da noite 2 — engenharia de dados. Bronze: dado cru do ERP/CRM. Silver: dado curado. Gold: métricas para BI.'"

echo "=== Catálogo criado com sucesso ==="
