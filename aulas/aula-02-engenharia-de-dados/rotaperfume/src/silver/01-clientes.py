# Databricks notebook source
# notebook: src/silver/01-clientes.py
# proposito: Limpa e tipa clientes - CNPJ normalizado, dedup por CNPJ (mantem mais antigo),
#             data_cadastro nos dois formatos, ativo como boolean.

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# -----------------------------------------------------------------------------
# CTE unica: normaliza -> deduplica -> resultado final
# -----------------------------------------------------------------------------
df = spark.sql(f"""
WITH normalizado AS (
  SELECT
    cliente_id,
    -- CNPJ: trim -> remove nao-digitos -> lpad 14 zeros a esquerda
    lpad(regexp_replace(trim(cnpj), '[^0-9]', ''), 14, '0') AS cnpj,
    -- Razao social: initcap + collapse de espacos duplos
    regexp_replace(initcap(trim(razao_social)), ' +', ' ') AS razao_social,
    -- Data: tenta ISO primeiro, depois BR. Nunca usa to_date (ANSI mode aborta)
    coalesce(
      try_to_date(data_cadastro),
      try_to_date(data_cadastro, 'dd/MM/yyyy')
    ) AS data_cadastro,
    ativo
  FROM {CATALOG}.bronze.clientes
),

com_ordem AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY cnpj
      ORDER BY data_cadastro ASC NULLS LAST,
               cliente_id ASC
    ) AS ordem
  FROM normalizado
)

SELECT
  CASE WHEN ordem = 1 THEN cliente_id END        AS cliente_id,
  cnpj,
  CASE WHEN ordem = 1 THEN razao_social END     AS razao_social,
  CASE WHEN ordem = 1 THEN data_cadastro END    AS data_cadastro,
  CASE WHEN ordem = 1
       THEN CASE WHEN trim(ativo) = 'S' THEN true ELSE false END
  END                                            AS ativo,
  current_timestamp()                            AS _processado_em,
  (SELECT COUNT(*) FROM {CATALOG}.bronze.clientes) AS _linhas_origem
FROM com_ordem
WHERE ordem = 1
""")

# Cria ou substitui a tabela silver
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.silver.clientes"
)

# -----------------------------------------------------------------------------
# Metadado + contrato: a tabela REJEITA escritas invalidas
# -----------------------------------------------------------------------------
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.clientes IS 'Clientes limpos - CNPJ normalizado a 14 digitos, dedup por CNPJ (mantem cadastro mais antigo), data_cadastro e ativo tipados.'")

spark.sql(f"ALTER TABLE {CATALOG}.silver.clientes DROP CONSTRAINT IF EXISTS cnpj_14_digitos")
spark.sql(f"ALTER TABLE {CATALOG}.silver.clientes ADD CONSTRAINT cnpj_14_digitos CHECK (length(cnpj) = 14)")
spark.sql(f"ALTER TABLE {CATALOG}.silver.clientes DROP CONSTRAINT IF EXISTS data_cadastro_existe")
spark.sql(f"ALTER TABLE {CATALOG}.silver.clientes ADD CONSTRAINT data_cadastro_existe CHECK (data_cadastro IS NOT NULL)")

print(f"Silver.clientes: {spark.read.table(f'{CATALOG}.silver.clientes').count():,} clientes")
