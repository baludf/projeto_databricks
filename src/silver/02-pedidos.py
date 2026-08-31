# Databricks notebook source
# notebook: src/silver/02-pedidos.py
# proposito: Limpa pedidos - data_pedido nos dois formatos, valor_total -> DECIMAL,
#             cancelado boolean, valor_liquido (zero se cancelado).

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

df = spark.sql(f"""
SELECT
  pedido_id,
  cliente_id,
  vendedor_id,
  coalesce(
    try_to_date(data_pedido),
    try_to_date(data_pedido, 'dd/MM/yyyy')
  )                                                       AS data_pedido,
  year(coalesce(
    try_to_date(data_pedido),
    try_to_date(data_pedido, 'dd/MM/yyyy')
  ))                                                      AS ano,
  month(coalesce(
    try_to_date(data_pedido),
    try_to_date(data_pedido, 'dd/MM/yyyy')
  ))                                                      AS mes,
  canal,
  status,
  -- Pedido cancelado: status 'Cancelado' -> boolean
  CASE WHEN trim(status) = 'Cancelado' THEN true ELSE false END  AS cancelado,
  -- valor_total como DECIMAL(18,2)
  CAST(valor_total AS DECIMAL(18,2))                       AS valor_total,
  -- valor_liquido: zero se cancelado, valor_total caso contrario
  CASE
    WHEN trim(status) = 'Cancelado' THEN CAST(0.00 AS DECIMAL(18,2))
    ELSE CAST(valor_total AS DECIMAL(18,2))
  END                                                      AS valor_liquido,
  current_timestamp()                                      AS _processado_em,
  (SELECT COUNT(*) FROM {CATALOG}.bronze.pedidos)          AS _linhas_origem
FROM {CATALOG}.bronze.pedidos
""")

df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.silver.pedidos"
)

# -----------------------------------------------------------------------------
# Contrato: cancelado -> valor_liquido = 0
# (NAO usar valor_liquido >= 0 - 135 pedidos tem valor negativo legitimo)
# -----------------------------------------------------------------------------
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.pedidos IS 'Pedidos limpos - data_pedido tipada (ISO + BR), valor_total DECIMAL, cancelado boolean, valor_liquido = 0 quando cancelado.'")

spark.sql(f"ALTER TABLE {CATALOG}.silver.pedidos DROP CONSTRAINT IF EXISTS data_pedido_existe")
spark.sql(f"ALTER TABLE {CATALOG}.silver.pedidos ADD CONSTRAINT data_pedido_existe CHECK (data_pedido IS NOT NULL)")
spark.sql(f"ALTER TABLE {CATALOG}.silver.pedidos DROP CONSTRAINT IF EXISTS pedido_cancelado_zerado")
spark.sql(f"ALTER TABLE {CATALOG}.silver.pedidos ADD CONSTRAINT pedido_cancelado_zerado CHECK (NOT cancelado OR valor_liquido = 0)")

n_total    = spark.read.table(f"{CATALOG}.silver.pedidos").count()
n_cancelados = spark.read.table(f"{CATALOG}.silver.pedidos").filter("cancelado").count()
print(f"Silver.pedidos: {n_total:,} pedidos, {n_cancelados:,} cancelados")
