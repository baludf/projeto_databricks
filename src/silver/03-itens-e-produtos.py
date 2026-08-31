# Databricks notebook source
# notebook: src/silver/03-itens-e-produtos.py
# proposito: Limpa produtos e itens_pedido. SKU e a chave real de produtos.

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# PRODUTOS (chave = sku, nao produto_id)
df_prod = spark.sql(f"""
SELECT
  sku,
  descricao                              AS nome,
  categoria,
  marca,
  nota_olfativa,
  CAST(preco_tabela AS DECIMAL(12,2))    AS preco_tabela,
  CAST(custo_unitario AS DECIMAL(12,2))  AS custo_unitario,
  unidade,
  coalesce(try_to_date(data_lancamento), try_to_date(data_lancamento, 'dd/MM/yyyy'))
                                                       AS data_lancamento,
  CASE WHEN trim(ativo) = 'S' THEN true ELSE false END  AS ativo,
  current_timestamp()                                    AS _processado_em,
  (SELECT COUNT(*) FROM {CATALOG}.bronze.produtos)      AS _linhas_origem
FROM {CATALOG}.bronze.produtos
""")
df_prod.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.produtos")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.produtos IS 'Produtos limpos - chave = sku, tipos corrigidos, ativo boolean.'")

# ITENS_PEDIDO - join com produtos por sku
df_itens = spark.sql(f"""
WITH produtos_ativo AS (
  SELECT sku, ativo FROM {CATALOG}.silver.produtos
)
SELECT
  i.item_id,
  i.pedido_id,
  i.sku,
  CASE WHEN CAST(i.quantidade AS INT) < 0 THEN true ELSE false END AS devolucao,
  CAST(i.quantidade AS INT)                  AS quantidade,
  ABS(CAST(i.quantidade AS INT))             AS quantidade_abs,
  CAST(i.valor_bruto AS DECIMAL(12,2))       AS valor_bruto,
  CAST(i.desconto_pct AS DECIMAL(6,2))       AS desconto_pct,
  CAST(i.preco_praticado AS DECIMAL(12,2))   AS preco_praticado,
  CASE WHEN p.ativo IS NULL OR p.ativo = false THEN true ELSE false END
                                            AS sku_descontinuado,
  current_timestamp()                       AS _processado_em,
  (SELECT COUNT(*) FROM {CATALOG}.bronze.itens_pedido) AS _linhas_origem
FROM {CATALOG}.bronze.itens_pedido i
LEFT JOIN produtos_ativo p ON i.sku = p.sku
""")
df_itens.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.itens_pedido")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.itens_pedido IS 'Itens de pedido - devolucao boolean, quantidade_abs para somas, sku_descontinuado por join. Devolucao NAO e descartada.'")
spark.sql(f"ALTER TABLE {CATALOG}.silver.itens_pedido DROP CONSTRAINT IF EXISTS quantidade_abs_positiva")
spark.sql(f"ALTER TABLE {CATALOG}.silver.itens_pedido ADD CONSTRAINT quantidade_abs_positiva CHECK (quantidade_abs > 0)")

n_dev = spark.read.table(f"{CATALOG}.silver.itens_pedido").filter("devolucao").count()
n_des = spark.read.table(f"{CATALOG}.silver.itens_pedido").filter("sku_descontinuado").count()
print(f"silver.produtos:  {spark.read.table(f'{CATALOG}.silver.produtos').count():,}")
print(f"silver.itens_pedido: {spark.read.table(f'{CATALOG}.silver.itens_pedido').count():,} ({n_dev:,} devolucoes, {n_des} com SKU descontinuado)")
