# Databricks notebook source
# notebook: src/gold/06-fato-vendas.py
# proposito: fato_vendas — o contrato, escrito antes do SQL.

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# =============================================================================
# CONTRATO — fato_vendas
#
# Granularidade: uma linha por ITEM de pedido.
#
# Filtro de carga: exclui pedidos cancelados (silver.pedidos.cancelado = true).
# NAO exclui devolucao — devolucao entra com quantidade e receita NEGATIVAS
# e a flag devolucao = true.
#
# Por que a devolucao fica DENTRO: se ficar de fora, a gold soma
# R$ 103,6 mi e a silver R$ 102,3 mi. R$ 1,26 mi de diferença entre duas
# camadas do mesmo pipeline. Quem quiser o bruto pede:
#   SUM(receita) FILTER (WHERE NOT devolucao)
#
# Dimensoes: data_pedido, ano, mes, canal, cliente_id, razao_social, segmento,
#            cidade, vendedor_id, sku, categoria, marca, nota_olfativa.
#
# Metricas:
#   quantidade     = silver.itens_pedido.quantidade (negativo se devolucao)
#   preco_praticado = valor unitario real (pode diferir do preco de tabela)
#   receita        = quantidade * preco_praticado (negativa se devolucao)
#   custo          = quantidade * ABS(custo_unitario)
#   margem         = receita - custo
# =============================================================================

spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.fato_vendas
USING DELTA
PARTITIONED BY (ano, mes)
AS
SELECT
  p.data_pedido,
  YEAR(p.data_pedido)  AS ano,
  MONTH(p.data_pedido) AS mes,
  p.canal,
  p.pedido_id,
  p.vendedor_id,
  p.cliente_id,
  c.razao_social,
  COALESCE(b.segmento, 'Nao informado') AS segmento,
  COALESCE(b.cidade,   'Nao informada') AS cidade,
  COALESCE(b.uf,        'N/A')         AS uf,
  i.sku,
  pr.categoria,
  pr.marca,
  pr.nota_olfativa,
  i.quantidade,
  i.preco_praticado,
  i.quantidade * i.preco_praticado                AS receita,
  i.quantidade * ABS(pr.custo_unitario)           AS custo,
  i.quantidade * i.preco_praticado
    - i.quantidade * ABS(pr.custo_unitario)       AS margem,
  i.devolucao,
  current_timestamp() AS _processado_em
FROM {CATALOG}.silver.itens_pedido i
JOIN {CATALOG}.silver.pedidos   p  ON p.pedido_id   = i.pedido_id
JOIN {CATALOG}.silver.clientes  c  ON c.cliente_id  = p.cliente_id
LEFT JOIN {CATALOG}.bronze.clientes b ON b.cliente_id = c.cliente_id
JOIN {CATALOG}.silver.produtos  pr ON pr.sku         = i.sku
WHERE NOT p.cancelado
""")

# COMMENTs — significado de NEGOCIO, nao o tecnico
spark.sql(f"COMMENT ON TABLE  {CATALOG}.gold.fato_vendas IS 'Fato de vendas — grão item de pedido. Exclui cancelados. Devolucao entra com valor negativo e flag. Particionado por ano/mes. Receita = quantidade * preco_praticado. Margem = receita - custo.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.data_pedido IS 'Data do pedido. Usada para filtrar por periodo e calcular recencia.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.ano IS 'Ano do pedido. Particao da tabela.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.mes IS 'Mes do pedido (1-12). Particao da tabela.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.canal IS 'Canal de venda (ex: Representantes, E-commerce). Atributo do pedido, nao do item — repetido por item para evitar JOIN no consumo.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.pedido_id IS 'Identificador unico do pedido. Um pedido pode conter varios itens (SKUs).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.vendedor_id IS 'Identificador do vendedor responsavel pelo pedido.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.cliente_id IS 'Identificador unico do cliente que fez o pedido.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.razao_social IS 'Nome oficial do cliente (razao social). Vem de silver.clientes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.segmento IS 'Segmento do cliente (ex: Atacado, Varejo). COALESCE com Nao informado se ausente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.cidade IS 'Cidade do cliente. COALESCE com Nao informada se ausente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.uf IS 'Unidade federativa (estado) do cliente. COALESCE com N/A se ausente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.sku IS 'Codigo unico do produto (SKU). Chave para joins com dim_produto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.categoria IS 'Categoria do produto (ex: Perfumaria, Cosmetico). Vem de dim_produto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.marca IS 'Marca do produto. Vem de dim_produto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.nota_olfativa IS 'Nota olfativa do produto (ex: floral, amadeirado). Vem de dim_produto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.quantidade IS 'Quantidade do item no pedido. Positivo para venda, negativo para devolucao.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.preco_praticado IS 'Preco unitario real transacionado (pode diferir do preco de tabela por desconto). NUNCA multiplicar pela quantidade absoluta.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.receita IS 'Receita = quantidade * preco_praticado. Negativa para devolucoes. Nao desconta impostos nem frete (politica do contrato).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.custo IS 'Custo = quantidade * custo_unitario. Usa ABS no custo para que devolucoes nao gerem custo credito — devolucao e devolucao de receita, nao de custo.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.margem IS 'Receita menos custo do produto. NAO considera desconto comercial nem frete. A regra de margem vive aqui — quem pedir margem nao reinventa a conta.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas.devolucao IS 'TRUE para itens devolvidos. Receita e quantidade serao negativas. Para o bruto vendido: SUM(receita) FILTER (WHERE NOT devolucao).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.fato_vendas._processado_em IS 'Timestamp de processamento do registro. Gerado automaticamente no INSERT.'")

# Resumo
df_resumo = spark.sql(f"""
SELECT
  COUNT(*)                                                    AS linhas,
  ROUND(SUM(receita), 2)                                      AS receita_liquida,
  ROUND(SUM(receita) FILTER (WHERE NOT devolucao), 2)         AS bruto_vendido,
  ROUND(SUM(receita) FILTER (WHERE    devolucao), 2)          AS devolucoes,
  ROUND(SUM(margem), 2)                                       AS margem,
  ROUND(100 * SUM(margem) / SUM(receita), 1)                  AS margem_pct
FROM {CATALOG}.gold.fato_vendas
""")
df_resumo.show(truncate=False)
