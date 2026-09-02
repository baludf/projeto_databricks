# Databricks notebook source
# notebook: src/gold/07-marts.py
# proposito: Tres marts por diretoria, todos sobre o MESMO fato_vendas.

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# =============================================================================
# mart_vendas_por_vendedor — Diretoria Comercial
# Grão: vendedor_id × ano × mes
# =============================================================================
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.mart_vendas_por_vendedor AS
WITH bruto AS (
  SELECT vendedor_id,
         ano,
         mes,
         COUNT(DISTINCT pedido_id)              AS pedidos,
         COUNT(DISTINCT cliente_id)             AS clientes_atendidos,
         SUM(quantidade)                        AS itens_vendidos,
         SUM(receita)                           AS receita,
         SUM(margem)                            AS margem
  FROM {CATALOG}.gold.fato_vendas
  WHERE NOT devolucao
  GROUP BY vendedor_id, ano, mes
)
SELECT b.vendedor_id,
       v.nome                                   AS vendedor_nome,
       v.regiao,
       v.uf,
       v.ativo                                  AS vendedor_ativo,
       b.ano,
       b.mes,
       b.pedidos,
       b.clientes_atendidos,
       b.itens_vendidos,
       b.receita,
       b.margem,
       ROUND(100 * b.margem / NULLIF(b.receita, 0), 1) AS margem_pct,
       CASE WHEN b.pedidos > 0
            THEN ROUND(b.receita / b.pedidos, 2)
            ELSE 0 END                          AS ticket_medio,
       v.meta_mensal,
       CASE WHEN v.meta_mensal IS NOT NULL AND v.meta_mensal > 0
            THEN ROUND(100 * b.receita / v.meta_mensal, 1)
            ELSE NULL END                       AS atingimento_meta_pct,
       current_timestamp()                      AS _processado_em
FROM bruto b
LEFT JOIN {CATALOG}.gold.dim_vendedor v
       ON v.vendedor_id = b.vendedor_id
""")

spark.sql(f"COMMENT ON TABLE  {CATALOG}.gold.mart_vendas_por_vendedor IS 'Mart Comercial — grão vendedor x mes. Receita exclui devolucao. ticket_medio = receita/pedidos. meta_mensal vem do cadastro.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.vendedor_id IS 'Identificador do vendedor.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.vendedor_nome IS 'Nome do vendedor.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.regiao IS 'Regiao de atuacao do vendedor.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.uf IS 'Unidade federativa de atuacao do vendedor.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.vendedor_ativo IS 'TRUE se vendedor nao foi desligado.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.ano IS 'Ano de referencia.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.mes IS 'Mes de referencia (1-12).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.pedidos IS 'Quantidade de pedidos distintos no mes (exclui devolucao).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.clientes_atendidos IS 'Quantidade de clientes distintos atendidos no mes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.itens_vendidos IS 'Soma de quantidade de itens vendidos no mes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.receita IS 'Soma da receita no mes (exclui devolucao).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.margem IS 'Soma da margem no mes (exclui devolucao).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.margem_pct IS 'margem / receita * 100. Em branco quando receita = 0 no mes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.ticket_medio IS 'Receita dividida por numero de PEDIDOS, nao de itens. 1 pedido com 5 itens = 1 ticket.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.meta_mensal IS 'Meta mensal em R$ do vendedor. NULL se nunca teve meta.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor.atingimento_meta_pct IS 'receita / meta_mensal * 100. NULL se vendedor nao tem meta. 100% = bateu a meta.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_vendas_por_vendedor._processado_em IS 'Timestamp de processamento do registro.'")

# =============================================================================
# mart_produto_performance — Diretoria de Produto
# Grão: sku × ano × mes (com curva ABC no horizonte completo)
# =============================================================================
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.mart_produto_performance AS
WITH base AS (
  SELECT sku,
         marca,
         categoria,
         ano,
         mes,
         SUM(quantidade)                          AS quantidade,
         SUM(receita)                             AS receita,
         SUM(margem)                              AS margem
  FROM {CATALOG}.gold.fato_vendas
  GROUP BY sku, marca, categoria, ano, mes
),
abc_base AS (
  SELECT sku, SUM(receita) AS receita_total
  FROM base GROUP BY sku
),
abc_ordenado AS (
  SELECT sku,
         receita_total,
         SUM(receita_total) OVER (ORDER BY receita_total DESC) AS receita_acumulada,
         SUM(receita_total) OVER ()                             AS receita_geral
  FROM abc_base
),
abc AS (
  SELECT sku,
         CASE WHEN receita_acumulada / NULLIF(receita_geral, 0) <= 0.80 THEN 'A'
              WHEN receita_acumulada / NULLIF(receita_geral, 0) <= 0.95 THEN 'B'
              ELSE 'C' END AS curva_abc
  FROM abc_ordenado
)
SELECT b.sku,
       pr.nome                                    AS produto_nome,
       b.marca,
       b.categoria,
       b.ano,
       b.mes,
       b.quantidade,
       b.receita,
       b.margem,
       ROUND(100 * b.margem / NULLIF(b.receita, 0), 1) AS margem_pct,
       a.curva_abc,
       DENSE_RANK() OVER (PARTITION BY b.ano, b.mes ORDER BY b.receita DESC) AS rank_mes,
       current_timestamp()                         AS _processado_em
FROM base b
LEFT JOIN abc   a ON a.sku = b.sku
LEFT JOIN {CATALOG}.gold.dim_produto pr ON pr.sku = b.sku
""")

spark.sql(f"COMMENT ON TABLE  {CATALOG}.gold.mart_produto_performance IS 'Mart de Produto — grão SKU x mes. Inclui devolucao (produto quer o saldo liquido). curva_abc classifica no horizonte completo.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.sku IS 'Codigo unico do produto (SKU).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.produto_nome IS 'Nome descritivo do produto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.marca IS 'Marca do produto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.categoria IS 'Categoria do produto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.ano IS 'Ano de referencia.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.mes IS 'Mes de referencia (1-12).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.quantidade IS 'Quantidade de itens vendidos (inclui devolucao negativa).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.receita IS 'Receita do SKU no mes (inclui devolucao negativa).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.margem IS 'Margem do SKU no mes (inclui devolucao negativa).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.margem_pct IS 'margem / receita * 100. Em branco quando receita = 0 no mes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.curva_abc IS 'A = 80% da receita acumulada · B = 80% a 95% · C = restante. Calculada na vida inteira do SKU.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance.rank_mes IS 'Ranking do SKU dentro do mes, ordenado por receita. 1 = lider do mes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_produto_performance._processado_em IS 'Timestamp de processamento do registro.'")

# =============================================================================
# mart_financeiro_recebimento — Diretoria Financeira
# Grão: ano × mes (de VENCIMENTO) × forma_pagamento
# =============================================================================
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.mart_financeiro_recebimento AS
WITH base AS (
  SELECT YEAR(data_vencimento)  AS ano,
         MONTH(data_vencimento) AS mes,
         forma_pagamento,
         SUM(valor)                          AS valor_a_receber,
         SUM(CASE WHEN status_pagamento = 'Pago'
                  THEN valor_liquido ELSE 0 END)        AS recebido,
         SUM(CASE WHEN status_pagamento = 'Pago'
                  THEN valor_liquido * taxa_pct / 100
                  ELSE 0 END)                  AS custo_taxa,
         AVG(CASE WHEN status_pagamento = 'Pago' AND data_pagamento IS NOT NULL
                  THEN DATEDIFF(data_pagamento, data_vencimento) END) AS atraso_medio_dias
  FROM {CATALOG}.silver.pagamentos
  GROUP BY YEAR(data_vencimento), MONTH(data_vencimento), forma_pagamento
)
SELECT ano,
       mes,
       forma_pagamento,
       valor_a_receber,
       recebido,
       valor_a_receber - recebido             AS em_aberto,
       ROUND(CASE WHEN valor_a_receber > 0
                  THEN 100 * recebido / valor_a_receber
                  ELSE 0 END, 1)              AS taxa_adimplencia_pct,
       custo_taxa,
       ROUND(atraso_medio_dias, 1)            AS atraso_medio_dias,
       current_timestamp()                    AS _processado_em
FROM base
""")

spark.sql(f"COMMENT ON TABLE  {CATALOG}.gold.mart_financeiro_recebimento IS 'Mart Financeiro — grão mes de VENCIMENTO x forma_pagamento. Planejado pelo que vai vencer, nao pelo que ja foi pago.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento.ano IS 'Ano de vencimento do titulo.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento.mes IS 'Mes de vencimento do titulo (1-12).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento.forma_pagamento IS 'Forma de pagamento (ex: Boleto, Cartao, Pix).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento.valor_a_receber IS 'Soma de valor (parcela) com vencimento no mes. E a previsao — independente do status de pagamento.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento.recebido IS 'Soma de valor_liquido de pagamentos COM status = Pago e vencimento no mes. Atrasados NAO migram de mes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento.em_aberto IS 'valor_a_receber - recebido. Diferenca entre o que era previsto e o que efetivamente entrou no mes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento.taxa_adimplencia_pct IS 'recebido / valor_a_receber * 100. Percentual de titulos pagos no mes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento.custo_taxa IS 'Custo das taxas de cartao/bandeira para os pagamentos recebidos no mes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento.atraso_medio_dias IS 'Media de dias entre data_vencimento e data_pagamento para os PAGOS no mes. NULL se ninguem pagou.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.mart_financeiro_recebimento._processado_em IS 'Timestamp de processamento do registro.'")

# Resumo
for mart in ["mart_vendas_por_vendedor", "mart_produto_performance", "mart_financeiro_recebimento"]:
    n = spark.read.table(f"{CATALOG}.gold.{mart}").count()
    print(f"  {mart:35s}: {n:,} linhas")
