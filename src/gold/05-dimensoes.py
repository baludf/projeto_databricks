# Databricks notebook source
# notebook: src/gold/05-dimensoes.py
# proposito: Quatro dimensoes conformadas lendo SOMENTE da silver.
#            Cada dim tem COMMENT e grão explícito.

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# =============================================================================
# dim_cliente — uma linha por cliente
#
# Granularidade: cliente_id.
# Decisoes de negocio:
#   - receita_acumulada soma valor_liquido (gold.pedidos: 0 quando cancelado)
#   - dias_sem_comprar e NULL para clientes sem pedido, e para quem so tem cancelado
#   - ativo_cliente = FALSE se o cliente foi desativado (silver.clientes.ativo = false)
# =============================================================================
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_cliente AS
WITH pedidos_cliente AS (
  SELECT cliente_id,
         COUNT(*)                                                AS total_pedidos,
         MIN(data_pedido)                                        AS primeiro_pedido,
         MAX(data_pedido)                                        AS ultimo_pedido,
         SUM(valor_liquido)                                      AS receita_acumulada
  FROM {CATALOG}.silver.pedidos
  GROUP BY cliente_id
),
bronze_info AS (
  SELECT DISTINCT cliente_id, segmento, cidade, uf, bairro
  FROM {CATALOG}.bronze.clientes
)
SELECT c.cliente_id,
       c.razao_social,
       c.cnpj,
       COALESCE(b.segmento, 'Nao informado') AS segmento,
       COALESCE(b.cidade,   'Nao informada') AS cidade,
       COALESCE(b.uf,        'N/A')          AS uf,
       COALESCE(b.bairro,    'Nao informado') AS bairro,
       c.data_cadastro,
       pc.primeiro_pedido,
       pc.ultimo_pedido,
       COALESCE(pc.total_pedidos, 0)                            AS total_pedidos,
       COALESCE(pc.receita_acumulada, CAST(0.00 AS DECIMAL(18,2))) AS receita_acumulada,
       CASE WHEN pc.ultimo_pedido IS NULL THEN NULL
            ELSE DATEDIFF(current_date(), pc.ultimo_pedido) END  AS dias_sem_comprar,
       c.ativo                                                   AS ativo_cliente,
       current_timestamp()                                       AS _processado_em
FROM {CATALOG}.silver.clientes c
LEFT JOIN pedidos_cliente pc USING (cliente_id)
LEFT JOIN bronze_info        b  USING (cliente_id)
""")

spark.sql(f"COMMENT ON TABLE  {CATALOG}.gold.dim_cliente IS 'Dimensao de clientes — grão cliente_id. receita_acumulada reflete pedidos não cancelados. dias_sem_comprar = NULL quando nunca comprou.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.cliente_id IS 'Identificador unico do cliente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.razao_social IS 'Nome oficial do cliente (razao social). Vem de silver.clientes.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.cnpj IS 'CNPJ normalizado a 14 digitos. Chave fiscal do cliente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.segmento IS 'Segmento do cliente (ex: Atacado, Varejo). COALESCE com Nao informado se ausente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.cidade IS 'Cidade do cliente. COALESCE com Nao informada se ausente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.uf IS 'Unidade federativa (estado) do cliente. COALESCE com N/A se ausente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.bairro IS 'Bairro do cliente. COALESCE com Nao informado se ausente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.data_cadastro IS 'Data em que o cliente foi cadastrado no sistema.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.primeiro_pedido IS 'Data do primeiro pedido nao cancelado do cliente. NULL se nunca comprou.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.ultimo_pedido IS 'Data do ultimo pedido nao cancelado do cliente. NULL se nunca comprou.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.total_pedidos IS 'Quantidade total de pedidos nao cancelados do cliente.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.receita_acumulada IS 'Soma de valor_liquido — ja zera pedidos cancelados. Nao desconta devolucao (politica da empresa: devolucao e evento de item, nao do pedido).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.dias_sem_comprar IS 'Dias desde o ultimo pedido nao cancelado. NULL para clientes sem pedido ou que so tem cancelados. Usado para campanhas de reativacao.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente.ativo_cliente IS 'TRUE enquanto o cliente esta ativo no CRM. FALSE para clientes desativados — dashboards de venda nao devem considera-los.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_cliente._processado_em IS 'Timestamp de processamento do registro.'")

# =============================================================================
# dim_produto — uma linha por SKU
#
# Granularidade: sku.
# =============================================================================
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_produto AS
WITH descontinuados AS (
  SELECT DISTINCT sku
  FROM {CATALOG}.silver.itens_pedido
  WHERE sku_descontinuado = TRUE
)
SELECT p.sku,
       p.nome,
       p.marca,
       p.categoria,
       p.nota_olfativa,
       p.unidade,
       p.custo_unitario,
       p.preco_tabela,
       p.data_lancamento,
       CASE WHEN d.sku IS NOT NULL THEN TRUE ELSE FALSE END AS descontinuado,
       p.ativo                                                AS ativo_cadastro,
       current_timestamp()                                    AS _processado_em
FROM {CATALOG}.silver.produtos p
LEFT JOIN descontinuados d ON p.sku = d.sku
""")

spark.sql(f"COMMENT ON TABLE  {CATALOG}.gold.dim_produto IS 'Dimensao de produtos — grão sku. Custo e preco de tabela sao snapshots do cadastro. descontinuado vem de itens_pedido.sku_descontinuado.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.sku IS 'Codigo unico do produto (SKU). Chave para joins com fato_vendas.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.nome IS 'Nome descritivo do produto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.marca IS 'Marca do produto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.categoria IS 'Categoria do produto (ex: Perfumaria, Cosmetico).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.nota_olfativa IS 'Classificacao olfativa do produto (ex: floral, amadeirado).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.unidade IS 'Unidade de medida do produto (ex: un, ml, g).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.custo_unitario IS 'Custo do produto no momento do cadastro. Usado em fato_vendas.custo = quantidade * custo_unitario.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.preco_tabela IS 'Preco de tabela (cheio). preco_praticado no fato_vendas e o valor real transacionado — pode ser diferente por desconto.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.data_lancamento IS 'Data de lancamento do SKU no mercado. Usada em efeito_lancamento para comparar performance nos primeiros 120 dias.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.descontinuado IS 'TRUE se o SKU aparece como descontinuado em qualquer item pedido — flag operacional para esconder de novos pedidos sem perder historico.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto.ativo_cadastro IS 'TRUE enquanto o produto esta ativo no cadastro. FALSE para produtos desativados.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_produto._processado_em IS 'Timestamp de processamento do registro.'")

# =============================================================================
# dim_vendedor — uma linha por vendedor
# =============================================================================
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_vendedor AS
SELECT vendedor_id,
       nome,
       regiao,
       uf,
       data_admissao,
       data_desligamento,
       CASE WHEN data_desligamento IS NULL THEN TRUE ELSE FALSE END AS ativo,
       meta_mensal,
       current_timestamp() AS _processado_em
FROM {CATALOG}.silver.vendedores
""")

spark.sql(f"COMMENT ON TABLE  {CATALOG}.gold.dim_vendedor IS 'Dimensao de vendedores — grão vendedor_id. Ativo = nao desligado. meta_mensal e o alvo do periodo (prompt 6: usado no calculo de atingimento).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_vendedor.vendedor_id IS 'Identificador unico do vendedor.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_vendedor.nome IS 'Nome do vendedor.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_vendedor.regiao IS 'Regiao de atuacao do vendedor.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_vendedor.uf IS 'Unidade federativa (estado) de atuacao do vendedor.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_vendedor.data_admissao IS 'Data de admissao do vendedor na empresa.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_vendedor.data_desligamento IS 'Data de desligamento do vendedor. NULL se ainda esta ativo.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_vendedor.ativo IS 'FALSE a partir da data de desligamento. Vendedores desligados nao devem aparecer no mart_vendas_por_vendedor como ativos.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_vendedor.meta_mensal IS 'Meta mensal em R$ do vendedor — prompt 6 calcula atingimento = receita / meta. NULL se vendedor nunca teve meta.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_vendedor._processado_em IS 'Timestamp de processamento do registro.'")

# =============================================================================
# dim_calendario — uma linha por dia (cobre min..max + 5 anos)
# =============================================================================
spark.sql(f"""
CREATE OR REPLACE TABLE {CATALOG}.gold.dim_calendario AS
WITH limites AS (
  SELECT DATE_TRUNC('month', MIN(data_pedido)) AS mes_inicial,
         DATE_TRUNC('month', MAX(data_pedido)) AS mes_final
  FROM {CATALOG}.silver.pedidos
),
datas AS (
  SELECT explode(sequence(mes_inicial, DATE_ADD(mes_final, 60), INTERVAL 1 DAY)) AS data
  FROM limites
)
SELECT data,
       YEAR(data)                                              AS ano,
       MONTH(data)                                             AS mes,
       DATE_FORMAT(data, 'MMMM')                               AS nome_mes,
       CONCAT(LPAD(CAST(YEAR(data) AS STRING), 4, '0'), '-',
              LPAD(CAST(MONTH(data) AS STRING), 2, '0'))       AS ano_mes,
       QUARTER(data)                                           AS trimestre,
       DAYOFWEEK(data)                                         AS dia_semana_num,
       DATE_FORMAT(data, 'EEEE')                               AS dia_semana_nome,
       CASE WHEN MONTH(data) IN (4, 6, 10) THEN TRUE
            ELSE FALSE END                                     AS mes_pico_setor,
       current_timestamp()                                     AS _processado_em
FROM datas
""")

spark.sql(f"COMMENT ON TABLE  {CATALOG}.gold.dim_calendario IS 'Dimensao calendario — grão dia. Cobre 5 anos a partir do mes do primeiro pedido. mes_pico_setor sinaliza datas de venda alta do setor.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario.data IS 'Data completa (dia). Chave da dimensao.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario.ano IS 'Ano da data.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario.mes IS 'Mes da data (1-12).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario.nome_mes IS 'Nome do mes por extenso (ex: janeiro, fevereiro).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario.ano_mes IS 'Ano e mes no formato YYYY-MM.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario.trimestre IS 'Trimestre da data (1-4).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario.dia_semana_num IS 'Numero do dia da semana (1=domingo, 7=sabado).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario.dia_semana_nome IS 'Nome do dia da semana por extenso (ex: segunda-feira).'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario.mes_pico_setor IS 'TRUE em abril (mes das maes), junho (dia dos namorados) e outubro (dia das criancas) — picos historicos do setor de perfumaria. Usado em mart_produto_performance para comparar performance em pico vs fora.'")
spark.sql(f"COMMENT ON COLUMN {CATALOG}.gold.dim_calendario._processado_em IS 'Timestamp de processamento do registro.'")

# Resumo
for dim in ["dim_cliente", "dim_produto", "dim_vendedor", "dim_calendario"]:
    n = spark.read.table(f"{CATALOG}.gold.{dim}").count()
    print(f"  {dim:20s}: {n:,} linhas")
