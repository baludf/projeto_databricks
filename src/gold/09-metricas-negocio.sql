-- =============================================================================
-- src/gold/09-metricas-negocio.sql
-- 6 views de negocio para o Genie e dashboards.
-- Cada view responde UMA pergunta de negocio especifica.
-- =============================================================================

-- VIEW 1: receita_mensal
-- Resposta: "Qual a receita, margem e pedidos por mes? Qual o mes pico do setor?"
CREATE OR REPLACE VIEW lakehouse_rotaperfume.gold.receita_mensal AS
SELECT ano,
       mes,
       CONCAT(LPAD(CAST(ano AS STRING), 4, '0'), '-',
              LPAD(CAST(mes AS STRING), 2, '0')) AS ano_mes,
       pedidos,
       clientes_atendidos,
       receita,
       margem,
       ROUND(margem / NULLIF(receita, 0) * 100, 1) AS margem_pct,
       ticket_medio,
       CASE WHEN receita = MAX(receita) OVER ()
            THEN 'Mes pico do setor'
            ELSE '' END AS mes_pico_setor,
       current_timestamp() AS _processado_em
FROM (
  SELECT ano,
         mes,
         COUNT(DISTINCT pedido_id)  AS pedidos,
         COUNT(DISTINCT cliente_id) AS clientes_atendidos,
         ROUND(SUM(receita), 2)     AS receita,
         ROUND(SUM(margem), 2)      AS margem,
         ROUND(AVG(receita), 2)     AS ticket_medio
  FROM lakehouse_rotaperfume.gold.fato_vendas
  WHERE NOT devolucao
  GROUP BY ano, mes
)
ORDER BY ano DESC, mes DESC;

COMMENT ON TABLE lakehouse_rotaperfume.gold.receita_mensal IS
  'Resposta: receita, margem e pedidos por mes. Identifica o mes pico do setor. Exclui devolucoes.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.ano IS 'Ano do pedido';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.mes IS 'Mes do pedido (1-12)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.ano_mes IS 'Ano e mes no formato YYYY-MM';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.pedidos IS 'Quantidade de pedidos distintos no mes';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.clientes_atendidos IS 'Quantidade de clientes distintos que compraram no mes';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.receita IS 'Soma da receita do mes (exclui devolucoes)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.margem IS 'Soma da margem do mes (exclui devolucoes)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.margem_pct IS 'margem / receita * 100';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.ticket_medio IS 'Receita media por pedido no mes';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal.mes_pico_setor IS 'Marca o mes com maior receita da historia';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.receita_mensal._processado_em IS 'Timestamp de processamento do registro';

-- VIEW 2: ranking_marcas
-- Resposta: "Qual a participacao de cada marca na receita total?"
CREATE OR REPLACE VIEW lakehouse_rotaperfume.gold.ranking_marcas AS
SELECT marca,
       pedidos,
       clientes,
       receita,
       margem,
       ROUND(margem / NULLIF(receita, 0) * 100, 1)       AS margem_pct,
       ROUND(receita / NULLIF(total_geral, 0) * 100, 2)  AS participacao_pct,
       current_timestamp() AS _processado_em
FROM (
  SELECT f.marca,
         COUNT(DISTINCT f.pedido_id)  AS pedidos,
         COUNT(DISTINCT f.cliente_id) AS clientes,
         ROUND(SUM(f.receita), 2)     AS receita,
         ROUND(SUM(f.margem), 2)      AS margem,
         rt.total AS total_geral
  FROM lakehouse_rotaperfume.gold.fato_vendas f
  CROSS JOIN (SELECT SUM(receita) AS total FROM lakehouse_rotaperfume.gold.fato_vendas WHERE NOT devolucao) rt
  WHERE NOT f.devolucao
  GROUP BY f.marca, rt.total
)
ORDER BY receita DESC;

COMMENT ON TABLE lakehouse_rotaperfume.gold.ranking_marcas IS
  'Resposta: marca para receita, margem %, participacao % no total. Exclui devolucoes.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ranking_marcas.marca IS 'Nome da marca do produto';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ranking_marcas.pedidos IS 'Quantidade de pedidos distintos da marca';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ranking_marcas.clientes IS 'Quantidade de clientes distintos que compraram a marca';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ranking_marcas.receita IS 'Soma da receita da marca (exclui devolucoes)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ranking_marcas.margem IS 'Soma da margem da marca (exclui devolucoes)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ranking_marcas.margem_pct IS 'margem / receita * 100 da marca';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ranking_marcas.participacao_pct IS 'receita da marca / receita total * 100';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ranking_marcas._processado_em IS 'Timestamp de processamento do registro';

-- VIEW 3: margem_por_categoria
-- Resposta: "Como varia a margem entre categorias?"
CREATE OR REPLACE VIEW lakehouse_rotaperfume.gold.margem_por_categoria AS
SELECT categoria,
       pedidos,
       skus,
       receita,
       margem,
       ROUND(margem / NULLIF(receita, 0) * 100, 1) AS margem_pct,
       current_timestamp() AS _processado_em
FROM (
  SELECT categoria,
         COUNT(DISTINCT pedido_id) AS pedidos,
         COUNT(DISTINCT sku)       AS skus,
         ROUND(SUM(receita), 2)    AS receita,
         ROUND(SUM(margem), 2)     AS margem
  FROM lakehouse_rotaperfume.gold.fato_vendas
  WHERE NOT devolucao
  GROUP BY categoria
)
ORDER BY margem DESC;

COMMENT ON TABLE lakehouse_rotaperfume.gold.margem_por_categoria IS
  'Resposta: categoria para receita, margem, margem %. Exclui devolucoes.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.margem_por_categoria.categoria IS 'Categoria do produto (ex: Perfumaria, Cosmetico)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.margem_por_categoria.pedidos IS 'Quantidade de pedidos distintos da categoria';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.margem_por_categoria.skus IS 'Quantidade de SKUs distintos da categoria';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.margem_por_categoria.receita IS 'Soma da receita da categoria (exclui devolucoes)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.margem_por_categoria.margem IS 'Soma da margem da categoria (exclui devolucoes)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.margem_por_categoria.margem_pct IS 'margem / receita * 100 da categoria';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.margem_por_categoria._processado_em IS 'Timestamp de processamento do registro';

-- VIEW 4: clientes_em_risco
-- Resposta: "Quais clientes nao compram ha mais de 90 dias?"
CREATE OR REPLACE VIEW lakehouse_rotaperfume.gold.clientes_em_risco AS
SELECT f.cliente_id,
       c.razao_social,
       c.cidade,
       c.uf,
       c.segmento,
       MAX(f.data_pedido)                        AS ultima_compra,
       DATEDIFF(CURRENT_DATE(), MAX(f.data_pedido)) AS dias_sem_comprar,
       COUNT(DISTINCT f.pedido_id)               AS total_pedidos,
       ROUND(SUM(f.receita), 2)                  AS receita_total,
       ROUND(SUM(f.receita) / NULLIF(COUNT(DISTINCT f.pedido_id), 0), 2) AS ticket_medio,
       ROUND(SUM(f.receita) / NULLIF(
         DATEDIFF(CURRENT_DATE(), MIN(f.data_pedido)) / 30, 0), 2) AS media_mensal_estimada
FROM lakehouse_rotaperfume.gold.fato_vendas f
JOIN lakehouse_rotaperfume.gold.dim_cliente c USING (cliente_id)
WHERE NOT f.devolucao
GROUP BY f.cliente_id, c.razao_social, c.cidade, c.uf, c.segmento
HAVING DATEDIFF(CURRENT_DATE(), MAX(f.data_pedido)) > 90
ORDER BY dias_sem_comprar DESC;

COMMENT ON TABLE lakehouse_rotaperfume.gold.clientes_em_risco IS
  'Resposta: clientes sem compra ha mais de 90 dias, com quanto compravam por mes. Risco de perder para concorrente.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.cliente_id IS 'ID unico do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.razao_social IS 'Nome oficial do cliente (razao social)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.cidade IS 'Cidade do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.uf IS 'Unidade federativa (estado) do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.segmento IS 'Segmento do cliente (ex: Atacado, Varejo)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.ultima_compra IS 'Data do ultimo pedido do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.dias_sem_comprar IS 'Dias desde a ultima compra ate hoje';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.total_pedidos IS 'Total de pedidos historicos do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.receita_total IS 'Soma total da receita do cliente (lifetime value)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.ticket_medio IS 'receita_total / total_pedidos';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.clientes_em_risco.media_mensal_estimada IS 'receita_total / meses desde primeiro pedido. Estimativa de quanto comprava por mes.';

-- VIEW 5: efeito_lancamento
-- Resposta: "Como foi o desempenho dos SKUs nos 120 dias apos o lancamento?"
CREATE OR REPLACE VIEW lakehouse_rotaperfume.gold.efeito_lancamento AS
SELECT f.sku,
       p.nome                                    AS produto_nome,
       f.marca,
       f.categoria,
       p.data_lancamento,
       DATEDIFF(f.data_pedido, p.data_lancamento) AS dias_desde_lancamento,
       CASE WHEN DATEDIFF(f.data_pedido, p.data_lancamento) <= 120
            THEN '120 dias apos lancamento'
            ELSE 'Apos 120 dias' END              AS fase_lancamento,
       COUNT(DISTINCT f.pedido_id)                AS pedidos,
       ROUND(SUM(f.receita), 2)                   AS receita,
       ROUND(SUM(f.margem), 2)                    AS margem
FROM lakehouse_rotaperfume.gold.fato_vendas f
JOIN lakehouse_rotaperfume.gold.dim_produto p USING (sku)
WHERE NOT f.devolucao
  AND p.data_lancamento IS NOT NULL
GROUP BY f.sku, p.nome, f.marca, f.categoria, p.data_lancamento,
         DATEDIFF(f.data_pedido, p.data_lancamento)
ORDER BY f.sku, dias_desde_lancamento;

COMMENT ON TABLE lakehouse_rotaperfume.gold.efeito_lancamento IS
  'Resposta: receita dos SKUs nos 120 dias pos-lancamento vs resto. Mostra se lancamentos decolam.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.sku IS 'Codigo unico do produto (SKU)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.produto_nome IS 'Nome do produto';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.marca IS 'Marca do produto';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.categoria IS 'Categoria do produto';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.data_lancamento IS 'Data de lancamento do SKU no mercado';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.dias_desde_lancamento IS 'Dias entre o pedido e o lancamento do SKU';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.fase_lancamento IS 'Fase: 120 dias apos lancamento ou apos 120 dias';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.pedidos IS 'Quantidade de pedidos distintos do SKU nessa fase';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.receita IS 'Soma da receita do SKU nessa fase';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.efeito_lancamento.margem IS 'Soma da margem do SKU nessa fase';

-- VIEW 6: ruptura_por_marca
-- Resposta: "Qual a taxa de ruptura por marca?"
CREATE OR REPLACE VIEW lakehouse_rotaperfume.gold.ruptura_por_marca AS
WITH snapshot_recente AS (
  SELECT sku, data_snapshot, saldo, ruptura
  FROM lakehouse_rotaperfume.silver.estoque
  WHERE data_snapshot = (SELECT MAX(data_snapshot) FROM lakehouse_rotaperfume.silver.estoque)
),
com_marca AS (
  SELECT e.sku,
         COALESCE(p.marca, 'Sem marca') AS marca,
         e.ruptura
  FROM snapshot_recente e
  LEFT JOIN lakehouse_rotaperfume.gold.dim_produto p ON p.sku = e.sku
)
SELECT marca,
       COUNT(*)                                    AS total_skus,
       SUM(CASE WHEN ruptura THEN 1 ELSE 0 END)   AS em_ruptura,
       ROUND(SUM(CASE WHEN ruptura THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) * 100, 1)
                                                   AS pct_ruptura,
       current_timestamp()                         AS _processado_em
FROM com_marca
GROUP BY marca
ORDER BY pct_ruptura DESC;

COMMENT ON TABLE lakehouse_rotaperfume.gold.ruptura_por_marca IS
  'Resposta: percentual de SKUs em ruptura por marca no snapshot mais recente de estoque.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ruptura_por_marca.marca IS 'Marca do produto (ou Sem marca se nao identificada)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ruptura_por_marca.total_skus IS 'Total de SKUs da marca no estoque';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ruptura_por_marca.em_ruptura IS 'Quantidade de SKUs com ruptura = true';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ruptura_por_marca.pct_ruptura IS 'em_ruptura / total_skus * 100. Taxa de ruptura da marca.';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.ruptura_por_marca._processado_em IS 'Timestamp de processamento do registro';
