-- =============================================================================
-- src/ml/13-fila.sql
-- Fila semanal de 200 contatos + 4 funcoes SQL + 3 testes.
-- Roda como sql_task no pipeline.
-- =============================================================================

-- PARTE 1: gold.fila_semanal — os 200 contatos da semana
-- ORDEM DAS OPERACOES:
--   1) JOIN com carteira e descartar nao-elegiveis
--   2) ORDER BY score DESC LIMIT 200
--   3) ROW_NUMBER por vendedor

CREATE OR REPLACE TABLE lakehouse_rotaperfume.gold.fila_semanal AS
WITH elegiveis AS (
  -- 1) Juntar com carteira e descartar quem nao e elegivel
  SELECT sp.cliente_id,
         sp.score,
         sp.faixa,
         sp.versao,
         fc.ticket_medio,
         fc.valor_total,
         fc.atraso_relativo,
         fc.comprou_lancamento,
         fc.skus_distintos,
         fc.marcas_distintas,
         fc.recencia_dias,
         c.vendedor_id,
         v.nome AS vendedor,
         dc.razao_social,
         dc.cidade,
         dc.uf
  FROM lakehouse_rotaperfume.gold.score_propensao sp
  JOIN lakehouse_rotaperfume.gold.features_cliente fc
    ON fc.cliente_id = sp.cliente_id
  JOIN lakehouse_rotaperfume.gold.dim_cliente dc
    ON dc.cliente_id = sp.cliente_id
  JOIN lakehouse_rotaperfume.silver.carteira c
    ON c.cliente_id = sp.cliente_id
   AND c.vigente = true
   AND c.orfao_vendedor_desligado = false
  JOIN lakehouse_rotaperfume.silver.vendedores v
    ON v.vendedor_id = c.vendedor_id
   AND v.ativo = true
),
ordenados AS (
  -- 2) Pegar os 200 maiores scores
  SELECT *, ROW_NUMBER() OVER (ORDER BY score DESC) AS _rank_global
  FROM elegiveis
),
com_ordem AS (
  -- 3) Numerar por vendedor, filtrar top 200
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY vendedor ORDER BY score DESC) AS ordem
  FROM ordenados
  WHERE _rank_global <= 200
),
-- SKU sugerido: mais comprado pelo cliente na marca preferida que NAO comprou em 90d
marca_preferida AS (
  SELECT cliente_id,
         marca,
         SUM(receita) AS receita_marca
  FROM lakehouse_rotaperfume.gold.fato_vendas
  WHERE NOT devolucao
  GROUP BY cliente_id, marca
),
ranked_marcas AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY receita_marca DESC) AS _rank
  FROM marca_preferida
),
marca_top AS (
  SELECT cliente_id, marca AS marca_preferida
  FROM ranked_marcas
  WHERE _rank = 1
),
comprados_recente AS (
  SELECT DISTINCT cliente_id, sku
  FROM lakehouse_rotaperfume.gold.fato_vendas
  WHERE data_pedido >= DATE_SUB(CURRENT_DATE(), 90)
),
skus_sugestao AS (
  SELECT f.cliente_id,
         f.sku,
         COUNT(*) AS qtd_comprada,
         ROW_NUMBER() OVER (PARTITION BY f.cliente_id ORDER BY COUNT(*) DESC) AS _rank
  FROM lakehouse_rotaperfume.gold.fato_vendas f
  JOIN marca_top mt ON mt.cliente_id = f.cliente_id AND mt.marca_preferida = f.marca
  LEFT JOIN comprados_recente cr
    ON cr.cliente_id = f.cliente_id AND cr.sku = f.sku
  WHERE cr.sku IS NULL  -- SKU NAO comprado nos ultimos 90d
    AND NOT f.devolucao
  GROUP BY f.cliente_id, f.sku
)
SELECT co.vendedor,
       co.ordem,
       CAST(co.cliente_id AS INT) AS cliente_id,
       co.razao_social,
       co.cidade,
       co.uf,
       ROUND(co.score, 4) AS score,
       co.faixa,
       ROUND(co.ticket_medio, 2) AS ticket_medio,
       -- Motivo em português — CASE WHEN do mais raro para o mais comum
       CASE
         WHEN co.atraso_relativo > 3 THEN
           CONCAT('Compra a cada ', CAST(ROUND(co.recencia_dias / NULLIF(co.atraso_relativo, 0), 0) AS STRING),
                  ' dias e esta ha ', CAST(CAST(co.recencia_dias AS INT) AS STRING),
                  ' sem pedido. Risco de perder para o concorrente.')
         WHEN co.atraso_relativo > 1.5 THEN
           CONCAT('Esta ', CAST(ROUND(co.atraso_relativo, 1) AS STRING),
                  'x mais atrasado que o ritmo dele.')
         WHEN co.comprou_lancamento = 1 THEN
           'Comprou lancamento recente. Alta chance de repetir.'
         WHEN co.valor_total > 50000 THEN
           CONCAT('Cliente grande, R$ ', FORMAT_NUMBER(co.valor_total, 2), ' no ano. Manter proximo.')
         ELSE
           'Dentro do ritmo. Contato de manutencao.'
       END AS motivo,
       -- Sugestao: SKU mais comprado na marca preferida que nao comprou em 90d
       ss.sku AS sugestao
  FROM com_ordem co
  LEFT JOIN skus_sugestao ss ON ss.cliente_id = co.cliente_id AND ss._rank = 1;

-- COMMENT na tabela
COMMENT ON TABLE lakehouse_rotaperfume.gold.fila_semanal IS
  'Fila semanal de 200 contatos para os vendedores. Score do modelo, motivo em portugues e sugestao de produto.';

-- COMMENT em todas as colunas
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.vendedor IS 'Nome do vendedor responsavel pelo contato';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.ordem IS 'Posicao do cliente dentro da carteira do vendedor (1 = maior score)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.cliente_id IS 'ID unico do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.razao_social IS 'Nome oficial do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.cidade IS 'Cidade do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.uf IS 'Unidade federativa (estado) do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.score IS 'Probabilidade de compra nos proximos 7 dias (0 a 1)';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.faixa IS 'NTILE(4): Fria, Morna, Quente, Muito quente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.ticket_medio IS 'Receita media por pedido do cliente';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.motivo IS 'Frase em portugues explicando por que este cliente esta na fila';
COMMENT ON COLUMN lakehouse_rotaperfume.gold.fila_semanal.sugestao IS 'SKU mais comprado na marca preferida que o cliente nao comprou nos ultimos 90 dias';

-- PARTE 2: 4 funcoes SQL no Unity Catalog

-- Funcao 1: priorizar_carteira
CREATE OR REPLACE FUNCTION lakehouse_rotaperfume.gold.priorizar_carteira(
  p_vendedor STRING,
  p_quantos INT
)
RETURNS TABLE (
  vendedor STRING,
  ordem INT,
  cliente_id INT,
  razao_social STRING,
  cidade STRING,
  uf STRING,
  score DOUBLE,
  faixa STRING,
  ticket_medio DOUBLE,
  motivo STRING,
  sugestao STRING
)
COMMENT 'Retorna a fatia da fila_semanal de um vendedor especifico, em ordem de score.'
RETURN
  SELECT vendedor, ordem, cliente_id, razao_social, cidade, uf,
         score, faixa, ticket_medio, motivo, sugestao
  FROM lakehouse_rotaperfume.gold.fila_semanal
  WHERE vendedor = p_vendedor
    AND ordem <= p_quantos
  ORDER BY ordem;

-- Funcao 2: contexto_cliente
CREATE OR REPLACE FUNCTION lakehouse_rotaperfume.gold.contexto_cliente(
  p_cliente_id INT
)
RETURNS TABLE (
  cliente_id INT,
  razao_social STRING,
  cidade STRING,
  uf STRING,
  recencia_dias DOUBLE,
  frequencia_pedidos DOUBLE,
  valor_total DOUBLE,
  ticket_medio DOUBLE,
  margem_percentual DOUBLE,
  skus_distintos DOUBLE,
  marcas_distintas DOUBLE,
  faixa STRING,
  score DOUBLE
)
COMMENT 'Historico do cliente: ticket medio, marcas, recencia, score. Consulta antes de ligar.'
RETURN
  SELECT sp.cliente_id,
         dc.razao_social,
         dc.cidade,
         dc.uf,
         fc.recencia_dias,
         fc.frequencia_pedidos,
         fc.valor_total,
         fc.ticket_medio,
         fc.margem_percentual,
         fc.skus_distintos,
         fc.marcas_distintas,
         sp.faixa,
         sp.score
  FROM lakehouse_rotaperfume.gold.score_propensao sp
  JOIN lakehouse_rotaperfume.gold.features_cliente fc ON fc.cliente_id = sp.cliente_id
  JOIN lakehouse_rotaperfume.gold.dim_cliente dc ON dc.cliente_id = sp.cliente_id
  WHERE sp.cliente_id = p_cliente_id;

-- Funcao 3: sugerir_produtos
CREATE OR REPLACE FUNCTION lakehouse_rotaperfume.gold.sugerir_produtos(
  p_cliente_id INT
)
RETURNS TABLE (
  sku STRING,
  produto_nome STRING,
  marca STRING,
  categoria STRING,
  qtd_comprada BIGINT,
  ultima_compra DATE
)
COMMENT 'O que o cliente compra e parou de comprar nos ultimos 90 dias. Sugestao de reabastecimento.'
RETURN
  SELECT f.sku,
         p.nome AS produto_nome,
         f.marca,
         f.categoria,
         COUNT(*) AS qtd_comprada,
         MAX(f.data_pedido) AS ultima_compra
  FROM lakehouse_rotaperfume.gold.fato_vendas f
  JOIN lakehouse_rotaperfume.gold.dim_produto p ON p.sku = f.sku
  LEFT JOIN (
    SELECT DISTINCT cliente_id, sku
    FROM lakehouse_rotaperfume.gold.fato_vendas
    WHERE data_pedido >= DATE_SUB(CURRENT_DATE(), 90)
  ) recente ON recente.cliente_id = f.cliente_id AND recente.sku = f.sku
  WHERE f.cliente_id = p_cliente_id
    AND recente.sku IS NULL
    AND NOT f.devolucao
  GROUP BY f.sku, p.nome, f.marca, f.categoria
  ORDER BY COUNT(*) DESC;

-- Funcao 4: checar_disponibilidade
CREATE OR REPLACE FUNCTION lakehouse_rotaperfume.gold.checar_disponibilidade(
  p_sku STRING
)
RETURNS TABLE (
  sku STRING,
  data_snapshot DATE,
  saldo DOUBLE,
  ruptura BOOLEAN
)
COMMENT 'Saldo e ruptura de um SKU no snapshot mais recente de estoque.'
RETURN
  SELECT e.sku,
         e.data_snapshot,
         e.saldo,
         e.ruptura
  FROM lakehouse_rotaperfume.silver.estoque e
  WHERE e.sku = p_sku
    AND e.data_snapshot = (
      SELECT MAX(data_snapshot)
      FROM lakehouse_rotaperfume.silver.estoque
      WHERE sku = p_sku
    );

-- PARTE 3: 3 testes que quebram o job

-- Teste 1: fila tem exatamente 200 linhas
SELECT CASE
         WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal) = 200
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE FILA 1 FALHOU: fila_semanal tem ',
           CAST((SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal) AS STRING),
           ' linhas (esperado 200)'
         ))
       END AS teste_fila_200_linhas;

-- Teste 2: nenhuma linha com motivo nulo ou vazio
SELECT CASE
         WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal
               WHERE motivo IS NULL OR TRIM(motivo) = '') = 0
         THEN 'PASSOU'
         ELSE raise_error('TESTE FILA 2 FALHOU: existe linha com motivo nulo ou vazio')
       END AS teste_fila_motivo_nao_nulo;

-- Teste 3: nenhum score fora do intervalo [0, 1]
SELECT CASE
         WHEN (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.fila_semanal
               WHERE score < 0 OR score > 1) = 0
         THEN 'PASSOU'
         ELSE raise_error('TESTE FILA 3 FALHOU: existe score fora do intervalo [0, 1]')
       END AS teste_fila_score_intervalo;

SELECT '=== FILA + FUNCOES + TESTES CONCLUIDOS ===' AS resultado;
