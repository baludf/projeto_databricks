-- @desc KPIs da semana: contatos, vendedores, receita esperada, lift, retorno
-- @param vendedor STRING = Todos

SELECT
  COUNT(*) AS contatos,
  COUNT(DISTINCT vendedor) AS vendedores,
  ROUND(SUM(score * ticket_medio), 2) AS receita_esperada,
  (SELECT lift_top200 FROM lakehouse_rotaperfume.gold.modelo_metricas ORDER BY _treinado_em DESC LIMIT 1) AS lift_top200,
  (SELECT acertos_top200 FROM lakehouse_rotaperfume.gold.modelo_metricas ORDER BY _treinado_em DESC LIMIT 1) AS acertos_top200,
  (SELECT ROUND(taxa_base * 100, 2) FROM lakehouse_rotaperfume.gold.modelo_metricas ORDER BY _treinado_em DESC LIMIT 1) AS taxa_base_pct,
  (SELECT MIN(_referencia) FROM lakehouse_rotaperfume.gold.fila_semanal) AS referencia_fila,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.retorno_ligacao) AS trabalhados,
  (SELECT COUNT(*) FROM lakehouse_rotaperfume.gold.retorno_ligacao WHERE status = 'vendeu') AS venderam
FROM lakehouse_rotaperfume.gold.fila_semanal
WHERE ('Todos' = '${vendedor}' OR vendedor = '${vendedor}')
