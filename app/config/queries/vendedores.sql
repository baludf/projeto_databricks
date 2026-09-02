-- @desc Lista de vendedores com contatos na fila

SELECT
  vendedor,
  COUNT(*) AS contatos,
  ROUND(AVG(score), 3) AS score_medio
FROM lakehouse_rotaperfume.gold.fila_semanal
GROUP BY vendedor
ORDER BY contatos DESC
