-- @desc Acompanhamento por vendedor: trabalhados e desfecho

SELECT
  f.vendedor,
  COUNT(*) AS na_fila,
  SUM(CASE WHEN r.cliente_id IS NOT NULL THEN 1 ELSE 0 END) AS trabalhados,
  SUM(CASE WHEN r.status = 'vendeu' THEN 1 ELSE 0 END) AS vendeu,
  SUM(CASE WHEN r.status = 'vai_pensar' THEN 1 ELSE 0 END) AS vai_pensar,
  SUM(CASE WHEN r.status = 'sem_interesse' THEN 1 ELSE 0 END) AS sem_interesse,
  SUM(CASE WHEN r.status = 'nao_atendeu' THEN 1 ELSE 0 END) AS nao_atendeu
FROM lakehouse_rotaperfume.gold.fila_semanal f
LEFT JOIN (
  SELECT cliente_id, status,
         ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY registrado_em DESC) AS _rank
  FROM lakehouse_rotaperfume.gold.retorno_ligacao
) r ON r.cliente_id = f.cliente_id AND r._rank = 1
GROUP BY f.vendedor
ORDER BY trabalhados DESC
