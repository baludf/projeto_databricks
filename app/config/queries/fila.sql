-- @desc Fila dos 200 com retorno mais recente
-- @param vendedor STRING = Todos

SELECT
  f.vendedor,
  f.ordem,
  f.cliente_id,
  f.razao_social,
  f.cidade,
  f.uf,
  ROUND(f.score, 2) AS score,
  f.faixa,
  ROUND(f.ticket_medio, 2) AS ticket_medio,
  f.motivo,
  f.sugestao,
  r.status AS retorno_status,
  r.comentario AS retorno_comentario,
  r.registrado_em AS retorno_em,
  r.registrado_por AS retorno_por
FROM lakehouse_rotaperfume.gold.fila_semanal f
LEFT JOIN (
  SELECT cliente_id, status, comentario, registrado_em, registrado_por,
         ROW_NUMBER() OVER (PARTITION BY cliente_id ORDER BY registrado_em DESC) AS _rank
  FROM lakehouse_rotaperfume.gold.retorno_ligacao
) r ON r.cliente_id = f.cliente_id AND r._rank = 1
WHERE ('Todos' = '${vendedor}' OR f.vendedor = '${vendedor}')
ORDER BY f.score DESC
