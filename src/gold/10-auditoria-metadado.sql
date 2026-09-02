-- =============================================================================
-- src/gold/10-auditoria-metadado.sql
-- Verifica que toda tabela/view da gold e suas colunas tem COMMENT.
-- Se faltar COMMENT em fato_vendas ou nas 6 views de negocio, quebra com raise_error().
-- Imprime relatorio de cobertura por objeto.
-- =============================================================================

-- PARTE 1: Relatorio de cobertura de metadado (imprime, nao quebra)
SELECT table_name,
       CASE WHEN table_comment IS NULL OR table_comment = ''
            THEN 'SEM COMMENT' ELSE 'OK' END AS status_tabela,
       total_colunas,
       colunas_com_comment,
       colunas_sem_comment
FROM (
  SELECT t.table_name,
         t.comment AS table_comment,
         COUNT(c.column_name)                    AS total_colunas,
         SUM(CASE WHEN c.comment IS NOT NULL AND c.comment != ''
                  THEN 1 ELSE 0 END)             AS colunas_com_comment,
         SUM(CASE WHEN c.comment IS NULL OR c.comment = ''
                  THEN 1 ELSE 0 END)             AS colunas_sem_comment
  FROM lakehouse_rotaperfume.information_schema.tables t
  JOIN lakehouse_rotaperfume.information_schema.columns c
    ON c.table_catalog = t.table_catalog
   AND c.table_schema  = t.table_schema
   AND c.table_name    = t.table_name
  WHERE t.table_schema = 'gold'
    AND t.table_type IN ('BASE TABLE', 'VIEW')
  GROUP BY t.table_name, t.comment
) AS relatorio
ORDER BY colunas_sem_comment DESC, table_name;

-- PARTE 2: Verificacao de tabelas/view da gold SEM COMMENT (quebra o job)
SELECT CASE
         WHEN EXISTS (
           SELECT 1
           FROM lakehouse_rotaperfume.information_schema.tables
           WHERE table_schema = 'gold'
             AND (comment IS NULL OR comment = '')
         )
         THEN raise_error('AUDITORIA FALHOU: existe tabela/view na gold sem COMMENT')
         ELSE 'PASSOU'
       END AS teste_tabela_com_comment;

-- PARTE 3: Verificacao de colunas de fato_vendas SEM COMMENT (quebra o job)
SELECT CASE
         WHEN EXISTS (
           SELECT 1
           FROM lakehouse_rotaperfume.information_schema.columns
           WHERE table_schema = 'gold'
             AND table_name = 'fato_vendas'
             AND (comment IS NULL OR comment = '')
         )
         THEN raise_error('AUDITORIA FALHOU: fato_vendas tem coluna sem COMMENT')
         ELSE 'PASSOU'
       END AS teste_fato_vendas_colunas;

-- PARTE 4: Verificacao de colunas das 6 views de negocio SEM COMMENT (quebra o job)
SELECT CASE
         WHEN EXISTS (
           SELECT 1
           FROM lakehouse_rotaperfume.information_schema.columns
           WHERE table_schema = 'gold'
             AND table_name IN (
               'receita_mensal', 'ranking_marcas', 'margem_por_categoria',
               'clientes_em_risco', 'efeito_lancamento', 'ruptura_por_marca'
             )
             AND (comment IS NULL OR comment = '')
         )
         THEN raise_error('AUDITORIA FALHOU: view de negocio tem coluna sem COMMENT')
         ELSE 'PASSOU'
       END AS teste_views_colunas;

SELECT '=== AUDITORIA DE METADADO CONCLUIDA ===' AS resultado;
