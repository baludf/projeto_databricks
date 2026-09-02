-- =============================================================================
-- src/gold/11-retorno-ligacao.sql
-- Tabela onde o time registra o que aconteceu apos a ligacao.
-- UNICA tabela do projeto cujo dado NAO vem do pipeline — vem do time.
-- IF NOT EXISTS: um redeploy nao pode apagar o que o vendedor respondeu.
-- =============================================================================

CREATE TABLE IF NOT EXISTS lakehouse_rotaperfume.gold.retorno_ligacao (
  cliente_id      INT           COMMENT 'ID do cliente que foi contatado',
  vendedor        STRING        COMMENT 'Nome do vendedor que fez a ligacao',
  status          STRING        COMMENT 'Resultado da ligacao: vendeu, vai_pensar, sem_interesse, nao_atendeu',
  comentario      STRING        COMMENT 'Texto livre do vendedor sobre a ligacao',
  registrado_em   TIMESTAMP     COMMENT 'Data e hora em que o retorno foi registrado',
  registrado_por  STRING        COMMENT 'E-mail de quem estava logado ao registrar',
  _referencia     DATE          COMMENT 'Data de referencia da fila (semana da ligacao)'
);

COMMENT ON TABLE lakehouse_rotaperfume.gold.retorno_ligacao IS
  'Retorno das ligacoes da fila semanal. Tabela preenchida pelo time via app. '
  'UNICA tabela do projeto cujo dado nao vem do pipeline. Comeca vazia.';
