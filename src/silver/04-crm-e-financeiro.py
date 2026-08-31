# Databricks notebook source
# notebook: src/silver/04-crm-e-financeiro.py
# proposito: Limpa CRM e financeiro conforme schema real do bronze.

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# VENDEDORES
df = spark.sql(f"""
SELECT vendedor_id, nome, regiao, uf,
       coalesce(try_to_date(data_admissao), try_to_date(data_admissao, 'dd/MM/yyyy'))    AS data_admissao,
       coalesce(try_to_date(data_desligamento), try_to_date(data_desligamento, 'dd/MM/yyyy')) AS data_desligamento,
       CAST(meta_mensal AS DECIMAL(12,2)) AS meta_mensal,
       current_timestamp() AS _processado_em,
       (SELECT COUNT(*) FROM {CATALOG}.bronze.vendedores) AS _linhas_origem
FROM {CATALOG}.bronze.vendedores
""")
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.vendedores")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.vendedores IS 'Vendedores - datas tipadas, meta_mensal decimal.'")

# CARTEIRA (com vigente + orfao_vendedor_desligado)
df = spark.sql(f"""
WITH v AS (SELECT vendedor_id, data_desligamento FROM {CATALOG}.silver.vendedores)
SELECT c.carteira_id, c.cliente_id, c.vendedor_id,
       coalesce(try_to_date(c.data_inicio), try_to_date(c.data_inicio, 'dd/MM/yyyy')) AS data_inicio,
       coalesce(try_to_date(c.data_fim),    try_to_date(c.data_fim,    'dd/MM/yyyy')) AS data_fim,
       CASE WHEN c.data_fim IS NULL AND v.data_desligamento IS NULL THEN true ELSE false END AS vigente,
       CASE WHEN c.data_fim IS NULL AND v.data_desligamento IS NOT NULL THEN true ELSE false END AS orfao_vendedor_desligado,
       current_timestamp() AS _processado_em,
       (SELECT COUNT(*) FROM {CATALOG}.bronze.carteira) AS _linhas_origem
FROM {CATALOG}.bronze.carteira c
LEFT JOIN v ON c.vendedor_id = v.vendedor_id
""")
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.carteira")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.carteira IS 'Carteira - vigente (data_fim null E vendedor ativo), orfao_vendedor_desligado expoe o problema.'")

# OPORTUNIDADES - normaliza etapa
df = spark.sql(f"""
SELECT oportunidade_id, cliente_id, vendedor_id, origem,
       coalesce(try_to_date(data_abertura), try_to_date(data_abertura, 'dd/MM/yyyy')) AS data_abertura,
       CASE etapa WHEN 'Fechado ganho' THEN 'Ganha' WHEN 'Fechado perdido' THEN 'Perdida' ELSE etapa END AS etapa,
       CAST(probabilidade_pct AS INT) AS probabilidade_pct,
       CAST(valor_estimado AS DECIMAL(18,2)) AS valor_estimado,
       coalesce(try_to_date(data_fechamento), try_to_date(data_fechamento, 'dd/MM/yyyy')) AS data_fechamento,
       CAST(ciclo_dias AS INT) AS ciclo_dias,
       motivo_perda,
       current_timestamp() AS _processado_em,
       (SELECT COUNT(*) FROM {CATALOG}.bronze.oportunidades) AS _linhas_origem
FROM {CATALOG}.bronze.oportunidades
""")
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.oportunidades")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.oportunidades IS 'Oportunidades - etapa normalizada (Fechado -> Ganha/Perdida), datas tipadas, valor_estimado decimal.'")

# VISITAS
df = spark.sql(f"""
SELECT visita_id, cliente_id, vendedor_id,
       coalesce(try_to_date(data_visita), try_to_date(data_visita, 'dd/MM/yyyy')) AS data_visita,
       resultado,
       CAST(duracao_min AS INT) AS duracao_minutos,
       current_timestamp() AS _processado_em,
       (SELECT COUNT(*) FROM {CATALOG}.bronze.visitas) AS _linhas_origem
FROM {CATALOG}.bronze.visitas
""")
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.visitas")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.visitas IS 'Visitas - data_visita tipada, duracao_minutos INT.'")

# PAGAMENTOS
df = spark.sql(f"""
SELECT pagamento_id, pedido_id, forma_pagamento, CAST(parcelas AS INT) AS parcelas,
       CAST(valor AS DECIMAL(12,2)) AS valor,
       CAST(taxa_pct AS DECIMAL(6,2)) AS taxa_pct,
       CAST(valor_liquido AS DECIMAL(12,2)) AS valor_liquido,
       coalesce(try_to_date(data_vencimento), try_to_date(data_vencimento, 'dd/MM/yyyy')) AS data_vencimento,
       coalesce(try_to_date(data_pagamento),  try_to_date(data_pagamento,  'dd/MM/yyyy')) AS data_pagamento,
       status_pagamento,
       current_timestamp() AS _processado_em,
       (SELECT COUNT(*) FROM {CATALOG}.bronze.pagamentos) AS _linhas_origem
FROM {CATALOG}.bronze.pagamentos
""")
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.pagamentos")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.pagamentos IS 'Pagamentos - valores como decimal, datas tipadas.'")

# ESTOQUE - bronze ja tem coluna ruptura (texto), mas calculamos via saldo
df = spark.sql(f"""
SELECT data_snapshot, sku, CAST(saldo AS INT) AS saldo,
       CASE WHEN CAST(saldo AS INT) = 0 THEN true ELSE false END AS ruptura,
       coalesce(try_to_date(data_snapshot), try_to_date(data_snapshot, 'dd/MM/yyyy')) AS data_snapshot_dt,
       current_timestamp() AS _processado_em,
       (SELECT COUNT(*) FROM {CATALOG}.bronze.estoque) AS _linhas_origem
FROM {CATALOG}.bronze.estoque
""")
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.estoque")
spark.sql(f"COMMENT ON TABLE {CATALOG}.silver.estoque IS 'Estoque - saldo INT, ruptura boolean (saldo=0).'")

# Resumo
for t in ["vendedores", "carteira", "oportunidades", "visitas", "pagamentos", "estoque"]:
    n = spark.read.table(f"{CATALOG}.silver.{t}").count()
    print(f"  silver.{t:15s}: {n:,}")
print()
n_orfao = spark.read.table(f"{CATALOG}.silver.carteira").filter("orfao_vendedor_desligado").count()
print(f"Carteiras de vendedor desligado: {n_orfao}")
