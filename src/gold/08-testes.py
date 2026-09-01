# Databricks notebook source
# notebook: src/gold/08-testes.py
# proposito: Os 9 testes de qualidade. Cada um levanta excecao com raise_error()
#            quando falha, para o job PARAR. NUNCA ajustar o teste — corrigir
#            a transformacao.

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# =============================================================================
# Mecanismo: raise_error() retorna tipo NOTHING, entao ele so funciona dentro
# de CASE WHEN (senao o tipo de retorno da query vira nada e o Databricks
# reclama). Usamos SELECT CASE WHEN <condicao de falha> THEN raise_error(...)
# ELSE 'PASSOU' END.
# =============================================================================

print("=== INICIO DOS 9 TESTES DE QUALIDADE ===")

def testar(nome, query):
    """Roda um teste. Levanta excecao com raise_error() se falhar."""
    try:
        spark.sql(query).collect()
        print(f"  [OK] {nome}")
        return True
    except Exception as e:
        # raise_error aborta o job — pegamos a mensagem para mostrar
        msg = str(e).split("\n")[-2] if len(str(e).split("\n")) > 1 else str(e)
        print(f"  [FALHOU] {nome}: {msg}")
        raise

# -----------------------------------------------------------------------------
# Teste 1: receita gold = silver (valor dinamico, tolerancia 0,01)
#
# Limitacao: 36 pedidos de clientes removidos pelo dedup (CNPJs duplicados
# com cliente_id diferente) entram na silver.pedidos mas nao no gold.fato_vendas
# porque silver.clientes nao tem esses cliente_id. A comparacao usa INNER JOIN
# para comparar apenas os pedidos que efetivamente entraram no gold.
# -----------------------------------------------------------------------------
testar("teste_1_receita_total", f"""
SELECT CASE
         WHEN ABS(
           (SELECT ROUND(SUM(receita), 2) FROM {CATALOG}.gold.fato_vendas)
           -
           (SELECT ROUND(SUM(p.valor_liquido), 2)
            FROM {CATALOG}.silver.pedidos p
            JOIN {CATALOG}.silver.clientes c ON c.cliente_id = p.cliente_id
            WHERE NOT p.cancelado)
         ) <= 0.01
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE 1 FALHOU: receita gold = ',
           CAST((SELECT ROUND(SUM(receita), 2) FROM {CATALOG}.gold.fato_vendas) AS STRING),
           ' | receita silver (pedidos com cliente valido) = ',
           CAST((SELECT ROUND(SUM(p.valor_liquido), 2)
                 FROM {CATALOG}.silver.pedidos p
                 JOIN {CATALOG}.silver.clientes c ON c.cliente_id = p.cliente_id
                 WHERE NOT p.cancelado) AS STRING)
         ))
       END AS teste_1_receita_total
""")

# -----------------------------------------------------------------------------
# Teste 2: CNPJ unico na silver.clientes (0 duplicados)
# -----------------------------------------------------------------------------
testar("teste_2_cnpj_unico", f"""
SELECT CASE
         WHEN (SELECT COUNT(*) - COUNT(DISTINCT cnpj) FROM {CATALOG}.silver.clientes) = 0
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE 2 FALHOU: ', CAST((SELECT COUNT(*) - COUNT(DISTINCT cnpj) FROM {CATALOG}.silver.clientes) AS STRING),
           ' CNPJs duplicados na silver.clientes'
         ))
       END AS teste_2_cnpj_unico
""")

# -----------------------------------------------------------------------------
# Teste 3: nenhuma data_pedido nula na silver.pedidos
# -----------------------------------------------------------------------------
testar("teste_3_data_pedido_nao_nula", f"""
SELECT CASE
         WHEN (SELECT COUNT(*) FROM {CATALOG}.silver.pedidos WHERE data_pedido IS NULL) = 0
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE 3 FALHOU: ', CAST((SELECT COUNT(*) FROM {CATALOG}.silver.pedidos WHERE data_pedido IS NULL) AS STRING),
           ' pedidos com data_pedido nula'
         ))
       END AS teste_3_data_pedido_nao_nula
""")

# -----------------------------------------------------------------------------
# Teste 4: receita negativa SO onde devolucao = true
# -----------------------------------------------------------------------------
testar("teste_4_receita_negativa_so_com_devolucao", f"""
SELECT CASE
         WHEN (SELECT COUNT(*) FROM {CATALOG}.gold.fato_vendas
               WHERE receita < 0 AND NOT devolucao) = 0
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE 4 FALHOU: ', CAST((SELECT COUNT(*) FROM {CATALOG}.gold.fato_vendas WHERE receita < 0 AND NOT devolucao) AS STRING),
           ' linhas com receita negativa sem flag de devolucao'
         ))
       END AS teste_4_receita_negativa_so_com_devolucao
""")

# -----------------------------------------------------------------------------
# Teste 5: volume da gold.fato_vendas entre 140.000 e 250.000 linhas
# -----------------------------------------------------------------------------
testar("teste_5_volume_fato_vendas", f"""
SELECT CASE
         WHEN (SELECT COUNT(*) FROM {CATALOG}.gold.fato_vendas) BETWEEN 140000 AND 250000
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE 5 FALHOU: fato_vendas tem ',
           CAST((SELECT COUNT(*) FROM {CATALOG}.gold.fato_vendas) AS STRING),
           ' linhas (esperado entre 140.000 e 250.000)'
         ))
       END AS teste_5_volume_fato_vendas
""")

# -----------------------------------------------------------------------------
# Teste 6: nenhum pedido_id na gold orfao
# -----------------------------------------------------------------------------
testar("teste_6_pedido_id_sem_orfao", f"""
SELECT CASE
         WHEN (SELECT COUNT(DISTINCT f.pedido_id)
               FROM {CATALOG}.gold.fato_vendas f
               LEFT JOIN {CATALOG}.silver.pedidos p ON p.pedido_id = f.pedido_id
               WHERE p.pedido_id IS NULL) = 0
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE 6 FALHOU: ', CAST((SELECT COUNT(DISTINCT f.pedido_id)
                                     FROM {CATALOG}.gold.fato_vendas f
                                     LEFT JOIN {CATALOG}.silver.pedidos p ON p.pedido_id = f.pedido_id
                                     WHERE p.pedido_id IS NULL) AS STRING),
           ' pedido_id orfaos na gold'
         ))
       END AS teste_6_pedido_id_sem_orfao
""")

# -----------------------------------------------------------------------------
# Teste 7: nenhum cliente_id na gold orfao
# -----------------------------------------------------------------------------
testar("teste_7_cliente_id_sem_orfao", f"""
SELECT CASE
         WHEN (SELECT COUNT(DISTINCT f.cliente_id)
               FROM {CATALOG}.gold.fato_vendas f
               LEFT JOIN {CATALOG}.silver.clientes c ON c.cliente_id = f.cliente_id
               WHERE c.cliente_id IS NULL) = 0
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE 7 FALHOU: ', CAST((SELECT COUNT(DISTINCT f.cliente_id)
                                     FROM {CATALOG}.gold.fato_vendas f
                                     LEFT JOIN {CATALOG}.silver.clientes c ON c.cliente_id = f.cliente_id
                                     WHERE c.cliente_id IS NULL) AS STRING),
           ' cliente_id orfaos na gold'
         ))
       END AS teste_7_cliente_id_sem_orfao
""")

# -----------------------------------------------------------------------------
# Teste 8: mart_produto_performance soma = fato_vendas (conformado)
# -----------------------------------------------------------------------------
testar("teste_8_mart_produto_conforma_com_fato", f"""
SELECT CASE
         WHEN ABS(
           (SELECT ROUND(SUM(receita), 2) FROM {CATALOG}.gold.fato_vendas)
           -
           (SELECT ROUND(SUM(receita), 2) FROM {CATALOG}.gold.mart_produto_performance)
         ) <= 0.01
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE 8 FALHOU: mart_produto = ',
           CAST((SELECT ROUND(SUM(receita), 2) FROM {CATALOG}.gold.mart_produto_performance) AS STRING),
           ' | fato_vendas = ',
           CAST((SELECT ROUND(SUM(receita), 2) FROM {CATALOG}.gold.fato_vendas) AS STRING)
         ))
       END AS teste_8_mart_produto_conforma_com_fato
""")

# -----------------------------------------------------------------------------
# Teste 9: todo CNPJ com exatamente 14 digitos
# -----------------------------------------------------------------------------
testar("teste_9_cnpj_14_digitos", f"""
SELECT CASE
         WHEN (SELECT COUNT(*) FROM {CATALOG}.silver.clientes WHERE LENGTH(cnpj) <> 14) = 0
         THEN 'PASSOU'
         ELSE raise_error(CONCAT(
           'TESTE 9 FALHOU: ', CAST((SELECT COUNT(*) FROM {CATALOG}.silver.clientes WHERE LENGTH(cnpj) <> 14) AS STRING),
           ' clientes com CNPJ != 14 digitos'
         ))
       END AS teste_9_cnpj_14_digitos
""")

print("=== TODOS OS 9 TESTES PASSARAM ===")
