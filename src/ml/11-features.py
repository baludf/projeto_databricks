# Databricks notebook source
# notebook: src/ml/11-features.py
# proposito: Feature engineering — UMA funcao montar_features(referencia) que
#            devolve uma linha por cliente com tudo que se sabia dele ATE essa data.
#            Gera gold.features_treino (com alvo) e gold.features_cliente (sem alvo).

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

def montar_features(referencia: str):
    """
    Retorna um DataFrame com uma linha por cliente contendo 20 features
    calculadas com dados ANTERIORES a `referencia` (formato 'YYYY-MM-DD').

    Fontes filtradas por data:
      gold.fato_vendas      data_pedido   < referencia
      silver.oportunidades  data_abertura < referencia
      silver.visitas        data_visita   < referencia

    NAO leia gold.dim_cliente — ela agrega a base inteira sem corte de data.
    O unico join permitido e com gold.dim_produto (para comprou_lancamento).
    """
    ref_date = F.lit(referencia).cast("date")

    # =========================================================================
    # FONTES — todas filtradas por data < referencia
    # =========================================================================

    vendas = (
        spark.read.table(f"{CATALOG}.gold.fato_vendas")
        .filter(F.col("data_pedido") < ref_date)
    )

    oportunidades = (
        spark.read.table(f"{CATALOG}.silver.oportunidades")
        .filter(F.col("data_abertura") < ref_date)
    )

    visitas = (
        spark.read.table(f"{CATALOG}.silver.visitas")
        .filter(F.col("data_visita") < ref_date)
    )

    dim_produto = spark.read.table(f"{CATALOG}.gold.dim_produto")

    # =========================================================================
    # GRUPO 1: RFM (6 features)
    # =========================================================================

    rfm = (
        vendas.groupBy("cliente_id")
        .agg(
            F.max("data_pedido").alias("_max_data_pedido"),
            F.min("data_pedido").alias("_min_data_pedido"),
            F.countDistinct("pedido_id").alias("frequencia_pedidos"),
            F.sum("receita").cast("double").alias("valor_total"),
            F.sum("margem").cast("double").alias("margem_total"),
            F.countDistinct("sku").alias("_skus_distintos"),
            F.countDistinct("categoria").alias("_categorias_distintas"),
            F.countDistinct("marca").alias("_marcas_distintas"),
        )
        .withColumn(
            "recencia_dias",
            F.datediff(ref_date, F.col("_max_data_pedido")).cast("double"),
        )
        .withColumn(
            "ticket_medio",
            F.col("valor_total") / F.nullif(F.col("frequencia_pedidos"), F.lit(0)),
        )
        .withColumn(
            "margem_percentual",
            F.col("margem_total") / F.nullif(F.col("valor_total"), F.lit(0)),
        )
    )

    # =========================================================================
    # GRUPO 2: RITMO (4 features)
    # Calcular gaps entre pedidos distintos por cliente
    # =========================================================================

    datas_distintas = (
        vendas.select("cliente_id", "data_pedido")
        .distinct()
    )

    w_cliente = Window.partitionBy("cliente_id").orderBy("data_pedido")

    gaps = (
        datas_distintas
        .withColumn("_prev_data", F.lag("data_pedido").over(w_cliente))
        .withColumn(
            "gap_dias",
            F.datediff(F.col("data_pedido"), F.col("_prev_data")).cast("double"),
        )
        .filter(F.col("gap_dias").isNotNull())
    )

    ritmo = (
        gaps.groupBy("cliente_id")
        .agg(
            F.avg("gap_dias").alias("intervalo_medio_dias"),
            F.stddev_pop("gap_dias").alias("desvio_intervalo_dias"),
        )
    )

    # pedidos nos ultimos 90 dias antes do corte
    vendas_90d = vendas.filter(
        F.col("data_pedido") >= F.date_sub(ref_date, 90)
    )
    ritmo_90d = (
        vendas_90d.groupBy("cliente_id")
        .agg(F.countDistinct("pedido_id").alias("pedidos_ultimos_90d"))
    )

    # =========================================================================
    # GRUPO 3: CRM (5 features)
    # =========================================================================

    crm = (
        oportunidades.groupBy("cliente_id")
        .agg(
            F.sum(
                F.when(
                    ~F.col("etapa").isin("Ganha", "Perdida"), F.lit(1)
                ).otherwise(F.lit(0))
            ).alias("oportunidades_abertas"),
            F.sum(
                F.when(F.col("etapa") == "Ganha", F.lit(1)).otherwise(F.lit(0))
            ).alias("oportunidades_ganhas"),
            F.count("*").alias("_total_oportunidades"),
        )
        .withColumn(
            "taxa_ganho",
            F.col("oportunidades_ganhas")
            / F.nullif(F.col("_total_oportunidades"), F.lit(0)),
        )
    )

    # Visitas nos ultimos 90 dias antes do corte
    visitas_90d = visitas.filter(
        F.col("data_visita") >= F.date_sub(ref_date, 90)
    )
    conv = (
        visitas_90d.groupBy("cliente_id")
        .agg(
            F.count("*").alias("visitas_90d"),
            F.sum(
                F.when(F.lower(F.col("resultado")).contains("pedido"), F.lit(1)).otherwise(F.lit(0))
            ).alias("_visitas_converteram"),
        )
        .withColumn(
            "conversao_visita",
            F.col("_visitas_converteram")
            / F.nullif(F.col("visitas_90d"), F.lit(0)),
        )
    )

    # =========================================================================
    # GRUPO 4: MIX (5 features)
    # skus_distintos, categorias_distintas, marcas_distintas ja foram calculados no RFM
    # concentracao_marca_top e comprou_lancamento precisam de joins extras
    # =========================================================================

    # Marca top por receita e concentracao
    marca_top = (
        vendas.groupBy("cliente_id", "marca")
        .agg(F.sum("receita").cast("double").alias("_receita_marca"))
    )
    w_rank = Window.partitionBy("cliente_id").orderBy(F.desc("_receita_marca"))
    concentracao = (
        marca_top
        .withColumn("_rank", F.row_number().over(w_rank))
        .filter(F.col("_rank") == 1)
        .select(
            "cliente_id",
            F.col("marca").alias("_marca_top"),
            F.col("_receita_marca").alias("_receita_top"),
        )
    )

    # comprou_lancamento: 1 se comprou SKU com data_lancamento nos 120 dias antes do corte
    skus_lancamento = dim_produto.filter(
        F.col("data_lancamento").isNotNull()
    ).filter(
        F.col("data_lancamento") >= F.date_sub(ref_date, 120)
    )
    comprou_lanc = (
        vendas.join(skus_lancamento.select("sku"), on="sku", how="inner")
        .select("cliente_id")
        .distinct()
        .withColumn("comprou_lancamento", F.lit(1))
    )

    # =========================================================================
    # MONTAGEM FINAL — juntar todos os grupos
    # =========================================================================

    # Base: todos os clientes unicos no fato
    clientes_base = vendas.select("cliente_id").distinct()

    features = (
        clientes_base
        # RFM
        .join(rfm.select(
            "cliente_id", "recencia_dias", "frequencia_pedidos", "valor_total",
            "ticket_medio", "margem_total", "margem_percentual",
            "_skus_distintos", "_categorias_distintas", "_marcas_distintas",
        ), on="cliente_id", how="left")
        # Ritmo
        .join(ritmo, on="cliente_id", how="left")
        .join(ritmo_90d, on="cliente_id", how="left")
        # CRM
        .join(crm.select(
            "cliente_id", "oportunidades_abertas", "oportunidades_ganhas", "taxa_ganho",
        ), on="cliente_id", how="left")
        .join(conv.select(
            "cliente_id", "visitas_90d", "conversao_visita",
        ), on="cliente_id", how="left")
        # Mix
        .join(concentracao, on="cliente_id", how="left")
        .join(comprou_lanc, on="cliente_id", how="left")
        # Coluna de referencia
        .withColumn("_referencia", ref_date)
    )

    # =========================================================================
    # FINALIZACAO — renomear colunas do mix, tratar nulos, cast double
    # =========================================================================

    features = (
        features
        .withColumnRenamed("_skus_distintos", "skus_distintos")
        .withColumnRenamed("_categorias_distintas", "categorias_distintas")
        .withColumnRenamed("_marcas_distintas", "marcas_distintas")
        # concentracao_marca_top: receita da marca top / valor_total
        .withColumn(
            "concentracao_marca_top",
            F.col("_receita_top") / F.nullif(F.col("valor_total"), F.lit(0)),
        )
        # comprou_lancamento: 0 se nao comprou, 1 se comprou
        .withColumn(
            "comprou_lancamento",
            F.coalesce(F.col("comprou_lancamento"), F.lit(0)),
        )
        # atraso_relativo: recencia / intervalo, com teto em 10
        # ARMADILHA: F.least() ignora nulo — envolver em when()
        .withColumn(
            "atraso_relativo",
            F.when(
                F.col("intervalo_medio_dias").isNotNull()
                & (F.col("intervalo_medio_dias") > 0),
                F.least(
                    F.col("recencia_dias") / F.col("intervalo_medio_dias"),
                    F.lit(10.0),
                ),
            ),
        )
        # pedidos_ultimos_90d: 0 se null
        .withColumn(
            "pedidos_ultimos_90d",
            F.coalesce(F.col("pedidos_ultimos_90d"), F.lit(0)),
        )
        # CRM: 0 se null (cliente sem oportunidade ou visita)
        .withColumn(
            "oportunidades_abertas",
            F.coalesce(F.col("oportunidades_abertas"), F.lit(0)),
        )
        .withColumn(
            "oportunidades_ganhas",
            F.coalesce(F.col("oportunidades_ganhas"), F.lit(0)),
        )
        .withColumn(
            "taxa_ganho",
            F.coalesce(F.col("taxa_ganho"), F.lit(0.0)),
        )
        .withColumn(
            "visitas_90d",
            F.coalesce(F.col("visitas_90d"), F.lit(0)),
        )
        .withColumn(
            "conversao_visita",
            F.coalesce(F.col("conversao_visita"), F.lit(0.0)),
        )
        # Cast TODAS as features numericas para double (evita Decimal not JSON serializable)
        .withColumn("recencia_dias", F.col("recencia_dias").cast("double"))
        .withColumn("frequencia_pedidos", F.col("frequencia_pedidos").cast("double"))
        .withColumn("valor_total", F.col("valor_total").cast("double"))
        .withColumn("ticket_medio", F.col("ticket_medio").cast("double"))
        .withColumn("margem_total", F.col("margem_total").cast("double"))
        .withColumn("margem_percentual", F.col("margem_percentual").cast("double"))
        .withColumn("intervalo_medio_dias", F.col("intervalo_medio_dias").cast("double"))
        .withColumn("desvio_intervalo_dias", F.col("desvio_intervalo_dias").cast("double"))
        .withColumn("atraso_relativo", F.col("atraso_relativo").cast("double"))
        .withColumn("skus_distintos", F.col("skus_distintos").cast("double"))
        .withColumn("categorias_distintas", F.col("categorias_distintas").cast("double"))
        .withColumn("marcas_distintas", F.col("marcas_distintas").cast("double"))
        .withColumn("concentracao_marca_top", F.col("concentracao_marca_top").cast("double"))
        # Selecionar apenas as 20 features + metadados
        .select(
            "cliente_id",
            "recencia_dias", "frequencia_pedidos", "valor_total", "ticket_medio",
            "margem_total", "margem_percentual",
            "intervalo_medio_dias", "desvio_intervalo_dias", "atraso_relativo",
            "pedidos_ultimos_90d",
            "oportunidades_abertas", "oportunidades_ganhas", "taxa_ganho",
            "visitas_90d", "conversao_visita",
            "skus_distintos", "categorias_distintas", "marcas_distintas",
            "concentracao_marca_top", "comprou_lancamento",
            "_referencia",
        )
    )

    return features

# COMMAND ----------

# =============================================================================
# TABELA 1: features_treino — referencia 2026-08-01, com alvo comprou_em_7d
# =============================================================================

print("=== Gerando gold.features_treino (referencia=2026-08-01) ===")

features_treino = montar_features("2026-08-01")

# Alvo: comprou entre 2026-08-01 e 2026-08-07 (7 dias apos o corte)
clientes_compra_7d = (
    spark.read.table(f"{CATALOG}.gold.fato_vendas")
    .filter(
        (F.col("data_pedido") >= F.lit("2026-08-01").cast("date"))
        & (F.col("data_pedido") < F.lit("2026-08-08").cast("date"))
    )
    .select("cliente_id")
    .distinct()
    .withColumn("comprou_em_7d", F.lit(1))
)

features_treino = (
    features_treino
    .join(clientes_compra_7d, on="cliente_id", how="left")
    .withColumn(
        "comprou_em_7d",
        F.coalesce(F.col("comprou_em_7d"), F.lit(0)),
    )
)

(
    features_treino.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.features_treino")
)

spark.sql(f"COMMENT ON TABLE {CATALOG}.gold.features_treino IS "
          "'Features de treino com referencia 2026-08-01. Uma linha por cliente. "
          "Alvo comprou_em_7d = 1 se fez pedido entre 01/08 e 07/08/2026.'")

print(f"  features_treino: {features_treino.count():,} linhas")

# COMMAND ----------

# =============================================================================
# TABELA 2: features_cliente — referencia 2026-08-31, sem alvo (sera pontuado)
# =============================================================================

print("=== Gerando gold.features_cliente (referencia=2026-08-31) ===")

features_cliente = montar_features("2026-08-31")

(
    features_cliente.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.features_cliente")
)

spark.sql(f"COMMENT ON TABLE {CATALOG}.gold.features_cliente IS "
          "'Features de scoring com referencia 2026-08-31. Uma linha por cliente. "
          "Sem alvo — sera pontuado pelo modelo treinado em features_treino.'")

print(f"  features_cliente: {features_cliente.count():,} linhas")

# COMMAND ----------

# =============================================================================
# VERIFICACOES
# =============================================================================

print("=== Verificacoes ===")

# 1. Contagem e datas de corte
spark.sql(f"""
SELECT '_treino' AS tabela, COUNT(*) AS clientes, MIN(_referencia) AS corte
FROM {CATALOG}.gold.features_treino
UNION ALL
SELECT '_cliente', COUNT(*), MIN(_referencia)
FROM {CATALOG}.gold.features_cliente
""").show()

# 2. Taxa base
spark.sql(f"""
SELECT COUNT(*)                                   AS clientes,
       SUM(comprou_em_7d)                         AS compraram,
       ROUND(100 * AVG(comprou_em_7d), 2)         AS taxa_base_pct
FROM {CATALOG}.gold.features_treino
""").show()

# 3. Prova de que nao ha vazamento (recencia minima >= 0)
spark.sql(f"""
SELECT MIN(recencia_dias) AS menor_recencia
FROM {CATALOG}.gold.features_treino
""").show()

print("=== Fase 2 (Features) concluida ===")
