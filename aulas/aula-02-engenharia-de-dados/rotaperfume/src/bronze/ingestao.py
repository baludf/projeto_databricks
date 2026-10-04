# Databricks notebook source
# ----------------------------------------------------------------------
# notebook: src/bronze/ingestao.py
# propósito:  Ingestão da camada bronze — lê os 10 CSVs do Volume e grava
#             10 tabelas Delta em {catalog}.bronze, com TUDO em string.
#
#             REGRAS DA BRONZE:
#             - Nada de inferSchema. Tudo é texto. Sempre.
#             - Não converte data, não tira espaço, não normaliza CNPJ.
#             - Só adiciona metadado técnico: _ingerido_em e _arquivo_origem.
#             - A sujeira é o conteúdo do próximo prompt.
# ----------------------------------------------------------------------

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# -------------------------------------------------------------------------
# 1. Lista única de 10 tabelas (5 ERP + 5 CRM)
# -------------------------------------------------------------------------
TABELAS = [
    # (tabela,        sistema, comentario)
    ("produtos",      "erp",   "Bronze de produtos — origem ERP."),
    ("pedidos",       "erp",   "Bronze de pedidos — origem ERP."),
    ("itens_pedido",  "erp",   "Bronze de itens de pedido — origem ERP."),
    ("pagamentos",    "erp",   "Bronze de pagamentos — origem ERP."),
    ("estoque",       "erp",   "Bronze de estoque — origem ERP."),
    ("clientes",      "crm",   "Bronze de clientes — origem CRM."),
    ("vendedores",    "crm",   "Bronze de vendedores — origem CRM."),
    ("carteira",      "crm",   "Bronze de carteira — origem CRM."),
    ("oportunidades", "crm",   "Bronze de oportunidades — origem CRM."),
    ("visitas",       "crm",   "Bronze de visitas — origem CRM."),
]

# -------------------------------------------------------------------------
# 2. Função de ingestão — escrita UMA vez
# -------------------------------------------------------------------------
def ingerir(tabela: str, sistema: str, comentario: str) -> int:
    """Lê o CSV do Volume e grava a tabela Delta bronze.{tabela}, tudo como string.

    Retorna a quantidade de linhas gravadas.
    """
    path_csv = f"/Volumes/{CATALOG}/bronze/raw/{sistema}/{tabela}.csv"

    # Tudo string: header=True, inferSchema=False, multiLine desativado.
    # Os CSVs são CRLF, mas o read_files do Databricks normaliza para LF.
    df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "false")
        .option("multiLine", "false")
        .option("escape", '"')
        .load(path_csv)
    )

    # Metadado técnico: de onde veio e quando entrou.
    # Usa _path_origem (não _arquivo_origem) porque alguns CSVs já têm coluna com esse nome.
    df = (
        df.withColumn("_ingerido_em", F.current_timestamp())
          .withColumn("_path_origem", F.lit(f"raw/{sistema}/{tabela}.csv"))
    )

    # Grava em modo overwrite, sem _rescued_data (a coluna que o Databricks
    # cria quando o CSV tem linha malformada — descartamos por padrão).
    cols = [c for c in df.columns if c not in ("_rescued_data",)]
    df_to_save = df.select(*cols)

    full_table = f"{CATALOG}.bronze.{tabela}"
    (
        df_to_save.write
        .mode("overwrite")
        .option("mergeSchema", "true")
        .option("overwriteSchema", "true")
        .saveAsTable(full_table)
    )

    # COMMENT na tabela
    spark.sql(f"COMMENT ON TABLE {full_table} IS '{comentario}'")

    return df_to_save.count()


# -------------------------------------------------------------------------
# 3. Itera sobre a lista — 10 ingestões idênticas
# -------------------------------------------------------------------------
import pyspark.sql.functions as F
from pyspark.sql.types import LongType

resultados = []
for tabela, sistema, comentario in TABELAS:
    n = ingerir(tabela, sistema, comentario)
    resultados.append((tabela, sistema, n))
    print(f"  {tabela:15s}  {n:>10,} linhas")

# -------------------------------------------------------------------------
# 4. Confere contra bronze._raw_arquivos (gravada pelo prompt 01)
# -------------------------------------------------------------------------
df_raw = spark.read.table(f"{CATALOG}.bronze._raw_arquivos").select(
    F.col("arquivo").alias("arquivo_csv"),
    F.col("linhas").cast(LongType()).alias("linhas_raw"),
)

print()
print("Conferência: linhas na tabela vs. linhas no arquivo de origem")
print(f"{'tabela':15s} {'tabela':>10s}  {'arquivo':>10s}  bate")
print("-" * 50)

faltou = []
for tabela, sistema, n in resultados:
    linhas_raw = (
        df_raw.filter(F.col("arquivo_csv") == f"{tabela}.csv")
        .first()["linhas_raw"]
    )
    bate = (n == linhas_raw)
    print(f"  {tabela:15s} {n:>10,}  {linhas_raw:>10,}  {bate}")
    if not bate:
        faltou.append(tabela)

if faltou:
    raise Exception(f"FALHA: contagens divergem em {faltou}. CSV pode ter sido lido errado.")

total_tabela = sum(n for _, _, n in resultados)
print()
print(f"Total de linhas nas 10 tabelas: {total_tabela:,}")
print("Conferência da bronze concluída.")
