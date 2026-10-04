# Databricks notebook source
# ----------------------------------------------------------------------
# notebook: src/raw/conferencia.py
# propósito:  Conferência de chegada — valida que os 10 CSVs (erp + crm)
#             chegaram ao Volume e grava a tabela bronze._raw_arquivos.
#
#             Se faltar arquivo ou vier vazio → levanta exceção e para.
#             Sem essa conferência, o pipeline segue verde com dado incompleto
#             e o dashboard mostra número menor com cara de número certo.
# ----------------------------------------------------------------------

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# -------------------------------------------------------------------------
# 1. Arquivos esperados
# -------------------------------------------------------------------------
ARQUIVOS_ERP = [
    "produtos.csv",
    "pedidos.csv",
    "itens_pedido.csv",
    "pagamentos.csv",
    "estoque.csv",
]
ARQUIVOS_CRM = [
    "clientes.csv",
    "vendedores.csv",
    "carteira.csv",
    "oportunidades.csv",
    "visitas.csv",
]
ARQUIVOS = {sistema: arquivos for sistema, arquivos in [("erp", ARQUIVOS_ERP), ("crm", ARQUIVOS_CRM)]}

# -------------------------------------------------------------------------
# 2. Lê metadata dos arquivos no Volume via dbutils.fs
# -------------------------------------------------------------------------
def bytes_do_arquivo(caminho: str) -> int:
    """Retorna tamanho em bytes de um arquivo no Volume."""
    try:
        info = dbutils.fs.ls(caminho)
        return int(info[0].size) if info else -1
    except Exception:
        return -1


def linhas_do_csv(caminho: str) -> int:
    """Conta linhas de dado (exclui cabeçalho) de um CSV no Volume."""
    try:
        df = spark.read.format("csv").option("header", "true").load(caminho)
        return int(df.count())
    except Exception:
        return -1


# -------------------------------------------------------------------------
# 3. Validação — arquivo existe e não está vazio
# -------------------------------------------------------------------------
from pyspark.sql import SparkSession

resultados = []

for sistema, arquivos in ARQUIVOS.items():
    for arquivo in arquivos:
        path = f"/Volumes/{CATALOG}/bronze/raw/{sistema}/{arquivo}"

        tamanho_bytes = bytes_do_arquivo(path)
        linhas = linhas_do_csv(path)

        status = "OK" if tamanho_bytes > 0 and linhas >= 0 else "FALHA"
        print(f"[{status}] {sistema}/{arquivo:20s}  {tamanho_bytes:>12,} bytes  {linhas:>10,} linhas")

        resultados.append({
            "sistema": sistema,
            "arquivo": arquivo,
            "bytes": tamanho_bytes,
            "linhas": linhas,
        })

        # Falha se arquivo não chegou
        if tamanho_bytes <= 0:
            raise Exception(f"FALHA: arquivo não encontrado ou vazio — {sistema}/{arquivo}")

        # Falha se veio sem dados (apenas cabeçalho)
        if linhas <= 0:
            raise Exception(f"FALHA: {sistema}/{arquivo} não tem linhas de dado")

print()

# -------------------------------------------------------------------------
# 4. Grava tabela de controle bronze._raw_arquivos
# -------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType, LongType
from pyspark.sql import functions as F
from datetime import datetime

schema = StructType([
    StructField("sistema",     StringType(), False),
    StructField("arquivo",     StringType(), False),
    StructField("bytes",       LongType(),   True),
    StructField("linhas",      LongType(),   True),
    StructField("conferido_em", StringType(), False),
])

# Gera timestamp uma vez para todos os arquivos da execução
agora = datetime.utcnow().isoformat(timespec="seconds")
for r in resultados:
    r["conferido_em"] = agora

df_resultado = spark.createDataFrame(resultados, schema)

# Sobrescreve a tabela de controle a cada execução
df_resultado.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
    f"{CATALOG}.bronze._raw_arquivos",
    format="delta"
)

print("Tabela bronze._raw_arquivos atualizada com sucesso.")

# -------------------------------------------------------------------------
# 5. Resumo final
# -------------------------------------------------------------------------
df_resumo = df_resultado.agg(
    F.count("*").alias("arquivos"),
    F.sum("linhas").alias("linhas_de_dado"),
    F.round(F.sum("bytes") / 1024 / 1024, 1).alias("mb"),
)
display(df_resumo)
print()
print("Conferência de chegada concluída.")
