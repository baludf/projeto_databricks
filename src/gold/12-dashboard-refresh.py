# Databricks notebook source
# src/gold/12-dashboard-refresh.py
# Placeholder: dashboard é atualizado automaticamente pelo bundle deploy.
# Este notebook existe apenas para o task do pipeline ter uma定义 válida.

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
catalog = dbutils.widgets.get("catalog")

spark.sql(f"USE CATALOG {catalog}")

# Verifica que as tabelas necessárias existem
tables = ["fila_semanal", "retorno_ligacao", "receita_mensal", "ranking_marcas"]
for t in tables:
    df = spark.sql(f"SHOW TABLES IN gold LIKE '{t}'")
    if df.count() == 0:
        raise ValueError(f"Tabela gold.{t} não encontrada — execute as tasks anteriores primeiro.")

print("Dashboard refresh: todas as tabelas necessárias existem. OK.")
