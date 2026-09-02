# Databricks notebook source
# notebook: src/ml/12-modelo.py
# proposito: Treinar modelo de propensao de compra, registrar no MLflow/UC,
#            gerar score_propensao, modelo_metricas e calibragem_holdout.

dbutils.widgets.text("catalog", "lakehouse_rotaperfume")
CATALOG = dbutils.widgets.get("catalog")

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, StratifiedKFold
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.inspection import permutation_importance
import mlflow
import mlflow.sklearn
from mlflow.models import infer_signature
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# =============================================================================
# CARREGAR DADOS
# =============================================================================

print("=== Carregando features ===")

feature_cols = [
    "recencia_dias", "frequencia_pedidos", "valor_total", "ticket_medio",
    "margem_total", "margem_percentual",
    "intervalo_medio_dias", "desvio_intervalo_dias", "atraso_relativo",
    "pedidos_ultimos_90d",
    "oportunidades_abertas", "oportunidades_ganhas", "taxa_ganho",
    "visitas_90d", "conversao_visita",
    "skus_distintos", "categorias_distintas", "marcas_distintas",
    "concentracao_marca_top", "comprou_lancamento",
]

target_col = "comprou_em_7d"

df_treino = (
    spark.read.table(f"{CATALOG}.gold.features_treino")
    .select("cliente_id", *feature_cols, target_col)
    .toPandas()
)

df_cliente = (
    spark.read.table(f"{CATALOG}.gold.features_cliente")
    .select("cliente_id", *feature_cols)
    .toPandas()
)

print(f"  features_treino: {len(df_treino):,} clientes")
print(f"  features_cliente: {len(df_cliente):,} clientes")

# Taxa base
taxa_base = df_treino[target_col].mean()
print(f"  taxa base: {taxa_base:.4f} ({taxa_base*100:.2f}%)")

# =============================================================================
# 1. BASELINE — 3 regras simples + moeda
# =============================================================================

print("\n=== BASELINE ===")

X_train, X_holdout, y_train, y_holdout = train_test_split(
    df_treino[feature_cols],
    df_treino[target_col],
    test_size=0.25,
    random_state=42,
    stratify=df_treino[target_col],
)

# Fill NaN para baselines (roc_auc_score nao aceita NaN)
X_holdout = X_holdout.fillna(0)

# Regra a: -recencia_dias (ligar para quem comprou recentemente)
auc_recencia = roc_auc_score(y_holdout, -X_holdout["recencia_dias"])

# Regra b: valor_total (ligar para quem compra mais)
auc_valor = roc_auc_score(y_holdout, X_holdout["valor_total"])

# Regra c: atraso_relativo (ligar para quem esta atrasado)
auc_atraso = roc_auc_score(y_holdout, X_holdout["atraso_relativo"])

# Moeda: 0.5000
auc_moeda = 0.5000

baseline_results = pd.DataFrame({
    "regra": [
        "ligar para quem comprou recentemente (-recencia)",
        "ligar para quem compra mais (valor_total)",
        "ligar para quem esta atrasado (atraso_relativo)",
        "jogar uma moeda (aleatorio)",
    ],
    "auc": [auc_recencia, auc_valor, auc_atraso, auc_moeda],
})
print(baseline_results.to_string(index=False))

melhor_baseline = max(auc_recencia, auc_valor, auc_atraso)
melhor_baseline_nome = (
    "recencia" if melhor_baseline == auc_recencia
    else "valor" if melhor_baseline == auc_valor
    else "atraso"
)
print(f"\nMelhor baseline: {melhor_baseline_nome} = {melhor_baseline:.4f}")

# =============================================================================
# 2. TREINO — HistGradientBoostingClassifier
# =============================================================================

print("\n=== TREINO ===")

# NAO impute NULL: HistGradientBoosting trata NaN nativamente
modelo = HistGradientBoostingClassifier(
    max_iter=300,
    learning_rate=0.05,
    max_depth=6,
    min_samples_leaf=20,
    random_state=42,
)

modelo.fit(X_train, y_train)
print("  Modelo treinado com sucesso.")

# =============================================================================
# 3. METRICAS — AUC holdout + lift_top200 out-of-fold
# =============================================================================

print("\n=== METRICAS ===")

# AUC no holdout
y_proba_holdout = modelo.predict_proba(X_holdout)[:, 1]
auc_holdout = roc_auc_score(y_holdout, y_proba_holdout)
print(f"  AUC holdout: {auc_holdout:.4f}")

# Lift top 200 — validacao cruzada out-of-fold (5 folds)
print("  Calculando lift_top200 out-of-fold (5 folds)...")
skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)

oof_scores = pd.Series(index=df_treino.index, dtype=float)
for fold_idx, (train_idx, val_idx) in enumerate(skf.split(df_treino[feature_cols], df_treino[target_col])):
    X_fold_train = df_treino.iloc[train_idx][feature_cols]
    y_fold_train = df_treino.iloc[train_idx][target_col]
    X_fold_val = df_treino.iloc[val_idx][feature_cols]

    fold_model = HistGradientBoostingClassifier(
        max_iter=300, learning_rate=0.05, max_depth=6,
        min_samples_leaf=20, random_state=42,
    )
    fold_model.fit(X_fold_train, y_fold_train)
    oof_scores.iloc[val_idx] = fold_model.predict_proba(X_fold_val)[:, 1]

# Pegar os 200 maiores scores
top200_idx = oof_scores.nlargest(200).index
top200_real = df_treino.loc[top200_idx, target_col]
acertos_top200 = int(top200_real.sum())
lift_top200 = top200_real.mean() / taxa_base

print(f"  AUC holdout: {auc_holdout:.4f}")
print(f"  lift_top200: {lift_top200:.2f}x")
print(f"  acertos_top200: {acertos_top200} / 200")

# =============================================================================
# 4. IMPORTANCIA POR PERMUTACAO — holdout, top 10
# =============================================================================

print("\n=== IMPORTANCIA POR PERMUTACAO (top 10) ===")

perm = permutation_importance(
    modelo, X_holdout, y_holdout,
    n_repeats=5, random_state=42, scoring="roc_auc",
)
importancia = (
    pd.DataFrame({
        "feature": feature_cols,
        "importancia_media": perm.importances_mean,
        "importancia_std": perm.importances_std,
    })
    .sort_values("importancia_media", ascending=False)
    .head(10)
)
print(importancia.to_string(index=False))

# =============================================================================
# 5. MLFLOW — registrar modelo no Unity Catalog
# =============================================================================

print("\n=== MLFLOW ===")

# Criar pasta pai antes de set_experiment
w = WorkspaceClient()
experiment_path = "/Users/"
try:
    w.workspace.mkdirs(f"/Workspace/Users/{w.current_user.me().user_name}/bundle/rotaperfume/experiments")
except Exception:
    pass  # pode ja existir

mlflow.set_experiment("/Users/baludf@gmail.com/propensao_compra")

with mlflow.start_run(run_name="propensao_compra_v1"):
    # Log params
    mlflow.log_param("algoritmo", "HistGradientBoostingClassifier")
    mlflow.log_param("max_iter", 300)
    mlflow.log_param("learning_rate", 0.05)
    mlflow.log_param("max_depth", 6)
    mlflow.log_param("min_samples_leaf", 20)
    mlflow.log_param("random_state", 42)
    mlflow.log_param("holdout_pct", 0.25)
    mlflow.log_param("feature_cols", len(feature_cols))

    # Log metrics
    mlflow.log_metric("auc", auc_holdout)
    mlflow.log_metric("lift_top200", lift_top200)
    mlflow.log_metric("acertos_top200", acertos_top200)
    mlflow.log_metric("taxa_base", taxa_base)
    mlflow.log_metric("baseline_melhor", melhor_baseline)
    mlflow.log_metric("baseline_recencia", auc_recencia)
    mlflow.log_metric("baseline_valor", auc_valor)
    mlflow.log_metric("baseline_atraso", auc_atraso)

    # Log modelo com signature (obrigatorio para UC)
    sample_X = X_holdout[:5]
    sample_pred = modelo.predict_proba(sample_X)[:, 1]
    signature = infer_signature(sample_X, sample_pred)

    mlflow.sklearn.log_model(
        modelo,
        artifact_path="modelo",
        registered_model_name="lakehouse_rotaperfume.gold.propensao_compra",
        signature=signature,
    )

    run_id = mlflow.active_run().info.run_id
    print(f"  Run ID: {run_id}")

# Alias @prod (UC: set_registered_model_alias direto)
client = mlflow.tracking.MlflowClient()
try:
    model_version = client.get_model_version_by_alias(
        "lakehouse_rotaperfume.gold.propensao_compra", "prod"
    )
    latest_version = model_version.version
except Exception:
    latest_version = "1"

client.set_registered_model_alias(
    "lakehouse_rotaperfume.gold.propensao_compra",
    "prod",
    latest_version,
)
print(f"  Alias @prod apontando para versao {latest_version}")

# =============================================================================
# 6. ASSERTS — quebram o job se algo estiver errado
# =============================================================================

print("\n=== ASSERTS ===")

# Assert 1: modelo ganha do melhor baseline por pelo menos 0.05 AUC
assert auc_holdout >= melhor_baseline + 0.05, (
    f"ASSERT 1 FALHOU: AUC ({auc_holdout:.4f}) nao ganha do melhor baseline "
    f"({melhor_baseline:.4f}) por 0.05. Diferenca: {auc_holdout - melhor_baseline:.4f}"
)
print(f"  [OK] Assert 1: AUC ({auc_holdout:.4f}) >= baseline+0.05 ({melhor_baseline+0.05:.4f})")

# Assert 2: AUC < 0.99 (bom demais e vazamento)
assert auc_holdout < 0.99, (
    f"ASSERT 2 FALHOU: AUC ({auc_holdout:.4f}) >= 0.99. Bom demais — provavel vazamento."
)
print(f"  [OK] Assert 2: AUC ({auc_holdout:.4f}) < 0.99")

# Assert 3: lift_top200 >= 2.5
assert lift_top200 >= 2.5, (
    f"ASSERT 3 FALHOU: lift_top200 ({lift_top200:.2f}) < 2.5. "
    f"A fila nao justifica o projeto."
)
print(f"  [OK] Assert 3: lift_top200 ({lift_top200:.2f}) >= 2.5")

# =============================================================================
# 7. SCORE — pontuar todos os 2.816 clientes
# =============================================================================

print("\n=== SCORE ===")

# Carregar modelo registrado com alias @prod
model_uri = "models:/lakehouse_rotaperfume.gold.propensao_compra@prod"
loaded_model = mlflow.sklearn.load_model(model_uri)

# Pontuar com EXATAMENTE as mesmas colunas, na mesma ordem
X_score = df_cliente[feature_cols].copy()
scores = loaded_model.predict_proba(X_score)[:, 1]

df_score = pd.DataFrame({
    "cliente_id": df_cliente["cliente_id"].astype(int),
    "score": scores,
})

# Faixa NTILE(4): Fria, Morna, Quente, Muito quente
df_score["faixa"] = pd.qcut(
    df_score["score"], q=4,
    labels=["Fria", "Morna", "Quente", "Muito quente"],
)

df_score["_referencia"] = "2026-08-31"
df_score["versao"] = int(latest_version) if latest_version else 1

# Salvar
sdf_score = spark.createDataFrame(df_score)
(
    sdf_score.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.score_propensao")
)

spark.sql(f"COMMENT ON TABLE {CATALOG}.gold.score_propensao IS "
          "'Score de propensao de compra para os {len(df_score):,} clientes. "
          "Faixa NTILE(4): Fria/Morna/Quente/Muito quente. Versao {latest_version}.'")

print(f"  score_propensao: {len(df_score):,} clientes")

# =============================================================================
# 8. METRICAS COMO TABELA — modelo_metricas + calibragem_holdout
# =============================================================================

print("\n=== TABELAS DE METRICAS ===")

# Top feature
top_feature = importancia.iloc[0]["feature"]

# modelo_metricas — uma linha por treino
df_metricas = pd.DataFrame({
    "versao": [int(latest_version) if latest_version else 1],
    "auc": [round(auc_holdout, 4)],
    "lift_top200": [round(lift_top200, 2)],
    "acertos_top200": [acertos_top200],
    "taxa_base": [round(taxa_base, 4)],
    "baseline_recencia": [round(auc_recencia, 4)],
    "baseline_valor": [round(auc_valor, 4)],
    "baseline_atraso": [round(auc_atraso, 4)],
    "feature_top": [top_feature],
    "_treinado_em": [pd.Timestamp.now()],
})

sdf_metricas = spark.createDataFrame(df_metricas)
(
    sdf_metricas.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.modelo_metricas")
)
spark.sql(f"COMMENT ON TABLE {CATALOG}.gold.modelo_metricas IS "
          "'Metricas do modelo de propensao: AUC, lift_top200, acertos, baselines. "
          "Uma linha por versao treinada.'")

# calibragem_holdout — taxa de compra por faixa (no holdout)
df_holdout_calib = pd.DataFrame({
    "score": y_proba_holdout,
    "comprou": y_holdout.values,
})
df_holdout_calib["faixa"] = pd.qcut(
    df_holdout_calib["score"], q=4,
    labels=["Fria", "Morna", "Quente", "Muito quente"],
)

df_calib = (
    df_holdout_calib.groupby("faixa", observed=False)
    .agg(
        clientes=("comprou", "count"),
        compraram=("comprou", "sum"),
        taxa_de_compra=("comprou", "mean"),
        score_medio=("score", "mean"),
    )
    .reset_index()
    .sort_values("score_medio")
)

sdf_calib = spark.createDataFrame(df_calib)
(
    sdf_calib.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.gold.calibragem_holdout")
)
spark.sql(f"COMMENT ON TABLE {CATALOG}.gold.calibragem_holdout IS "
          "'Calibragem do modelo no holdout: taxa de compra por faixa de score. "
          'A taxa DEVE subir da faixa fria para a muito quente. Prova de que o score ordena."'"'")

print(f"  modelo_metricas: 1 linha")
print(f"  calibragem_holdout: {len(df_calib)} faixas")

# Mostrar calibragem
print("\n=== CALIBRAGEM (holdout) ===")
print(df_calib.to_string(index=False))

print("\n=== Fase 3 (Modelo) concluida ===")
