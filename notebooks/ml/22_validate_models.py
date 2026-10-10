# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

from mlflow import MlflowClient

cfg = config()
expected_models = [
    "transaction_fraud",
    "account_takeover",
    "aml_anomaly",
    "alert_confirmation",
    "credit_pd",
    "credit_lgd",
    "credit_ead",
    "delinquency_propensity",
    "cashflow_forecast",
    "macro_regime",
    "portfolio_stress_loss",
    "pricing_acceptance",
    "next_best_action",
    "customer_churn",
    "customer_clv",
    "merchant_sales_forecast",
    "insurance_fraud",
    "insurance_severity",
    "support_intent",
    "investment_suitability",
    "investment_product_recommender",
    "portfolio_risk_forecast",
]

results = spark.read.table(ml_table(cfg, "model_training_results")).filter(F.col("snapshot") == cfg["snapshot"])
latest = results.groupBy("model_key").agg(F.max("recorded_at").alias("latest_at"))
available = {row.model_key for row in latest.collect()}
missing_results = sorted(set(expected_models) - available)
if missing_results:
    raise AssertionError(f"Missing model training results: {missing_results}")

missing_traces = [
    row.model_key
    for row in results.groupBy("model_key").agg(F.max("trace_id").alias("trace_id")).collect()
    if not row.trace_id
]
if missing_traces:
    raise AssertionError(f"Missing MLflow trace evidence: {sorted(missing_traces)}")

client = MlflowClient(registry_uri="databricks-uc")
evidence = []
for model_key in expected_models:
    model_name = f"{cfg['catalog']}.{cfg['ml_schema']}.quilla_{model_key}"
    champion = client.get_model_version_by_alias(model_name, "Champion")
    if not champion.version:
        raise AssertionError(f"Champion alias missing for {model_name}")
    evidence.append((model_key, model_name, str(champion.version), "PASSED"))

validation = (
    spark.createDataFrame(evidence, "model_key string, model_name string, model_version string, status string")
    .withColumn("validated_at", F.current_timestamp())
)
validation.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ml_table(cfg, "model_validation_evidence"))

manifest = (
    validation.select(F.col("model_name").alias("asset_name"), "status")
    .withColumn("run_id", F.lit(f"model-validation-{cfg['snapshot']}"))
    .withColumn("phase", F.lit("model_validation"))
    .withColumn("row_count", F.lit(None).cast("long"))
    .withColumn("details", F.lit("Champion alias resolved in Unity Catalog"))
    .withColumn("recorded_at", F.current_timestamp())
    .select("run_id", "phase", "asset_name", "status", "row_count", "details", "recorded_at")
)
manifest.write.mode("append").saveAsTable(ml_table(cfg, "ml_run_manifest"))

print(json.dumps({"status": "PASSED", "registered_models": len(evidence), "models": evidence}))
