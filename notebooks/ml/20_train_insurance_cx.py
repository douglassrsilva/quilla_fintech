# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------
# MAGIC %run ./_training

# COMMAND ----------

cfg = config()
experiment_path = widget("experiment_path", "/Shared/quilla/ml")
model_key = widget("model_key", "insurance_fraud")
allowed_models = {"insurance_fraud", "insurance_severity", "support_intent"}
if model_key not in allowed_models:
    raise ValueError(f"Unsupported insurance/CX model_key: {model_key!r}")
results = []

if model_key in {"insurance_fraud", "insurance_severity"}:
    insurance = spark.read.table(source_table(cfg, "insurance_cases"))
    outcomes = spark.read.table(ml_table(cfg, "source_case_outcomes")).filter(
        F.col("insurance_case_id").isNotNull()
    ).select("insurance_case_id", "confirmed_flag", "loss_amount_usd")
    training = (
        insurance.join(outcomes, "insurance_case_id")
        .withColumn("claimed_amount_log", F.log1p(F.coalesce("claimed_amount", F.lit(0.0))))
        .withColumn("has_previous_fraud_signal", F.col("fraud_indicator").cast("double"))
        .withColumn("severity_target_usd", F.coalesce(F.col("approved_amount"), F.col("claimed_amount") * F.lit(0.65)))
    )
    numeric = ["claimed_amount", "claimed_amount_log", "has_previous_fraud_signal"]
    categorical = ["country_code", "case_type", "coverage_type", "currency_code"]
    if model_key == "insurance_fraud":
        results.append(train_tabular(cfg, model_key, training, "confirmed_flag", "binary", numeric, categorical, "opened_at", max_rows=60_000, experiment_path=experiment_path))
    else:
        claims = training.filter((F.col("case_type") == "claim") & F.col("severity_target_usd").isNotNull())
        results.append(train_tabular(cfg, model_key, claims, "severity_target_usd", "regression", numeric, categorical, "opened_at", max_rows=50_000, experiment_path=experiment_path))
else:
    interactions = (
        spark.read.table(ml_table(cfg, "fs_interaction_context"))
        .join(spark.read.table(ml_table(cfg, "labels_interaction_intent")), ["interaction_id", "country_code", "interaction_ts"])
    )
    results.append(
        train_text_classifier(
            cfg, model_key, interactions, "intent", "message_text",
            ["language_code", "channel"], "interaction_ts", max_rows=100_000, experiment_path=experiment_path,
        )
    )

print(json.dumps({"status": "complete", "family": "insurance_cx", "model_key": model_key, "models": results}, default=str))
