# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------
# MAGIC %run ./_training

# COMMAND ----------

cfg = config()
experiment_path = widget("experiment_path", "/Shared/quilla/ml")
model_key = widget("model_key", "credit_pd")
allowed_models = {"credit_pd", "delinquency_propensity", "credit_lgd", "credit_ead"}
if model_key not in allowed_models:
    raise ValueError(f"Unsupported credit model_key: {model_key!r}")
results = []

performance = spark.read.table(ml_table(cfg, "source_credit_performance_monthly"))
cases = spark.read.table(source_table(cfg, "credit_cases")).select(
    "credit_case_id", "requested_at", "requested_amount", "term_months", "risk_score", "policy_band", "product_id"
)
accounts = spark.read.table(source_table(cfg, "accounts")).select(
    "account_id", "current_balance", "available_balance", "credit_limit", F.col("risk_band").alias("account_risk_band"), "account_type"
)

labels = performance.groupBy("credit_case_id").agg(
    F.max(F.col("default_flag").cast("int")).alias("default_label"),
    F.max("days_past_due").alias("max_days_past_due"),
    F.max("exposure_at_default_local").alias("ead_target"),
    F.max("loss_amount_local").alias("loss_target"),
)

first_month = performance.filter(F.col("months_on_book") == 0).select(
    "credit_case_id", "account_id", "country_code", "observation_month", "scheduled_payment_local",
    "outstanding_principal_local", "utilization_pct",
)

macro = spark.read.table(ml_table(cfg, "fs_macro_country_daily")).select(
    "country_code", F.col("observation_date").alias("macro_date"), "policy_rate_pct", "inflation_yoy_pct",
    "unemployment_pct", "gdp_nowcast_yoy_pct", "energy_price_index", "stress_regime",
)

credit_training = (
    first_month.join(cases, "credit_case_id")
    .join(accounts, "account_id", "left")
    .join(labels, "credit_case_id")
    .withColumn("macro_date", F.least(F.to_date(F.lit("2026-08-31")), F.greatest(F.to_date(F.lit(START_DATE)), F.to_date("observation_month"))))
    .join(macro, ["country_code", "macro_date"], "left")
    .withColumn("delinquent_30_label", (F.col("max_days_past_due") >= 30).cast("int"))
    .withColumn("lgd_target", F.when(F.col("ead_target") > 0, F.col("loss_target") / F.col("ead_target")))
)

numeric = [
    "requested_amount", "term_months", "risk_score", "scheduled_payment_local", "outstanding_principal_local",
    "utilization_pct", "current_balance", "available_balance", "credit_limit", "policy_rate_pct",
    "inflation_yoy_pct", "unemployment_pct", "gdp_nowcast_yoy_pct", "energy_price_index",
]
categorical = ["country_code", "policy_band", "product_id", "account_risk_band", "account_type", "stress_regime"]

if model_key == "credit_pd":
    results.append(train_tabular(cfg, model_key, credit_training, "default_label", "binary", numeric, categorical, "requested_at", max_rows=120_000, experiment_path=experiment_path))
elif model_key == "delinquency_propensity":
    results.append(train_tabular(cfg, model_key, credit_training, "delinquent_30_label", "binary", numeric, categorical, "requested_at", max_rows=120_000, experiment_path=experiment_path))
else:
    defaults = credit_training.filter(F.col("default_label") == 1)
    label = "lgd_target" if model_key == "credit_lgd" else "ead_target"
    results.append(train_tabular(cfg, model_key, defaults, label, "regression", numeric, categorical, "requested_at", max_rows=100_000, experiment_path=experiment_path))

print(json.dumps({"status": "complete", "family": "credit", "model_key": model_key, "models": results}, default=str))
