# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------
# MAGIC %run ./_training

# COMMAND ----------

from pyspark.sql.window import Window

cfg = config()
experiment_path = widget("experiment_path", "/Shared/quilla/ml")
model_key = widget("model_key", "pricing_acceptance")
allowed_models = {
    "pricing_acceptance", "next_best_action", "customer_churn", "customer_clv",
    "cashflow_forecast", "macro_regime", "portfolio_stress_loss", "merchant_sales_forecast",
}
if model_key not in allowed_models:
    raise ValueError(f"Unsupported growth/forecast model_key: {model_key!r}")
results = []

if model_key == "pricing_acceptance":
    pricing = spark.read.table(ml_table(cfg, "source_pricing_experiments")).withColumn(
        "price_delta_bps", F.col("offered_apr_bps") - F.col("reference_apr_bps")
    )
    results.append(
        train_tabular(
            cfg, model_key, pricing, "accepted_flag", "binary",
            ["reference_policy_rate_pct", "reference_apr_bps", "offered_apr_bps", "price_delta_bps", "fee_bps", "limit_local", "term_months", "eligibility_score"],
            ["country_code", "product_id", "offer_type", "channel", "treatment_group"],
            "offer_ts", max_rows=300_000, sample_fraction=0.7, experiment_path=experiment_path,
        )
    )
elif model_key in {"next_best_action", "customer_churn", "customer_clv"}:
    customer = spark.read.table(ml_table(cfg, "source_customer_lifecycle_monthly")).withColumn(
        "best_action_label",
        F.when(F.col("complaint_count_90d") > 0, "service_recovery")
        .when((F.col("product_count") <= 2) & (F.col("debit_volume_usd_30d") > 2500), "credit")
        .when(F.col("average_balance_usd_30d") > 9000, "investment")
        .when(F.col("digital_engagement_score") < 0.35, "engagement")
        .when(F.col("product_count") >= 5, "insurance")
        .otherwise("payments"),
    )
    numeric = [
        "active_days_30d", "transaction_count_30d", "debit_volume_usd_30d", "credit_volume_usd_30d",
        "average_balance_usd_30d", "product_count", "interaction_count_30d", "complaint_count_90d",
        "digital_engagement_score",
    ]
    categorical = ["country_code", "treatment_group"]
    labels = {"next_best_action": ("best_action_label", "multiclass"), "customer_churn": ("churned_90d", "binary"), "customer_clv": ("clv_usd_12m", "regression")}
    label, kind = labels[model_key]
    if model_key != "next_best_action":
        categorical.append("offer_exposed_code")
    results.append(train_tabular(cfg, model_key, customer, label, kind, numeric, categorical, "observation_month", max_rows=300_000, sample_fraction=0.2, experiment_path=experiment_path))
elif model_key == "cashflow_forecast":
    financial = spark.read.table(ml_table(cfg, "fs_financial_daily"))
    window = Window.partitionBy("country_code", "event_type").orderBy("observation_date")
    training = (
        financial
        .withColumn("target_net_flow_next_day", F.lead("net_flow_usd", 1).over(window))
        .withColumn("day_of_week", F.dayofweek("observation_date"))
        .withColumn("month_of_year", F.month("observation_date"))
        .filter(F.col("target_net_flow_next_day").isNotNull())
    )
    results.append(train_tabular(cfg, model_key, training, "target_net_flow_next_day", "regression", ["event_count", "gross_volume_usd", "net_flow_usd", "average_ticket_usd", "cross_border_rate", "day_of_week", "month_of_year"], ["country_code", "event_type"], "observation_date", max_rows=150_000, experiment_path=experiment_path))
elif model_key in {"macro_regime", "portfolio_stress_loss"}:
    macro = spark.read.table(ml_table(cfg, "source_macroeconomic_daily"))
    macro_numeric = [
        "policy_rate_pct", "inflation_yoy_pct", "cpi_index", "fx_per_usd", "unemployment_pct",
        "gdp_nowcast_yoy_pct", "energy_price_index", "commodity_price_index", "consumer_confidence_index",
    ]
    if model_key == "macro_regime":
        results.append(train_tabular(cfg, model_key, macro, "stress_regime", "multiclass", macro_numeric, ["country_code"], "observation_date", max_rows=20_000, experiment_path=experiment_path))
    else:
        credit = spark.read.table(ml_table(cfg, "source_credit_performance_monthly"))
        training = (
            credit.groupBy("country_code", "observation_month")
            .agg(
                F.sum("outstanding_principal_local").alias("portfolio_exposure"),
                F.sum(F.coalesce("loss_amount_local", F.lit(0.0))).alias("portfolio_loss"),
                F.avg("utilization_pct").alias("average_utilization"),
                F.avg("days_past_due").alias("average_dpd"),
            )
            .withColumn("loss_rate_target", F.col("portfolio_loss") / F.greatest(F.col("portfolio_exposure"), F.lit(1.0)))
            .withColumn("macro_date", F.least(F.to_date(F.lit("2026-08-31")), F.greatest(F.to_date(F.lit(START_DATE)), F.to_date("observation_month"))))
            .join(macro.select("country_code", F.col("observation_date").alias("macro_date"), *macro_numeric, "stress_regime"), ["country_code", "macro_date"], "left")
        )
        results.append(train_tabular(cfg, model_key, training, "loss_rate_target", "regression", ["portfolio_exposure", "average_utilization", "average_dpd", *macro_numeric], ["country_code", "stress_regime"], "observation_month", max_rows=10_000, experiment_path=experiment_path))
else:
    merchant = spark.read.table(ml_table(cfg, "fs_merchant_daily"))
    window = Window.partitionBy("merchant_id").orderBy("observation_date")
    training = (
        merchant
        .withColumn("sales_next_observation", F.lead("sales_usd", 1).over(window))
        .withColumn("day_of_week", F.dayofweek("observation_date"))
        .withColumn("month_of_year", F.month("observation_date"))
        .filter(F.col("sales_next_observation").isNotNull())
    )
    results.append(train_tabular(cfg, model_key, training, "sales_next_observation", "regression", ["transaction_count", "sales_usd", "average_ticket_usd", "fraud_rate", "day_of_week", "month_of_year"], ["country_code"], "observation_date", max_rows=250_000, sample_fraction=0.5, experiment_path=experiment_path))

print(json.dumps({"status": "complete", "family": "growth_forecast", "model_key": model_key, "models": results}, default=str))
