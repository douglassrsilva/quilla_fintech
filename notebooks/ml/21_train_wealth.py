# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------
# MAGIC %run ./_training

# COMMAND ----------

from pyspark.sql.window import Window

cfg = config()
experiment_path = widget("experiment_path", "/Shared/quilla/ml")
model_key = widget("model_key", "investment_suitability")
allowed_models = {"investment_suitability", "investment_product_recommender", "portfolio_risk_forecast"}
if model_key not in allowed_models:
    raise ValueError(f"Unsupported wealth model_key: {model_key!r}")
results = []

if model_key == "investment_suitability":
    profiles = spark.read.table(ml_table(cfg, "source_investment_profiles"))
    results.append(
        train_tabular(
            cfg, model_key, profiles, "modeled_risk_class", "multiclass",
            ["annual_income_usd", "investable_assets_usd", "horizon_months", "emergency_reserve_months", "loss_capacity_pct", "investment_knowledge_score"],
            ["country_code", "liquidity_need", "sustainability_preference", "investment_objective", "declared_risk_tolerance"],
            "assessment_ts", max_rows=72_000, experiment_path=experiment_path,
        )
    )
elif model_key == "investment_product_recommender":
    profiles = spark.read.table(ml_table(cfg, "source_investment_profiles"))
    events = spark.read.table(ml_table(cfg, "source_investment_events"))
    products = spark.read.table(ml_table(cfg, "source_investment_products")).select(
        "investment_product_id", "asset_class", "risk_class", "fee_bps", "liquidity_days", "sustainability_score"
    )
    recommendation = (
        events.filter(F.col("recommendation_exposed"))
        .join(products, "investment_product_id")
        .join(profiles.select("profile_id", "horizon_months", "loss_capacity_pct", "investment_knowledge_score", "liquidity_need", "investment_objective", "modeled_risk_class"), "profile_id")
        .withColumn("recommendation_target", F.when(F.col("accepted_recommendation"), F.col("asset_class")).otherwise(F.lit("no_action")))
    )
    results.append(train_tabular(cfg, model_key, recommendation, "recommendation_target", "multiclass", ["amount_usd", "portfolio_value_before_usd", "recommendation_rank", "risk_class", "fee_bps", "liquidity_days", "sustainability_score", "horizon_months", "loss_capacity_pct", "investment_knowledge_score"], ["country_code", "channel", "event_type", "liquidity_need", "investment_objective", "modeled_risk_class"], "event_ts", max_rows=300_000, sample_fraction=0.6, experiment_path=experiment_path))
else:
    market = spark.read.table(ml_table(cfg, "source_market_daily"))
    window = Window.partitionBy("investment_product_id").orderBy("observation_date").rowsBetween(1, 30)
    training = (
        market
        .withColumn("forward_volatility_30d", F.stddev_pop("daily_return_pct").over(window))
        .withColumn("day_of_week", F.dayofweek("observation_date"))
        .withColumn("month_of_year", F.month("observation_date"))
        .filter(F.col("forward_volatility_30d").isNotNull())
    )
    results.append(train_tabular(cfg, model_key, training, "forward_volatility_30d", "regression", ["daily_return_pct", "volatility_30d_pct", "yield_pct", "duration_years", "fx_to_usd", "benchmark_return_pct", "day_of_week", "month_of_year"], ["asset_class", "market_country", "currency_code", "stress_regime"], "observation_date", country_col="market_country", max_rows=50_000, experiment_path=experiment_path))

print(json.dumps({"status": "complete", "family": "wealth", "model_key": model_key, "models": results}, default=str))
