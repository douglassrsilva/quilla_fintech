# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

from databricks.feature_engineering import FeatureEngineeringClient

cfg = config({"event_rows": 13_758_648, "interaction_rows": 100_000})
fe = FeatureEngineeringClient()


def publish_source(dataset: str, table: str):
    frame = read_landing(cfg, dataset)
    name = ml_table(cfg, table)
    (
        frame.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(name)
    )
    spark.sql(
        f"ALTER TABLE {name} SET TBLPROPERTIES "
        "('quilla.synthetic'='true','quilla.asset.type'='ml_training_source')"
    )
    return frame


def publish_feature(table: str, frame, primary_keys, timeseries_columns=None, description=None):
    name = ml_table(cfg, table)
    if spark.catalog.tableExists(name):
        fe.write_table(name=name, df=frame, mode="overwrite")
    else:
        kwargs = {
            "name": name,
            "primary_keys": primary_keys,
            "df": frame,
            "description": description or f"Quilla governed features for {table}",
        }
        if timeseries_columns:
            kwargs["timeseries_columns"] = timeseries_columns
        fe.create_table(**kwargs)
    spark.sql(
        f"ALTER TABLE {name} SET TBLPROPERTIES "
        "('quilla.synthetic'='true','quilla.asset.type'='feature_table')"
    )
    return name


# COMMAND ----------

macro = publish_source("macroeconomic_daily", "source_macroeconomic_daily")
credit = publish_source("credit_performance_monthly", "source_credit_performance_monthly")
customer = publish_source("customer_lifecycle_monthly", "source_customer_lifecycle_monthly")
security = publish_source("security_sessions", "source_security_sessions")
cases = publish_source("case_outcomes", "source_case_outcomes")
pricing = publish_source("pricing_experiments", "source_pricing_experiments")
products = publish_source("investment_products", "source_investment_products")
profiles = publish_source("investment_profiles", "source_investment_profiles")
investment_events = publish_source("investment_events", "source_investment_events")
market = publish_source("market_daily", "source_market_daily")

# COMMAND ----------

feature_assets = []
feature_assets.append(
    publish_feature(
        "fs_macro_country_daily",
        macro.drop("source_name", "synthetic_flag"),
        ["country_code", "observation_date"],
        "observation_date",
        "Daily country macroeconomic scenario features with data-vintage timestamps.",
    )
)

feature_assets.append(
    publish_feature(
        "fs_customer_monthly",
        customer.drop(
            "lifecycle_id", "converted_30d", "churned_90d", "revenue_usd_90d",
            "cost_to_serve_usd_90d", "clv_usd_12m", "label_available_at",
            "source_system", "synthetic_flag", "ingested_at",
        ),
        ["party_id", "observation_month"],
        "observation_month",
        "Customer behavior and engagement features for growth, churn and value models.",
    )
)

feature_assets.append(
    publish_feature(
        "fs_credit_performance_monthly",
        credit.drop(
            "performance_id", "default_flag", "cure_flag", "prepayment_flag",
            "exposure_at_default_local", "loss_amount_local", "recovery_amount_local",
            "label_available_at", "source_system", "synthetic_flag", "ingested_at",
        ),
        ["credit_case_id", "observation_month"],
        "observation_month",
        "Point-in-time credit behavior features for PD, LGD, EAD and collections.",
    )
)

feature_assets.append(
    publish_feature(
        "fs_security_session",
        security.drop(
            "account_takeover_label", "label_available_at", "source_system",
            "synthetic_flag", "ingested_at", "ip_token",
        ),
        ["session_id"],
        description="Authentication and device context features for account-takeover scoring.",
    )
)

feature_assets.append(
    publish_feature(
        "fs_pricing_offer",
        pricing.drop(
            "accepted_flag", "activated_flag", "utilization_90d", "revenue_usd_90d",
            "expected_loss_usd_90d", "contribution_margin_usd_90d", "label_available_at",
            "source_system", "synthetic_flag", "ingested_at",
        ),
        ["experiment_id"],
        description="Offer, price and treatment features for elasticity and uplift models.",
    )
)

feature_assets.append(
    publish_feature(
        "fs_investment_profile",
        profiles.drop(
            "modeled_risk_class", "suitability_status", "max_product_risk_class",
            "label_available_at", "source_system", "synthetic_flag", "ingested_at",
        ),
        ["profile_id"],
        description="Investor goals, horizon, liquidity and loss-capacity features.",
    )
)

feature_assets.append(
    publish_feature(
        "fs_investment_event",
        investment_events.drop(
            "accepted_recommendation", "realized_return_90d_pct", "suitability_breach_flag",
            "label_available_at", "source_system", "synthetic_flag", "ingested_at",
        ),
        ["investment_event_id"],
        description="Investment interaction and portfolio context for recommendation models.",
    )
)

feature_assets.append(
    publish_feature(
        "fs_market_daily",
        market.drop("source_name", "synthetic_flag", "ingested_at"),
        ["investment_product_id", "observation_date"],
        "observation_date",
        "Daily market, yield, volatility and macro-regime features for wealth models.",
    )
)

# COMMAND ----------

events = spark.read.table(source_table(cfg, "financial_events"))
if cfg["snapshot"].startswith("smoke-"):
    events = events.limit(cfg["event_rows"])
parties = spark.read.table(source_table(cfg, "parties")).select(
    "party_id", F.col("risk_band").alias("party_risk_band"), "customer_segment"
)
devices = spark.read.table(source_table(cfg, "devices")).select(
    "device_id", F.col("trust_status").alias("device_trust_status"), "device_type", "operating_system"
)
merchants = spark.read.table(source_table(cfg, "merchants")).select(
    "merchant_id", F.col("risk_band").alias("merchant_risk_band"), "merchant_category", "mcc"
)

transaction_context = (
    events
    .join(parties, "party_id", "left")
    .join(devices, "device_id", "left")
    .join(merchants, "merchant_id", "left")
    .withColumn("event_hour", F.hour("event_ts_utc"))
    .withColumn("event_day_of_week", F.dayofweek("event_ts_utc"))
    .withColumn("amount_log", F.log1p(F.coalesce("amount_usd", F.lit(0.0))))
    .select(
        "event_id", "country_code", "event_ts_utc", "party_id", "account_id", "card_id", "merchant_id",
        "device_id", "product_id", "event_type", "event_status", "channel", "direction", "amount_usd",
        "is_cross_border", "counterparty_country_code", "event_hour", "event_day_of_week", "amount_log",
        "party_risk_band", "customer_segment", "device_trust_status", "device_type", "operating_system",
        "merchant_risk_band", "merchant_category", "mcc",
    )
)
feature_assets.append(
    publish_feature(
        "fs_transaction_context",
        transaction_context,
        ["event_id"],
        description="Leakage-safe event, customer, device and merchant context for transaction fraud.",
    )
)

(
    events.select("event_id", "country_code", "event_ts_utc", "fraud_label")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(ml_table(cfg, "labels_transaction_fraud"))
)

interactions = spark.read.table(source_table(cfg, "interactions"))
if cfg["snapshot"].startswith("smoke-"):
    interactions = interactions.limit(cfg["interaction_rows"])
feature_assets.append(
    publish_feature(
        "fs_interaction_context",
        interactions.select(
            "interaction_id", "party_id", "country_code", "interaction_ts", "channel",
            "language_code", "message_text", "sentiment_score", "agent_type",
        ),
        ["interaction_id"],
        description="Native Portuguese and Spanish interaction context for intent routing.",
    )
)
(
    interactions.select("interaction_id", "country_code", "interaction_ts", "intent")
    .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(ml_table(cfg, "labels_interaction_intent"))
)

# Daily financial and merchant aggregates provide forecasting features without
# materializing sensitive event-level payloads into every forecasting dataset.
financial_daily = (
    events.withColumn("observation_date", F.to_date("event_ts_utc"))
    .groupBy("country_code", "observation_date", "event_type")
    .agg(
        F.count(F.lit(1)).alias("event_count"),
        F.sum("amount_usd").alias("gross_volume_usd"),
        F.sum(F.when(F.col("direction") == "credit", F.col("amount_usd")).otherwise(-F.col("amount_usd"))).alias("net_flow_usd"),
        F.avg("amount_usd").alias("average_ticket_usd"),
        F.avg(F.col("is_cross_border").cast("double")).alias("cross_border_rate"),
    )
)
feature_assets.append(
    publish_feature(
        "fs_financial_daily",
        financial_daily,
        ["country_code", "event_type", "observation_date"],
        "observation_date",
        "Daily country and product flow features for liquidity forecasting.",
    )
)

merchant_daily = (
    events.filter(F.col("merchant_id").isNotNull())
    .withColumn("observation_date", F.to_date("event_ts_utc"))
    .groupBy("merchant_id", "country_code", "observation_date")
    .agg(
        F.count(F.lit(1)).alias("transaction_count"),
        F.sum("amount_usd").alias("sales_usd"),
        F.avg("amount_usd").alias("average_ticket_usd"),
        F.avg(F.col("fraud_label").cast("double")).alias("fraud_rate"),
    )
)
feature_assets.append(
    publish_feature(
        "fs_merchant_daily",
        merchant_daily,
        ["merchant_id", "observation_date"],
        "observation_date",
        "Daily merchant sales and risk features for forecasting and working-capital decisions.",
    )
)

manifest = spark.createDataFrame(
    [(asset, "PASSED") for asset in feature_assets],
    "asset_name string, status string",
).withColumn("run_id", F.lit(f"feature-store-{cfg['snapshot']}")) \
 .withColumn("phase", F.lit("feature_store")) \
 .withColumn("row_count", F.lit(None).cast("long")) \
 .withColumn("details", F.lit("Unity Catalog governed feature table")) \
 .withColumn("recorded_at", F.current_timestamp()) \
 .select("run_id", "phase", "asset_name", "status", "row_count", "details", "recorded_at")
manifest.write.mode("append").saveAsTable(ml_table(cfg, "ml_run_manifest"))

print(json.dumps({"status": "complete", "feature_tables": feature_assets}))
