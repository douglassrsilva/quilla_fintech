# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql.window import Window

cfg = config()

# The values below are explicitly synthetic scenario baselines. They are not
# represented as official economic observations.
macro = (
    spark.range(730 * 12, numPartitions=12)
    .withColumnRenamed("id", "row_id")
    .withColumn("day_id", F.floor(F.col("row_id") / F.lit(12)).cast("int"))
    .withColumn("country_index", F.pmod(F.col("row_id"), F.lit(12)).cast("int"))
    .withColumn("observation_date", F.date_add(F.to_date(F.lit(START_DATE)), F.col("day_id")))
    .withColumn("country_code", F.element_at(country_array("code"), (F.col("country_index") + 1).cast("int")))
    .withColumn("currency_code", country_value(F.col("country_code"), "currency"))
    .withColumn("base_policy_rate", country_value(F.col("country_code"), "policy_rate").cast("double"))
    .withColumn("base_inflation", country_value(F.col("country_code"), "inflation").cast("double"))
    .withColumn("base_fx", country_value(F.col("country_code"), "fx").cast("double"))
    .withColumn("base_unemployment", country_value(F.col("country_code"), "unemployment").cast("double"))
    .withColumn(
        "stress_regime",
        F.when(F.col("day_id").between(170, 239), "rate_hike")
        .when(F.col("day_id").between(240, 329), "inflation")
        .when(F.col("day_id").between(330, 389), "deflation")
        .when(F.col("day_id").between(390, 469), "energy_crisis")
        .when(F.col("day_id").between(470, 559), "recession")
        .otherwise("normal"),
    )
    .withColumn("season", F.sin(F.col("day_id") * F.lit(2.0 * 3.141592653589793 / 365.0)))
    .withColumn("noise", stable_uniform("row_id", seed=cfg["seed"] + 11) - F.lit(0.5))
    .withColumn(
        "policy_rate_pct",
        F.round(
            F.col("base_policy_rate")
            + F.when(F.col("stress_regime") == "rate_hike", 3.0).otherwise(0.0)
            + F.when(F.col("stress_regime") == "deflation", -1.5).otherwise(0.0)
            + F.col("season") * 0.3
            + F.col("noise") * 0.4,
            4,
        ),
    )
    .withColumn(
        "inflation_yoy_pct",
        F.round(
            F.col("base_inflation")
            + F.when(F.col("stress_regime") == "inflation", 7.0).otherwise(0.0)
            + F.when(F.col("stress_regime") == "deflation", -5.0).otherwise(0.0)
            + F.when(F.col("stress_regime") == "energy_crisis", 3.0).otherwise(0.0)
            + F.col("season") * 0.5
            + F.col("noise"),
            4,
        ),
    )
    .withColumn(
        "cpi_index",
        F.round(F.lit(100.0) * F.exp((F.col("base_inflation") / 100.0) * F.col("day_id") / 365.0), 4),
    )
    .withColumn(
        "fx_per_usd",
        F.round(
            F.col("base_fx")
            * (
                F.lit(1.0)
                + F.col("day_id") / 7300.0
                + F.when(F.col("stress_regime").isin("inflation", "recession"), 0.12).otherwise(0.0)
                + F.col("noise") * 0.03
            ),
            6,
        ),
    )
    .withColumn(
        "unemployment_pct",
        F.round(
            F.col("base_unemployment")
            + F.when(F.col("stress_regime") == "recession", 3.5).otherwise(0.0)
            + F.when(F.col("stress_regime") == "energy_crisis", 1.0).otherwise(0.0)
            + F.col("noise") * 0.5,
            4,
        ),
    )
    .withColumn(
        "gdp_nowcast_yoy_pct",
        F.round(
            F.lit(2.8)
            + F.col("season") * 0.8
            + F.when(F.col("stress_regime") == "recession", -6.0).otherwise(0.0)
            + F.when(F.col("stress_regime") == "energy_crisis", -2.0).otherwise(0.0)
            + F.col("noise"),
            4,
        ),
    )
    .withColumn(
        "energy_price_index",
        F.round(
            F.lit(100.0)
            + F.col("day_id") * 0.015
            + F.when(F.col("stress_regime") == "energy_crisis", 65.0).otherwise(0.0)
            + F.col("season") * 8.0
            + F.col("noise") * 5.0,
            4,
        ),
    )
    .withColumn(
        "commodity_price_index",
        F.round(F.lit(100.0) + F.col("season") * 12.0 + F.col("noise") * 10.0, 4),
    )
    .withColumn(
        "consumer_confidence_index",
        F.round(
            F.lit(100.0)
            + F.col("season") * 4.0
            + F.when(F.col("stress_regime").isin("recession", "inflation", "energy_crisis"), -18.0).otherwise(0.0)
            + F.col("noise") * 6.0,
            4,
        ),
    )
    .withColumn("vintage_date", F.date_add(F.col("observation_date"), 7))
    .withColumn("available_at", F.to_timestamp(F.concat_ws(" ", F.col("vintage_date"), F.lit("12:00:00"))))
    .withColumn("source_name", F.lit("QUILLA_SYNTHETIC_SCENARIO_ENGINE"))
    .withColumn("synthetic_flag", F.lit(True))
    .select(
        "observation_date",
        "country_code",
        "currency_code",
        "policy_rate_pct",
        "inflation_yoy_pct",
        "cpi_index",
        "fx_per_usd",
        "unemployment_pct",
        "gdp_nowcast_yoy_pct",
        "energy_price_index",
        "commodity_price_index",
        "consumer_confidence_index",
        "stress_regime",
        "vintage_date",
        "available_at",
        "source_name",
        "synthetic_flag",
    )
)
write_parquet(macro, cfg, "macroeconomic_daily", ["country_code"])

# COMMAND ----------

asset_classes = ["cash", "fixed_income", "equity", "multi_asset", "commodity", "real_estate"]
objectives = ["capital_preservation", "income", "balanced_growth", "growth"]
all_country_codes = F.array(*[F.lit(country["code"]) for country in COUNTRIES])

products = (
    spark.range(240, numPartitions=4)
    .withColumnRenamed("id", "row_id")
    .withColumn("investment_product_id", F.format_string("QINV-%04d", F.col("row_id") + 1))
    .withColumn("asset_class", F.element_at(F.array(*[F.lit(value) for value in asset_classes]), (F.pmod("row_id", F.lit(6)) + 1).cast("int")))
    .withColumn("risk_class", (F.pmod(F.col("row_id") * 7, F.lit(5)) + 1).cast("int"))
    .withColumn("domicile_country", F.element_at(country_array("code"), (F.pmod("row_id", F.lit(12)) + 1).cast("int")))
    .withColumn("currency_code", country_value(F.col("domicile_country"), "currency"))
    .withColumn("name_pt", F.concat(F.lit("Quilla Investimento "), F.lpad((F.col("row_id") + 1).cast("string"), 3, "0")))
    .withColumn("name_es", F.concat(F.lit("Quilla Inversión "), F.lpad((F.col("row_id") + 1).cast("string"), 3, "0")))
    .withColumn(
        "availability_countries",
        F.when(F.pmod("row_id", F.lit(4)) == 0, all_country_codes).otherwise(F.array(F.col("domicile_country"))),
    )
    .withColumn("minimum_investment_usd", F.round(F.lit(5.0) + F.pmod("row_id", F.lit(200)) * 2.5, 2))
    .withColumn("fee_bps", (F.lit(10) + F.pmod("row_id", F.lit(190))).cast("int"))
    .withColumn("liquidity_days", F.element_at(F.array(F.lit(0), F.lit(1), F.lit(5), F.lit(30), F.lit(90)), F.col("risk_class").cast("int")))
    .withColumn("duration_years", F.round(F.col("risk_class") * 1.4 + stable_uniform("row_id", seed=53) * 2.0, 2))
    .withColumn("sustainability_score", F.round(stable_uniform("row_id", seed=59) * 100.0, 2))
    .withColumn("benchmark_code", F.concat(F.upper(F.substring("asset_class", 1, 3)), F.lit("_LATAM")))
    .withColumn(
        "eligible_risk_profiles",
        F.when(F.col("risk_class") <= 2, F.array(F.lit("conservative"), F.lit("moderate"), F.lit("aggressive")))
        .when(F.col("risk_class") <= 4, F.array(F.lit("moderate"), F.lit("aggressive")))
        .otherwise(F.array(F.lit("aggressive"))),
    )
    .withColumn("is_active", F.lit(True))
    .withColumn("effective_from", F.to_date(F.lit(START_DATE)))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "investment_product_id",
        "name_pt",
        "name_es",
        "asset_class",
        "domicile_country",
        "availability_countries",
        "currency_code",
        "risk_class",
        "minimum_investment_usd",
        "fee_bps",
        "liquidity_days",
        "duration_years",
        "sustainability_score",
        "benchmark_code",
        "eligible_risk_profiles",
        "is_active",
        "effective_from",
        "synthetic_flag",
        "ingested_at",
    )
)
write_parquet(products, cfg, "investment_products")

# COMMAND ----------

market_base = (
    spark.range(60 * 730, numPartitions=12)
    .withColumnRenamed("id", "row_id")
    .withColumn("instrument_index", F.floor(F.col("row_id") / F.lit(730)).cast("int"))
    .withColumn("day_id", F.pmod(F.col("row_id"), F.lit(730)).cast("int"))
    .withColumn("investment_product_id", F.format_string("QINV-%04d", F.col("instrument_index") + 1))
    .withColumn("observation_date", F.date_add(F.to_date(F.lit(START_DATE)), F.col("day_id")))
    .withColumn("market_country", F.element_at(country_array("code"), (F.pmod("instrument_index", F.lit(12)) + 1).cast("int")))
    .join(
        products.select("investment_product_id", "asset_class", "currency_code", "duration_years"),
        "investment_product_id",
    )
    .join(
        macro.select(
            F.col("country_code").alias("market_country"),
            "observation_date",
            "policy_rate_pct",
            "inflation_yoy_pct",
            "fx_per_usd",
            "stress_regime",
        ),
        ["market_country", "observation_date"],
    )
    .withColumn("noise", stable_uniform("row_id", seed=cfg["seed"] + 71) - F.lit(0.5))
    .withColumn(
        "daily_return_pct",
        F.round(
            F.col("noise") * 3.0
            + F.when(F.col("stress_regime") == "recession", -0.35).otherwise(0.0)
            + F.when((F.col("stress_regime") == "energy_crisis") & (F.col("asset_class") == "commodity"), 0.55).otherwise(0.0)
            + F.when(F.col("asset_class") == "cash", 0.01).otherwise(0.0),
            6,
        ),
    )
)

price_window = Window.partitionBy("investment_product_id").orderBy("observation_date").rowsBetween(Window.unboundedPreceding, 0)
market = (
    market_base
    .withColumn("cum_log_return", F.sum(F.col("daily_return_pct") / 100.0).over(price_window))
    .withColumn("close_price", F.round((F.lit(20.0) + F.col("instrument_index") * 1.3) * F.exp(F.col("cum_log_return")), 6))
    .withColumn("open_price", F.round(F.col("close_price") * (1.0 - F.col("daily_return_pct") / 100.0), 6))
    .withColumn("high_price", F.round(F.greatest("open_price", "close_price") * (1.0 + F.abs("noise") * 0.01), 6))
    .withColumn("low_price", F.round(F.least("open_price", "close_price") * (1.0 - F.abs("noise") * 0.01), 6))
    .withColumn("total_return_index", F.round(F.lit(100.0) * F.exp(F.col("cum_log_return")), 6))
    .withColumn("volatility_30d_pct", F.round(F.lit(4.0) + F.abs("noise") * 28.0 + F.when(F.col("stress_regime") != "normal", 8.0).otherwise(0.0), 4))
    .withColumn("yield_pct", F.round(F.greatest(F.lit(0.0), F.col("policy_rate_pct") + F.col("noise") * 2.0), 4))
    .withColumn("benchmark_return_pct", F.round(F.col("daily_return_pct") * 0.82 + F.col("noise") * 0.2, 6))
    .withColumn("market_observation_id", F.sha2(F.concat_ws("|", "investment_product_id", "observation_date"), 256))
    .withColumn("available_at", F.to_timestamp(F.concat_ws(" ", "observation_date", F.lit("23:59:00"))))
    .withColumn("source_name", F.lit("QUILLA_SYNTHETIC_MARKET"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "market_observation_id",
        "observation_date",
        "investment_product_id",
        "asset_class",
        "market_country",
        "currency_code",
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "daily_return_pct",
        "total_return_index",
        "volatility_30d_pct",
        "yield_pct",
        "duration_years",
        F.col("fx_per_usd").alias("fx_to_usd"),
        "benchmark_return_pct",
        "stress_regime",
        "available_at",
        "source_name",
        "synthetic_flag",
        "ingested_at",
    )
)
write_parquet(market, cfg, "market_daily", ["market_country"])

print(json.dumps({"status": "complete", "datasets": ["macroeconomic_daily", "investment_products", "market_daily"]}))
