# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

cfg = config({
    "pricing_rows": 720_000,
    "profile_rows": 72_000,
    "investment_event_rows": 1_200_000,
    "party_rows": 144_000,
    "account_rows": 240_000,
})

macro = read_landing(cfg, "macroeconomic_daily")

pricing_base = (
    spark.range(cfg["pricing_rows"], numPartitions=24)
    .withColumnRenamed("id", "row_id")
    .withColumn("country_index", F.pmod("row_id", F.lit(12)).cast("int"))
    .withColumn("country_code", F.element_at(country_array("code"), (F.col("country_index") + 1).cast("int")))
    .withColumn("h", F.xxhash64("row_id", F.lit(cfg["seed"] + 301)))
    .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
    .withColumn("account_id", same_country_id(F.col("h"), F.col("country_index"), cfg["account_rows"], "QACC"))
    .withColumn("product_id", (F.pmod("h", F.lit(25)) + 1).cast("int"))
    .withColumn("offer_date", F.date_add(F.to_date(F.lit(START_DATE)), F.pmod("h", F.lit(730)).cast("int")))
    .join(
        macro.select(
            "country_code",
            F.col("observation_date").alias("offer_date"),
            F.col("policy_rate_pct").alias("reference_policy_rate_pct"),
            "inflation_yoy_pct",
            "stress_regime",
        ),
        ["country_code", "offer_date"],
    )
)

pricing = (
    pricing_base
    .withColumn("experiment_id", F.format_string("QEXP-%014d", F.col("row_id") + 1))
    .withColumn("offer_ts", F.to_timestamp(F.concat_ws(" ", "offer_date", F.lit("14:00:00"))))
    .withColumn("offer_type", F.element_at(F.array(*[F.lit(value) for value in ["personal_credit", "card_limit", "receivables", "insurance", "investment", "fx"]]), (F.pmod("h", F.lit(6)) + 1).cast("int")))
    .withColumn("channel", F.element_at(F.array(F.lit("mobile"), F.lit("web"), F.lit("email"), F.lit("whatsapp_demo")), (F.pmod("h", F.lit(4)) + 1).cast("int")))
    .withColumn("treatment_group", F.when(F.pmod("row_id", F.lit(5)) == 0, "control").otherwise("treatment"))
    .withColumn("reference_apr_bps", F.round(F.col("reference_policy_rate_pct") * 100 + 900 + F.pmod("h", F.lit(1800))).cast("int"))
    .withColumn("price_delta_bps", F.when(F.col("treatment_group") == "control", 0).otherwise(F.round((stable_uniform("row_id", seed=307) - 0.5) * 1400.0)).cast("int"))
    .withColumn("offered_apr_bps", F.greatest(F.lit(100), F.col("reference_apr_bps") + F.col("price_delta_bps")).cast("int"))
    .withColumn("fee_bps", F.round(stable_uniform("row_id", seed=311) * 450.0).cast("int"))
    .withColumn("limit_local", F.round(F.lit(100.0) + stable_uniform("row_id", seed=313) * 50000.0, 2))
    .withColumn("term_months", F.element_at(F.array(F.lit(3), F.lit(6), F.lit(9), F.lit(12), F.lit(18), F.lit(24), F.lit(36)), (F.pmod("h", F.lit(7)) + 1).cast("int")))
    .withColumn("eligibility_score", F.round(stable_uniform("party_id", seed=317), 4))
    .withColumn(
        "acceptance_probability",
        sigmoid(
            F.lit(-1.0)
            + F.col("eligibility_score") * 2.1
            - F.col("price_delta_bps") / 850.0
            - F.col("fee_bps") / 900.0
            + F.when(F.col("channel") == "mobile", 0.28).otherwise(0.0)
            + F.when(F.col("stress_regime") == "recession", -0.55).otherwise(0.0)
        ),
    )
    .withColumn("accepted_flag", stable_uniform("row_id", seed=331) < F.col("acceptance_probability"))
    .withColumn("activated_flag", F.col("accepted_flag") & (stable_uniform("row_id", seed=337) < 0.86))
    .withColumn("utilization_90d", F.round(F.when(F.col("activated_flag"), 0.05 + stable_uniform("row_id", seed=347) * 0.92).otherwise(0.0), 4))
    .withColumn("revenue_usd_90d", F.round(F.col("limit_local") * F.col("utilization_90d") * (F.col("offered_apr_bps") + F.col("fee_bps")) / 40000.0, 2))
    .withColumn(
        "expected_loss_usd_90d",
        F.round(
            F.col("limit_local")
            * F.col("utilization_90d")
            * (0.005 + (1.0 - F.col("eligibility_score")) * 0.08 + F.when(F.col("stress_regime") == "recession", 0.035).otherwise(0.0)),
            2,
        ),
    )
    .withColumn("contribution_margin_usd_90d", F.round(F.col("revenue_usd_90d") - F.col("expected_loss_usd_90d") - F.when(F.col("activated_flag"), 3.0).otherwise(0.5), 2))
    .withColumn("label_available_at", F.col("offer_ts") + F.expr("INTERVAL 90 DAYS"))
    .withColumn("source_system", F.concat(F.lit("QUILLA_ML_PRICING_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "experiment_id", "party_id", "account_id", "country_code", "product_id", "offer_ts", "offer_type", "channel",
        "treatment_group", "reference_policy_rate_pct", "reference_apr_bps", "offered_apr_bps", "fee_bps", "limit_local",
        "term_months", "eligibility_score", "accepted_flag", "activated_flag", "utilization_90d", "revenue_usd_90d",
        "expected_loss_usd_90d", "contribution_margin_usd_90d", "label_available_at", "source_system", "synthetic_flag", "ingested_at",
    )
)
write_parquet(pricing, cfg, "pricing_experiments", ["country_code"])

# COMMAND ----------

profiles = (
    spark.range(cfg["profile_rows"], numPartitions=12)
    .withColumnRenamed("id", "row_id")
    .withColumn("country_index", F.pmod("row_id", F.lit(12)).cast("int"))
    .withColumn("country_code", F.element_at(country_array("code"), (F.col("country_index") + 1).cast("int")))
    .withColumn("h", F.xxhash64("row_id", F.lit(cfg["seed"] + 353)))
    .withColumn("profile_id", F.format_string("QWPR-%012d", F.col("row_id") + 1))
    .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
    .withColumn("locale_code", country_value(F.col("country_code"), "locale"))
    .withColumn("assessment_ts", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit(f"{START_DATE} 09:00:00")) + F.pmod("h", F.lit(63_072_000)))))
    .withColumn("annual_income_usd", F.round(F.lit(3000.0) + stable_uniform("row_id", seed=359) * 180000.0, 2))
    .withColumn("investable_assets_usd", F.round(F.lit(50.0) + stable_uniform("row_id", seed=367) * F.col("annual_income_usd") * 3.5, 2))
    .withColumn("horizon_months", F.round(F.lit(3.0) + stable_uniform("row_id", seed=373) * 237.0).cast("int"))
    .withColumn("emergency_reserve_months", F.round(stable_uniform("row_id", seed=379) * 18.0, 2))
    .withColumn("liquidity_need", F.element_at(F.array(F.lit("high"), F.lit("medium"), F.lit("low")), (F.pmod("h", F.lit(3)) + 1).cast("int")))
    .withColumn("loss_capacity_pct", F.round(2.0 + stable_uniform("row_id", seed=383) * 38.0, 2))
    .withColumn("investment_knowledge_score", F.round(stable_uniform("row_id", seed=389), 4))
    .withColumn("sustainability_preference", F.element_at(F.array(F.lit("required"), F.lit("preferred"), F.lit("neutral")), (F.pmod("h", F.lit(3)) + 1).cast("int")))
    .withColumn("investment_objective", F.element_at(F.array(*[F.lit(value) for value in ["reserve", "income", "education", "home", "retirement", "growth"]]), (F.pmod("h", F.lit(6)) + 1).cast("int")))
    .withColumn("declared_risk_tolerance", F.element_at(F.array(F.lit("conservative"), F.lit("moderate"), F.lit("aggressive")), (F.pmod("h", F.lit(3)) + 1).cast("int")))
    .withColumn(
        "risk_capacity_score",
        F.col("loss_capacity_pct") / 40.0 * 0.45
        + F.least(F.lit(1.0), F.col("horizon_months") / 120.0) * 0.25
        + F.col("investment_knowledge_score") * 0.20
        + F.when(F.col("liquidity_need") == "low", 0.10).otherwise(0.0),
    )
    .withColumn("modeled_risk_class", F.when(F.col("risk_capacity_score") < 0.38, "conservative").when(F.col("risk_capacity_score") < 0.70, "moderate").otherwise("aggressive"))
    .withColumn("suitability_status", F.when(F.col("emergency_reserve_months") < 2.0, "restricted").when(F.col("investment_knowledge_score") < 0.12, "education_required").otherwise("eligible"))
    .withColumn("max_product_risk_class", F.when(F.col("modeled_risk_class") == "conservative", 2).when(F.col("modeled_risk_class") == "moderate", 4).otherwise(5).cast("int"))
    .withColumn("label_available_at", F.col("assessment_ts") + F.expr("INTERVAL 1 DAYS"))
    .withColumn("source_system", F.concat(F.lit("QUILLA_WEALTH_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "profile_id", "party_id", "country_code", "locale_code", "assessment_ts", "annual_income_usd", "investable_assets_usd",
        "horizon_months", "emergency_reserve_months", "liquidity_need", "loss_capacity_pct", "investment_knowledge_score",
        "sustainability_preference", "investment_objective", "declared_risk_tolerance", "modeled_risk_class", "suitability_status",
        "max_product_risk_class", "label_available_at", "source_system", "synthetic_flag", "ingested_at",
    )
)
write_parquet(profiles, cfg, "investment_profiles", ["country_code"])

# COMMAND ----------

products = read_landing(cfg, "investment_products").select(
    "investment_product_id", "asset_class", "risk_class", "minimum_investment_usd", "fee_bps"
)

investment_events = (
    spark.range(cfg["investment_event_rows"], numPartitions=32)
    .withColumnRenamed("id", "row_id")
    .withColumn("country_index", F.pmod("row_id", F.lit(12)).cast("int"))
    .withColumn("h", F.xxhash64("row_id", F.lit(cfg["seed"] + 397)))
    .withColumn("profile_id", same_country_id(F.col("h"), F.col("country_index"), cfg["profile_rows"], "QWPR"))
    .join(profiles.select("profile_id", "party_id", "country_code", "modeled_risk_class", "max_product_risk_class"), "profile_id")
    .withColumn("product_numeric", (F.pmod("h", F.lit(240)) + 1).cast("int"))
    .withColumn("investment_product_id", F.format_string("QINV-%04d", "product_numeric"))
    .join(products, "investment_product_id")
    .withColumn("investment_event_id", F.format_string("QIEV-%014d", F.col("row_id") + 1))
    .withColumn("event_ts", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit(f"{START_DATE} 00:00:00")) + F.pmod("h", F.lit(63_072_000)))))
    .withColumn("event_type", F.element_at(F.array(F.lit("view"), F.lit("simulation"), F.lit("buy"), F.lit("sell"), F.lit("hold")), (F.pmod("h", F.lit(5)) + 1).cast("int")))
    .withColumn("channel", F.element_at(F.array(F.lit("mobile"), F.lit("web"), F.lit("advisor_demo")), (F.pmod("h", F.lit(3)) + 1).cast("int")))
    .withColumn("amount_usd", F.round(F.col("minimum_investment_usd") + stable_uniform("row_id", seed=401) * 12000.0, 2))
    .withColumn("unit_price_usd", F.round(F.lit(5.0) + F.col("product_numeric") * 0.75 + stable_uniform("row_id", seed=409) * 20.0, 4))
    .withColumn("units", F.round(F.col("amount_usd") / F.col("unit_price_usd"), 6))
    .withColumn("portfolio_value_before_usd", F.round(stable_uniform("profile_id", seed=419) * 180000.0, 2))
    .withColumn("portfolio_value_after_usd", F.round(F.col("portfolio_value_before_usd") + F.when(F.col("event_type") == "buy", F.col("amount_usd")).when(F.col("event_type") == "sell", -F.col("amount_usd")).otherwise(0.0), 2))
    .withColumn("recommendation_exposed", stable_uniform("row_id", seed=421) < 0.62)
    .withColumn("recommendation_rank", F.when(F.col("recommendation_exposed"), (F.pmod("h", F.lit(10)) + 1).cast("int")))
    .withColumn(
        "recommendation_probability",
        sigmoid(
            F.lit(-2.0)
            + F.when(F.col("risk_class") <= F.col("max_product_risk_class"), 1.4).otherwise(-2.0)
            + F.when(F.col("recommendation_rank") <= 3, 0.9).otherwise(0.0)
            - F.col("fee_bps") / 250.0
        ),
    )
    .withColumn("accepted_recommendation", F.col("recommendation_exposed") & (stable_uniform("row_id", seed=431) < F.col("recommendation_probability")))
    .withColumn("realized_return_90d_pct", F.round((stable_uniform("row_id", seed=433) - 0.45) * (F.col("risk_class") * 5.0), 4))
    .withColumn("suitability_breach_flag", F.col("risk_class") > F.col("max_product_risk_class"))
    .withColumn("label_available_at", F.col("event_ts") + F.expr("INTERVAL 90 DAYS"))
    .withColumn("source_system", F.concat(F.lit("QUILLA_WEALTH_EVENTS_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "investment_event_id", "profile_id", "party_id", "investment_product_id", "country_code", "event_ts", "event_type",
        "channel", "amount_usd", "units", "unit_price_usd", "portfolio_value_before_usd", "portfolio_value_after_usd",
        "recommendation_exposed", "recommendation_rank", "accepted_recommendation", "realized_return_90d_pct",
        "suitability_breach_flag", "label_available_at", "source_system", "synthetic_flag", "ingested_at",
    )
)
write_parquet(investment_events, cfg, "investment_events", ["country_code"])

print(json.dumps({"status": "complete", "datasets": ["pricing_experiments", "investment_profiles", "investment_events"]}))
