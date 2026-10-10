# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

cfg = config({"credit_case_rows": 120_000, "party_rows": 144_000})
macro = read_landing(cfg, "macroeconomic_daily")

credit_cases = spark.read.table(source_table(cfg, "credit_cases")).orderBy("credit_case_id").limit(cfg["credit_case_rows"]).select(
    "credit_case_id",
    "party_id",
    "account_id",
    "country_code",
    "requested_at",
    "requested_amount",
    "approved_amount",
    "term_months",
    "risk_score",
    "policy_band",
)

credit_months = (
    credit_cases.crossJoin(spark.range(12).withColumnRenamed("id", "months_on_book"))
    .withColumn("observation_month", F.add_months(F.trunc(F.to_date("requested_at"), "month"), F.col("months_on_book").cast("int")))
    .withColumn("macro_date", F.least(F.to_date(F.lit("2026-08-31")), F.greatest(F.to_date(F.lit(START_DATE)), F.col("observation_month"))))
    .join(
        macro.select(
            "country_code",
            F.col("observation_date").alias("macro_date"),
            "policy_rate_pct",
            "inflation_yoy_pct",
            "unemployment_pct",
            "stress_regime",
        ),
        ["country_code", "macro_date"],
        "left",
    )
    .withColumn("approved_principal", F.coalesce("approved_amount", F.col("requested_amount") * 0.6))
    .withColumn("scheduled_payment_local", F.round(F.col("approved_principal") / F.greatest(F.col("term_months"), F.lit(1)), 2))
    .withColumn(
        "default_probability",
        sigmoid(
            F.lit(-5.2)
            + F.col("risk_score") * 4.2
            + F.log1p(F.col("requested_amount")) * 0.08
            + F.coalesce(F.col("inflation_yoy_pct"), F.lit(3.0)) * 0.035
            + F.coalesce(F.col("unemployment_pct"), F.lit(6.0)) * 0.09
            + F.when(F.col("stress_regime").isin("recession", "energy_crisis"), 0.8).otherwise(0.0)
        ),
    )
    .withColumn("case_uniform", stable_uniform("credit_case_id", seed=cfg["seed"] + 101))
    .withColumn("ever_default", F.col("case_uniform") < F.col("default_probability"))
    .withColumn(
        "default_month",
        F.when(
            F.col("ever_default"),
            (F.lit(3) + F.floor(stable_uniform("credit_case_id", seed=cfg["seed"] + 103) * 8.0)).cast("int"),
        ),
    )
    .withColumn("prepayment_flag", (~F.col("ever_default")) & (F.col("months_on_book") == 8) & (stable_uniform("credit_case_id", seed=107) < 0.09))
    .withColumn("default_flag", F.col("ever_default") & (F.col("months_on_book") == F.col("default_month")))
    .withColumn("cure_flag", F.col("ever_default") & (F.col("months_on_book") == F.col("default_month") + 3) & (stable_uniform("credit_case_id", seed=109) < 0.34))
    .withColumn(
        "days_past_due",
        F.when(F.col("months_on_book") < F.coalesce("default_month", F.lit(99)), F.floor(stable_uniform("credit_case_id", "months_on_book", seed=113) * 18.0))
        .when(F.col("months_on_book") == F.col("default_month"), F.lit(90))
        .otherwise(F.least(F.lit(180), F.lit(90) + (F.col("months_on_book") - F.col("default_month")) * 15))
        .cast("int"),
    )
    .withColumn(
        "delinquency_bucket",
        F.when(F.col("days_past_due") == 0, "current")
        .when(F.col("days_past_due") <= 30, "dpd_1_30")
        .when(F.col("days_past_due") <= 60, "dpd_31_60")
        .when(F.col("days_past_due") <= 89, "dpd_61_89")
        .otherwise("default"),
    )
    .withColumn(
        "outstanding_principal_local",
        F.round(
            F.greatest(
                F.lit(0.0),
                F.col("approved_principal")
                * (F.lit(1.0) - F.least(F.col("months_on_book"), F.col("term_months")) / F.greatest(F.col("term_months"), F.lit(1))),
            ),
            2,
        ),
    )
    .withColumn("utilization_pct", F.round(F.least(F.lit(1.5), F.lit(0.15) + F.col("risk_score") + stable_uniform("credit_case_id", "months_on_book", seed=127) * 0.3), 4))
    .withColumn(
        "paid_amount_local",
        F.round(
            F.when(F.col("days_past_due") >= 90, F.col("scheduled_payment_local") * 0.05)
            .when(F.col("days_past_due") > 30, F.col("scheduled_payment_local") * 0.55)
            .otherwise(F.col("scheduled_payment_local") * (0.94 + stable_uniform("credit_case_id", "months_on_book", seed=131) * 0.12)),
            2,
        ),
    )
    .withColumn("exposure_at_default_local", F.when(F.col("default_flag"), F.round(F.col("outstanding_principal_local") * (1.0 + F.col("utilization_pct") * 0.15), 2)))
    .withColumn("lgd_rate", F.lit(0.18) + F.col("risk_score") * 0.52 + F.when(F.col("stress_regime") == "recession", 0.12).otherwise(0.0))
    .withColumn("loss_amount_local", F.when(F.col("default_flag"), F.round(F.col("exposure_at_default_local") * F.least(F.lit(0.95), F.col("lgd_rate")), 2)))
    .withColumn("recovery_amount_local", F.when(F.col("default_flag"), F.round(F.col("exposure_at_default_local") - F.col("loss_amount_local"), 2)))
    .withColumn("performance_id", F.sha2(F.concat_ws("|", "credit_case_id", "observation_month"), 256))
    .withColumn("label_available_at", F.to_timestamp(F.add_months(F.col("observation_month"), 1)))
    .withColumn("source_system", F.concat(F.lit("QUILLA_ML_CREDIT_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "performance_id",
        "credit_case_id",
        "party_id",
        "account_id",
        "country_code",
        "observation_month",
        F.col("months_on_book").cast("int").alias("months_on_book"),
        "scheduled_payment_local",
        "paid_amount_local",
        "outstanding_principal_local",
        "utilization_pct",
        "days_past_due",
        "delinquency_bucket",
        "default_flag",
        "cure_flag",
        "prepayment_flag",
        "exposure_at_default_local",
        "loss_amount_local",
        "recovery_amount_local",
        "label_available_at",
        "source_system",
        "synthetic_flag",
        "ingested_at",
    )
)
write_parquet(credit_months, cfg, "credit_performance_monthly", ["country_code"])

# COMMAND ----------

customer_base = (
    spark.range(cfg["party_rows"] * 24, numPartitions=48)
    .withColumnRenamed("id", "row_id")
    .withColumn("party_numeric", F.pmod(F.col("row_id"), F.lit(cfg["party_rows"])))
    .withColumn("month_index", F.floor(F.col("row_id") / F.lit(cfg["party_rows"])).cast("int"))
    .withColumn("country_index", F.pmod(F.col("party_numeric"), F.lit(12)).cast("int"))
    .withColumn("party_id", F.format_string("QPTY-%012d", F.col("party_numeric") + 1))
    .withColumn("country_code", F.element_at(country_array("code"), (F.col("country_index") + 1).cast("int")))
    .withColumn("observation_month", F.add_months(F.to_date(F.lit(START_DATE)), F.col("month_index")))
    .withColumn("u", stable_uniform("party_id", "month_index", seed=cfg["seed"] + 151))
    .withColumn("latent_value", stable_uniform("party_id", seed=157))
    .withColumn("latent_engagement", stable_uniform("party_id", seed=163))
)

customer_lifecycle = (
    customer_base
    .withColumn("active_days_30d", F.floor(F.lit(2.0) + F.col("latent_engagement") * 27.0 + F.col("u") * 2.0).cast("int"))
    .withColumn("transaction_count_30d", F.floor(F.lit(3.0) + F.col("latent_engagement") * 85.0 + F.col("month_index") * 0.7).cast("long"))
    .withColumn("debit_volume_usd_30d", F.round(F.lit(40.0) + F.col("latent_value") * 6400.0 + F.col("transaction_count_30d") * 9.0, 2))
    .withColumn("credit_volume_usd_30d", F.round(F.lit(30.0) + F.col("latent_value") * 5200.0 + F.col("transaction_count_30d") * 6.0, 2))
    .withColumn("average_balance_usd_30d", F.round(F.lit(25.0) + F.col("latent_value") * 18000.0 + F.col("month_index") * 17.0, 2))
    .withColumn("product_count", (F.lit(1) + F.floor(F.col("latent_value") * 6.0)).cast("int"))
    .withColumn("interaction_count_30d", F.floor(F.col("u") * 8.0).cast("int"))
    .withColumn("complaint_count_90d", F.when(F.col("u") > 0.92, F.floor((F.col("u") - 0.90) * 30.0)).otherwise(0).cast("int"))
    .withColumn("digital_engagement_score", F.round(F.least(F.lit(1.0), F.col("latent_engagement") * 0.85 + F.col("active_days_30d") / 150.0), 4))
    .withColumn("offer_exposed_code", F.element_at(F.array(*[F.lit(value) for value in ["none", "credit", "investment", "insurance", "merchant", "fx"]]), (F.pmod("row_id", F.lit(6)) + 1).cast("int")))
    .withColumn("treatment_group", F.when(F.pmod("row_id", F.lit(5)) == 0, "control").otherwise("treatment"))
    .withColumn(
        "conversion_probability",
        sigmoid(F.lit(-2.4) + F.col("digital_engagement_score") * 2.1 + F.col("latent_value") * 1.2 + F.when(F.col("treatment_group") == "treatment", 0.45).otherwise(0.0)),
    )
    .withColumn("converted_30d", stable_uniform("party_id", "month_index", seed=167) < F.col("conversion_probability"))
    .withColumn(
        "churn_probability",
        sigmoid(F.lit(-3.8) - F.col("digital_engagement_score") * 2.2 + F.col("complaint_count_90d") * 0.55 - F.col("product_count") * 0.18),
    )
    .withColumn("churned_90d", stable_uniform("party_id", "month_index", seed=173) < F.col("churn_probability"))
    .withColumn("revenue_usd_90d", F.round(F.col("debit_volume_usd_30d") * 0.011 + F.col("product_count") * 14.0 + F.when(F.col("converted_30d"), 35.0).otherwise(0.0), 2))
    .withColumn("cost_to_serve_usd_90d", F.round(F.lit(4.0) + F.col("interaction_count_30d") * 3.4 + F.col("complaint_count_90d") * 8.0, 2))
    .withColumn("clv_usd_12m", F.round(F.greatest(F.lit(0.0), (F.col("revenue_usd_90d") - F.col("cost_to_serve_usd_90d")) * 4.0 * (1.0 - F.col("churn_probability"))), 2))
    .withColumn("lifecycle_id", F.sha2(F.concat_ws("|", "party_id", "observation_month"), 256))
    .withColumn("label_available_at", F.to_timestamp(F.add_months(F.col("observation_month"), 3)))
    .withColumn("source_system", F.concat(F.lit("QUILLA_ML_CUSTOMER_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "lifecycle_id",
        "party_id",
        "country_code",
        "observation_month",
        "active_days_30d",
        "transaction_count_30d",
        "debit_volume_usd_30d",
        "credit_volume_usd_30d",
        "average_balance_usd_30d",
        "product_count",
        "interaction_count_30d",
        "complaint_count_90d",
        "digital_engagement_score",
        "offer_exposed_code",
        "treatment_group",
        "converted_30d",
        "churned_90d",
        "revenue_usd_90d",
        "cost_to_serve_usd_90d",
        "clv_usd_12m",
        "label_available_at",
        "source_system",
        "synthetic_flag",
        "ingested_at",
    )
)
write_parquet(customer_lifecycle, cfg, "customer_lifecycle_monthly", ["country_code"])

print(json.dumps({"status": "complete", "datasets": ["credit_performance_monthly", "customer_lifecycle_monthly"]}))
