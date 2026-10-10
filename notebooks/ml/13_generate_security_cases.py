# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

cfg = config({
    "session_rows": 1_200_000,
    "party_rows": 144_000,
    "account_rows": 240_000,
    "device_rows": 150_000,
    "risk_alert_rows": 206_380,
    "insurance_case_rows": 60_000,
})

sessions_base = (
    spark.range(cfg["session_rows"], numPartitions=32)
    .withColumnRenamed("id", "row_id")
    .withColumn("country_index", F.pmod("row_id", F.lit(12)).cast("int"))
    .withColumn("country_code", F.element_at(country_array("code"), (F.col("country_index") + 1).cast("int")))
    .withColumn("h", F.xxhash64("row_id", F.lit(cfg["seed"] + 201)))
    .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
    .withColumn("account_id", same_country_id(F.col("h"), F.col("country_index"), cfg["account_rows"], "QACC"))
    .withColumn("device_id", same_country_id(F.col("h"), F.col("country_index"), cfg["device_rows"], "QDEV"))
    .withColumn("u", stable_uniform("row_id", seed=cfg["seed"] + 211))
    .withColumn("session_ts", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit(f"{START_DATE} 00:00:00")) + F.pmod(F.col("h"), F.lit(63_072_000)))))
)

security_sessions = (
    sessions_base
    .withColumn("session_id", F.format_string("QSES-%014d", F.col("row_id") + 1))
    .withColumn("channel", F.element_at(F.array(F.lit("mobile"), F.lit("web"), F.lit("api")), (F.pmod("h", F.lit(3)) + 1).cast("int")))
    .withColumn("network_type", F.element_at(F.array(F.lit("wifi"), F.lit("mobile"), F.lit("corporate"), F.lit("proxy")), (F.pmod("h", F.lit(4)) + 1).cast("int")))
    .withColumn("ip_token", F.sha2(F.concat_ws("|", F.lit("QUILLA_IP"), "row_id", F.lit(cfg["seed"])), 256))
    .withColumn("geo_distance_km", F.round(F.when(F.col("u") > 0.97, 1500.0 + F.col("u") * 8000.0).otherwise(F.col("u") * 85.0), 2))
    .withColumn("minutes_since_previous_session", F.round(F.lit(2.0) + stable_uniform("row_id", seed=223) * 2880.0, 2))
    .withColumn("failed_logins_1h", F.when(F.col("u") > 0.985, F.floor((F.col("u") - 0.98) * 500.0)).otherwise(F.floor(F.col("u") * 2.0)).cast("int"))
    .withColumn("sessions_10m", F.when(F.col("u") > 0.975, F.floor((F.col("u") - 0.95) * 250.0)).otherwise(F.floor(F.col("u") * 4.0) + 1).cast("int"))
    .withColumn("new_device", stable_uniform("row_id", seed=227) < 0.11)
    .withColumn("impossible_travel", (F.col("geo_distance_km") > 1000.0) & (F.col("minutes_since_previous_session") < 180.0))
    .withColumn("credential_reset_24h", stable_uniform("row_id", seed=229) < 0.035)
    .withColumn("mfa_method", F.element_at(F.array(F.lit("push"), F.lit("biometric"), F.lit("otp"), F.lit("none")), (F.pmod("h", F.lit(4)) + 1).cast("int")))
    .withColumn("mfa_result", F.when(F.col("mfa_method") == "none", "not_requested").when(F.col("u") > 0.965, "failed").otherwise("passed"))
    .withColumn(
        "device_trust_score",
        F.round(
            F.greatest(
                F.lit(0.0),
                F.lit(0.96)
                - F.when(F.col("new_device"), 0.28).otherwise(0.0)
                - F.when(F.col("impossible_travel"), 0.38).otherwise(0.0)
                - F.when(F.col("mfa_result") == "failed", 0.30).otherwise(0.0)
                - F.col("failed_logins_1h") * 0.025,
            ),
            4,
        ),
    )
    .withColumn(
        "ato_probability",
        sigmoid(
            F.lit(-6.0)
            + F.when(F.col("new_device"), 1.4).otherwise(0.0)
            + F.when(F.col("impossible_travel"), 2.5).otherwise(0.0)
            + F.when(F.col("credential_reset_24h"), 1.1).otherwise(0.0)
            + F.when(F.col("mfa_result") == "failed", 2.0).otherwise(0.0)
            + F.col("failed_logins_1h") * 0.3
            + F.col("sessions_10m") * 0.08
        ),
    )
    .withColumn("account_takeover_label", stable_uniform("row_id", seed=233) < F.col("ato_probability"))
    .withColumn("label_available_at", F.col("session_ts") + F.expr("INTERVAL 7 DAYS"))
    .withColumn("source_system", F.concat(F.lit("QUILLA_SECURITY_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "session_id",
        "party_id",
        "account_id",
        "device_id",
        "country_code",
        "session_ts",
        "channel",
        "network_type",
        "ip_token",
        "geo_distance_km",
        "minutes_since_previous_session",
        "failed_logins_1h",
        "sessions_10m",
        "new_device",
        "impossible_travel",
        "credential_reset_24h",
        "mfa_method",
        "mfa_result",
        "device_trust_score",
        "account_takeover_label",
        "label_available_at",
        "source_system",
        "synthetic_flag",
        "ingested_at",
    )
)
write_parquet(security_sessions, cfg, "security_sessions", ["country_code"])

# COMMAND ----------

alerts = (
    spark.read.table(source_table(cfg, "risk_alerts"))
    .orderBy("alert_id")
    .limit(cfg["risk_alert_rows"])
)
risk_outcomes = (
    alerts
    .withColumn("domain", F.when(F.col("alert_type") == "aml_pattern", "aml_alert").otherwise("fraud_alert"))
    .withColumn("case_id", F.col("alert_id"))
    .withColumn("insurance_case_id", F.lit(None).cast("string"))
    .withColumn("opened_at", F.col("created_at"))
    .withColumn("resolved_at", F.col("created_at") + F.expr("INTERVAL 3 DAYS"))
    .withColumn("case_type", F.col("alert_type"))
    .withColumn(
        "confirmation_probability",
        sigmoid(
            F.lit(-5.0)
            + F.col("risk_score") * 5.2
            + F.log1p(F.col("amount_at_risk")) * 0.12
            + F.when(F.col("alert_type").isin("account_takeover", "aml_pattern"), 0.65).otherwise(0.0)
        ),
    )
    .withColumn("confirmed_flag", stable_uniform("alert_id", seed=cfg["seed"] + 251) < F.col("confirmation_probability"))
    .withColumn("false_positive_flag", ~F.col("confirmed_flag"))
    .withColumn("investigator_disposition", F.when(F.col("confirmed_flag"), "confirmed").otherwise("false_positive"))
    .withColumn("loss_amount_usd", F.round(F.when(F.col("confirmed_flag"), F.col("amount_at_risk") * (0.08 + stable_uniform("alert_id", seed=257) * 0.52)).otherwise(0.0), 2))
    .withColumn("prevented_loss_usd", F.round(F.when(F.col("confirmed_flag"), F.col("amount_at_risk") - F.col("loss_amount_usd")).otherwise(0.0), 2))
    .withColumn("recovery_amount_usd", F.round(F.col("loss_amount_usd") * stable_uniform("alert_id", seed=263) * 0.45, 2))
    .withColumn("investigation_minutes", F.round(F.lit(8.0) + (1.0 - F.col("risk_score")) * 110.0 + stable_uniform("alert_id", seed=269) * 35.0).cast("int"))
    .withColumn("escalation_level", F.when(F.col("confirmed_flag") & (F.col("amount_at_risk") > 25000), "L3").when(F.col("confirmed_flag"), "L2").otherwise("L1"))
    .withColumn("label_available_at", F.col("resolved_at") + F.expr("INTERVAL 1 DAYS"))
    .withColumn("outcome_id", F.sha2(F.concat_ws("|", F.lit("RISK"), "alert_id"), 256))
    .withColumn("source_system", F.concat(F.lit("QUILLA_ML_OUTCOMES_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "outcome_id", "domain", "case_id", "alert_id", "insurance_case_id", "event_id", "party_id", "account_id",
        "country_code", "opened_at", "resolved_at", "case_type", "investigator_disposition", "confirmed_flag",
        "false_positive_flag", "loss_amount_usd", "prevented_loss_usd", "recovery_amount_usd", "investigation_minutes",
        "escalation_level", "label_available_at", "source_system", "synthetic_flag", "ingested_at",
    )
)

insurance = (
    spark.read.table(source_table(cfg, "insurance_cases"))
    .orderBy("insurance_case_id")
    .limit(cfg["insurance_case_rows"])
)
insurance_outcomes = (
    insurance
    .withColumn("domain", F.lit("insurance_claim"))
    .withColumn("case_id", F.col("insurance_case_id"))
    .withColumn("alert_id", F.lit(None).cast("string"))
    .withColumn("event_id", F.lit(None).cast("string"))
    .withColumn("opened_at", F.col("opened_at"))
    .withColumn("resolved_at", F.coalesce("resolved_at", F.col("opened_at") + F.expr("INTERVAL 21 DAYS")))
    .withColumn(
        "confirmation_probability",
        sigmoid(
            F.lit(-5.4)
            + F.when(F.col("case_type") == "claim", 0.8).otherwise(-1.5)
            + F.when(F.col("fraud_indicator"), 2.8).otherwise(0.0)
            + F.log1p(F.coalesce("claimed_amount", F.lit(0.0))) * 0.10
        ),
    )
    .withColumn("confirmed_flag", stable_uniform("insurance_case_id", seed=cfg["seed"] + 271) < F.col("confirmation_probability"))
    .withColumn("false_positive_flag", ~F.col("confirmed_flag"))
    .withColumn("investigator_disposition", F.when(F.col("confirmed_flag"), "confirmed").otherwise("cleared"))
    .withColumn("loss_amount_usd", F.round(F.when(F.col("confirmed_flag"), F.coalesce("approved_amount", "claimed_amount", F.lit(0.0))).otherwise(0.0), 2))
    .withColumn("prevented_loss_usd", F.round(F.when(F.col("confirmed_flag"), F.coalesce("claimed_amount", F.lit(0.0)) * 0.65).otherwise(0.0), 2))
    .withColumn("recovery_amount_usd", F.round(F.col("loss_amount_usd") * stable_uniform("insurance_case_id", seed=277) * 0.25, 2))
    .withColumn("investigation_minutes", F.round(F.lit(20.0) + stable_uniform("insurance_case_id", seed=281) * 220.0).cast("int"))
    .withColumn("escalation_level", F.when(F.col("confirmed_flag") & (F.coalesce("claimed_amount", F.lit(0.0)) > 20000), "L3").when(F.col("confirmed_flag"), "L2").otherwise("L1"))
    .withColumn("label_available_at", F.col("resolved_at") + F.expr("INTERVAL 1 DAYS"))
    .withColumn("outcome_id", F.sha2(F.concat_ws("|", F.lit("INSURANCE"), "insurance_case_id"), 256))
    .withColumn("source_system", F.concat(F.lit("QUILLA_ML_OUTCOMES_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.to_timestamp(F.lit(REFERENCE_TS)))
    .select(
        "outcome_id", "domain", "case_id", "alert_id", "insurance_case_id", "event_id", "party_id", "account_id",
        "country_code", "opened_at", "resolved_at", "case_type", "investigator_disposition", "confirmed_flag",
        "false_positive_flag", "loss_amount_usd", "prevented_loss_usd", "recovery_amount_usd", "investigation_minutes",
        "escalation_level", "label_available_at", "source_system", "synthetic_flag", "ingested_at",
    )
)

case_outcomes = risk_outcomes.unionByName(insurance_outcomes)
write_parquet(case_outcomes, cfg, "case_outcomes", ["domain", "country_code"])

print(json.dumps({"status": "complete", "datasets": ["security_sessions", "case_outcomes"]}))
