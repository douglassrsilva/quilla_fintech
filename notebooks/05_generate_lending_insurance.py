# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

cfg = load_config({
    "party_rows": 144_000,
    "account_rows": 240_000,
    "receivable_rows": 300_000,
    "credit_case_rows": 120_000,
    "insurance_case_rows": 60_000,
})


def business_backbone(rows: int):
    return (
        spark.range(rows, numPartitions=max(4, rows // 100_000))
        .withColumnRenamed("id", "row_id")
        .withColumn("country_index", F.pmod("row_id", F.lit(12)).cast("int"))
        .withColumn("country_code", F.element_at(country_array("code"), (F.col("country_index") + 1).cast("int")))
        .withColumn("h", stable_hash("row_id", seed=cfg["seed"] + 503))
        .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
        .withColumn("account_id", same_country_id(F.col("h"), F.col("country_index"), cfg["account_rows"], "QACC"))
        .withColumn("currency_code", country_value(F.col("country_code"), "currency"))
    )


def random_date(hash_col, start="2024-09-01", days=730):
    return F.date_add(F.to_date(F.lit(start)), F.pmod(hash_col, F.lit(days)).cast("int"))


# COMMAND ----------

receivable_base = business_backbone(cfg["receivable_rows"])
issued_date = random_date(F.col("h"), days=700)
due_date = F.date_add(issued_date, 15 + F.pmod(F.col("h"), F.lit(76)).cast("int"))
receivable_status = F.when(F.pmod(F.col("h"), F.lit(100)) < 58, "settled").when(F.pmod(F.col("h"), F.lit(100)) < 78, "open").when(F.pmod(F.col("h"), F.lit(100)) < 94, "overdue").otherwise("disputed")
receivables_df = (
    receivable_base
    .withColumn("receivable_id", F.format_string("QREC-%012d", F.col("row_id") + 1))
    .withColumn("invoice_token", F.sha2(F.concat_ws("|", F.lit("QUILLA_INVOICE"), "row_id", F.lit(cfg["seed"])), 256))
    .withColumn("counterparty_name", F.when(F.col("country_code") == "BR", F.format_string("Empresa Horizonte %05d", F.pmod("h", F.lit(100000)))).otherwise(F.format_string("Empresa Horizonte %05d", F.pmod("h", F.lit(100000)))))
    .withColumn("issued_date", issued_date)
    .withColumn("due_date", due_date)
    .withColumn("status", receivable_status)
    .withColumn("settled_date", F.when(F.col("status") == "settled", F.date_add(F.col("issued_date"), 5 + F.pmod(F.col("h"), F.lit(40)).cast("int"))))
    .withColumn("face_value", F.round(100 + F.pmod(F.col("h"), F.lit(25_000_000)) / 100.0, 2))
    .withColumn("outstanding_value", F.when(F.col("status") == "settled", F.lit(0.0)).otherwise(F.round(F.col("face_value") * (F.lit(0.25) + F.pmod(F.col("h"), F.lit(76)) / 100.0), 2)))
    .withColumn("eligible_for_advance", (F.col("status") == "open") & (F.pmod(F.col("h"), F.lit(100)) < 72))
    .withColumn("advance_rate", F.when(F.col("eligible_for_advance"), F.round(F.lit(0.70) + F.pmod(F.col("h"), F.lit(21)) / 100.0, 2)))
    .withColumn("source_system", F.concat(F.lit("QUILLA_RECEIVABLES_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select("receivable_id", "party_id", "account_id", "country_code", "invoice_token", "counterparty_name", "issued_date", "due_date", "settled_date", "currency_code", "face_value", "outstanding_value", "status", "eligible_for_advance", "advance_rate", "source_system", "synthetic_flag", "ingested_at")
)
write_parquet(receivables_df, cfg, "receivables", ["country_code"])

# COMMAND ----------

credit_base = business_backbone(cfg["credit_case_rows"])
requested_date = random_date(F.col("h"))
decision_bucket = F.pmod(F.col("h"), F.lit(100))
credit_cases_df = (
    credit_base
    .withColumn("credit_case_id", F.format_string("QCRD-%012d", F.col("row_id") + 1))
    .withColumn("product_id", F.element_at(F.array(F.lit(13), F.lit(14), F.lit(15), F.lit(16)), (F.pmod(F.col("h"), F.lit(4)) + 1).cast("int")))
    .withColumn("requested_at", F.to_timestamp(F.concat_ws(" ", requested_date, F.lit("10:00:00"))))
    .withColumn("decision", F.when(decision_bucket < 64, "approved").when(decision_bucket < 82, "declined").when(decision_bucket < 95, "manual_review").otherwise("withdrawn"))
    .withColumn("decided_at", F.when(F.col("decision") != "withdrawn", F.col("requested_at") + F.expr("INTERVAL 4 HOURS")))
    .withColumn("requested_amount", F.round(500 + F.pmod(F.col("h"), F.lit(15_000_000)) / 100.0, 2))
    .withColumn("approved_amount", F.when(F.col("decision") == "approved", F.round(F.col("requested_amount") * (F.lit(0.55) + F.pmod(F.col("h"), F.lit(46)) / 100.0), 2)))
    .withColumn("risk_score", F.round(F.pmod(F.col("h"), F.lit(10000)) / 10000.0, 4))
    .withColumn("policy_band", F.when(F.col("risk_score") < 0.35, "A").when(F.col("risk_score") < 0.60, "B").when(F.col("risk_score") < 0.82, "C").otherwise("D"))
    .withColumn("annual_rate_bps", F.when(F.col("decision") == "approved", 650 + F.pmod(F.col("h"), F.lit(4200)).cast("int")))
    .withColumn("term_months", F.element_at(F.array(F.lit(3), F.lit(6), F.lit(9), F.lit(12), F.lit(18), F.lit(24), F.lit(36)), (F.pmod(F.col("h"), F.lit(7)) + 1).cast("int")))
    .withColumn("decision_reason", F.when(F.col("country_code") == "BR", F.when(F.col("decision") == "approved", "Capacidade de pagamento compatível").otherwise("Revisão conforme política local")).otherwise(F.when(F.col("decision") == "approved", "Capacidad de pago compatible").otherwise("Revisión según política local")))
    .withColumn("source_system", F.concat(F.lit("QUILLA_CREDIT_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select("credit_case_id", "party_id", "account_id", "country_code", "product_id", "requested_at", "decided_at", "requested_amount", "approved_amount", "currency_code", "decision", "risk_score", "policy_band", "annual_rate_bps", "term_months", "decision_reason", "source_system", "synthetic_flag", "ingested_at")
)
write_parquet(credit_cases_df, cfg, "credit_cases", ["country_code"])

# COMMAND ----------

insurance_base = business_backbone(cfg["insurance_case_rows"])
opened_date = random_date(F.col("h"))
insurance_cases_df = (
    insurance_base
    .withColumn("insurance_case_id", F.format_string("QINS-%012d", F.col("row_id") + 1))
    .withColumn("policy_token", F.sha2(F.concat_ws("|", F.lit("QUILLA_POLICY"), "row_id", F.lit(cfg["seed"])), 256))
    .withColumn("case_type", F.when(F.pmod(F.col("h"), F.lit(100)) < 72, "policy_service").otherwise("claim"))
    .withColumn("coverage_type", F.element_at(F.array(*[F.lit(v) for v in ["mobile", "life", "business", "purchase_protection"]]), (F.pmod(F.col("h"), F.lit(4)) + 1).cast("int")))
    .withColumn("opened_at", F.to_timestamp(F.concat_ws(" ", opened_date, F.lit("09:30:00"))))
    .withColumn("status", F.when(F.pmod(F.col("h"), F.lit(100)) < 52, "resolved").when(F.pmod(F.col("h"), F.lit(100)) < 82, "open").when(F.pmod(F.col("h"), F.lit(100)) < 95, "investigation").otherwise("denied"))
    .withColumn("resolved_at", F.when(F.col("status").isin("resolved", "denied"), F.col("opened_at") + F.expr("INTERVAL 12 DAYS")))
    .withColumn("claimed_amount", F.when(F.col("case_type") == "claim", F.round(50 + F.pmod(F.col("h"), F.lit(5_000_000)) / 100.0, 2)))
    .withColumn("approved_amount", F.when((F.col("case_type") == "claim") & (F.col("status") == "resolved"), F.round(F.col("claimed_amount") * (F.lit(0.45) + F.pmod(F.col("h"), F.lit(56)) / 100.0), 2)))
    .withColumn("fraud_indicator", (F.col("case_type") == "claim") & (F.pmod(F.col("h"), F.lit(1000)) < 18))
    .withColumn("source_system", F.concat(F.lit("QUILLA_INSURANCE_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select("insurance_case_id", "party_id", "account_id", "country_code", "policy_token", "case_type", "coverage_type", "opened_at", "resolved_at", "currency_code", "claimed_amount", "approved_amount", "status", "fraud_indicator", "source_system", "synthetic_flag", "ingested_at")
)
write_parquet(insurance_cases_df, cfg, "insurance_cases", ["country_code"])
