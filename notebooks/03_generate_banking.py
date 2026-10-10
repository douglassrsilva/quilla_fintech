# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

cfg = load_config({
    "party_rows": 144_000,
    "account_rows": 240_000,
    "card_rows": 160_000,
    "merchant_rows": 20_000,
    "device_rows": 150_000,
})


def backbone(rows: int):
    return (
        spark.range(rows, numPartitions=max(4, rows // 100_000))
        .withColumnRenamed("id", "row_id")
        .withColumn("country_index", F.pmod(F.col("row_id"), F.lit(12)).cast("int"))
        .withColumn("country_code", F.element_at(country_array("code"), (F.col("country_index") + 1).cast("int")))
        .withColumn("h", stable_hash("row_id", seed=cfg["seed"]))
    )


# COMMAND ----------

account_base = backbone(cfg["account_rows"])
account_id = F.format_string("QACC-%012d", F.col("row_id") + 1)
opened_at = F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit("2019-01-01 00:00:00")) + F.pmod(F.col("h"), F.lit(241_747_199))))
account_status_bucket = F.pmod(F.col("h"), F.lit(100))
accounts_df = (
    account_base
    .withColumn("account_id", account_id)
    .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
    .withColumn("product_id", F.element_at(F.array(*[F.lit(v) for v in [1, 2, 3, 4, 13, 14]]), (F.pmod(F.col("h"), F.lit(6)) + 1).cast("int")))
    .withColumn("account_type", F.element_at(F.array(*[F.lit(v) for v in ["checking", "savings", "wallet", "credit", "business", "escrow"]]), (F.pmod(F.col("h"), F.lit(6)) + 1).cast("int")))
    .withColumn("currency_code", country_value(F.col("country_code"), "currency"))
    .withColumn("status", F.when(account_status_bucket < 88, "active").when(account_status_bucket < 96, "dormant").otherwise("closed"))
    .withColumn("opened_at", opened_at)
    .withColumn("closed_at", F.when(account_status_bucket >= 96, F.least(utc_now_literal(), opened_at + F.expr("INTERVAL 900 DAYS"))))
    .withColumn("current_balance", F.round((F.pmod(F.col("h"), F.lit(2_000_000)) / 100.0) * F.when(F.pmod(F.col("h"), F.lit(20)) == 0, 8.0).otherwise(1.0), 2))
    .withColumn("available_balance", F.round(F.col("current_balance") * (F.lit(0.82) + F.pmod(F.col("h"), F.lit(18)) / 100.0), 2))
    .withColumn("credit_limit", F.when(F.col("account_type").isin("credit", "business"), F.round(500 + F.pmod(F.col("h"), F.lit(5_000_000)) / 100.0, 2)))
    .withColumn("risk_band", F.when(F.pmod(F.col("h"), F.lit(100)) < 69, "low").when(F.pmod(F.col("h"), F.lit(100)) < 94, "medium").otherwise("high"))
    .withColumn("source_system", F.concat(F.lit("QUILLA_CORE_"), F.col("country_code")))
    .withColumn("record_version", F.lit(1))
    .withColumn("updated_at", utc_now_literal())
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select("account_id", "party_id", "product_id", "country_code", "account_type", "currency_code", "status", "opened_at", "closed_at", "available_balance", "current_balance", "credit_limit", "risk_band", "source_system", "record_version", "updated_at", "synthetic_flag", "ingested_at")
)
write_parquet(accounts_df, cfg, "accounts", ["country_code"])

# COMMAND ----------

card_base = backbone(cfg["card_rows"])
cards_df = (
    card_base
    .withColumn("card_id", F.format_string("QCARD-%012d", F.col("row_id") + 1))
    .withColumn("card_token", F.sha2(F.concat_ws("|", F.lit("QUILLA_CARD"), F.col("row_id"), F.lit(cfg["seed"])), 256))
    .withColumn("account_id", same_country_id(F.col("h"), F.col("country_index"), cfg["account_rows"], "QACC"))
    .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
    .withColumn("card_type", F.when(F.pmod(F.col("h"), F.lit(100)) < 58, "credit").otherwise("debit"))
    .withColumn("network", F.element_at(F.array(F.lit("VISA_DEMO"), F.lit("MASTERCARD_DEMO")), (F.pmod(F.col("h"), F.lit(2)) + 1).cast("int")))
    .withColumn("form_factor", F.when(F.pmod(F.col("h"), F.lit(100)) < 64, "physical").when(F.pmod(F.col("h"), F.lit(100)) < 91, "virtual").otherwise("tokenized"))
    .withColumn("status", F.when(F.pmod(F.col("h"), F.lit(100)) < 91, "active").when(F.pmod(F.col("h"), F.lit(100)) < 97, "blocked").otherwise("cancelled"))
    .withColumn("issued_at", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit("2020-01-01 00:00:00")) + F.pmod(F.col("h"), F.lit(210_383_999)))))
    .withColumn("expiry_ym", F.date_format(F.add_months(F.col("issued_at"), 48), "yyyy-MM"))
    .withColumn("spending_limit", F.round(500 + F.pmod(F.col("h"), F.lit(5_000_000)) / 100.0, 2))
    .withColumn("available_limit", F.round(F.col("spending_limit") * (F.lit(0.15) + F.pmod(F.col("h"), F.lit(80)) / 100.0), 2))
    .withColumn("source_system", F.concat(F.lit("QUILLA_CARDS_"), F.col("country_code")))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select("card_id", "card_token", "account_id", "party_id", "country_code", "card_type", "network", "form_factor", "status", "issued_at", "expiry_ym", "spending_limit", "available_limit", "source_system", "synthetic_flag", "ingested_at")
)
write_parquet(cards_df, cfg, "cards", ["country_code"])

# COMMAND ----------

merchant_base = backbone(cfg["merchant_rows"])
category_pt = F.element_at(F.array(*[F.lit(v) for v in ["Mercado", "Restaurante", "Mobilidade", "Tecnologia", "Saúde", "Educação", "Viagem", "Serviços"]]), (F.pmod(F.col("h"), F.lit(8)) + 1).cast("int"))
category_es = F.element_at(F.array(*[F.lit(v) for v in ["Mercado", "Restaurante", "Movilidad", "Tecnología", "Salud", "Educación", "Viajes", "Servicios"]]), (F.pmod(F.col("h"), F.lit(8)) + 1).cast("int"))
merchants_df = (
    merchant_base
    .withColumn("merchant_id", F.format_string("QMRC-%012d", F.col("row_id") + 1))
    .withColumn("merchant_name", F.when(F.col("country_code") == "BR", F.format_string("Comércio Aurora %06d", F.col("row_id") + 1)).otherwise(F.format_string("Comercio Luna %06d", F.col("row_id") + 1)))
    .withColumn("merchant_category", F.when(F.col("country_code") == "BR", category_pt).otherwise(category_es))
    .withColumn("mcc", F.format_string("%04d", 5000 + F.pmod(F.col("h"), F.lit(1000))))
    .withColumn("city", country_value(F.col("country_code"), "city"))
    .withColumn("onboarded_at", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit("2020-01-01 00:00:00")) + F.pmod(F.col("h"), F.lit(210_383_999)))))
    .withColumn("risk_band", F.when(F.pmod(F.col("h"), F.lit(100)) < 72, "low").when(F.pmod(F.col("h"), F.lit(100)) < 95, "medium").otherwise("high"))
    .withColumn("status", F.when(F.pmod(F.col("h"), F.lit(100)) < 94, "active").otherwise("review"))
    .withColumn("source_system", F.concat(F.lit("QUILLA_MERCHANTS_"), F.col("country_code")))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select("merchant_id", "country_code", "merchant_name", "merchant_category", "mcc", "city", "onboarded_at", "risk_band", "status", "source_system", "synthetic_flag", "ingested_at")
)
write_parquet(merchants_df, cfg, "merchants", ["country_code"])

# COMMAND ----------

device_base = backbone(cfg["device_rows"])
devices_df = (
    device_base
    .withColumn("device_id", F.format_string("QDEV-%012d", F.col("row_id") + 1))
    .withColumn("device_token", F.sha2(F.concat_ws("|", F.lit("QUILLA_DEVICE"), F.col("row_id"), F.lit(cfg["seed"])), 256))
    .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
    .withColumn("device_type", F.when(F.pmod(F.col("h"), F.lit(100)) < 79, "mobile").when(F.pmod(F.col("h"), F.lit(100)) < 95, "web").otherwise("pos"))
    .withColumn("operating_system", F.element_at(F.array(*[F.lit(v) for v in ["Android", "iOS", "Windows", "macOS", "Linux", "POS_OS"]]), (F.pmod(F.col("h"), F.lit(6)) + 1).cast("int")))
    .withColumn("trust_status", F.when(F.pmod(F.col("h"), F.lit(100)) < 87, "trusted").when(F.pmod(F.col("h"), F.lit(100)) < 97, "new").otherwise("blocked"))
    .withColumn("first_seen_at", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit("2021-01-01 00:00:00")) + F.pmod(F.col("h"), F.lit(178_847_999)))))
    .withColumn("last_seen_at", F.least(utc_now_literal(), F.col("first_seen_at") + F.expr("INTERVAL 720 DAYS")))
    .withColumn("source_system", F.concat(F.lit("QUILLA_DEVICE_GRAPH_"), F.col("country_code")))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select("device_id", "device_token", "party_id", "country_code", "device_type", "operating_system", "trust_status", "first_seen_at", "last_seen_at", "source_system", "synthetic_flag", "ingested_at")
)
write_parquet(devices_df, cfg, "devices", ["country_code"])
