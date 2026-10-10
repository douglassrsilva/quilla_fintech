# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

from functools import reduce

cfg = load_config({
    "event_rows": 13_758_648,
    "party_rows": 144_000,
    "account_rows": 240_000,
    "card_rows": 160_000,
    "merchant_rows": 20_000,
    "device_rows": 150_000,
})

counts = event_counts(cfg["event_rows"])
assert_expected(sum(counts.values()) == cfg["event_rows"], "Country allocation does not sum to event_rows")
print(json.dumps({"event_rows": cfg["event_rows"], "country_counts": counts}, ensure_ascii=False))

# COMMAND ----------

currency_per_usd = {
    "BR": 5.40, "MX": 19.20, "CO": 4100.0, "AR": 1050.0,
    "CL": 940.0, "PE": 3.75, "EC": 1.0, "GT": 7.70,
    "DO": 60.0, "CR": 515.0, "PA": 1.0, "UY": 42.0,
}
currency_rate_expr = F.create_map(*sum(([F.lit(code), F.lit(rate)] for code, rate in currency_per_usd.items()), []))

event_frames = []
global_offset = 0

for country_index, country in enumerate(COUNTRIES):
    code = country["code"]
    count = counts[code]
    partitions = max(2, count // 250_000)
    base = (
        spark.range(count, numPartitions=partitions)
        .withColumnRenamed("id", "local_id")
        .withColumn("source_record_id", F.format_string("QSRC-%014d", F.col("local_id") + F.lit(global_offset) + 1))
        .withColumn("country_code", F.lit(code))
        .withColumn("country_index", F.lit(country_index))
        .withColumn("is_duplicate_key", (F.col("local_id") > 0) & (F.pmod(F.col("local_id"), F.lit(500)) == 499))
        .withColumn("canonical_local_id", F.when(F.col("is_duplicate_key"), F.col("local_id") - 1).otherwise(F.col("local_id")))
        .withColumn("h", stable_hash("canonical_local_id", F.lit(code), seed=cfg["seed"]))
        .withColumn("physical_h", stable_hash("local_id", F.lit(code), seed=cfg["seed"] + 101))
    )

    event_bucket = F.pmod(F.col("h"), F.lit(100))
    event_type = (
        F.when(event_bucket < 30, "merchant_payment")
        .when(event_bucket < 48, "p2p_transfer")
        .when(event_bucket < 58, "card_authorization")
        .when(event_bucket < 66, "cash_withdrawal")
        .when(event_bucket < 74, "cash_deposit")
        .when(event_bucket < 82, "bill_payment")
        .when(event_bucket < 87, "credit_repayment")
        .when(event_bucket < 91, "credit_disbursement")
        .when(event_bucket < 96, "cross_border_transfer")
        .when(event_bucket < 98, "insurance_premium")
        .otherwise("fx_conversion")
    )

    u = (F.pmod(F.col("h"), F.lit(1_000_000)) + 1) / F.lit(1_000_001.0)
    day_offset = F.floor(F.pow(u, F.lit(0.86)) * F.lit(730)).cast("int")
    raw_event_date = F.date_add(F.to_date(F.lit(EVENT_START)), day_offset)
    payday_bucket = F.pmod(F.col("physical_h"), F.lit(100))
    event_date = (
        F.when(payday_bucket < 8, F.trunc(raw_event_date, "month"))
        .when(payday_bucket < 16, F.date_add(F.trunc(raw_event_date, "month"), 14))
        .otherwise(raw_event_date)
    )
    hour = F.when(F.pmod(F.col("h"), F.lit(100)) < 75, 8 + F.pmod(F.col("h"), F.lit(13))).otherwise(F.pmod(F.col("h"), F.lit(24)))
    minute = F.pmod(stable_hash("canonical_local_id", F.lit(code), F.lit("minute"), seed=cfg["seed"]), F.lit(60))
    second = F.pmod(stable_hash("canonical_local_id", F.lit(code), F.lit("second"), seed=cfg["seed"]), F.lit(60))

    df = (
        base
        .withColumn("event_id", F.format_string(f"QEVT-{code}-%012d", F.col("canonical_local_id") + 1))
        .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
        .withColumn("account_id", same_country_id(F.col("h"), F.col("country_index"), cfg["account_rows"], "QACC"))
        .withColumn("event_type", event_type)
        .withColumn("card_id", F.when(F.col("event_type").isin("merchant_payment", "card_authorization"), same_country_id(F.col("h"), F.col("country_index"), cfg["card_rows"], "QCARD")))
        .withColumn("merchant_id", F.when(F.col("event_type").isin("merchant_payment", "card_authorization"), same_country_id(F.col("h"), F.col("country_index"), cfg["merchant_rows"], "QMRC")))
        .withColumn("device_id", same_country_id(F.col("h"), F.col("country_index"), cfg["device_rows"], "QDEV"))
        .withColumn("product_id", F.pmod(F.col("h"), F.lit(25)).cast("int") + 1)
        .withColumn("event_status", F.when(F.pmod(F.col("h"), F.lit(100)) < 91, "completed").when(F.pmod(F.col("h"), F.lit(100)) < 96, "pending").when(F.pmod(F.col("h"), F.lit(100)) < 99, "declined").otherwise("reversed"))
        .withColumn("channel", F.element_at(F.array(*[F.lit(v) for v in ["mobile", "web", "pos", "api", "atm", "branch"]]), (F.pmod(F.col("h"), F.lit(6)) + 1).cast("int")))
        .withColumn("direction", F.when(F.col("event_type").isin("cash_deposit", "credit_disbursement"), "credit").otherwise("debit"))
        .withColumn("is_missing_amount", F.pmod(F.col("local_id"), F.lit(1000)) == 117)
        .withColumn("is_currency_mismatch", F.pmod(F.col("local_id"), F.lit(2000)) == 777)
        .withColumn("base_amount", F.least(F.lit(250_000.0), -F.log(u) * (F.lit(28.0) + F.pmod(F.col("h"), F.lit(600)))))
        .withColumn("season_factor", F.when(F.month(event_date).isin(11, 12), 1.35).when(F.month(event_date).isin(6, 7), 1.12).otherwise(1.0))
        .withColumn("amount_local", F.when(~F.col("is_missing_amount"), F.round(F.greatest(F.lit(0.50), F.col("base_amount") * F.col("season_factor")), 2)))
        .withColumn("expected_currency", F.lit(country["currency"]))
        .withColumn("currency_code", F.when(F.col("is_currency_mismatch"), F.when(F.lit(country["currency"]) == "USD", "BRL").otherwise("USD")).otherwise(F.col("expected_currency")))
        .withColumn("amount_usd", F.round(F.col("amount_local") / F.element_at(currency_rate_expr, F.col("country_code")), 2))
        .withColumn("event_date", event_date)
        .withColumn("event_ts_local", F.to_timestamp(F.format_string("%s %02d:%02d:%02d", F.date_format("event_date", "yyyy-MM-dd"), hour, minute, second)))
        .withColumn("event_timezone", F.lit(country["timezone"]))
        .withColumn("event_ts_utc", F.expr("to_utc_timestamp(event_ts_local, event_timezone)"))
        .withColumn("authorized_ts", F.col("event_ts_utc") + F.expr("INTERVAL 2 SECONDS"))
        .withColumn("settled_ts", F.when(F.col("event_status") == "completed", F.col("event_ts_utc") + F.expr("INTERVAL 1 DAYS")))
        .withColumn("is_late_arrival", F.pmod(F.col("local_id"), F.lit(50)) == 13)
        .withColumn("delay_hours", F.when(F.col("is_late_arrival"), 1 + F.pmod(F.col("physical_h"), F.lit(72))).otherwise(F.lit(0)))
        .withColumn("ingested_at", F.to_timestamp(F.from_unixtime(F.unix_timestamp("event_ts_utc") + F.when(F.col("is_late_arrival"), F.col("delay_hours") * 3600).otherwise(60 + F.pmod(F.col("physical_h"), F.lit(240))))))
        .withColumn("is_cross_border", F.col("event_type").isin("cross_border_transfer", "fx_conversion") | (F.pmod(F.col("h"), F.lit(100)) < 5))
        .withColumn("counterparty_country_code", F.when(F.col("is_cross_border"), F.element_at(country_array("code"), (F.pmod(F.col("country_index") + 1 + F.pmod(F.col("h"), F.lit(11)), F.lit(12)) + 1).cast("int"))))
        .withColumn("risk_score", F.round(F.least(F.lit(0.999), F.pmod(F.col("h"), F.lit(8500)) / 10000.0 + F.when(F.col("is_cross_border"), 0.08).otherwise(0.0) + F.when(F.col("amount_usd") > 10_000, 0.07).otherwise(0.0)), 4))
        .withColumn("fraud_label", (F.col("risk_score") > 0.92) & (F.pmod(F.col("h"), F.lit(5)) == 0))
        .withColumn("description", F.when(F.col("country_code") == "BR", F.concat(F.lit("Evento financeiro: "), F.regexp_replace("event_type", "_", " "))).otherwise(F.concat(F.lit("Evento financiero: "), F.regexp_replace("event_type", "_", " "))))
        .withColumn("source_system", F.concat(F.lit("QUILLA_EVENTS_"), F.col("country_code")))
        .withColumn("schema_version", F.when(F.pmod(F.col("local_id"), F.lit(1000)) == 333, "1.1-beta").otherwise("1.0"))
        .withColumn("payload_variant", F.when(F.pmod(F.col("local_id"), F.lit(400)) == 77, "STATUS_UPPERCASE").when(F.pmod(F.col("local_id"), F.lit(400)) == 177, "STATUS_WHITESPACE").otherwise("CANONICAL"))
        .withColumn("event_status", F.when(F.col("payload_variant") == "STATUS_UPPERCASE", F.upper("event_status")).when(F.col("payload_variant") == "STATUS_WHITESPACE", F.concat(F.lit(" "), F.col("event_status"), F.lit(" "))).otherwise(F.col("event_status")))
        .withColumn("event_month", F.date_format("event_date", "yyyy-MM"))
        .withColumn("synthetic_flag", F.lit(True))
    )
    event_frames.append(df)
    global_offset += count

events_work = reduce(lambda left, right: left.unionByName(right), event_frames)

# COMMAND ----------

truth_common = ["source_record_id", "event_id", "country_code", "local_id", "delay_hours", "expected_currency", "currency_code", "amount_local", "event_status", "schema_version"]
truth_source = events_work.select(*truth_common, "is_duplicate_key", "is_late_arrival", "is_missing_amount", "is_currency_mismatch", "payload_variant")


def truth_rows(predicate, issue_code, column_name, expected, observed, severity):
    return (
        truth_source.filter(predicate)
        .withColumn("dataset_name", F.lit("financial_events"))
        .withColumn("issue_code", F.lit(issue_code))
        .withColumn("column_name", F.lit(column_name))
        .withColumn("expected_value", expected)
        .withColumn("observed_value", observed)
        .withColumn("severity", F.lit(severity))
        .withColumn("is_intentional", F.lit(True))
        .withColumn("seed", F.lit(cfg["seed"]))
        .withColumn("created_at", utc_now_literal())
        .withColumn("truth_id", F.sha2(F.concat_ws("|", "source_record_id", "issue_code"), 256))
        .withColumnRenamed("event_id", "business_key")
        .select("truth_id", "dataset_name", "source_record_id", "business_key", "country_code", "issue_code", "column_name", "expected_value", "observed_value", "severity", "is_intentional", "seed", "created_at")
    )


truth_frames = [
    truth_rows(F.col("is_duplicate_key"), "DUPLICATE_BUSINESS_KEY", "event_id", F.lit("unique event_id"), F.col("event_id"), "high"),
    truth_rows(F.col("is_late_arrival"), "LATE_ARRIVAL", "ingested_at", F.lit("latency <= 300 seconds"), F.concat(F.col("delay_hours").cast("string"), F.lit(" hours")), "medium"),
    truth_rows(F.col("is_missing_amount"), "MISSING_REQUIRED_VALUE", "amount_local", F.lit("non-null decimal"), F.lit("NULL"), "high"),
    truth_rows(F.col("is_currency_mismatch"), "COUNTRY_CURRENCY_MISMATCH", "currency_code", F.col("expected_currency"), F.col("currency_code"), "high"),
    truth_rows(F.col("payload_variant") == "STATUS_UPPERCASE", "INCONSISTENT_CASE", "event_status", F.lower("event_status"), F.col("event_status"), "low"),
    truth_rows(F.col("payload_variant") == "STATUS_WHITESPACE", "EXTRA_WHITESPACE", "event_status", F.trim("event_status"), F.col("event_status"), "low"),
    truth_rows(F.col("schema_version") == "1.1-beta", "SCHEMA_VARIANT", "schema_version", F.lit("1.0"), F.col("schema_version"), "medium"),
]
quality_truth_df = reduce(lambda left, right: left.unionByName(right), truth_frames)

financial_events_df = events_work.select(
    "source_record_id", "event_id", "country_code", "party_id", "account_id", "card_id",
    "merchant_id", "device_id", "product_id", "event_type", "event_status", "channel",
    "direction", "amount_local", "currency_code", "amount_usd", "event_ts_utc",
    "event_timezone", "authorized_ts", "settled_ts", "ingested_at", "is_cross_border",
    "counterparty_country_code", "risk_score", "fraud_label", "description", "source_system",
    "schema_version", "payload_variant", "event_month", "synthetic_flag",
)

write_parquet(financial_events_df, cfg, "financial_events", ["country_code", "event_month"])
write_parquet(quality_truth_df, cfg, "quality_truth", ["issue_code"])
