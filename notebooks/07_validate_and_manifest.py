# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import types as T

cfg = load_config({
    "event_rows": 13_758_648,
    "party_rows": 144_000,
    "account_rows": 240_000,
    "card_rows": 160_000,
    "merchant_rows": 20_000,
    "device_rows": 150_000,
    "receivable_rows": 300_000,
    "credit_case_rows": 120_000,
    "insurance_case_rows": 60_000,
    "risk_alert_rows": 206_380,
    "consent_rows": 300_000,
    "interaction_rows": 100_000,
    "document_rows": 25_000,
})

expected = {
    "countries": 12,
    "products": 25,
    "fx_rates": 8_772,
    "parties": cfg["party_rows"],
    "accounts": cfg["account_rows"],
    "cards": cfg["card_rows"],
    "merchants": cfg["merchant_rows"],
    "devices": cfg["device_rows"],
    "financial_events": cfg["event_rows"],
    "receivables": cfg["receivable_rows"],
    "credit_cases": cfg["credit_case_rows"],
    "insurance_cases": cfg["insurance_case_rows"],
    "risk_alerts": cfg["risk_alert_rows"],
    "consents": cfg["consent_rows"],
    "interactions": cfg["interaction_rows"],
    "documents": cfg["document_rows"],
    "quality_truth": None,
}

dataframes = {name: spark.read.parquet(dataset_path(cfg, name)) for name in expected}
actual = {name: df.count() for name, df in dataframes.items()}

for name, expected_rows in expected.items():
    if expected_rows is None:
        assert_expected(actual[name] > 0, f"{name} must contain intentional quality truth rows")
    else:
        assert_expected(actual[name] == expected_rows, f"{name}: expected {expected_rows}, found {actual[name]}")

# COMMAND ----------

events = dataframes["financial_events"]
# The approved 24-month window is defined in each market's local calendar. UTC can
# legitimately cross into the following day for late-evening Latin American events.
event_bounds = events.agg(
    F.min(F.expr("to_date(from_utc_timestamp(event_ts_utc, event_timezone))")).alias("min_date"),
    F.max(F.expr("to_date(from_utc_timestamp(event_ts_utc, event_timezone))")).alias("max_date"),
).first()
assert_expected(event_bounds.min_date >= date.fromisoformat(EVENT_START), "financial_events starts before approved local-date window")
assert_expected(event_bounds.max_date <= date.fromisoformat(EVENT_END), "financial_events ends after approved local-date window")

actual_country_counts = {row["country_code"]: row["count"] for row in events.groupBy("country_code").count().collect()}
assert_expected(actual_country_counts == event_counts(cfg["event_rows"]), f"Unexpected event country allocation: {actual_country_counts}")

invalid_language_rows = (
    dataframes["parties"]
    .filter(((F.col("country_code") == "BR") & (F.col("preferred_language") != "pt")) | ((F.col("country_code") != "BR") & (F.col("preferred_language") != "es")))
    .count()
)
assert_expected(invalid_language_rows == 0, f"Found {invalid_language_rows} party locale violations")

official_documents = dataframes["documents"].filter(F.col("is_official") | ~F.col("synthetic_flag")).count()
assert_expected(official_documents == 0, "Synthetic documents must never be marked official")

# Validate bounded distinct FKs to keep the full validation efficient on serverless.
party_keys = dataframes["parties"].select("party_id")
account_keys = dataframes["accounts"].select("account_id")
event_keys = events.select("event_id")

orphan_account_parties = dataframes["accounts"].select("party_id").distinct().join(party_keys, "party_id", "left_anti").count()
orphan_event_accounts = events.select("account_id").distinct().join(account_keys, "account_id", "left_anti").count()
orphan_alert_events = dataframes["risk_alerts"].select("event_id").distinct().join(event_keys, "event_id", "left_anti").count()
assert_expected(orphan_account_parties == 0, f"Found {orphan_account_parties} account→party orphans")
assert_expected(orphan_event_accounts == 0, f"Found {orphan_event_accounts} event→account orphans")
assert_expected(orphan_alert_events == 0, f"Found {orphan_alert_events} alert→event orphans")

quality_counts = {row["issue_code"]: row["count"] for row in dataframes["quality_truth"].groupBy("issue_code").count().collect()}
required_issues = {
    "DUPLICATE_BUSINESS_KEY", "LATE_ARRIVAL", "MISSING_REQUIRED_VALUE",
    "COUNTRY_CURRENCY_MISMATCH", "INCONSISTENT_CASE", "EXTRA_WHITESPACE", "SCHEMA_VARIANT",
}
assert_expected(required_issues.issubset(quality_counts), f"Missing quality truth classes: {required_issues - set(quality_counts)}")

# COMMAND ----------

manifest_schema = T.StructType([
    T.StructField("run_id", T.StringType(), False),
    T.StructField("snapshot", T.StringType(), False),
    T.StructField("dataset_name", T.StringType(), False),
    T.StructField("volume_path", T.StringType(), False),
    T.StructField("format", T.StringType(), False),
    T.StructField("expected_rows", T.LongType(), True),
    T.StructField("actual_rows", T.LongType(), False),
    T.StructField("validation_status", T.StringType(), False),
    T.StructField("min_business_date", T.StringType(), True),
    T.StructField("max_business_date", T.StringType(), True),
    T.StructField("schema_json", T.StringType(), False),
    T.StructField("seed", T.IntegerType(), False),
    T.StructField("generated_at", T.TimestampType(), False),
])

run_id = f"quilla-{cfg['snapshot']}-seed-{cfg['seed']}"
generated_at = datetime(2026, 8, 31, 23, 59, 59)
manifest_rows = []
for name, df in dataframes.items():
    min_date = event_bounds.min_date.isoformat() if name == "financial_events" else None
    max_date = event_bounds.max_date.isoformat() if name == "financial_events" else None
    manifest_rows.append((
        run_id,
        cfg["snapshot"],
        name,
        dataset_path(cfg, name),
        "parquet",
        expected[name],
        actual[name],
        "PASSED",
        min_date,
        max_date,
        df.schema.json(),
        cfg["seed"],
        generated_at,
    ))

manifest_df = spark.createDataFrame(manifest_rows, manifest_schema)
write_parquet(manifest_df, cfg, "generation_manifest")

summary = {
    "run_id": run_id,
    "snapshot": cfg["snapshot"],
    "validation_status": "PASSED",
    "datasets": actual,
    "financial_events_by_country": actual_country_counts,
    "quality_truth_by_issue": quality_counts,
    "event_min_local_date": event_bounds.min_date.isoformat(),
    "event_max_local_date": event_bounds.max_date.isoformat(),
    "orphan_counts": {
        "account_party": orphan_account_parties,
        "event_account": orphan_event_accounts,
        "alert_event": orphan_alert_events,
    },
}
print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
