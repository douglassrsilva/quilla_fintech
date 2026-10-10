# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

cfg = config({
    "credit_case_rows": 120_000,
    "party_rows": 144_000,
    "session_rows": 1_200_000,
    "risk_alert_rows": 206_380,
    "insurance_case_rows": 60_000,
    "pricing_rows": 720_000,
    "profile_rows": 72_000,
    "investment_event_rows": 1_200_000,
})

expected = {
    "macroeconomic_daily": 730 * 12,
    "credit_performance_monthly": cfg["credit_case_rows"] * 12,
    "customer_lifecycle_monthly": cfg["party_rows"] * 24,
    "security_sessions": cfg["session_rows"],
    "case_outcomes": cfg["risk_alert_rows"] + cfg["insurance_case_rows"],
    "pricing_experiments": cfg["pricing_rows"],
    "investment_products": 240,
    "investment_profiles": cfg["profile_rows"],
    "investment_events": cfg["investment_event_rows"],
    "market_daily": 60 * 730,
}

primary_keys = {
    "macroeconomic_daily": ["country_code", "observation_date"],
    "credit_performance_monthly": ["performance_id"],
    "customer_lifecycle_monthly": ["lifecycle_id"],
    "security_sessions": ["session_id"],
    "case_outcomes": ["outcome_id"],
    "pricing_experiments": ["experiment_id"],
    "investment_products": ["investment_product_id"],
    "investment_profiles": ["profile_id"],
    "investment_events": ["investment_event_id"],
    "market_daily": ["market_observation_id"],
}

manifest_rows = []
for dataset, expected_rows in expected.items():
    frame = read_landing(cfg, dataset)
    actual_rows = frame.count()
    if actual_rows != expected_rows:
        raise AssertionError(f"{dataset}: expected {expected_rows}, got {actual_rows}")
    duplicate_keys = frame.groupBy(*primary_keys[dataset]).count().filter(F.col("count") > 1).limit(1).count()
    if duplicate_keys:
        raise AssertionError(f"{dataset}: duplicated primary key")
    invalid_synthetic = frame.filter(~F.col("synthetic_flag") | F.col("synthetic_flag").isNull()).limit(1).count()
    if invalid_synthetic:
        raise AssertionError(f"{dataset}: synthetic guardrail failed")
    manifest_rows.append((dataset, actual_rows, "PASSED"))

binary_labels = {
    "credit_performance_monthly": "default_flag",
    "customer_lifecycle_monthly": "churned_90d",
    "security_sessions": "account_takeover_label",
    "case_outcomes": "confirmed_flag",
    "pricing_experiments": "accepted_flag",
    "investment_events": "accepted_recommendation",
}
for dataset, label in binary_labels.items():
    values = read_landing(cfg, dataset).select(label).distinct().count()
    if values < 2:
        raise AssertionError(f"{dataset}.{label}: label has fewer than two classes")

profile_orphans = (
    read_landing(cfg, "investment_events")
    .select("profile_id")
    .distinct()
    .join(read_landing(cfg, "investment_profiles").select("profile_id"), "profile_id", "left_anti")
    .limit(1)
    .count()
)
if profile_orphans:
    raise AssertionError("investment_events contains unknown profile_id")

manifest = (
    spark.createDataFrame(manifest_rows, "asset_name string, row_count long, status string")
    .withColumn("run_id", F.lit(f"ml-data-{cfg['snapshot']}"))
    .withColumn("phase", F.lit("landing_validation"))
    .withColumn("details", F.lit(json.dumps({"root": cfg["root"], "seed": cfg["seed"]})))
    .withColumn("recorded_at", F.current_timestamp())
    .select("run_id", "phase", "asset_name", "status", "row_count", "details", "recorded_at")
)
manifest.write.mode("append").saveAsTable(ml_table(cfg, "ml_run_manifest"))

try:
    dbutils.jobs.taskValues.set(key="ml_landing_validated", value="true")
except Exception:
    pass

print(json.dumps({"status": "PASSED", "datasets": len(expected), "rows": sum(expected.values())}))
