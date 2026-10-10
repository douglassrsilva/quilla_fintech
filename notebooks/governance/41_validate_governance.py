# Databricks notebook source
"""Validate governed tags, classifier specifications and DQ monitoring assets."""

import json
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import types as T


for name, default in {
    "catalog": "example_catalog",
    "silver_schema": "example_silver",
    "governance_schema": "example_governance",
    "monitoring_schema": "example_monitoring",
}.items():
    dbutils.widgets.text(name, default)

CATALOG = dbutils.widgets.get("catalog")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOVERNANCE_SCHEMA = dbutils.widgets.get("governance_schema")
MONITORING_SCHEMA = dbutils.widgets.get("monitoring_schema")

samples_table = f"{CATALOG}.{GOVERNANCE_SCHEMA}.classifier_sample_values"
specs_table = f"{CATALOG}.{GOVERNANCE_SCHEMA}.custom_classifier_specs"
financial_events_table = f"{CATALOG}.{SILVER_SCHEMA}.financial_events"

sample_stats = spark.sql(
    f"""
    SELECT COUNT(*) AS rows, COUNT(DISTINCT country_code) AS countries,
           SUM(CASE WHEN synthetic_flag THEN 0 ELSE 1 END) AS non_synthetic
    FROM {samples_table}
    """
).first()
spec_stats = spark.sql(
    f"""
    SELECT COUNT(*) AS rows, COUNT(DISTINCT governed_tag) AS tags,
           COUNT(DISTINCT CASE WHEN size(countries) = 1 THEN countries[0] END) AS country_specific
    FROM {specs_table}
    """
).first()

assert sample_stats.rows == 12, f"Expected 12 country samples, found {sample_stats.rows}"
assert sample_stats.countries == 12, f"Expected 12 countries, found {sample_stats.countries}"
assert sample_stats.non_synthetic == 0, "Classifier samples must be explicitly synthetic"
assert spec_stats.rows == 23, f"Expected 23 classifier specifications, found {spec_stats.rows}"
assert spec_stats.tags == 3, f"Expected 3 governed tags, found {spec_stats.tags}"
assert spec_stats.country_specific == 12, "Every Quilla market needs a national identifier classifier"

for tag in (
    "quilla_national_identifier",
    "quilla_financial_identifier",
    "quilla_sensitive_context",
):
    spark.sql(f"DESCRIBE GOVERNED TAG {tag}").collect()

workspace = WorkspaceClient()
classification_config = workspace.data_classification.get_catalog_config(f"catalogs/{CATALOG}/config")
classified_schemas = set(classification_config.included_schemas.names)
required_schemas = {SILVER_SCHEMA, GOVERNANCE_SCHEMA}
assert required_schemas.issubset(classified_schemas), (
    f"Data Classification missing schemas: {required_schemas - classified_schemas}"
)

monitor = workspace.quality_monitors.get(financial_events_table)
monitor_configured = monitor.table_name == financial_events_table
assert monitor_configured, f"Data Quality Monitor missing for {financial_events_table}"
assert set(monitor.slicing_exprs or []) == {"country_code", "event_type", "event_status"}
assert len(monitor.custom_metrics or []) == 3

monitoring_schema = workspace.schemas.get(f"{CATALOG}.{MONITORING_SCHEMA}")
assert monitoring_schema.full_name == f"{CATALOG}.{MONITORING_SCHEMA}"

evidence_schema = T.StructType(
    [
        T.StructField("validated_at", T.TimestampType(), False),
        T.StructField("status", T.StringType(), False),
        T.StructField("catalog", T.StringType(), False),
        T.StructField("country_count", T.IntegerType(), False),
        T.StructField("classifier_spec_count", T.IntegerType(), False),
        T.StructField("governed_tag_count", T.IntegerType(), False),
        T.StructField("classification_schema_count", T.IntegerType(), False),
        T.StructField("quality_monitor_table", T.StringType(), False),
        T.StructField("custom_classifier_state", T.StringType(), False),
    ]
)
validated_at = datetime.now(timezone.utc).replace(tzinfo=None)
evidence = [
    (
        validated_at,
        "PASSED",
        CATALOG,
        12,
        int(spec_stats.rows),
        int(spec_stats.tags),
        len(classified_schemas),
        financial_events_table,
        "READY_FOR_CUSTOM_CLASSIFIER_UI",
    )
]
evidence_table = f"{CATALOG}.{GOVERNANCE_SCHEMA}.governance_validation_runs"
spark.createDataFrame(evidence, evidence_schema).write.mode("append").saveAsTable(evidence_table)

print(
    json.dumps(
        {
            "status": "PASSED",
            "countries": 12,
            "classifier_specs": int(spec_stats.rows),
            "governed_tags": int(spec_stats.tags),
            "classification_schemas": sorted(classified_schemas),
            "quality_monitor": financial_events_table,
            "monitor_custom_metrics": len(monitor.custom_metrics or []),
            "custom_classifier_execution": "UI_ONLY_BETA",
        },
        ensure_ascii=False,
    )
)
