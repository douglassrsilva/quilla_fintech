# Databricks notebook source
"""Validate the rich, localized and intentionally varied human-review trace battery."""

# COMMAND ----------

# MAGIC %run ./_genai_governance_common

# COMMAND ----------

# MAGIC %run ./_offline_eval_common

# COMMAND ----------

import json
import re
from datetime import datetime, timezone

import mlflow
from pyspark.sql import types as T

dbutils.widgets.text("review_release", "2026-09-01-rich-v1")
review_release = dbutils.widgets.get("review_release")
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,63}", review_release):
    raise ValueError(f"Invalid review release: {review_release!r}")
cfg = genai_governance_config()
experiment = set_genai_experiment(cfg)
manifest_table = ai_governance_table(cfg, "genai_review_trace_manifest")

stats = spark.sql(
    f"""
    SELECT COUNT(DISTINCT scenario_key) AS scenarios,
           COUNT(DISTINCT trace_id) AS traces,
           COUNT(DISTINCT country_code) AS countries,
           COUNT(DISTINCT language) AS locales,
           COUNT(DISTINCT use_case) AS use_cases,
           COUNT(DISTINCT scenario_variant) AS variants,
           COUNT(DISTINCT review_queue) AS review_queues,
           SUM(CASE WHEN scenario_variant = 'approved' THEN 1 ELSE 0 END) AS approved,
           SUM(CASE WHEN scenario_variant = 'human_review' THEN 1 ELSE 0 END) AS human_review,
           SUM(CASE WHEN scenario_variant = 'control_failure' THEN 1 ELSE 0 END) AS control_failure,
           SUM(CASE WHEN expected_failed_scorers <> 'none' THEN 1 ELSE 0 END) AS expected_failures,
           SUM(CASE WHEN synthetic_flag THEN 0 ELSE 1 END) AS non_synthetic
    FROM {manifest_table}
    WHERE review_release = '{review_release}'
    """
).first()

assert stats.scenarios == 216, stats
assert stats.traces == 216, stats
assert stats.countries == 12, stats
assert stats.locales == 12, stats
assert stats.use_cases == 6, stats
assert stats.variants == 3, stats
assert stats.review_queues == 6, stats
assert stats.approved == 72, stats
assert stats.human_review == 72, stats
assert stats.control_failure == 72, stats
assert stats.expected_failures == 72, stats
assert stats.non_synthetic == 0, stats

# Validate the canonical manifest against its governed UC trace location. The generic
# search API can lag the underlying Delta table and produce false negatives for fresh
# traces, while this join proves that every active scenario has durable trace metadata.
trace_metadata_table = (
    f"{cfg['catalog']}.{cfg['ai_schema']}.{cfg['trace_table_prefix']}_trace_metadata"
)
durable_trace_count = spark.sql(
    f"""
    SELECT COUNT(DISTINCT manifest.trace_id) AS traces
    FROM {manifest_table} manifest
    INNER JOIN {trace_metadata_table} metadata
      ON metadata.trace_id = regexp_extract(manifest.trace_id, '([^/]+)$', 1)
    WHERE manifest.review_release = '{review_release}'
      AND metadata.tags['battery_release'] = '{review_release}'
    """
).first().traces
assert durable_trace_count == 216, (
    f"Expected 216 canonical traces in UC, found {durable_trace_count}"
)

validated_at = datetime.now(timezone.utc).replace(tzinfo=None)
schema = T.StructType([
    T.StructField("validated_at", T.TimestampType(), False),
    T.StructField("review_release", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("experiment_id", T.StringType(), False),
    T.StructField("trace_count", T.IntegerType(), False),
    T.StructField("country_count", T.IntegerType(), False),
    T.StructField("locale_count", T.IntegerType(), False),
    T.StructField("use_case_count", T.IntegerType(), False),
    T.StructField("variant_count", T.IntegerType(), False),
    T.StructField("review_queue_count", T.IntegerType(), False),
    T.StructField("expected_failure_count", T.IntegerType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
validation_table = ai_governance_table(cfg, "genai_review_battery_validation_runs")
row = [(
    validated_at,
    review_release,
    "PASSED",
    experiment.experiment_id,
    int(stats.traces),
    int(stats.countries),
    int(stats.locales),
    int(stats.use_cases),
    int(stats.variants),
    int(stats.review_queues),
    int(stats.expected_failures),
    True,
)]
spark.createDataFrame(row, schema).write.mode("append").saveAsTable(validation_table)
print(json.dumps({
    "status": "PASSED",
    "review_release": review_release,
    "experiment_id": experiment.experiment_id,
    "traces": int(stats.traces),
    "countries": int(stats.countries),
    "locales": int(stats.locales),
    "use_cases": int(stats.use_cases),
    "variants": int(stats.variants),
    "review_queues": int(stats.review_queues),
    "expected_failures": int(stats.expected_failures),
    "ui_filter": "Tag / battery_release / = / " + review_release,
}, ensure_ascii=False))
