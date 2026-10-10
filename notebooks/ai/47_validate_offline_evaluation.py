# Databricks notebook source
"""Validate golden datasets, MLflow evaluation runs and promotion gates."""

# COMMAND ----------

# MAGIC %run ./_genai_governance_common

# COMMAND ----------

# MAGIC %run ./_offline_eval_common

# COMMAND ----------

import json
from datetime import datetime, timezone

import mlflow
from pyspark.sql import functions as F, types as T

cfg = genai_governance_config()
experiment = set_genai_experiment(cfg)
full_dataset = mlflow.genai.get_dataset(
    name=ai_governance_table(cfg, "quilla_genai_eval_gold")
)
semantic_dataset = mlflow.genai.get_dataset(
    name=ai_governance_table(cfg, "quilla_genai_eval_semantic")
)
full_record_count = len(full_dataset.to_df())
semantic_record_count = len(semantic_dataset.to_df())
assert full_record_count >= 72, full_record_count
assert semantic_record_count >= cfg["semantic_rows"], semantic_record_count

runs_table = ai_governance_table(cfg, "genai_offline_evaluation_runs")
latest = spark.sql(
    f"""
    WITH ranked AS (
      SELECT *, ROW_NUMBER() OVER (
        PARTITION BY variant, evaluation_type ORDER BY evaluated_at DESC
      ) AS rn
      FROM {runs_table}
      WHERE evaluation_release = '{cfg['evaluation_release']}'
    )
    SELECT variant, evaluation_type, run_id, record_count, metrics_json, passed
    FROM ranked WHERE rn = 1
    """
).collect()
index = {(row.variant, row.evaluation_type): row for row in latest}
required_runs = {
    ("baseline", "DETERMINISTIC_FULL"),
    ("candidate", "DETERMINISTIC_FULL"),
    ("candidate", "SEMANTIC_SAMPLE"),
}
assert required_runs.issubset(index), f"Missing evaluation runs: {required_runs - set(index)}"

baseline_metrics = json.loads(index[("baseline", "DETERMINISTIC_FULL")].metrics_json)
candidate_metrics = json.loads(index[("candidate", "DETERMINISTIC_FULL")].metrics_json)
critical = [
    "offline_language_policy/mean",
    "offline_country_scope/mean",
    "offline_financial_disclaimer/mean",
    "offline_citation_policy/mean",
    "offline_fact_coverage/mean",
    "offline_sensitive_data_guardrail/mean",
]
assert all(candidate_metrics[key] >= 0.95 for key in critical)
improved = [key for key in critical if candidate_metrics[key] > baseline_metrics[key]]
assert len(improved) >= 4, f"Candidate must improve at least four controls: {improved}"
semantic_passed = bool(index[("candidate", "SEMANTIC_SAMPLE")].passed)
promotion_decision = "APPROVED" if semantic_passed else "CONDITIONAL_REVIEW"

validated_at = datetime.now(timezone.utc).replace(tzinfo=None)
schema = T.StructType([
    T.StructField("validated_at", T.TimestampType(), False),
    T.StructField("evaluation_release", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("experiment_id", T.StringType(), False),
    T.StructField("baseline_run_id", T.StringType(), False),
    T.StructField("candidate_run_id", T.StringType(), False),
    T.StructField("semantic_run_id", T.StringType(), False),
    T.StructField("golden_record_count", T.IntegerType(), False),
    T.StructField("semantic_record_count", T.IntegerType(), False),
    T.StructField("improved_control_count", T.IntegerType(), False),
    T.StructField("promotion_decision", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
validation_table = ai_governance_table(cfg, "genai_offline_evaluation_validation_runs")
row = [(
    validated_at,
    cfg["evaluation_release"],
    "PASSED",
    experiment.experiment_id,
    index[("baseline", "DETERMINISTIC_FULL")].run_id,
    index[("candidate", "DETERMINISTIC_FULL")].run_id,
    index[("candidate", "SEMANTIC_SAMPLE")].run_id,
    full_record_count,
    semantic_record_count,
    len(improved),
    promotion_decision,
    True,
)]
spark.createDataFrame(row, schema).write.mode("append").saveAsTable(validation_table)
print(json.dumps({"status": "PASSED", "evaluation_release": cfg["evaluation_release"], "experiment_id": experiment.experiment_id, "baseline_run_id": row[0][4], "candidate_run_id": row[0][5], "semantic_run_id": row[0][6], "golden_records": full_record_count, "semantic_records": semantic_record_count, "improved_controls": improved, "promotion_decision": promotion_decision}, ensure_ascii=False))
