# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Golden datasets de avaliação GenAI
# MAGIC
# MAGIC Cria datasets versionados no Unity Catalog e associados ao experimento MLflow.

# COMMAND ----------

# MAGIC %run ./_genai_governance_common

# COMMAND ----------

# MAGIC %run ./_offline_eval_common

# COMMAND ----------

import json
from datetime import datetime, timezone

import mlflow
from pyspark.sql import types as T

cfg = genai_governance_config()
experiment = set_genai_experiment(cfg)
all_records = golden_records(cfg["evaluation_release"])
sample_records = semantic_sample(all_records, cfg["semantic_rows"])


def upsert_dataset(name, records):
    try:
        dataset = mlflow.genai.get_dataset(name=name)
    except Exception:
        dataset = mlflow.genai.create_dataset(
            name=name, experiment_id=experiment.experiment_id
        )
    dataset.merge_records(records)
    return dataset


full_name = ai_governance_table(cfg, "quilla_genai_eval_gold")
semantic_name = ai_governance_table(cfg, "quilla_genai_eval_semantic")
full_dataset = upsert_dataset(full_name, all_records)
semantic_dataset = upsert_dataset(semantic_name, sample_records)

# Precreate the shared append target before the three evaluation tasks fan out.
# This avoids a first-run CTAS race while preserving parallel evaluation execution.
evaluation_runs_table = ai_governance_table(cfg, "genai_offline_evaluation_runs")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {evaluation_runs_table} (
      evaluated_at TIMESTAMP NOT NULL,
      evaluation_release STRING NOT NULL,
      experiment_id STRING NOT NULL,
      run_id STRING NOT NULL,
      variant STRING NOT NULL,
      evaluation_type STRING NOT NULL,
      record_count INT NOT NULL,
      metrics_json STRING NOT NULL,
      passed BOOLEAN NOT NULL,
      synthetic_flag BOOLEAN NOT NULL
    ) USING DELTA
    TBLPROPERTIES (
      'project' = 'quilla',
      'managed_by' = 'dabs',
      'data_classification' = 'synthetic'
    )
    """
)

created_at = datetime.now(timezone.utc).replace(tzinfo=None)
manifest_schema = T.StructType([
    T.StructField("created_at", T.TimestampType(), False),
    T.StructField("evaluation_release", T.StringType(), False),
    T.StructField("dataset_name", T.StringType(), False),
    T.StructField("dataset_id", T.StringType(), False),
    T.StructField("record_count", T.IntegerType(), False),
    T.StructField("country_count", T.IntegerType(), False),
    T.StructField("use_case_count", T.IntegerType(), False),
    T.StructField("languages", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
rows = [
    (created_at, cfg["evaluation_release"], full_name, str(full_dataset.dataset_id), len(all_records), 12, 6, "pt-BR,es-LATAM", True),
    (created_at, cfg["evaluation_release"], semantic_name, str(semantic_dataset.dataset_id), len(sample_records), len({r["inputs"]["country_code"] for r in sample_records}), len({r["inputs"]["use_case"] for r in sample_records}), "pt-BR,es-LATAM", True),
]
manifest = ai_governance_table(cfg, "genai_evaluation_dataset_manifest")
spark.createDataFrame(rows, manifest_schema).write.mode("append").saveAsTable(manifest)

assert len(all_records) == 72
assert len({r["inputs"]["country_code"] for r in all_records}) == 12
assert len({r["inputs"]["use_case"] for r in all_records}) == 6
assert len(sample_records) == cfg["semantic_rows"]
print(json.dumps({"status": "PASSED", "release": cfg["evaluation_release"], "golden_dataset": full_name, "golden_records": len(all_records), "semantic_dataset": semantic_name, "semantic_records": len(sample_records)}, ensure_ascii=False))
