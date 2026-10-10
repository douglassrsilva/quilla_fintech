# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

cfg = config()

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {cfg['catalog']}.{cfg['ml_schema']}
    COMMENT 'Quilla ML — feature tables, training datasets, experiment evidence and predictions.'
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {ml_table(cfg, 'model_training_results')} (
      snapshot STRING,
      model_key STRING,
      model_kind STRING,
      mlflow_run_id STRING,
      model_version STRING,
      trace_id STRING,
      training_rows BIGINT,
      metrics_json STRING,
      recorded_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES (
      'quilla.synthetic' = 'true',
      'quilla.layer' = 'ml',
      'quilla.asset.type' = 'model_evidence'
    )
    """
)
spark.sql(
    f"""
    CREATE VOLUME IF NOT EXISTS {cfg['catalog']}.{cfg['raw_schema']}.{cfg['volume']}
    COMMENT 'Immutable synthetic outcomes and macro scenarios for the Quilla ML demo.'
    """
)

dbutils.fs.mkdirs(cfg["root"])

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {ml_table(cfg, 'ml_run_manifest')} (
      run_id STRING,
      phase STRING,
      asset_name STRING,
      status STRING,
      row_count BIGINT,
      details STRING,
      recorded_at TIMESTAMP
    ) USING DELTA
    TBLPROPERTIES ('quilla.synthetic' = 'true', 'quilla.layer' = 'ml')
    """
)

print(json.dumps({"status": "ready", "root": cfg["root"], "ml_schema": cfg["ml_schema"]}))
