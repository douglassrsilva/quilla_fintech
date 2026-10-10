# Databricks notebook source
"""Validação end-to-end e auditável da execução realtime Quilla."""

import re

from pyspark.sql import functions as F


# COMMAND ----------

dbutils.widgets.text("catalog", "example_catalog")
dbutils.widgets.text("bronze_schema", "example_bronze")
dbutils.widgets.text("silver_schema", "example_silver")
dbutils.widgets.text("gold_schema", "example_gold")
dbutils.widgets.text("simulation_id", "")
dbutils.widgets.text("expected_events", "1000")
dbutils.widgets.text("scenario", "mixed")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SILVER_SCHEMA = dbutils.widgets.get("silver_schema")
GOLD_SCHEMA = dbutils.widgets.get("gold_schema")
SIMULATION_ID = dbutils.widgets.get("simulation_id")
EXPECTED_EVENTS = int(dbutils.widgets.get("expected_events"))
SCENARIO = dbutils.widgets.get("scenario")

for value, label in (
    (CATALOG, "catalog"),
    (BRONZE_SCHEMA, "bronze_schema"),
    (SILVER_SCHEMA, "silver_schema"),
    (GOLD_SCHEMA, "gold_schema"),
):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{label} inválido: {value!r}")
if not re.fullmatch(r"[A-Za-z0-9_.-]+", SIMULATION_ID):
    raise ValueError("simulation_id inválido")
if not 1 <= EXPECTED_EVENTS <= 100_000:
    raise ValueError("expected_events fora do intervalo permitido")


# COMMAND ----------

zerobus = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.zerobus_financial_events").filter(
    F.col("_producer_run_id") == SIMULATION_ID
)
bronze = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_financial_events").filter(
    F.col("_producer_run_id") == SIMULATION_ID
)
silver = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.financial_events").filter(
    F.col("_producer_run_id") == SIMULATION_ID
)
quarantine = spark.table(
    f"{CATALOG}.{SILVER_SCHEMA}.financial_events_quarantine"
).filter(F.col("_producer_run_id") == SIMULATION_ID)

zerobus_rows = zerobus.count()
zerobus_distinct = zerobus.select("event_id").distinct().count()
bronze_distinct = bronze.select("event_id").distinct().count()
silver_distinct = silver.select("event_id").distinct().count()
quarantine_distinct = quarantine.select("event_id").distinct().count()
reconciled_distinct = (
    silver.select("event_id")
    .unionByName(quarantine.select("event_id"))
    .distinct()
    .count()
)

assert zerobus_distinct == EXPECTED_EVENTS, (
    f"Zerobus distinto={zerobus_distinct}, esperado={EXPECTED_EVENTS}"
)
assert bronze_distinct == EXPECTED_EVENTS, (
    f"Bronze realtime distinto={bronze_distinct}, esperado={EXPECTED_EVENTS}"
)
assert reconciled_distinct == EXPECTED_EVENTS, (
    f"Silver+quarentena distinto={reconciled_distinct}, esperado={EXPECTED_EVENTS}"
)
if SCENARIO != "dq_violations":
    assert silver_distinct > 0, "Nenhum evento válido chegou à Silver"
if SCENARIO == "dq_violations":
    assert quarantine_distinct > 0, "Cenário de qualidade não gerou quarentena"


# COMMAND ----------

accepted_slices = (
    silver.withColumn(
        "event_date",
        F.to_date(
            F.from_utc_timestamp("event_ts_utc", F.col("event_timezone"))
        ),
    )
    .select("event_date", "country_code")
    .distinct()
)
gold_slices = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.mv_financial_performance_daily"
).select("event_date", "country_code").distinct()

missing_gold_slices = accepted_slices.join(
    gold_slices,
    ["event_date", "country_code"],
    "left_anti",
).count()
assert missing_gold_slices == 0, (
    f"Há {missing_gold_slices} combinações data/país da Silver ausentes na Gold"
)

dq_unreconciled = spark.table(
    f"{CATALOG}.{GOLD_SCHEMA}.mv_data_quality_overview"
).filter(~F.col("reconciled")).count()
assert dq_unreconciled == 0, f"Há {dq_unreconciled} datasets não reconciliados"

metric_views = {
    row.viewName
    for row in spark.sql(f"SHOW VIEWS IN {CATALOG}.{GOLD_SCHEMA}").collect()
}
expected_metric_views = {
    "metrics_financial_performance",
    "metrics_risk_operations",
    "metrics_credit_portfolio",
}
missing_metric_views = sorted(expected_metric_views - metric_views)
assert not missing_metric_views, f"Metric Views ausentes: {missing_metric_views}"


# COMMAND ----------

validation_table = f"{CATALOG}.{GOLD_SCHEMA}.realtime_demo_validation_runs"
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {validation_table} (
      simulation_id STRING,
      scenario STRING,
      expected_events BIGINT,
      zerobus_rows BIGINT,
      zerobus_distinct_events BIGINT,
      bronze_distinct_events BIGINT,
      silver_distinct_events BIGINT,
      quarantine_distinct_events BIGINT,
      reconciled_distinct_events BIGINT,
      status STRING,
      validated_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Evidências auditáveis das demonstrações realtime Quilla.'
    TBLPROPERTIES ('quilla.synthetic' = 'true', 'quality' = 'gold-observability')
    """
)

result = spark.createDataFrame(
    [
        (
            SIMULATION_ID,
            SCENARIO,
            EXPECTED_EVENTS,
            zerobus_rows,
            zerobus_distinct,
            bronze_distinct,
            silver_distinct,
            quarantine_distinct,
            reconciled_distinct,
            "PASSED",
        )
    ],
    """
    simulation_id string, scenario string, expected_events long,
    zerobus_rows long, zerobus_distinct_events long,
    bronze_distinct_events long, silver_distinct_events long,
    quarantine_distinct_events long, reconciled_distinct_events long,
    status string
    """,
).withColumn("validated_at", F.current_timestamp())
result.createOrReplaceTempView("_quilla_realtime_validation_result")

spark.sql(
    f"""
    MERGE INTO {validation_table} AS target
    USING _quilla_realtime_validation_result AS source
      ON target.simulation_id = source.simulation_id
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """
)

display(result)
