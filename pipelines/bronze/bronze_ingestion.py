"""Quilla Bronze: incremental Parquet ingestion with Auto Loader."""

from pyspark import pipelines as dp
from pyspark.sql import functions as F


CATALOG = spark.conf.get("quilla.catalog")
RAW_SCHEMA = spark.conf.get("quilla.raw_schema")
RAW_VOLUME = spark.conf.get("quilla.raw_volume")
SNAPSHOT = spark.conf.get("quilla.snapshot")
ZEROBUS_SOURCE_TABLE = spark.conf.get(
    "quilla.zerobus_source_table",
    f"{CATALOG}.quilla_bronze.zerobus_financial_events",
)

LANDING_ROOT = f"/Volumes/{CATALOG}/{RAW_SCHEMA}/{RAW_VOLUME}/snapshot={SNAPSHOT}"

DATASETS = (
    "countries",
    "products",
    "fx_rates",
    "parties",
    "accounts",
    "cards",
    "merchants",
    "devices",
    "financial_events",
    "receivables",
    "credit_cases",
    "insurance_cases",
    "risk_alerts",
    "consents",
    "interactions",
    "documents",
    "quality_truth",
)


def _historical_stream(dataset_name: str):
    source_path = f"{LANDING_ROOT}/{dataset_name}"

    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn(
            "_source_file_modification_time",
            F.col("_metadata.file_modification_time"),
        )
        .withColumn("_source_file_size", F.col("_metadata.file_size"))
        .withColumn("_snapshot", F.lit(SNAPSHOT))
        .withColumn("_bronze_ingested_at", F.current_timestamp())
    )


def _register_bronze_table(dataset_name: str) -> None:
    table_name = f"bronze_{dataset_name}"

    @dp.table(
        name=table_name,
        comment=(
            f"Copia Bronze incremental e fiel de {dataset_name}; fonte sintética PT/ES, "
            f"snapshot {SNAPSHOT}."
        ),
        table_properties={
            "quality": "bronze",
            "delta.enableChangeDataFeed": "true",
            "pipelines.autoOptimize.managed": "true",
            "quilla.synthetic": "true",
        },
    )
    def bronze_table():
        return _historical_stream(dataset_name)


def _register_financial_events_table() -> None:
    table_name = "bronze_financial_events"

    # Two named append flows preserve independent checkpoints for the file backfill
    # and Zerobus. This is the SDP-native pattern for a canonical multi-source
    # streaming table and avoids ambiguous source identities in a streaming UNION.
    dp.create_streaming_table(
        name=table_name,
        comment=(
            "Copia Bronze incremental e fiel de financial_events; backfill Parquet "
            "e eventos Zerobus PT/ES na mesma tabela canônica."
        ),
        table_properties={
            "quality": "bronze",
            "delta.enableChangeDataFeed": "true",
            "pipelines.autoOptimize.managed": "true",
            "quilla.synthetic": "true",
        },
    )

    @dp.append_flow(target=table_name, name="financial_events_historical_autoloader")
    def historical_financial_events():
        return _historical_stream("financial_events")

    @dp.append_flow(target=table_name, name="financial_events_realtime_zerobus")
    def realtime_financial_events():
        return (
            spark.readStream.table(ZEROBUS_SOURCE_TABLE)
            .withColumn(
                "_source_file",
                F.concat(F.lit("zerobus://"), F.col("_producer_run_id")),
            )
            .withColumn(
                "_source_file_modification_time",
                F.col("_producer_ingested_at"),
            )
            .withColumn("_source_file_size", F.lit(None).cast("long"))
            .withColumn("_snapshot", F.lit("realtime"))
            .withColumn("_bronze_ingested_at", F.current_timestamp())
            .withColumn("_rescued_data", F.lit(None).cast("string"))
        )

for _dataset_name in (name for name in DATASETS if name != "financial_events"):
    _register_bronze_table(_dataset_name)

_register_financial_events_table()
