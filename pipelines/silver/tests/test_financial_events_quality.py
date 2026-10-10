"""Native Lakeflow Pipelines tests. Run only from the Lakeflow Pipelines Editor."""

from datetime import datetime

import pytest
from pyspark import pipelines as dp  # noqa: F401 - activates pipeline APIs in the editor
from pyspark.pipelines.testing import TestPipeline, test_spark
from pyspark.sql import types as T


test_pipeline = TestPipeline.active()

CATALOG = "example_catalog"
BRONZE_INPUT = f"{CATALOG}.quilla_bronze.bronze_financial_events"
SILVER_OUTPUT = f"{CATALOG}.quilla_silver.financial_events"
QUARANTINE_OUTPUT = f"{CATALOG}.quilla_silver.financial_events_quarantine"


EVENT_SCHEMA = T.StructType(
    [
        T.StructField("source_record_id", T.StringType(), False),
        T.StructField("event_id", T.StringType(), False),
        T.StructField("country_code", T.StringType(), False),
        T.StructField("party_id", T.StringType(), False),
        T.StructField("account_id", T.StringType(), False),
        T.StructField("card_id", T.StringType(), True),
        T.StructField("merchant_id", T.StringType(), True),
        T.StructField("device_id", T.StringType(), True),
        T.StructField("product_id", T.LongType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("event_status", T.StringType(), False),
        T.StructField("channel", T.StringType(), False),
        T.StructField("direction", T.StringType(), False),
        T.StructField("amount_local", T.DoubleType(), True),
        T.StructField("currency_code", T.StringType(), False),
        T.StructField("amount_usd", T.DoubleType(), True),
        T.StructField("event_ts_utc", T.TimestampType(), False),
        T.StructField("event_timezone", T.StringType(), False),
        T.StructField("authorized_ts", T.TimestampType(), False),
        T.StructField("settled_ts", T.TimestampType(), True),
        T.StructField("ingested_at", T.TimestampType(), False),
        T.StructField("is_cross_border", T.BooleanType(), True),
        T.StructField("counterparty_country_code", T.StringType(), True),
        T.StructField("risk_score", T.DoubleType(), False),
        T.StructField("fraud_label", T.BooleanType(), True),
        T.StructField("description", T.StringType(), True),
        T.StructField("source_system", T.StringType(), False),
        T.StructField("schema_version", T.StringType(), False),
        T.StructField("payload_variant", T.StringType(), False),
        T.StructField("event_month", T.StringType(), False),
        T.StructField("synthetic_flag", T.BooleanType(), False),
        T.StructField("_rescued_data", T.StringType(), True),
        T.StructField("_source_file", T.StringType(), False),
        T.StructField("_source_file_modification_time", T.TimestampType(), False),
        T.StructField("_source_file_size", T.LongType(), False),
        T.StructField("_snapshot", T.StringType(), False),
        T.StructField("_bronze_ingested_at", T.TimestampType(), False),
    ]
)


def event_row(**overrides):
    event_ts = datetime(2026, 8, 31, 12, 0, 0)
    base = {
        "source_record_id": "QSRC-TEST-001",
        "event_id": "QEVT-BR-TEST-001",
        "country_code": "BR",
        "party_id": "QPTY-BR-TEST-001",
        "account_id": "QACC-BR-TEST-001",
        "card_id": None,
        "merchant_id": None,
        "device_id": "QDEV-BR-TEST-001",
        "product_id": 1,
        "event_type": "p2p_transfer",
        "event_status": "completed",
        "channel": "mobile",
        "direction": "debit",
        "amount_local": 100.0,
        "currency_code": "BRL",
        "amount_usd": 18.52,
        "event_ts_utc": event_ts,
        "event_timezone": "America/Sao_Paulo",
        "authorized_ts": datetime(2026, 8, 31, 12, 0, 2),
        "settled_ts": datetime(2026, 9, 1, 12, 0, 0),
        "ingested_at": datetime(2026, 8, 31, 12, 1, 0),
        "is_cross_border": False,
        "counterparty_country_code": None,
        "risk_score": 0.12,
        "fraud_label": False,
        "description": "Evento financeiro de teste",
        "source_system": "QUILLA_TEST",
        "schema_version": "1.0",
        "payload_variant": "CANONICAL",
        "event_month": "2026-08",
        "synthetic_flag": True,
        "_rescued_data": None,
        "_source_file": "mock://events/001",
        "_source_file_modification_time": datetime(2026, 8, 31, 12, 2, 0),
        "_source_file_size": 1024,
        "_snapshot": "unit-test",
        "_bronze_ingested_at": datetime(2026, 8, 31, 12, 2, 1),
    }
    base.update(overrides)
    return base


def mock_bronze(session, rows):
    session.createDataFrame(rows, EVENT_SCHEMA).write.mode("overwrite").saveAsTable(BRONZE_INPUT)


def run_financial_events(session):
    test_pipeline.run(session, {SILVER_OUTPUT, QUARANTINE_OUTPUT})


def test_valid_event_is_accepted(test_spark):
    mock_bronze(test_spark, [event_row()])
    run_financial_events(test_spark)

    assert test_spark.table(SILVER_OUTPUT).count() == 1
    assert test_spark.table(QUARANTINE_OUTPUT).count() == 0


def test_contract_violation_is_quarantined(test_spark):
    mock_bronze(
        test_spark,
        [event_row(event_id="QEVT-BR-TEST-INVALID", currency_code="USD")],
    )
    run_financial_events(test_spark)

    assert test_spark.table(SILVER_OUTPUT).count() == 0
    rejected = test_spark.table(QUARANTINE_OUTPUT).first()
    assert rejected._quarantine_reason == "CONTRACT_VIOLATION"
    assert len(rejected._dq_errors) >= 1


def test_deduplication_keeps_latest_clean_record(test_spark):
    older = event_row(description="versão anterior")
    newer = event_row(
        source_record_id="QSRC-TEST-002",
        description="versão mais recente",
        _source_file="mock://events/002",
        _source_file_modification_time=datetime(2026, 8, 31, 12, 3, 0),
    )
    mock_bronze(test_spark, [older, newer])
    run_financial_events(test_spark)

    accepted = test_spark.table(SILVER_OUTPUT).collect()
    quarantine = test_spark.table(QUARANTINE_OUTPUT).collect()
    assert len(accepted) == 1
    assert accepted[0].description == "versão mais recente"
    assert len(quarantine) == 1
    assert quarantine[0]._quarantine_reason == "DUPLICATE_LOSER"


def test_warning_is_normalized_without_rejection(test_spark):
    mock_bronze(
        test_spark,
        [
            event_row(
                event_id="QEVT-BR-TEST-WARN",
                event_status=" COMPLETED ",
                payload_variant="STATUS_WHITESPACE",
            )
        ],
    )
    run_financial_events(test_spark)

    accepted = test_spark.table(SILVER_OUTPUT).first()
    assert accepted.event_status == "completed"
    assert len(accepted._dq_warnings) >= 1
