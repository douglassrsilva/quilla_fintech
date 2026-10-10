# Databricks notebook source
"""Shared helpers for Quilla ML synthetic outcomes and feature engineering."""

import json
import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


COUNTRIES = [
    {"code": "BR", "currency": "BRL", "locale": "pt-BR", "language": "pt", "policy_rate": 10.5, "inflation": 4.2, "fx": 5.1, "unemployment": 7.4},
    {"code": "MX", "currency": "MXN", "locale": "es-MX", "language": "es", "policy_rate": 9.8, "inflation": 4.0, "fx": 18.0, "unemployment": 3.0},
    {"code": "CO", "currency": "COP", "locale": "es-CO", "language": "es", "policy_rate": 9.0, "inflation": 5.4, "fx": 4100.0, "unemployment": 10.1},
    {"code": "AR", "currency": "ARS", "locale": "es-AR", "language": "es", "policy_rate": 32.0, "inflation": 42.0, "fx": 1200.0, "unemployment": 7.6},
    {"code": "CL", "currency": "CLP", "locale": "es-CL", "language": "es", "policy_rate": 5.0, "inflation": 4.1, "fx": 950.0, "unemployment": 8.5},
    {"code": "PE", "currency": "PEN", "locale": "es-PE", "language": "es", "policy_rate": 4.8, "inflation": 2.3, "fx": 3.75, "unemployment": 6.6},
    {"code": "EC", "currency": "USD", "locale": "es-EC", "language": "es", "policy_rate": 7.0, "inflation": 2.0, "fx": 1.0, "unemployment": 4.2},
    {"code": "GT", "currency": "GTQ", "locale": "es-GT", "language": "es", "policy_rate": 4.8, "inflation": 3.8, "fx": 7.8, "unemployment": 3.1},
    {"code": "DO", "currency": "DOP", "locale": "es-DO", "language": "es", "policy_rate": 6.0, "inflation": 3.6, "fx": 60.0, "unemployment": 5.1},
    {"code": "CR", "currency": "CRC", "locale": "es-CR", "language": "es", "policy_rate": 4.0, "inflation": 1.2, "fx": 520.0, "unemployment": 7.8},
    {"code": "PA", "currency": "PAB", "locale": "es-PA", "language": "es", "policy_rate": 6.0, "inflation": 1.8, "fx": 1.0, "unemployment": 7.0},
    {"code": "UY", "currency": "UYU", "locale": "es-UY", "language": "es", "policy_rate": 8.5, "inflation": 5.1, "fx": 41.0, "unemployment": 8.2},
]

REFERENCE_TS = "2026-08-31 23:59:59"
START_DATE = "2024-09-01"


def widget(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


def safe_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def safe_snapshot(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", value):
        raise ValueError(f"Invalid snapshot: {value!r}")
    return value


def config(extra_ints=None):
    result = {
        "catalog": safe_identifier(widget("catalog", "example_catalog"), "catalog"),
        "raw_schema": safe_identifier(widget("raw_schema", "example_raw"), "raw_schema"),
        "silver_schema": safe_identifier(widget("silver_schema", "example_silver"), "silver_schema"),
        "ml_schema": safe_identifier(widget("ml_schema", "example_ml"), "ml_schema"),
        "volume": safe_identifier(widget("ml_volume", "ml_landing"), "ml_volume"),
        "snapshot": safe_snapshot(widget("ml_snapshot", "2026-08-31")),
        "seed": int(widget("seed", "42")),
    }
    for key, value in (extra_ints or {}).items():
        result[key] = int(widget(key, str(value)))
        if result[key] <= 0:
            raise ValueError(f"{key} must be positive")
    result["root"] = (
        f"/Volumes/{result['catalog']}/{result['raw_schema']}/"
        f"{result['volume']}/snapshot={result['snapshot']}"
    )
    return result


def country_array(field: str):
    return F.array(*[F.lit(country[field]) for country in COUNTRIES])


def country_value(country_column, field: str):
    pairs = []
    for country in COUNTRIES:
        pairs.extend([F.lit(country["code"]), F.lit(country[field])])
    return F.element_at(F.create_map(*pairs), country_column)


def stable_uniform(*columns, seed: int = 42):
    spark_columns = [F.col(column) if isinstance(column, str) else column for column in columns]
    return (F.pmod(F.xxhash64(*spark_columns, F.lit(seed)), F.lit(1_000_000)) + 1) / F.lit(1_000_001.0)


def same_country_id(hash_column, country_index_column, parent_rows: int, prefix: str):
    capacity = max(1, parent_rows // len(COUNTRIES))
    numeric = country_index_column + F.lit(12) * F.pmod(hash_column, F.lit(capacity)) + F.lit(1)
    return F.format_string(f"{prefix}-%012d", numeric)


def dataset_path(cfg, dataset: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", dataset):
        raise ValueError(f"Invalid dataset: {dataset!r}")
    return f"{cfg['root']}/{dataset}"


def write_parquet(df: DataFrame, cfg, dataset: str, partitions=None) -> str:
    path = dataset_path(cfg, dataset)
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partitions:
        writer = writer.partitionBy(*partitions)
    writer.parquet(path)
    print(json.dumps({"dataset": dataset, "path": path, "status": "written"}))
    return path


def read_landing(cfg, dataset: str) -> DataFrame:
    return spark.read.parquet(dataset_path(cfg, dataset))


def sigmoid(column):
    return F.lit(1.0) / (F.lit(1.0) + F.exp(-column))


def source_table(cfg, table: str) -> str:
    return f"{cfg['catalog']}.{cfg['silver_schema']}.{table}"


def ml_table(cfg, table: str) -> str:
    return f"{cfg['catalog']}.{cfg['ml_schema']}.{table}"

