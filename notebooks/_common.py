# Databricks notebook source
"""Shared deterministic helpers for the Quilla synthetic raw notebooks."""

import json
import math
import re
from datetime import date, datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


COUNTRIES = [
    {"code": "BR", "name_pt": "Brasil", "name_es": "Brasil", "locale": "pt-BR", "language": "pt", "currency": "BRL", "timezone": "America/Sao_Paulo", "tier": "anchor", "city": "São Paulo"},
    {"code": "MX", "name_pt": "México", "name_es": "México", "locale": "es-MX", "language": "es", "currency": "MXN", "timezone": "America/Mexico_City", "tier": "anchor", "city": "Ciudad de México"},
    {"code": "CO", "name_pt": "Colômbia", "name_es": "Colombia", "locale": "es-CO", "language": "es", "currency": "COP", "timezone": "America/Bogota", "tier": "scale", "city": "Bogotá"},
    {"code": "AR", "name_pt": "Argentina", "name_es": "Argentina", "locale": "es-AR", "language": "es", "currency": "ARS", "timezone": "America/Argentina/Buenos_Aires", "tier": "scale", "city": "Buenos Aires"},
    {"code": "CL", "name_pt": "Chile", "name_es": "Chile", "locale": "es-CL", "language": "es", "currency": "CLP", "timezone": "America/Santiago", "tier": "scale", "city": "Santiago"},
    {"code": "PE", "name_pt": "Peru", "name_es": "Perú", "locale": "es-PE", "language": "es", "currency": "PEN", "timezone": "America/Lima", "tier": "scale", "city": "Lima"},
    {"code": "EC", "name_pt": "Equador", "name_es": "Ecuador", "locale": "es-EC", "language": "es", "currency": "USD", "timezone": "America/Guayaquil", "tier": "growth", "city": "Quito"},
    {"code": "GT", "name_pt": "Guatemala", "name_es": "Guatemala", "locale": "es-GT", "language": "es", "currency": "GTQ", "timezone": "America/Guatemala", "tier": "growth", "city": "Ciudad de Guatemala"},
    {"code": "DO", "name_pt": "República Dominicana", "name_es": "República Dominicana", "locale": "es-DO", "language": "es", "currency": "DOP", "timezone": "America/Santo_Domingo", "tier": "growth", "city": "Santo Domingo"},
    {"code": "CR", "name_pt": "Costa Rica", "name_es": "Costa Rica", "locale": "es-CR", "language": "es", "currency": "CRC", "timezone": "America/Costa_Rica", "tier": "growth", "city": "San José"},
    {"code": "PA", "name_pt": "Panamá", "name_es": "Panamá", "locale": "es-PA", "language": "es", "currency": "PAB", "timezone": "America/Panama", "tier": "growth", "city": "Ciudad de Panamá"},
    {"code": "UY", "name_pt": "Uruguai", "name_es": "Uruguay", "locale": "es-UY", "language": "es", "currency": "UYU", "timezone": "America/Montevideo", "tier": "growth", "city": "Montevideo"},
]

DEMO_EVENT_COUNTS = {
    "BR": 4_265_181,
    "MX": 2_889_316,
    "CO": 1_651_038,
    "AR": 1_100_692,
    "CL": 963_105,
    "PE": 963_105,
    "EC": 481_553,
    "GT": 412_759,
    "DO": 343_966,
    "CR": 275_173,
    "PA": 206_380,
    "UY": 206_380,
}

EVENT_START = "2024-09-01"
EVENT_END = "2026-08-31"
REFERENCE_TS = "2026-08-31 23:59:59"


def widget(name: str, default: str) -> str:
    """Create/read a task parameter without relying on environment variables."""
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


def load_config(extra_ints=None):
    cfg = {
        "catalog": safe_identifier(widget("catalog", "example_catalog"), "catalog"),
        "schema": safe_identifier(widget("schema", "example_raw"), "schema"),
        "volume": safe_identifier(widget("volume", "synthetic_landing"), "volume"),
        "snapshot": safe_snapshot(widget("snapshot", "2026-08-31")),
        "seed": int(widget("seed", "42")),
    }
    for name, default in (extra_ints or {}).items():
        cfg[name] = int(widget(name, str(default)))
        if cfg[name] <= 0:
            raise ValueError(f"{name} must be positive")
    cfg["root"] = f"/Volumes/{cfg['catalog']}/{cfg['schema']}/{cfg['volume']}/snapshot={cfg['snapshot']}"
    return cfg


def dataset_path(cfg, dataset_name: str) -> str:
    if not re.fullmatch(r"[a-z][a-z0-9_]*", dataset_name):
        raise ValueError(f"Invalid dataset name: {dataset_name}")
    return f"{cfg['root']}/{dataset_name}"


def write_parquet(df: DataFrame, cfg, dataset_name: str, partition_cols=None) -> str:
    """Deterministically replace one dataset inside the selected logical snapshot."""
    output = dataset_path(cfg, dataset_name)
    writer = df.write.mode("overwrite").option("compression", "snappy")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(output)
    print(json.dumps({"dataset": dataset_name, "path": output, "status": "written"}))
    return output


def country_array(field: str):
    return F.array(*[F.lit(c[field]) for c in COUNTRIES])


def country_value(country_col, field: str):
    mapping = []
    for country in COUNTRIES:
        mapping.extend([F.lit(country["code"]), F.lit(country[field])])
    return F.element_at(F.create_map(*mapping), country_col)


def round_robin_country(row_col):
    return F.element_at(country_array("code"), (F.pmod(row_col, F.lit(len(COUNTRIES))) + F.lit(1)).cast("int"))


def same_country_id(row_hash_col, country_index_col, parent_rows: int, prefix: str):
    """Return a token whose numeric component maps to the same round-robin country."""
    minimum_capacity = parent_rows // len(COUNTRIES)
    if minimum_capacity < 1:
        minimum_capacity = 1
    numeric = country_index_col + F.lit(12) * F.pmod(row_hash_col, F.lit(minimum_capacity)) + F.lit(1)
    return F.format_string(f"{prefix}-%012d", numeric)


def stable_hash(*columns, seed: int = 42):
    spark_cols = [F.col(c) if isinstance(c, str) else c for c in columns]
    return F.xxhash64(*spark_cols, F.lit(seed))


def event_counts(total_rows: int):
    """Preserve the approved exact allocation for demo and proportionality for smoke."""
    approved_total = sum(DEMO_EVENT_COUNTS.values())
    if total_rows == approved_total:
        return dict(DEMO_EVENT_COUNTS)
    raw = {code: total_rows * count / approved_total for code, count in DEMO_EVENT_COUNTS.items()}
    allocated = {code: math.floor(value) for code, value in raw.items()}
    remainder = total_rows - sum(allocated.values())
    order = sorted(raw, key=lambda code: (raw[code] - allocated[code], DEMO_EVENT_COUNTS[code]), reverse=True)
    for code in order[:remainder]:
        allocated[code] += 1
    return allocated


def assert_expected(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def utc_now_literal():
    return F.to_timestamp(F.lit(REFERENCE_TS))
