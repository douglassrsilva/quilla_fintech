# Databricks notebook source
"""Shared settings for Quilla Managed GenAI governance assets."""

import os
import re


def _widget(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


def _identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def genai_governance_config():
    catalog = _identifier(
        _widget("catalog", "example_catalog"), "catalog"
    )
    ai_schema = _identifier(_widget("ai_schema", "example_ai"), "AI schema")
    table_prefix = _identifier(
        _widget("trace_table_prefix", "quilla_genai_demo"), "trace table prefix"
    )
    warehouse_id = _widget("sql_warehouse_id", "")
    if not re.fullmatch(r"[0-9a-f]{16}", warehouse_id):
        raise ValueError(f"Invalid SQL warehouse id: {warehouse_id!r}")
    experiment_path = _widget("experiment_path", "/Shared/quilla-genai-demo")
    if not experiment_path.startswith("/") or ".." in experiment_path:
        raise ValueError(f"Invalid experiment path: {experiment_path!r}")
    os.environ["MLFLOW_TRACING_SQL_WAREHOUSE_ID"] = warehouse_id
    evaluation_release = _widget("evaluation_release", "2026-09-01-v1")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,63}", evaluation_release):
        raise ValueError(f"Invalid evaluation release: {evaluation_release!r}")
    semantic_rows = int(_widget("semantic_rows", "12"))
    if not 1 <= semantic_rows <= 72:
        raise ValueError("semantic_rows must be between 1 and 72")
    return {
        "catalog": catalog,
        "ai_schema": ai_schema,
        "warehouse_id": warehouse_id,
        "experiment_path": experiment_path,
        "trace_table_prefix": table_prefix,
        "evaluation_release": evaluation_release,
        "semantic_rows": semantic_rows,
    }


def prompt_name(cfg, key: str) -> str:
    return f"{cfg['catalog']}.{cfg['ai_schema']}.quilla_{_identifier(key, 'prompt key')}"


def ai_governance_table(cfg, table: str) -> str:
    return f"{cfg['catalog']}.{cfg['ai_schema']}.{_identifier(table, 'table')}"
