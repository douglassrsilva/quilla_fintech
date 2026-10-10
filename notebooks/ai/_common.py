# Databricks notebook source
"""Shared configuration and Lakebase OAuth helpers for Quilla AI Search."""

import json
import re


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


def ai_config():
    project = widget("lakebase_project", "quilla-ltap")
    if not re.fullmatch(r"[a-z][a-z0-9-]{2,62}", project):
        raise ValueError(f"Invalid Lakebase project: {project!r}")
    result = {
        "catalog": safe_identifier(widget("catalog", "example_catalog"), "catalog"),
        "silver_schema": safe_identifier(widget("silver_schema", "example_silver"), "silver_schema"),
        "ai_schema": safe_identifier(widget("ai_schema", "example_ai"), "ai_schema"),
        "lakebase_project": project,
        "lakebase_database": safe_identifier(widget("lakebase_database", "quilla"), "lakebase_database"),
        "vs_endpoint": widget("vs_endpoint", "quilla-ai-search"),
        "embedding_endpoint": widget("embedding_endpoint", "databricks-qwen3-embedding-0-6b"),
        "prep_docs_per_segment": int(widget("prep_docs_per_segment", "5")),
        "enrich_docs_per_segment": int(widget("enrich_docs_per_segment", "1")),
        "quality_results_per_query": int(widget("quality_results_per_query", "10")),
    }
    if not 1 <= result["prep_docs_per_segment"] <= 100:
        raise ValueError("prep_docs_per_segment must be between 1 and 100")
    if not 1 <= result["enrich_docs_per_segment"] <= 20:
        raise ValueError("enrich_docs_per_segment must be between 1 and 20")
    if not 1 <= result["quality_results_per_query"] <= 50:
        raise ValueError("quality_results_per_query must be between 1 and 50")
    result["lakebase_endpoint"] = (
        f"projects/{project}/branches/production/endpoints/primary"
    )
    return result


def ai_table(cfg, table: str) -> str:
    return f"{cfg['catalog']}.{cfg['ai_schema']}.{safe_identifier(table, 'table')}"


def lakebase_connection(cfg):
    import psycopg
    from databricks.sdk import WorkspaceClient

    workspace = WorkspaceClient()
    endpoint = workspace.api_client.do(
        "GET", f"/api/2.0/postgres/{cfg['lakebase_endpoint']}"
    )
    credential = workspace.api_client.do(
        "POST",
        "/api/2.0/postgres/credentials",
        body={"endpoint": cfg["lakebase_endpoint"]},
    )
    user = workspace.current_user.me().user_name
    return psycopg.connect(
        host=endpoint["status"]["hosts"]["host"],
        port=5432,
        dbname=cfg["lakebase_database"],
        user=user,
        password=credential["token"],
        sslmode="require",
        connect_timeout=30,
    )
