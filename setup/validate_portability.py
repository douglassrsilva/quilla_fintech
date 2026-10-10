#!/usr/bin/env python3
"""Static guardrail against coupling Quilla to a source workspace."""

from __future__ import annotations

from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[1]
EXCLUDED_PARTS = {
    ".git", ".generated", ".databricks", "mlruns", "node_modules",
    "__pycache__", ".venv",
}
TEXT_SUFFIXES = {
    ".css", ".env", ".example", ".html", ".js", ".json", ".md", ".py",
    ".sql", ".toml", ".ts", ".tsx", ".txt", ".yaml", ".yml",
}

FORBIDDEN = {
    "personal domain marker": re.compile(r"\[Douglas\]", re.IGNORECASE),
    "source catalog": re.compile(r"classic_stable_douglas_s_catalog", re.IGNORECASE),
    "source workspace": re.compile(r"fevm-classic-stable", re.IGNORECASE),
    "source workspace id": re.compile(r"7474654097678606"),
    "source resource uuid": re.compile(r"c9014dfe-b3e7-4675-b2c1-9ca3f8610f2e"),
    "concrete Databricks workspace URL": re.compile(
        r"https://(?!(?:<|npm-proxy\.))[A-Za-z0-9.-]+\.cloud\.databricks\.com",
        re.IGNORECASE,
    ),
}

REQUIRED_DECLARATIONS = (
    "catalogs:\n    quilla:",
    "sql_warehouses:\n    quilla_serverless:",
    "vector_search_endpoints:\n    quilla_ai_search:",
    "postgres_projects:\n    quilla_ltap:",
    "postgres_databases:\n    quilla_ltap_database:",
    "volumes:\n    quilla_raw_landing:",
    "quilla_ml_landing:",
)

FORBIDDEN_REFERENCES = (
    "${var.sql_warehouse_id}",
    "${var.lakebase_database_resource_id}",
    "${var.lakebase_owner_role}",
)


def text_files():
    for path in ROOT.rglob("*"):
        if not path.is_file() or any(part in EXCLUDED_PARTS for part in path.parts):
            continue
        if path == Path(__file__).resolve():
            continue
        if path.name == ".env.example" or path.suffix.lower() in TEXT_SUFFIXES:
            yield path


def main() -> None:
    failures: list[str] = []
    for path in text_files():
        try:
            content = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        relative = path.relative_to(ROOT)
        for label, pattern in FORBIDDEN.items():
            match = pattern.search(content)
            if match:
                line = content.count("\n", 0, match.start()) + 1
                failures.append(f"{relative}:{line}: {label}")
        for reference in FORBIDDEN_REFERENCES:
            if reference in content:
                failures.append(f"{relative}: referencia externa obligatoria {reference}")

    platform = (ROOT / "resources" / "platform.yml").read_text(encoding="utf-8")
    for declaration in REQUIRED_DECLARATIONS:
        if declaration not in platform:
            failures.append(f"resources/platform.yml: falta {declaration!r}")

    bundle = (ROOT / "databricks.yml").read_text(encoding="utf-8")
    for variable in (
        "catalog", "schema", "bronze_schema", "silver_schema", "gold_schema",
        "governance_schema", "monitoring_schema", "volume", "ml_schema",
        "ml_volume", "ai_schema", "lakebase_project", "lakebase_database",
        "lakebase_branch", "lakebase_endpoint", "vector_search_endpoint",
    ):
        block = re.search(
            rf"(?ms)^  {re.escape(variable)}:\n(?P<body>(?:    .*\n)+)", bundle
        )
        if not block or "default:" not in block.group("body"):
            failures.append(f"databricks.yml: variable {variable!r} sin default portátil")

    if failures:
        print("Portabilidad: FALLÓ")
        for failure in failures:
            print(f"- {failure}")
        raise SystemExit(1)

    print("Portabilidad: OK — no hay identificadores del ambiente de origen.")
    print("Infraestructura declarativa: catálogo, volúmenes, Warehouse, AI Search y Lakebase.")


if __name__ == "__main__":
    main()
