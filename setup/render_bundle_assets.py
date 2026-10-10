#!/usr/bin/env python3
"""Render environment-specific DAB assets without committing workspace identifiers."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import shutil


IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
PLACEHOLDERS = {
    "catalog": "example_catalog",
    "schema": "example_raw",
    "bronze_schema": "example_bronze",
    "silver_schema": "example_silver",
    "gold_schema": "example_gold",
    "ml_schema": "example_ml",
    "ai_schema": "example_ai",
    "governance_schema": "example_governance",
    "monitoring_schema": "example_monitoring",
}
DEFAULTS = {
    "catalog": "quilla_fintech_dev",
    "schema": "quilla_raw",
    "bronze_schema": "quilla_bronze",
    "silver_schema": "quilla_silver",
    "gold_schema": "quilla_gold",
    "ml_schema": "quilla_ml",
    "ai_schema": "quilla_ai",
    "governance_schema": "quilla_governance",
    "monitoring_schema": "quilla_monitoring",
}
TARGET_CATALOGS = {
    "smoke": "quilla_fintech_smoke",
    "demo": "quilla_fintech_demo",
}
SOURCE_TREES = ("sql", "dashboards", "genie_spaces")


def configured_value(name: str, explicit: str | None, target: str) -> str:
    value = (explicit or os.getenv(f"DATABRICKS_BUNDLE_VAR_{name}", "")).strip()
    if not value or value == "REPLACE_ME":
        value = TARGET_CATALOGS.get(target, DEFAULTS[name]) if name == "catalog" else DEFAULTS[name]
    if not IDENTIFIER.fullmatch(value):
        raise ValueError(f"Invalid Unity Catalog identifier for {name}: {value!r}")
    return value


def render_file(source: Path, target: Path, replacements: dict[str, str]) -> None:
    content = source.read_text(encoding="utf-8")
    for placeholder, value in replacements.items():
        content = content.replace(placeholder, value)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=("smoke", "demo"), default="demo")
    for name in PLACEHOLDERS:
        parser.add_argument(f"--{name.replace('_', '-')}")
    args = parser.parse_args()

    values = {
        name: configured_value(name, getattr(args, name), args.target)
        for name in PLACEHOLDERS
    }
    replacements = {
        placeholder: values[name] for name, placeholder in PLACEHOLDERS.items()
    }
    root = Path(__file__).resolve().parents[1]
    output = root / ".generated"
    if output.exists():
        shutil.rmtree(output)

    rendered = 0
    for tree in SOURCE_TREES:
        source_root = root / tree
        for source in source_root.rglob("*"):
            if source.is_file():
                render_file(source, output / tree / source.relative_to(source_root), replacements)
                rendered += 1

    print(f"Rendered {rendered} deployment assets under {output}")


if __name__ == "__main__":
    main()
