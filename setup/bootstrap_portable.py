#!/usr/bin/env python3
"""Valida y despliega Quilla sin depender de identificadores de otro workspace."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shutil
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
TARGET_DEFAULTS = {
    "smoke": {"project": "quilla-ltap-smoke", "database": "quilla"},
    "demo": {"project": "quilla-ltap-demo", "database": "quilla"},
}


def load_local_env(path: Path) -> dict[str, str]:
    env = os.environ.copy()
    if not path.exists():
        return env
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env.setdefault(key.strip(), value.strip().strip("'\""))
    return env


def run(command: list[str], env: dict[str, str]) -> None:
    print(f"\n$ {' '.join(command)}", flush=True)
    subprocess.run(command, cwd=ROOT, env=env, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bootstrap portátil de Quilla para un workspace Databricks."
    )
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--target", choices=("smoke", "demo"), default="demo")
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Renderiza y valida, pero no crea recursos.",
    )
    parser.add_argument(
        "--skip-lakebase-schema",
        action="store_true",
        help="No inicializa las tablas LTAP después del deploy.",
    )
    args = parser.parse_args()

    if not shutil.which("databricks"):
        raise SystemExit("No se encontró Databricks CLI en PATH.")

    env = load_local_env(ROOT / ".env.local")
    env.setdefault("DATABRICKS_AUTH_STORAGE", "plaintext")

    run(["databricks", "version"], env)
    run(["databricks", "current-user", "me", "--profile", args.profile], env)
    run(
        [sys.executable, "setup/render_bundle_assets.py", "--target", args.target],
        env,
    )
    run(
        [
            "databricks",
            "bundle",
            "validate",
            "-t",
            args.target,
            "--profile",
            args.profile,
        ],
        env,
    )
    if args.validate_only:
        print("\nValidación concluida. No se creó ningún recurso.")
        return

    run(
        [
            "databricks",
            "bundle",
            "deploy",
            "-t",
            args.target,
            "--profile",
            args.profile,
        ],
        env,
    )

    if not args.skip_lakebase_schema:
        defaults = TARGET_DEFAULTS[args.target]
        project = env.get("DATABRICKS_BUNDLE_VAR_lakebase_project", defaults["project"])
        database = env.get("DATABRICKS_BUNDLE_VAR_lakebase_database", defaults["database"])
        if not shutil.which("uv"):
            raise SystemExit(
                "El bundle fue desplegado, pero falta 'uv' para inicializar Lakebase. "
                "Instálelo o ejecute setup/lakebase_bootstrap.py con psycopg disponible."
            )
        run(
            [
                "uv",
                "run",
                "--with",
                "psycopg[binary]==3.2.9",
                "python",
                "setup/lakebase_bootstrap.py",
                "--profile",
                args.profile,
                "--project",
                project,
                "--database",
                database,
            ],
            env,
        )

    run(
        [
            "databricks",
            "bundle",
            "summary",
            "-t",
            args.target,
            "--profile",
            args.profile,
        ],
        env,
    )
    print(
        "\nBootstrap concluido. El catálogo, schemas, volúmenes, SQL Warehouse, "
        "AI Search, Lakebase y recursos de aplicación pertenecen a este deploy."
    )


if __name__ == "__main__":
    main()
