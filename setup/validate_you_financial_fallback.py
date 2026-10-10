#!/usr/bin/env python3
"""Spend exactly one governed You.com fallback call for release validation."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quilla_supervisor.auth import DatabricksAuth
from quilla_supervisor.config import SupervisorSettings
from quilla_supervisor.mcp import McpServer, McpToolbox


class UnavailablePrimary:
    """Deterministically simulates primary discovery failure without a network call."""

    def __init__(self, server):
        self.server = server

    def list_tools(self):
        return []


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument(
        "--question",
        default=(
            "Investiga señales externas actuales de Argentina que puedan confirmar o invalidar "
            "una campaña de crédito personal. Cita fuentes y fechas y separa hechos de inferencias."
        ),
    )
    args = parser.parse_args()
    os.environ.update(
        {
            "DATABRICKS_PROFILE": args.profile,
            "QUILLA_MEMORY_ENABLED": "true",
            "QUILLA_LAKEBASE_PROJECT": "quilla-ltap",
            "QUILLA_LAKEBASE_BRANCH": "production",
            "QUILLA_LAKEBASE_ENDPOINT": "primary",
            "QUILLA_LAKEBASE_DATABASE": "quilla",
        }
    )
    settings = SupervisorSettings.from_env()
    primary = McpServer("web_search", settings.web_search_mcp_path, "primary")
    fallback = McpServer(
        "you_financial",
        settings.you_financial_mcp_path,
        "financial fallback",
        fallback_for="web_search",
    )
    toolbox = McpToolbox(
        DatabricksAuth(profile=args.profile),
        [primary, fallback],
        required_server="web_search",
        web_search_timeout_seconds=settings.web_search_timeout_seconds,
        you_financial_timeout_seconds=settings.you_financial_timeout_seconds,
        you_financial_max_input_chars=settings.you_financial_max_input_chars,
        you_financial_daily_call_limit=settings.you_financial_daily_call_limit,
        you_financial_total_call_limit=settings.you_financial_total_call_limit,
        you_financial_conversation_call_limit=settings.you_financial_conversation_call_limit,
        quota_context="release-v21-controlled-fallback",
        quota_request_id="release-v21-validation-1",
    )
    toolbox.clients["web_search"] = UnavailablePrimary(primary)
    output, evidence = toolbox.execute_external_search(args.question)
    print(
        json.dumps(
            {
                "status": evidence.status,
                "server": evidence.server,
                "tool": evidence.tool,
                "quota": evidence.preview.split("; result=", 1)[0],
                "result_preview": output[:1200],
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
