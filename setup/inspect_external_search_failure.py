#!/usr/bin/env python3
"""Read the latest external-search evidence and paid-fallback ledger from Lakebase."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quilla_supervisor.memory import LakebaseConversationStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
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
    store = LakebaseConversationStore.from_env()
    with store._connection() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT s.external_conversation_id, m.created_at, m.mlflow_trace_id,
                   m.citations, left(m.content, 500)
            FROM quilla_app.app_messages m
            JOIN quilla_app.app_sessions s ON s.session_id = m.session_id
            WHERE m.role = 'assistant'
              AND m.content ILIKE '%%No fue posible completar la investigación externa%%'
            ORDER BY m.created_at DESC
            LIMIT 5
            """
        )
        messages = [
            {
                "conversation_id": row[0],
                "created_at": row[1].isoformat(),
                "trace_id": row[2],
                "citations": row[3],
                "answer_preview": row[4],
            }
            for row in cursor.fetchall()
        ]
        cursor.execute(
            """
            SELECT provider, daily_window, daily_calls, total_calls, updated_at
            FROM quilla_app.external_tool_budget
            ORDER BY provider
            """
        )
        budgets = [
            {
                "provider": row[0],
                "daily_window": row[1].isoformat(),
                "daily_calls": row[2],
                "total_calls": row[3],
                "updated_at": row[4].isoformat(),
            }
            for row in cursor.fetchall()
        ]
        cursor.execute(
            """
            SELECT provider, conversation_id, request_id, status, latency_ms,
                   error_message, created_at, completed_at
            FROM quilla_app.external_tool_usage
            ORDER BY created_at DESC
            LIMIT 10
            """
        )
        usage = [
            {
                "provider": row[0],
                "conversation_id": row[1],
                "request_id": row[2],
                "status": row[3],
                "latency_ms": row[4],
                "error": row[5],
                "created_at": row[6].isoformat(),
                "completed_at": row[7].isoformat() if row[7] else None,
            }
            for row in cursor.fetchall()
        ]
    print(json.dumps({"messages": messages, "budgets": budgets, "usage": usage}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
