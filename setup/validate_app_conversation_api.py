#!/usr/bin/env python3
"""Valida o contrato read-only que o App usará para recuperar conversas."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--actor-id", default="quilla-demo-user")
    parser.add_argument(
        "--conversation-id", default="quilla-memory-contract-smoke-v1"
    )
    parser.add_argument("--expected-messages", type=int, default=2)
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(project_root))
    os.environ.update(
        {
            "DATABRICKS_PROFILE": args.profile,
            "QUILLA_MEMORY_ENABLED": "true",
            "QUILLA_MEMORY_REQUIRED": "true",
            "QUILLA_LAKEBASE_PROJECT": "quilla-ltap",
            "QUILLA_LAKEBASE_BRANCH": "production",
            "QUILLA_LAKEBASE_ENDPOINT": "primary",
            "QUILLA_LAKEBASE_DATABASE": "quilla",
            "QUILLA_TENANT_ID": "quilla-demo",
            "QUILLA_ALLOW_DEMO_CONTEXT": "false",
        }
    )

    from fastapi.testclient import TestClient

    from app import app

    client = TestClient(app)
    owner_headers = {"X-Forwarded-Email": args.actor_id}
    listed = client.get("/api/conversations", headers=owner_headers)
    assert listed.status_code == 200, listed.text
    conversations = listed.json()["conversations"]
    session = next(
        item
        for item in conversations
        if item["external_conversation_id"] == args.conversation_id
    )

    recovered = client.get(
        f"/api/conversations/{session['session_id']}", headers=owner_headers
    )
    assert recovered.status_code == 200, recovered.text
    assert len(recovered.json()["messages"]) == args.expected_messages

    forbidden = client.get(
        f"/api/conversations/{session['session_id']}",
        headers={"X-Forwarded-Email": "another-user@example.com"},
    )
    assert forbidden.status_code == 404, forbidden.text
    print(
        {
            "status": "PASSED",
            "list": listed.status_code,
            "recovery": recovered.status_code,
            "cross_user_isolation": forbidden.status_code,
            "session_id": session["session_id"],
        }
    )


if __name__ == "__main__":
    main()
