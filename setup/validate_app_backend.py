#!/usr/bin/env python3
"""Smoke remoto do contrato HTTP consumido pelo frontend Quilla."""

from __future__ import annotations

import argparse
import json
import subprocess
import urllib.error
import urllib.request
import uuid


def bearer(profile: str) -> str:
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile],
        check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout)["access_token"]


def invoke(url: str, token: str, *, body: dict | None = None, headers: dict | None = None,
           method: str | None = None):
    request = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            **({"Content-Type": "application/json"} if body is not None else {}),
            **(headers or {}),
        },
        method=method or ("POST" if body is not None else "GET"),
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode()[:1500]}") from exc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument(
        "--url",
        required=True,
    )
    parser.add_argument("--skip-chat", action="store_true")
    args = parser.parse_args()
    token = bearer(args.profile)
    live_status, live = invoke(f"{args.url}/api/health/live", token)
    ready_status, ready = invoke(f"{args.url}/api/health/ready", token)
    genie_status, genie = invoke(f"{args.url}/api/genie/mode", token)
    result = {
        "live": [live_status, live],
        "ready": [ready_status, ready],
        "genie": [genie_status, genie],
    }
    if not args.skip_chat:
        key = f"remote-smoke-{uuid.uuid4()}"
        chat_body = {
            "message": "Responda somente: Quilla backend operacional.",
            "conversation_id": key,
            "country_code": "BR",
            "locale": "pt-BR",
        }
        chat_headers = {"X-Idempotency-Key": key}
        chat_status, chat = invoke(
            f"{args.url}/api/chat",
            token,
            body=chat_body,
            headers=chat_headers,
        )
        replay_status, replay = invoke(
            f"{args.url}/api/chat", token, body=chat_body, headers=chat_headers
        )
        result["chat"] = [chat_status, {
            "conversation_id": chat.get("conversation_id"),
            "trace_id": chat.get("trace_id"),
            "route": chat.get("route"),
            "has_message": bool(chat.get("message")),
            "idempotent_replay": chat.get("idempotent_replay"),
        }]
        result["idempotency"] = [replay_status, replay.get("idempotent_replay") is True]

        conversations_status, conversations = invoke(
            f"{args.url}/api/conversations?include_archived=true", token
        )
        session = next(
            item for item in conversations["conversations"]
            if item["external_conversation_id"] == key
        )
        session_id = session["session_id"]
        history_status, history = invoke(
            f"{args.url}/api/conversations/{session_id}", token
        )
        assistant = next(
            item for item in reversed(history["messages"])
            if item["role"] == "assistant" and item.get("mlflow_trace_id") == chat["trace_id"]
        )
        feedback_status, feedback = invoke(
            f"{args.url}/api/feedback",
            token,
            body={
                "conversation_id": session_id,
                "message_id": assistant["message_id"],
                "trace_id": chat["trace_id"],
                "rating": 1,
                "category": "remote_smoke",
                "comment": "Contrato remoto validado.",
            },
            headers={"X-Idempotency-Key": f"feedback-{key}"},
        )
        lifecycle_status, lifecycle = invoke(
            f"{args.url}/api/conversations/{session_id}",
            token,
            body={"title": "Smoke remoto concluído", "archived": True},
            method="PATCH",
        )
        result["conversation_lifecycle"] = [
            conversations_status,
            history_status,
            lifecycle_status,
            {"session_id": session_id, "status": lifecycle.get("status")},
        ]
        result["feedback"] = [feedback_status, {
            "trace_id": feedback.get("mlflow_trace_id"),
            "review_status": feedback.get("review_status"),
        }]
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
