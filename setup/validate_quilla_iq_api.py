#!/usr/bin/env python3
"""Validate two Quilla IQ turns through the exact Databricks App SSE API."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid


QUESTIONS = (
    "Como está o momento econômico do Equador no último mês?",
    "E quais são os dois principais riscos para investimentos nas próximas semanas?",
)


def bearer(profile: str) -> str:
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)["access_token"]


def stream(url: str, token: str, body: dict, key: str) -> tuple[list[dict], float]:
    request = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
            "X-Idempotency-Key": key,
            "X-Request-ID": key,
        },
        method="POST",
    )
    started = time.monotonic()
    events: list[dict] = []
    try:
        with urllib.request.urlopen(request, timeout=90) as response:
            event_type = "message"
            data: list[str] = []
            for raw in response:
                line = raw.decode("utf-8").rstrip("\r\n")
                if line.startswith("event:"):
                    event_type = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[5:].lstrip())
                elif not line and data:
                    events.append({"type": event_type, "data": json.loads("\n".join(data))})
                    event_type, data = "message", []
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"HTTP {exc.code}: {exc.read().decode()[:1500]}") from exc
    return events, round(time.monotonic() - started, 2)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument(
        "--url",
        required=True,
    )
    parser.add_argument("--question", action="append")
    parser.add_argument("--country-code", default="EC")
    parser.add_argument("--locale", default="pt-BR")
    args = parser.parse_args()
    token = bearer(args.profile)
    conversation_id = f"quilla-iq-two-turn-{uuid.uuid4()}"
    results = []
    questions = tuple(args.question) if args.question else QUESTIONS
    for number, question in enumerate(questions, start=1):
        key = f"{conversation_id}-{number}"
        events, elapsed = stream(
            f"{args.url.rstrip('/')}/api/chat/stream",
            token,
            {
                "message": question,
                "conversation_id": conversation_id,
                "country_code": args.country_code,
                "locale": args.locale,
                "experience": "market_intelligence",
            },
            key,
        )
        completed = next((item["data"] for item in events if item["type"] == "completed"), None)
        error = next((item["data"] for item in events if item["type"] == "error"), None)
        answer = completed.get("message", "") if completed else ""
        protocol_artifacts = [
            marker for marker in ("<tool_call>", "<think>", "<arg_key>", "<arg_value>")
            if marker in answer.lower()
        ]
        incomplete_answer = len(answer.strip()) < 200 or answer.rstrip().endswith(("##", "###"))
        tool_failures = [
            item for item in (completed.get("evidence") or [])
            if item.get("status") != "SUCCEEDED"
        ] if completed else []
        result = {
            "turn": number,
            "question": question,
            "elapsed_seconds": elapsed,
            "heartbeat_count": sum(item["type"] == "heartbeat" for item in events),
            "status": "SUCCEEDED" if completed and not protocol_artifacts and not tool_failures and not incomplete_answer else "FAILED",
            "route": completed.get("route") if completed else None,
            "trace_id": completed.get("trace_id") if completed else None,
            "tools": [
                {
                    "server": item.get("server"),
                    "tool": item.get("tool"),
                    "status": item.get("status"),
                    "preview": item.get("preview"),
                }
                for item in (completed.get("evidence") or [])
            ] if completed else [],
            "answer_preview": answer[:240] if completed else None,
            "protocol_artifacts": protocol_artifacts,
            "incomplete_answer": incomplete_answer,
            "error": error,
        }
        results.append(result)
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        if not completed or protocol_artifacts or tool_failures or incomplete_answer:
            return 1
    print(json.dumps({"conversation_id": conversation_id, "turns": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
