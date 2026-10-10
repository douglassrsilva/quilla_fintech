#!/usr/bin/env python3
"""Probe direto da API SSE usada pela tela Genie Research; não cria Databricks Job."""

from __future__ import annotations

import argparse
import json
import subprocess
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--url", required=True)
    parser.add_argument("--domain", default="executive")
    parser.add_argument("--question", default="Resuma o desempenho interno da Quilla por país.")
    args = parser.parse_args()
    token = json.loads(subprocess.run(
        ["databricks", "auth", "token", "--profile", args.profile],
        check=True, capture_output=True, text=True,
    ).stdout)["access_token"]
    request = urllib.request.Request(
        f"{args.url}/api/genie/research/stream",
        data=json.dumps({"question": args.question, "domain": args.domain}).encode(),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "text/event-stream",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    event = None
    counts: dict[str, int] = {}
    completed = None
    with urllib.request.urlopen(request, timeout=240) as response:
        for raw in response:
            line = raw.decode("utf-8", "replace").strip()
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:") and event:
                data = json.loads(line[5:].strip())
                counts[event] = counts.get(event, 0) + 1
                if event == "completed":
                    completed = data
                if event == "error":
                    raise RuntimeError(data.get("message"))
    if not completed or not counts.get("query"):
        raise RuntimeError(f"Incomplete Genie Agent stream: {counts}")
    if not completed.get("persisted") or not completed.get("session_id"):
        raise RuntimeError(f"Genie answer was not persisted: {completed}")
    history_request = urllib.request.Request(
        f"{args.url}/api/conversations/{completed['session_id']}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    with urllib.request.urlopen(history_request, timeout=30) as response:
        restored = json.load(response)
    if len(restored.get("messages", [])) < 2:
        raise RuntimeError("Persisted Genie exchange could not be restored")
    print(json.dumps({
        "status": "PASSED",
        "events": counts,
        "mode": completed.get("mode"),
        "conversation_id": completed.get("conversation_id"),
        "has_answer": bool(completed.get("answer")),
        "persisted": completed.get("persisted"),
        "restored_messages": len(restored.get("messages", [])),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
