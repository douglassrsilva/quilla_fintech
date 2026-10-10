#!/usr/bin/env python3
"""Validate the Quilla supervisor through the same API contract used by the App."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import urllib.error
import urllib.request


QUESTIONS = [
    "Quais os títulos que tiveram a maior alta na bolsa brasileira ontem?",
    (
        "Entre os papéis citados, quais altas parecem ligadas ao petróleo e quais "
        "exigem cautela por baixa liquidez? Preserve as fontes do contexto anterior."
    ),
    (
        "Transforme a análise em um briefing executivo para o comitê de investimentos "
        "da Quilla: fatos verificados, inferências, riscos e três próximos passos. "
        "Não invente dados novos."
    ),
]


def token(profile: str) -> str:
    completed = subprocess.run(
        ["databricks", "auth", "token", profile, "-o", "json"],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(completed.stdout)["access_token"]


def invoke(url: str, bearer: str, body: dict) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=900) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def answer_text(payload: dict) -> str:
    return "\n".join(
        content.get("text", "")
        for item in payload.get("output", [])
        if item.get("type") == "message"
        for content in item.get("content", [])
        if content.get("type") == "output_text" and content.get("text")
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument(
        "--host",
        required=True,
    )
    parser.add_argument("--endpoint", default="quilla-custom-supervisor")
    parser.add_argument("--conversation-id", default="quilla-api-three-turn-validation")
    args = parser.parse_args()

    url = f"{args.host.rstrip('/')}/serving-endpoints/{args.endpoint}/invocations"
    bearer = token(args.profile)
    history: list[dict] = []
    results: list[dict] = []

    for number, question in enumerate(QUESTIONS, start=1):
        history.append(
            {"role": "user", "content": [{"type": "input_text", "text": question}]}
        )
        body = {
            "input": history,
            "context": {
                "user_id": "quilla-demo-validator",
            },
            "custom_inputs": {
                "conversation_id": args.conversation_id,
                "tenant_id": "quilla-demo",
                "country_code": "BR",
                "locale": "pt-BR",
                "demo": True,
            },
        }
        try:
            payload = invoke(url, bearer, body)
        except Exception as exc:
            result = {"turn": number, "question": question, "status": "FAILED", "error": str(exc)}
            results.append(result)
            print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
            print(json.dumps({"conversation": results}, ensure_ascii=False, indent=2))
            return 1

        text = answer_text(payload)
        custom = payload.get("custom_outputs", {})
        result = {
            "turn": number,
            "question": question,
            "status": "SUCCEEDED" if text else "EMPTY",
            "answer": text,
            "route": custom.get("route"),
            "supporting_agents": custom.get("supporting_agents", []),
            "models": custom.get("models", []),
            "tools": custom.get("tools", []),
            "warnings": custom.get("warnings", []),
            "trace_id": custom.get("trace_id"),
            "requires_human_review": custom.get("requires_human_review"),
            "release": custom.get("release"),
        }
        results.append(result)
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        history.extend(payload.get("output", []))

    print(json.dumps({"conversation": results}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
