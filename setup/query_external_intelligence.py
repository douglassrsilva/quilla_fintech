#!/usr/bin/env python3
"""Query a Supervisor Agent Responses endpoint without losing response fields."""

from __future__ import annotations

import argparse
import json
import subprocess
import urllib.error
import urllib.request


DEFAULT_PROMPT = (
    "Pesquise exclusivamente eventos publicados ou ocorridos em 1º de setembro de 2026 "
    "que possam impactar o cenário financeiro, fiscal ou de investimentos na América "
    "Latina. Priorize fontes oficiais, informe país, data, URL, fato verificado, impacto "
    "potencial e incertezas. Separe fatos, inferências e recomendações. Use Kimi K3 se "
    "precisar consolidar contexto longo e DeepSeek V4 Pro para verificar divergências."
)


def auth_token(profile: str) -> str:
    completed = subprocess.run(
        ["databricks", "auth", "token", profile, "-o", "json"],
        check=True,
        text=True,
        capture_output=True,
    )
    payload = json.loads(completed.stdout)
    return payload["access_token"]


def invoke(url: str, token: str, body: dict) -> dict:
    request = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=900) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Endpoint retornou HTTP {exc.code}: {detail}") from exc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument(
        "--host",
        required=True,
    )
    parser.add_argument("--endpoint", default="quilla-custom-supervisor")
    parser.add_argument("--prompt", default=DEFAULT_PROMPT)
    parser.add_argument("--user-id", default="quilla-demo-validator")
    parser.add_argument("--conversation-id", default="quilla-direct-validation")
    parser.add_argument("--tenant-id", default="quilla-demo")
    parser.add_argument("--country-code", default="BR")
    parser.add_argument("--locale", default="pt-BR")
    parser.add_argument("--experience", default="market_intelligence")
    args = parser.parse_args()

    body = {
        "input": [
            {
                "role": "user",
                "content": [{"type": "input_text", "text": args.prompt}],
            }
        ],
        "context": {
            "user_id": args.user_id,
            "conversation_id": args.conversation_id,
        },
        "custom_inputs": {
            "tenant_id": args.tenant_id,
            "country_code": args.country_code,
            "locale": args.locale,
            "demo": True,
            "experience": args.experience,
        },
    }
    url = f"{args.host.rstrip('/')}/serving-endpoints/{args.endpoint}/invocations"
    token = auth_token(args.profile)
    history = list(body["input"])
    payload = invoke(url, token, body)
    approvals = []
    for _ in range(5):
        requests = [
            item for item in payload.get("output", [])
            if item.get("type") == "mcp_approval_request"
        ]
        if not requests:
            break
        unexpected = [item for item in requests if item.get("name") != "web_search"]
        if unexpected:
            raise RuntimeError(f"Refusing non-Web Search approval requests: {unexpected}")
        approvals.extend(
            {
                "approval_request_id": item["id"],
                "name": item.get("name"),
                "arguments": item.get("arguments"),
            }
            for item in requests
        )
        for item in payload.get("output", []):
            history.append(item)
            if item.get("type") == "mcp_approval_request":
                history.append(
                    {
                        "type": "mcp_approval_response",
                        "approval_request_id": item["id"],
                        "approve": True,
                    }
                )
        payload = invoke(url, token, {"input": history})
    else:
        raise RuntimeError("Approval loop exceeded five rounds")

    messages = []
    for item in payload.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            text = content.get("text")
            if text:
                messages.append(text)
    print(
        json.dumps(
            {
                "id": payload.get("id"),
                "status": payload.get("status"),
                "approved_web_search_requests": approvals,
                "messages": messages,
                "output": payload.get("output", []),
                "error": payload.get("error"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
