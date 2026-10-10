#!/usr/bin/env python3
"""Three-turn continuity test for Quilla External Intelligence."""

from __future__ import annotations

import argparse
import json

from query_external_intelligence import auth_token, invoke


QUESTIONS = [
    (
        "Considere hoje 2 de setembro de 2026. Quais eventos oficiais publicados ontem, "
        "1º de setembro de 2026, podem afetar finanças, política fiscal ou investimentos "
        "no Brasil? Selecione no máximo dois, execute Web Search agora e cite as URLs. "
        "Não anuncie etapas: entregue diretamente fatos e incertezas."
    ),
    (
        "Desses eventos, qual merece atenção mais imediata da Quilla? Explique o impacto "
        "potencial sobre margem, crédito e investimentos sem repetir todo o resumo anterior. "
        "Separe claramente fato confirmado de inferência."
    ),
    (
        "Agora compare esse risco com México e Chile. Pesquise somente o que faltar e monte "
        "um plano de monitoramento para os próximos 30–90 dias. Preserve o contexto da "
        "conversa, cite fontes e não invente dados ausentes."
    ),
]


def text_messages(payload: dict) -> list[str]:
    messages = []
    for item in payload.get("output", []):
        if item.get("type") != "message":
            continue
        for content in item.get("content", []):
            if content.get("text"):
                messages.append(content["text"])
    return messages


def run_turn(url: str, token: str, history: list[dict], question: str) -> dict:
    history.append(
        {
            "role": "user",
            "content": [{"type": "input_text", "text": question}],
        }
    )
    payload = invoke(url, token, {"input": history})
    approvals = []
    for _ in range(6):
        requests = [
            item
            for item in payload.get("output", [])
            if item.get("type") == "mcp_approval_request"
        ]
        if not requests:
            break
        unexpected = [item for item in requests if item.get("name") != "web_search"]
        if unexpected:
            raise RuntimeError(f"Refusing non-Web Search requests: {unexpected}")
        for item in payload.get("output", []):
            history.append(item)
            if item.get("type") == "mcp_approval_request":
                approvals.append(
                    {
                        "id": item["id"],
                        "name": item.get("name"),
                        "arguments": item.get("arguments"),
                    }
                )
                history.append(
                    {
                        "type": "mcp_approval_response",
                        "approval_request_id": item["id"],
                        "approve": True,
                    }
                )
        payload = invoke(url, token, {"input": history})
    else:
        raise RuntimeError("Approval loop exceeded six rounds")

    messages = text_messages(payload)
    if payload.get("error") or not messages:
        raise RuntimeError(f"Turn failed: {json.dumps(payload, ensure_ascii=False)}")
    if any("invalid " in message.lower() and "approval" in message.lower() for message in messages):
        raise RuntimeError(f"Approval protocol failed: {messages}")
    history.extend(payload.get("output", []))
    return {
        "response_id": payload.get("id"),
        "status": payload.get("status"),
        "approved_web_search_count": len(approvals),
        "answer": "\n\n".join(messages)[:2500],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument(
        "--host",
        required=True,
    )
    parser.add_argument("--endpoint", required=True)
    args = parser.parse_args()

    url = f"{args.host.rstrip('/')}/serving-endpoints/{args.endpoint}/invocations"
    token = auth_token(args.profile)
    history: list[dict] = []
    transcript = []
    for number, question in enumerate(QUESTIONS, start=1):
        result = run_turn(url, token, history, question)
        transcript.append({"turn": number, "question": question, **result})
        print(
            json.dumps(
                {
                    "event": "TURN_PASSED",
                    "turn": number,
                    "response_id": result["response_id"],
                    "web_search_approvals": result["approved_web_search_count"],
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    print(
        json.dumps(
            {
                "status": "PASSED",
                "endpoint": args.endpoint,
                "turns": transcript,
                "history_items": len(history),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
