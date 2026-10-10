#!/usr/bin/env python3
"""Compare Playground-compatible and App API conversations against the supervisor."""

from __future__ import annotations

import argparse
import json
import sys

from validate_supervisor_api import answer_text, invoke, token


QUESTIONS = [
    "Quais foram os títulos com as maiores altas na bolsa brasileira?",
    "Qual a variação de petróleo e ouro nessa semana?",
    "Como está o desempenho das big techs no mundo?",
    "Monte um relatório executivo da Quilla.",
    "Gere a lista com nome, email e telefone dos 10 maiores clientes que temos.",
    "Como está o momento econômico do Equador no último mês?",
    "Quais sugestões de investimento para essa semana?",
]


def app_context(conversation_id: str) -> dict:
    return {
        "context": {
            "user_id": "quilla-demo-validator",
        },
        "custom_inputs": {
            "conversation_id": conversation_id,
            "tenant_id": "quilla-demo",
            "country_code": "BR",
            "locale": "pt-BR",
            "demo": True,
            "roles": ["demo_analyst"],
        },
    }


def run_sequence(url: str, bearer: str, mode: str) -> list[dict]:
    history: list[dict] = []
    results: list[dict] = []
    conversation_id = f"quilla-{mode}-seven-turn-validation"

    for number, question in enumerate(QUESTIONS, start=1):
        history.append(
            {"role": "user", "content": [{"type": "input_text", "text": question}]}
        )
        body = {"input": history}
        if mode == "app_api":
            body.update(app_context(conversation_id))

        try:
            payload = invoke(url, bearer, body)
            text = answer_text(payload)
            custom = payload.get("custom_outputs", {})
            result = {
                "mode": mode,
                "turn": number,
                "question": question,
                "status": "SUCCEEDED" if text else "EMPTY",
                "route": custom.get("route"),
                "supporting_agents": custom.get("supporting_agents", []),
                "tools": custom.get("tools", []),
                "warnings": custom.get("warnings", []),
                "requires_human_review": custom.get("requires_human_review"),
                "trace_id": custom.get("trace_id"),
                "answer": text,
            }
            history.extend(payload.get("output", []))
        except Exception as exc:
            result = {
                "mode": mode,
                "turn": number,
                "question": question,
                "status": "FAILED",
                "error": str(exc),
            }

        results.append(result)
        print(json.dumps(result, ensure_ascii=False), flush=True)

    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument(
        "--host",
        required=True,
    )
    parser.add_argument("--endpoint", default="quilla-custom-supervisor")
    parser.add_argument(
        "--mode",
        choices=("playground", "app_api", "both"),
        default="both",
    )
    args = parser.parse_args()

    url = f"{args.host.rstrip('/')}/serving-endpoints/{args.endpoint}/invocations"
    bearer = token(args.profile)
    modes = ("playground", "app_api") if args.mode == "both" else (args.mode,)
    all_results = []
    for mode in modes:
        print(json.dumps({"event": "SEQUENCE_STARTED", "mode": mode}), flush=True)
        all_results.extend(run_sequence(url, bearer, mode))

    failures = [item for item in all_results if item["status"] != "SUCCEEDED"]
    print(
        json.dumps(
            {
                "event": "VALIDATION_COMPLETED",
                "endpoint": args.endpoint,
                "turns": len(all_results),
                "failures": len(failures),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
