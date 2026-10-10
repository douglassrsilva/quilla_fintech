#!/usr/bin/env python3
"""Run the Quilla agent locally while calling governed Databricks services."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quilla_supervisor.agent import QuillaSupervisor
from quilla_supervisor.auth import DatabricksAuth
from quilla_supervisor.config import SupervisorSettings
from quilla_supervisor.contracts import TrustedContext


FULL_QUESTIONS = [
    "Quais foram os títulos com as maiores altas na bolsa brasileira?",
    "Qual a variação de petróleo e ouro nessa semana?",
    "Como está o desempenho das big techs no mundo?",
    "Monte um relatório executivo da Quilla.",
    "Gere a lista com nome, email e telefone dos 10 maiores clientes que temos.",
    "Como está o momento econômico do Equador no último mês?",
    "Quais sugestões de investimento para essa semana?",
]

SMOKE_QUESTIONS = [FULL_QUESTIONS[1], FULL_QUESTIONS[3], FULL_QUESTIONS[4]]

GENIE_SPACES = {
    "executive": os.getenv("QUILLA_GENIE_EXECUTIVE_REGIONAL", ""),
    "fraud_risk": os.getenv("QUILLA_GENIE_RISK_FRAUD", ""),
    "responsible_credit": os.getenv("QUILLA_GENIE_RESPONSIBLE_CREDIT", ""),
    "customer_service": os.getenv("QUILLA_GENIE_CUSTOMER_OPERATIONS", ""),
    "investment_suitability": os.getenv("QUILLA_GENIE_GROWTH_WEALTH", ""),
    "next_best_action": os.getenv("QUILLA_GENIE_GROWTH_WEALTH", ""),
}
GENIE_SPACES = {key: value for key, value in GENIE_SPACES.items() if value}

CRITICAL_WARNING_MARKERS = (
    "planner semântico indisponível",
    "verificação independente indisponível",
    "revisão governada indisponível",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--suite", choices=("smoke", "full", "pii"), default="smoke")
    args = parser.parse_args()

    auth = DatabricksAuth(profile=args.profile)
    supervisor = QuillaSupervisor(
        SupervisorSettings(genie_spaces=GENIE_SPACES),
        auth=auth,
    )
    context = TrustedContext(
        user_id="quilla-local-integration",
        tenant_id="quilla-demo",
        country_code="BR",
        locale="pt-BR",
        conversation_id=f"local-{args.suite}-integration",
        roles=("demo_analyst",),
        demo=True,
    )
    history: list[dict] = []
    questions = (
        SMOKE_QUESTIONS
        if args.suite == "smoke"
        else [FULL_QUESTIONS[4]]
        if args.suite == "pii"
        else FULL_QUESTIONS
    )
    failures = 0

    for turn, question in enumerate(questions, start=1):
        history.append({"role": "user", "content": question})
        try:
            result = supervisor.answer(history, context)
            tools = [item.__dict__ for item in result.tool_evidence]
            critical_warnings = [
                warning
                for warning in result.warnings
                if any(marker in warning.lower() for marker in CRITICAL_WARNING_MARKERS)
            ]
            status = "DEGRADED" if critical_warnings else "SUCCEEDED"
            if critical_warnings:
                failures += 1
            history.append({"role": "assistant", "content": result.text})
            error = " | ".join(critical_warnings) or None
        except Exception as exc:
            status = "FAILED"
            failures += 1
            result = None
            tools = []
            error = str(exc)
        print(
            json.dumps(
                {
                    "turn": turn,
                    "question": question,
                    "status": status,
                    "route": result.route.primary if result else None,
                    "supporting": list(result.route.supporting) if result else [],
                    "external_data": result.route.external_data if result else None,
                    "models": result.model_sequence if result else [],
                    "tools": tools,
                    "warnings": result.warnings if result else [],
                    "answer": result.text if result else None,
                    "error": error,
                },
                ensure_ascii=False,
            ),
            flush=True,
        )

    print(json.dumps({"status": "PASSED" if not failures else "FAILED", "failures": failures}))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
