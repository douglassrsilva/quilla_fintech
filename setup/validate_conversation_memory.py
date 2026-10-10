#!/usr/bin/env python3
"""Prova idempotente de gravação e recuperação do ledger conversacional."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quilla_supervisor.contracts import RouteDecision, SupervisorResult, TrustedContext
from quilla_supervisor.memory import LakebaseConversationStore


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--project", default="quilla-ltap")
    parser.add_argument("--database", default="quilla")
    args = parser.parse_args()

    conversation_id = "quilla-memory-contract-smoke-v1"
    context = TrustedContext(
        user_id="quilla-demo-user",
        tenant_id="quilla-demo",
        country_code="BR",
        locale="pt-BR",
        conversation_id=conversation_id,
        roles=("demo_analyst",),
        demo=True,
    )
    store = LakebaseConversationStore(
        project=args.project,
        branch="production",
        endpoint_id="primary",
        database=args.database,
        profile=args.profile,
    )
    history = [
        {
            "role": "user",
            "content": "Registre esta conversa sintética para validar a memória do App.",
        }
    ]
    trace_id = str(
        uuid.uuid5(uuid.NAMESPACE_URL, f"quilla:{conversation_id}:validation")
    )
    result = SupervisorResult(
        text="Conversa sintética registrada no Lakebase.",
        route=RouteDecision(
            primary="customer_service",
            supporting=(),
            scores={},
            reasons=("memory contract validation",),
            high_impact=False,
            external_data=False,
        ),
        trace_id=trace_id,
        model_sequence=["memory-contract-validator"],
    )
    receipt = store.save_exchange(context, history, result)
    restored = store.restore(
        context,
        [{"role": "user", "content": "Recupere o contexto anterior."}],
    )
    conversation = store.get_conversation(
        session_id=receipt.session_id,
        tenant_id=context.tenant_id,
        actor_id=context.user_id,
    )
    sessions = store.list_sessions(
        tenant_id=context.tenant_id,
        actor_id=context.user_id,
        limit=100,
    )

    assert conversation is not None
    assert len(conversation["messages"]) == 2, conversation
    assert len(restored) == 3, restored
    assert any(str(item["session_id"]) == receipt.session_id for item in sessions)
    print(
        json.dumps(
            {
                "status": "PASSED",
                "store": "lakebase",
                "session_id": receipt.session_id,
                "persisted_messages": receipt.persisted_messages,
                "restored_messages_with_new_turn": len(restored),
                "app_recovery_contract": "PASSED",
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
