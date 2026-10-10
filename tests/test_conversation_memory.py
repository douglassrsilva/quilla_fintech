from __future__ import annotations

import os
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from mlflow.types.responses import ResponsesAgentRequest

from quilla_supervisor.contracts import (
    RouteDecision,
    SupervisorResult,
    TrustedContext,
)
from quilla_supervisor.memory import (
    LakebaseConversationStore,
    MemoryReceipt,
    merge_history,
    session_uuid,
)
from quilla_supervisor.model import QuillaResponsesAgent


class FakeSupervisor:
    def answer(self, messages, context):
        return SupervisorResult(
            text="Resposta persistida.",
            route=RouteDecision(
                primary="customer_service",
                supporting=(),
                scores={},
                reasons=("teste",),
                high_impact=False,
                external_data=False,
            ),
            trace_id="5a096ba7-646c-4586-87a8-d7d567870bf2",
            model_sequence=["catalog.schema.model"],
            custom={"conversation_id": context.conversation_id},
        )


class FakeConversationStore:
    enabled = True
    required = True

    def __init__(self):
        self.restored = None
        self.saved = None

    def restore(self, context, incoming):
        self.restored = (context, incoming)
        return [
            {"role": "user", "content": "Pergunta anterior"},
            {"role": "assistant", "content": "Resposta anterior"},
            *incoming,
        ]

    def save_exchange(self, context, history, result):
        self.saved = (context, history, result)
        return MemoryReceipt(
            session_id="a9ef864c-b46e-5194-ae65-f6eb1060eac2",
            persisted_messages=4,
        )


class ConversationMemoryTests(unittest.TestCase):
    def test_serving_connection_uses_injected_host_and_obo_token(self):
        store = LakebaseConversationStore(
            project="quilla-ltap",
            branch="production",
            endpoint_id="primary",
            database="quilla",
        )
        client = SimpleNamespace(
            current_user=SimpleNamespace(
                me=lambda: SimpleNamespace(
                    user_name="analyst@quilla.example", display_name=None
                )
            ),
            config=SimpleNamespace(
                authenticate=lambda: {"Authorization": "Bearer temporary-obo-token"}
            ),
        )
        connect = Mock(return_value=object())
        fake_psycopg = SimpleNamespace(connect=connect)
        with (
            patch.object(store, "_workspace_client", return_value=client),
            patch.dict(os.environ, {"QUILLA_LAKEBASE_HOST": "lakebase.example"}),
            patch.dict(sys.modules, {"psycopg": fake_psycopg}),
        ):
            store._connection()

        connect.assert_called_once_with(
            host="lakebase.example",
            port=5432,
            dbname="quilla",
            user="analyst@quilla.example",
            password="temporary-obo-token",
            sslmode="require",
            connect_timeout=20,
        )

    def test_session_key_is_stable_and_tenant_isolated(self):
        first = TrustedContext("user-1", "tenant-a", "BR", "pt-BR", "conversation-1")
        same = TrustedContext("user-1", "tenant-a", "BR", "pt-BR", "conversation-1")
        other_tenant = TrustedContext("user-1", "tenant-b", "BR", "pt-BR", "conversation-1")
        self.assertEqual(session_uuid(first), session_uuid(same))
        self.assertNotEqual(session_uuid(first), session_uuid(other_tenant))

    def test_single_new_turn_restores_saved_history(self):
        stored = [
            {"role": "user", "content": "Pergunta anterior"},
            {"role": "assistant", "content": "Resposta anterior"},
        ]
        incoming = [{"role": "user", "content": "Continue a análise"}]
        self.assertEqual(merge_history(stored, incoming), [*stored, *incoming])

    def test_full_client_history_is_not_duplicated(self):
        stored = [{"role": "user", "content": "Pergunta anterior"}]
        incoming = [
            *stored,
            {"role": "assistant", "content": "Resposta anterior"},
            {"role": "user", "content": "Continue a análise"},
        ]
        self.assertEqual(merge_history(stored, incoming), incoming)

    def test_responses_agent_restores_and_persists_conversation(self):
        store = FakeConversationStore()
        agent = QuillaResponsesAgent(
            supervisor=FakeSupervisor(), conversation_store=store
        )
        request = ResponsesAgentRequest(
            input=[
                {
                    "role": "user",
                    "content": [{"type": "input_text", "text": "Continue a análise"}],
                }
            ],
            context={"user_id": "user-1"},
            custom_inputs={
                "conversation_id": "conversation-1",
                "tenant_id": "tenant-a",
                "country_code": "BR",
                "locale": "pt-BR",
                "demo": True,
            },
        )
        response = agent.predict(request)
        self.assertIsNotNone(store.restored)
        self.assertIsNotNone(store.saved)
        self.assertEqual(len(store.saved[1]), 3)
        self.assertEqual(response.custom_outputs["memory"]["status"], "PERSISTED")
        self.assertEqual(
            response.custom_outputs["memory"]["session_id"],
            "a9ef864c-b46e-5194-ae65-f6eb1060eac2",
        )


if __name__ == "__main__":
    unittest.main()
