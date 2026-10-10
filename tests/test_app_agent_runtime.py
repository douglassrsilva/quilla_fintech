from __future__ import annotations

import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from quilla_supervisor.agents_sdk import AgentsSDKGateway, _agent_input
from quilla_supervisor.auth import DatabricksAuth
from quilla_supervisor.config import SupervisorSettings
from quilla_supervisor.memory import LakebaseConversationStore
from quilla_supervisor.mcp import McpServer, servers_for_route
from quilla_supervisor.routing import IntentRouter
from server.supervisor import SupervisorClient


GENIE_ENV = {
    "QUILLA_GENIE_EXECUTIVE_REGIONAL": "executive-space",
    "QUILLA_GENIE_PAYMENTS_PERFORMANCE": "payments-space",
    "QUILLA_GENIE_RISK_FRAUD": "risk-space",
    "QUILLA_GENIE_RESPONSIBLE_CREDIT": "credit-space",
    "QUILLA_GENIE_CUSTOMER_OPERATIONS": "customer-space",
    "QUILLA_GENIE_GROWTH_WEALTH": "growth-space",
}


class AppAgentRuntimeTests(unittest.TestCase):
    def test_all_six_genie_spaces_are_routable_through_managed_mcp(self):
        with patch.dict(os.environ, GENIE_ENV, clear=False):
            settings = SupervisorSettings.from_env()
        cases = {
            "Monte um relatório executivo regional.": ("executive", "executive-space"),
            "Mostre recusas e volume de pagamentos Pix.": ("payments_performance", "payments-space"),
            "Analise os alertas de fraude.": ("fraud_risk", "risk-space"),
            "Como está a carteira de crédito?": ("responsible_credit", "credit-space"),
            "Resuma os atendimentos e reclamações.": ("customer_service", "customer-space"),
            "Como está a adequação da carteira de investimentos?": ("investment_suitability", "growth-space"),
        }
        resolved = set()
        for question, (expected_route, expected_space) in cases.items():
            with self.subTest(question=question):
                route = IntentRouter().route(question)
                self.assertEqual(route.primary, expected_route)
                servers = servers_for_route(route, settings)
                genie = next(server for server in servers if server.label.startswith("genie_"))
                self.assertEqual(genie.path, f"/api/2.0/mcp/genie/{expected_space}")
                resolved.add(expected_space)
        self.assertEqual(resolved, set(GENIE_ENV.values()))

    def test_agents_sdk_is_selected_only_for_internal_governed_mcp(self):
        internal = SimpleNamespace(clients={
            "genie_executive": SimpleNamespace(
                server=McpServer("genie_executive", "/api/2.0/mcp/genie/id", "analytics")
            )
        })
        external = SimpleNamespace(clients={
            "web_search": SimpleNamespace(
                server=McpServer("web_search", "/ai-gateway/mcp-services/system.ai.web_search", "web")
            )
        })
        self.assertTrue(AgentsSDKGateway._uses_sdk_mcp(internal))
        self.assertFalse(AgentsSDKGateway._uses_sdk_mcp(external))

    def test_agent_input_keeps_system_policy_out_of_user_transcript(self):
        instructions, transcript = _agent_input([
            {"role": "system", "content": "Política governada."},
            {"role": "user", "content": "Pergunta em português."},
            {"role": "assistant", "content": "Resposta anterior."},
            {"role": "user", "content": "Continúe en español."},
        ])
        self.assertEqual(instructions, "Política governada.")
        self.assertNotIn("Política governada", transcript)
        self.assertIn("USUÁRIO:\nPergunta em português.", transcript)
        self.assertIn("USUÁRIO:\nContinúe en español.", transcript)

    def test_explicit_obo_authorization_never_calls_unified_auth(self):
        auth = DatabricksAuth(
            authorization="trusted-user-token",
            workspace_host="workspace.example",
        )
        self.assertEqual(auth.headers(), {"Authorization": "Bearer trusted-user-token"})
        self.assertEqual(auth.host(), "https://workspace.example")

    def test_app_lakebase_store_uses_request_scoped_obo_identity(self):
        environment = {
            "QUILLA_MEMORY_ENABLED": "true",
            "QUILLA_MEMORY_REQUIRED": "true",
            "PGHOST": "lakebase.example",
            "PGDATABASE": "quilla",
        }
        with patch.dict(os.environ, environment, clear=False):
            store = LakebaseConversationStore.for_app(
                user_id="analista@quilla.example",
                authorization="Bearer user-token",
            )
        self.assertEqual(store.explicit_host, "lakebase.example")
        self.assertEqual(store.explicit_user, "analista@quilla.example")
        self.assertEqual(store.explicit_authorization, "Bearer user-token")

    def test_openai_agents_sdk_runtime_dependency_is_importable(self):
        classes = AgentsSDKGateway._sdk_classes()
        self.assertEqual(len(classes), 7)


class InProcessOboTests(unittest.IsolatedAsyncioTestCase):
    async def test_in_process_runtime_fails_closed_without_forwarded_obo(self):
        client = SupervisorClient()
        fake_settings = SimpleNamespace(
            runtime_mode="in_process",
            obo_required=True,
            request_timeout_seconds=30,
            supervisor_endpoint="unused",
        )
        with (
            patch("server.supervisor.settings", fake_settings),
            patch.object(client, "_auth", return_value=("https://workspace.example", "Bearer service")),
        ):
            with self.assertRaisesRegex(Exception, "OBO token is required"):
                await client.invoke(
                    message="Olá",
                    conversation_id="conversation-1",
                    actor_id="analista@quilla.example",
                    tenant_id="quilla-demo",
                    roles=("demo-user",),
                    demo=True,
                    country_code="BR",
                    locale="pt-BR",
                    request_id="request-1",
                    user_authorization=None,
                )


if __name__ == "__main__":
    unittest.main()
