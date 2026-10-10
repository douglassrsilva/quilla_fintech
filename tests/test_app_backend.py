from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from fastapi.testclient import TestClient

from app import app
from server.db import LakebasePool
from server.idempotency import decode_response_payload
from server.security import trusted_geo
from server.supervisor import SupervisorResponse


HEADERS = {"X-Forwarded-Email": "analista@quilla.example"}


def answer():
    return SupervisorResponse(
        text="Cenário financeiro resumido.",
        trace_id="trace-123",
        route="external_intelligence",
        evidence=[{"url": "https://example.com", "title": "Fonte"}],
        requires_human_review=False,
        raw_custom_outputs={},
    )


class BackendContractTests(unittest.TestCase):
    def test_health_and_request_correlation(self):
        with TestClient(app) as client:
            response = client.get("/api/health/live", headers={"X-Request-ID": "test-correlation"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Request-ID"], "test-correlation")

    def test_chat_rejects_client_supplied_trusted_context(self):
        with TestClient(app) as client:
            response = client.post(
                "/api/chat",
                headers=HEADERS,
                json={"message": "Olá", "tenant_id": "attacker", "roles": ["admin"], "demo": False},
            )
        self.assertEqual(response.status_code, 422)

    def test_chat_derives_identity_and_is_idempotent(self):
        invoke = AsyncMock(return_value=answer())
        with (
            patch("server.chat.claim", AsyncMock(side_effect=[None, {
                "conversation_id": "conversation-1", "message": answer().text,
                "trace_id": "trace-123", "route": "external_intelligence",
                "evidence": answer().evidence, "requires_human_review": False,
                "request_id": "first",
            }])),
            patch("server.chat.complete", AsyncMock()),
            patch("server.chat.supervisor_client.invoke", invoke),
            TestClient(app) as client,
        ):
            first = client.post(
                "/api/chat",
                headers={**HEADERS, "X-Idempotency-Key": "stable-key", "X-Request-ID": "first"},
                json={"message": "Analise", "conversation_id": "conversation-1", "country_code": "BR", "locale": "pt-BR"},
            )
            replay = client.post(
                "/api/chat",
                headers={**HEADERS, "X-Idempotency-Key": "stable-key", "X-Request-ID": "second"},
                json={"message": "Analise", "conversation_id": "conversation-1", "country_code": "BR", "locale": "pt-BR"},
            )
        self.assertEqual(first.status_code, 200)
        self.assertFalse(first.json()["idempotent_replay"])
        self.assertTrue(replay.json()["idempotent_replay"])
        self.assertEqual(invoke.await_count, 1)
        call = invoke.await_args.kwargs
        self.assertEqual(call["actor_id"], "analista@quilla.example")
        self.assertEqual(call["tenant_id"], "quilla-demo")

    def test_proxy_obo_token_is_forwarded_to_supervisor(self):
        invoke = AsyncMock(return_value=answer())
        with (
            patch("server.chat.claim", AsyncMock(return_value=None)),
            patch("server.chat.complete", AsyncMock()),
            patch("server.chat.supervisor_client.invoke", invoke),
            TestClient(app) as client,
        ):
            response = client.post(
                "/api/chat",
                headers={**HEADERS, "X-Forwarded-Access-Token": "trusted-user-token"},
                json={"message": "Analise", "conversation_id": "obo-1"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            invoke.await_args.kwargs["user_authorization"],
            "Bearer trusted-user-token",
        )

    def test_stream_accepts_immediately_and_heartbeats(self):
        async def delayed(**_kwargs):
            await asyncio.sleep(0.04)
            return answer()

        fast_settings = SimpleNamespace(
            heartbeat_seconds=0.01,
            request_timeout_seconds=1,
            database_required=True,
        )
        with (
            patch("server.chat.settings", fast_settings),
            patch("server.chat.claim", AsyncMock(return_value=None)),
            patch("server.chat.complete", AsyncMock()),
            patch("server.chat.supervisor_client.invoke", delayed),
            TestClient(app) as client,
        ):
            response = client.post(
                "/api/chat/stream", headers=HEADERS,
                json={"message": "Analise", "conversation_id": "stream-1"},
            )
        body = response.text
        self.assertLess(body.index("event: accepted"), body.index("event: heartbeat"))
        self.assertIn("event: delta", body)
        self.assertIn("event: evidence", body)
        self.assertIn("event: completed", body)
        self.assertIn('"stage": "request_accepted"', body)
        self.assertIn('"stage": "awaiting_supervisor"', body)
        self.assertIn('"elapsed_seconds":', body)

    def test_response_language_is_independent_from_market(self):
        context = trusted_geo("EC", "pt-BR")
        self.assertEqual(context, ("EC", "pt-BR"))

        context = trusted_geo("BR", "es-MX")
        self.assertEqual(context, ("BR", "es-MX"))

    def test_regional_analytics_returns_governed_metric_view_rows(self):
        payload = {
            "statement_id": "statement-1",
            "manifest": {"schema": {"columns": [
                {"name": "country_code"}, {"name": "gross_volume_usd"},
                {"name": "total_events"}, {"name": "completion_rate_pct"},
                {"name": "fraud_rate_pct"},
            ]}},
            "result": {"data_array": [["BR", "100.5", "20", "91.2", "0.01"]]},
        }
        with (
            patch("server.analytics._statement", AsyncMock(return_value=payload)),
            TestClient(app) as client,
        ):
            response = client.get("/api/analytics/regional", headers=HEADERS)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["countries"][0]["country_code"], "BR")
        self.assertEqual(response.json()["gross_volume_usd"], 100.5)

    def test_recommendations_expose_data_model_and_external_signal_without_fake_scoring(self):
        columns = [
            "offer_country", "offer_type", "offer_observations", "eligibility_score",
            "acceptance_rate", "offer_margin", "offers_as_of", "position", "payments_country",
            "declined_events", "declined_volume", "payments_as_of", "macro_country",
            "observation_date", "policy_rate_pct", "inflation_yoy_pct", "fx_per_usd",
            "stress_regime", "freshness", "offer_model", "payments_model", "macro_model",
        ]
        values = [
            "MX", "receivables", "12000", "0.82", "0.61", "114.2", "2026-08-31", "1", "BR",
            "340", "8700", "2026-08-31", "EC", "2026-08-31", "7.5", "2.2",
            "1.0", "stagflation", "1",
            "catalog.quilla_ml.quilla_next_best_action@3",
            "catalog.quilla_ml.quilla_cashflow_forecast@2",
            "catalog.quilla_ml.quilla_macro_regime@4",
        ]
        payload = {
            "statement_id": "statement-recommendations",
            "manifest": {"schema": {"columns": [{"name": name} for name in columns]}},
            "result": {"data_array": [values]},
        }
        with (
            patch("server.analytics._statement", AsyncMock(return_value=payload)),
            TestClient(app) as client,
        ):
            response = client.get(
                "/api/analytics/recommendations?country_code=REG&locale=es-MX",
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["contract"], "decision-candidate-v2")
        self.assertEqual(len(body["recommendations"]), 3)
        first = body["recommendations"][0]
        self.assertIn("Prioriza", first["title"])
        self.assertEqual(first["external_signal"]["tool"], "system.ai.web_search")
        self.assertEqual(first["model_status"], "validated_candidate")
        self.assertFalse(first["online_scoring"])
        explanation = first["explanation"]
        self.assertEqual(explanation["version"], "recommendation-explanation-v1")
        self.assertEqual(len(explanation["determining_signals"]), 4)
        self.assertEqual(explanation["provenance"]["statement_id"], "statement-recommendations")
        self.assertEqual(explanation["model"]["version"], "3")
        self.assertIn("causalidad", explanation["limitations"][0])
        self.assertEqual([item["key"] for item in explanation["alternatives"]], ["no_action", "educate", "protect", "offer"])

    def test_recommendations_reject_unknown_country(self):
        with TestClient(app) as client:
            response = client.get(
                "/api/analytics/recommendations?country_code=ZZ",
                headers=HEADERS,
            )
        self.assertEqual(response.status_code, 422)

    def test_business_overview_combines_three_governed_metric_view_queries(self):
        def payload(statement_id, columns, values):
            return {
                "statement_id": statement_id,
                "manifest": {"schema": {"columns": [{"name": item} for item in columns]}},
                "result": {"data_array": values},
            }

        async def statement(sql, _authorization):
            if "WITH finance AS" in sql:
                return payload("kpi", [
                    "gross_volume_usd", "net_flow_usd", "completion_rate_pct",
                    "declined_volume_usd", "revenue_usd_90d", "margin_usd_90d",
                ], [["1000", "250", "97.5", "30", "120", "42"]])
            if "GROUP BY event_date" in sql:
                return payload("trend", ["event_date", "net_flow_usd", "gross_volume_usd"], [
                    ["2026-09-02", "20", "100"], ["2026-09-01", "10", "90"],
                ])
            return payload("receivables", ["country_code", "declined_volume_usd", "declined_events"], [
                ["BR", "30", "3"],
            ])

        with (
            patch("server.analytics._statement", side_effect=statement),
            TestClient(app) as client,
        ):
            response = client.get("/api/analytics/business?country_code=BR&locale=es-MX", headers=HEADERS)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["scope"], "BR")
        self.assertEqual(body["locale"], "es-419")
        self.assertEqual(body["kpis"]["completion_rate_pct"], 97.5)
        self.assertEqual(body["trend"][0]["event_date"], "2026-09-01")
        self.assertEqual(body["statement_ids"], ["kpi", "trend", "receivables"])

    def test_decision_action_scores_growth_and_persists_auditable_evidence(self):
        captured = {}

        class Transaction:
            async def __aenter__(self): return self
            async def __aexit__(self, *_args): return False

        class Connection:
            def transaction(self): return Transaction()

            async def fetchrow(self, _query, request_id, country_code, decision_type, status, actor,
                               _key, _payload, _scores, _policy):
                captured["request_payload"] = json.loads(_payload)
                return {
                    "request_id": request_id, "country_code": country_code,
                    "decision_type": decision_type, "status": status, "requested_by": actor,
                    "created_at": "2026-09-03T12:00:00Z", "decided_at": "2026-09-03T12:00:00Z",
                }

            async def execute(self, *_args): return "INSERT 0 1"

        @asynccontextmanager
        async def connection():
            yield Connection()

        score = {"status": "scored", "prediction": "increase_limit", "endpoint": "quilla-next-best-action"}
        with (
            patch("server.platform.db.connection", connection),
            patch("server.platform._cohort_score", AsyncMock(return_value=score)),
            TestClient(app) as client,
        ):
            response = client.post(
                "/api/platform/decisions",
                headers={**HEADERS, "X-Idempotency-Key": "recommendation-action"},
                json={
                    "recommendation_id": "growth-BR-1", "country_code": "BR",
                    "decision_type": "growth", "selected_alternative": "offer",
                    "selected_alternative_label": "Ofertar",
                    "selected_alternative_rationale": "Elegibilidade e adequação validadas.",
                    "action": "approve", "reason": "Margem validada",
                },
            )
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertTrue(body["persisted"])
        self.assertEqual(body["model"]["prediction"], "increase_limit")
        self.assertEqual(body["selected_alternative"], "offer")
        self.assertEqual(captured["request_payload"]["selected_alternative"], "offer")
        self.assertEqual(captured["request_payload"]["selected_alternative_label"], "Ofertar")
        self.assertTrue(body["policy"]["human_initiated"])

    def test_decision_action_requires_an_explicit_alternative(self):
        with TestClient(app) as client:
            response = client.post(
                "/api/platform/decisions",
                headers={**HEADERS, "X-Idempotency-Key": "missing-alternative"},
                json={
                    "recommendation_id": "growth-BR-1", "country_code": "BR",
                    "decision_type": "growth", "action": "approve",
                },
            )
        self.assertEqual(response.status_code, 422)

    def test_cohort_score_uses_bound_endpoint_identity_and_returns_safe_feature_evidence(self):
        from server import platform

        columns = [
            "active_days_30d", "transaction_count_30d", "debit_volume_usd_30d",
            "credit_volume_usd_30d", "average_balance_usd_30d", "product_count",
            "interaction_count_30d", "complaint_count_90d", "digital_engagement_score",
            "country_code", "treatment_group",
        ]
        payload = {
            "statement_id": "feature-statement",
            "manifest": {"schema": {"columns": [{"name": name} for name in columns]}},
            "result": {"data_array": [["20", "45", "3500", "4200", "10000", "2", "3", "0", "0.75", "BR", "control"]]},
        }
        request = AsyncMock(return_value={"predictions": ["investment"]})
        with (
            patch("server.platform._statement", AsyncMock(return_value=payload)) as statement,
            patch("server.platform._workspace_request", request),
        ):
            score = asyncio.run(platform._cohort_score("BR", "Bearer user"))
        self.assertEqual(score["status"], "scored")
        self.assertEqual(score["prediction"], "investment")
        self.assertEqual(score["feature_snapshot"]["country_code"], "BR")
        self.assertNotIn("party_id", score["feature_snapshot"])
        self.assertEqual(score["authentication_scope"], "app_service_principal")
        self.assertEqual(statement.await_args.args[1], "Bearer user")
        self.assertIsNone(request.await_args.kwargs["user_authorization"])

    def test_realtime_control_room_starts_and_reads_job_without_validator_job(self):
        job_run = {
            "run_id": 123, "run_name": "quilla realtime",
            "start_time": 1000, "end_time": 2000,
            "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
            "tasks": [{
                "task_key": "bronze_realtime", "run_id": 124,
                "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
            }],
        }
        evidence = {
            "statement_id": "realtime-evidence",
            "manifest": {"schema": {"columns": [{"name": "simulation_id"}, {"name": "status"}]}},
            "result": {"data_array": [["123", "PASSED"]]},
        }
        with (
            patch("server.platform.REALTIME_JOB_ID", "42"),
            patch("server.platform._workspace_request", AsyncMock(side_effect=[{"run_id": 123}, job_run])),
            patch("server.platform._statement", AsyncMock(return_value=evidence)),
            TestClient(app) as client,
        ):
            started = client.post(
                "/api/platform/realtime", headers=HEADERS,
                json={"event_count": 500, "scenario": "mixed"},
            )
            status = client.get("/api/platform/realtime/123", headers=HEADERS)
        self.assertEqual(started.status_code, 200)
        self.assertEqual(started.json()["run_id"], 123)
        self.assertEqual(status.status_code, 200)
        self.assertEqual(status.json()["state"], "SUCCESS")
        self.assertEqual(status.json()["evidence"]["status"], "PASSED")

    def test_operations_overview_exposes_models_quality_realtime_and_lakeview(self):
        columns = [
            "total_models", "validated_models", "serving_endpoints", "ready_endpoints",
            "serving_validated_at", "datasets", "reconciliation_rate_pct", "quarantine_rows",
            "acceptance_rate_pct", "simulation_id", "scenario", "expected_events",
            "zerobus_distinct_events", "bronze_distinct_events", "silver_distinct_events",
            "quarantine_distinct_events", "reconciled_distinct_events", "realtime_status",
            "realtime_validated_at",
        ]
        values = [
            "22", "22", "6", "6", "2026-09-03", "17", "100", "13815698", "53.5739",
            "123", "mixed", "1000", "1000", "1000", "963", "37", "1000", "PASSED", "2026-09-03",
        ]
        sql_payload = {
            "statement_id": "operations",
            "manifest": {"schema": {"columns": [{"name": name} for name in columns]}},
            "result": {"data_array": [values]},
        }
        with (
            patch("server.platform._statement", AsyncMock(return_value=sql_payload)),
            patch("server.platform._workspace_request", AsyncMock(return_value={"state": {"ready": "READY"}})),
            patch("server.platform._workspace_auth", return_value=("https://workspace.example", "Bearer token")),
            TestClient(app) as client,
        ):
            response = client.get("/api/platform/operations", headers=HEADERS)
        self.assertEqual(response.status_code, 200)
        body = response.json()
        self.assertEqual(body["models"]["validated"], 22)
        self.assertEqual(body["data_quality"]["reconciled_datasets"], 17)
        self.assertEqual(body["data_quality"]["reconciliation_state"], "operational")
        self.assertEqual(body["data_quality"]["acceptance_state"], "requires_review")
        self.assertIsNone(body["data_quality"]["acceptance_target_pct"])
        dqx = next(control for control in body["controls"] if control["key"] == "dqx")
        self.assertEqual(dqx["state"], "requires_review")
        self.assertEqual(body["realtime"]["realtime_status"], "PASSED")
        self.assertEqual(len(body["dashboards"]), 4)
        self.assertTrue(body["dashboards"][0]["url"].endswith("/published"))

    def test_customer_360_returns_real_governed_identity_without_contact_fields(self):
        columns = [
            "party_id", "display_name", "country_code", "locale_code", "preferred_language",
            "customer_segment", "risk_band", "kyc_status", "onboarding_ts", "account_count",
            "current_balance_local", "available_balance_local", "last_account_update_at",
            "lifetime_event_count", "lifetime_volume_usd", "last_event_at",
            "average_event_risk_score", "risk_alert_count", "open_risk_alert_count",
            "last_alert_at", "interaction_count", "average_sentiment_score", "last_interaction_at",
        ]
        values = [
            "QPTY-000000086702", "Estefany Mondragón", "MX", "es-MX", "es",
            "mass_market", "low", "verified", "2025-01-10", "1", "14912.25",
            "12377.17", "2026-08-31", "223", "15834.92", "2026-08-30",
            "0.18", "2", "0", "2026-07-01", "8", "0.42", "2026-08-20",
        ]
        payload = {
            "statement_id": "statement-customer",
            "manifest": {"schema": {"columns": [{"name": name} for name in columns]}},
            "result": {"data_array": [values]},
        }
        with (
            patch("server.analytics._statement", AsyncMock(return_value=payload)),
            TestClient(app) as client,
        ):
            response = client.get("/api/analytics/customers?country_code=MX", headers=HEADERS)
        self.assertEqual(response.status_code, 200)
        customer = response.json()["customers"][0]
        self.assertEqual(customer["party_id"], "QPTY-000000086702")
        self.assertEqual(customer["currency_code"], "MXN")
        self.assertNotIn("email", customer)
        self.assertNotIn("phone", customer)

    def test_trust_status_distinguishes_operational_and_configured(self):
        with (
            patch("server.analytics.db.ping", AsyncMock(return_value=True)),
            patch("server.analytics._serving_state", AsyncMock(return_value=("operational", "READY"))),
            TestClient(app) as client,
        ):
            response = client.get("/api/analytics/trust", headers=HEADERS)
        self.assertEqual(response.status_code, 200)
        states = {item["key"]: item["state"] for item in response.json()["controls"]}
        self.assertEqual(states["lakebase"], "operational")
        self.assertEqual(states["supervisor"], "operational")


class PoolTests(unittest.IsolatedAsyncioTestCase):
    async def test_authentication_failure_refreshes_before_retry(self):
        database = LakebasePool()
        stale = SimpleNamespace(acquire=AsyncMock(side_effect=RuntimeError("password authentication failed")))
        connection = object()
        fresh = SimpleNamespace(
            acquire=AsyncMock(return_value=connection),
            release=AsyncMock(),
        )
        database.pool = AsyncMock(side_effect=[stale, fresh])
        database.refresh = AsyncMock()
        async with database.connection() as acquired:
            self.assertIs(acquired, connection)
        database.refresh.assert_awaited_once()
        fresh.release.assert_awaited_once_with(connection)


class IdempotencyTests(unittest.TestCase):
    def test_jsonb_string_is_decoded_on_replay(self):
        self.assertEqual(
            decode_response_payload('{"conversation_id":"conversation-1"}'),
            {"conversation_id": "conversation-1"},
        )


if __name__ == "__main__":
    unittest.main()
