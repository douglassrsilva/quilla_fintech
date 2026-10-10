"""Operações governadas do App: decisões, realtime, observabilidade e dashboards."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import json
import os
from time import perf_counter
from typing import Any, Literal
import uuid

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Query
from pydantic import BaseModel, ConfigDict, Field

from .analytics import CATALOG, COUNTRY_CODES, GOLD_SCHEMA, ML_SCHEMA, _rows, _statement
from .db import DatabaseUnavailable, db
from .security import ActorContext, trusted_actor, trusted_user_authorization


router = APIRouter(prefix="/api/platform", tags=["platform"])
REALTIME_JOB_ID = os.getenv("QUILLA_REALTIME_JOB_ID", "")
NEXT_BEST_ACTION_ENDPOINT = os.getenv("QUILLA_NEXT_BEST_ACTION_ENDPOINT", "quilla-next-best-action")
DASHBOARD_IDS = {
    "executive": os.getenv("QUILLA_DASHBOARD_EXECUTIVE", ""),
    "growth": os.getenv("QUILLA_DASHBOARD_GROWTH", ""),
    "wealth": os.getenv("QUILLA_DASHBOARD_WEALTH", ""),
    "risk": os.getenv("QUILLA_DASHBOARD_RISK", ""),
}


def _workspace_auth() -> tuple[str, str]:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    host = client.config.host.rstrip("/")
    authorization = client.config.authenticate().get("Authorization", "")
    if not host or not authorization:
        raise RuntimeError("Databricks authentication unavailable")
    return host, authorization


async def _workspace_request(
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
    user_authorization: str | None = None,
    timeout: float = 20.0,
) -> dict[str, Any]:
    host, service_authorization = await asyncio.to_thread(_workspace_auth)
    authorization = user_authorization or service_authorization
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout, connect=5.0)) as client:
        response = await client.request(
            method,
            f"{host}{path}",
            headers={"Authorization": authorization},
            json=body,
        )
        response.raise_for_status()
        return response.json()


def _dashboard_url(host: str, dashboard_id: str) -> str:
    return f"{host}/dashboardsv3/{dashboard_id}/published"


class DecisionActionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    recommendation_id: str = Field(min_length=3, max_length=180)
    country_code: str = Field(min_length=2, max_length=2)
    decision_type: Literal["growth", "payments", "risk"]
    selected_alternative: Literal["no_action", "educate", "protect", "offer"]
    selected_alternative_label: str = Field(min_length=2, max_length=120)
    selected_alternative_rationale: str = Field(min_length=3, max_length=1000)
    action: Literal["approve", "reject", "defer"]
    reason: str | None = Field(default=None, max_length=1000)


class RealtimeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_count: int = Field(default=500, ge=100, le=5000)
    scenario: Literal["normal", "fraud_burst", "cross_border", "dq_violations", "mixed"] = "mixed"


async def _cohort_score(country_code: str, user_authorization: str | None) -> dict[str, Any]:
    """Executa o Champion NBA sobre um registro sintético de coorte, nunca sobre PII."""
    feature_sql = f"""
      SELECT active_days_30d, transaction_count_30d, debit_volume_usd_30d,
        credit_volume_usd_30d, average_balance_usd_30d, product_count,
        interaction_count_30d, complaint_count_90d, digital_engagement_score,
        country_code, treatment_group
      FROM {CATALOG}.{ML_SCHEMA}.source_customer_lifecycle_monthly
      WHERE country_code = '{country_code}'
      ORDER BY observation_month DESC, clv_usd_12m DESC
      LIMIT 1
    """
    feature_payload = await _statement(feature_sql, user_authorization)
    feature_rows = _rows(feature_payload)
    if not feature_rows:
        raise RuntimeError("No synthetic cohort feature row")
    features = feature_rows[0]
    for key in (
        "active_days_30d", "transaction_count_30d", "product_count",
        "interaction_count_30d", "complaint_count_90d",
    ):
        features[key] = int(features[key] or 0)
    for key in (
        "debit_volume_usd_30d", "credit_volume_usd_30d",
        "average_balance_usd_30d", "digital_engagement_score",
    ):
        features[key] = float(features[key] or 0)
    started_at = perf_counter()
    response = await _workspace_request(
        "POST",
        f"/serving-endpoints/{NEXT_BEST_ACTION_ENDPOINT}/invocations",
        body={"dataframe_records": [features]},
        # The endpoint is an App resource. Scoring uses the bound service identity;
        # the initiating human and OBO-filtered SQL statement remain in the audit trail.
        user_authorization=None,
        timeout=30.0,
    )
    elapsed_ms = round((perf_counter() - started_at) * 1000)
    predictions = response.get("predictions") or []
    prediction = predictions[0] if predictions else None
    return {
        "status": "scored" if prediction is not None else "empty",
        "prediction": prediction,
        "endpoint": NEXT_BEST_ACTION_ENDPOINT,
        "feature_source": f"{CATALOG}.{ML_SCHEMA}.source_customer_lifecycle_monthly",
        "statement_id": feature_payload.get("statement_id"),
        "feature_snapshot": features,
        "input_fields": list(features),
        "latency_ms": elapsed_ms,
        "scored_at": datetime.now(timezone.utc).isoformat(),
        "authentication_scope": "app_service_principal",
        "scope": "synthetic_cohort",
    }


@router.post("/decisions")
async def create_decision_action(
    body: DecisionActionRequest,
    actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
    idempotency_key: str | None = Header(default=None, alias="X-Idempotency-Key"),
):
    country_code = body.country_code.upper()
    if country_code not in COUNTRY_CODES - {"REG"}:
        raise HTTPException(status_code=422, detail="Decision requires one supported country")
    key = (idempotency_key or str(uuid.uuid4()))[:160]
    scoped_key = f"{actor.tenant_id}:{actor.actor_id}:{key}"
    request_id = uuid.uuid5(uuid.NAMESPACE_URL, scoped_key)
    action_id = uuid.uuid5(request_id, f"{body.action}:{body.selected_alternative}")
    status_by_action = {"approve": "approved", "reject": "declined", "defer": "manual_review"}
    model_scores: dict[str, Any] = {
        "status": "not_required",
        "reason": "Online next-best-action scoring only runs when an offer alternative is approved",
    }
    if (
        body.decision_type == "growth"
        and body.action == "approve"
        and body.selected_alternative == "offer"
    ):
        try:
            model_scores = await _cohort_score(country_code, user_authorization)
        except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
            model_scores = {
                "status": "unavailable",
                "error": type(exc).__name__,
                "endpoint": NEXT_BEST_ACTION_ENDPOINT,
                "scope": "synthetic_cohort",
            }
    request_payload = {
        "recommendation_id": body.recommendation_id,
        "decision_type": body.decision_type,
        "selected_alternative": body.selected_alternative,
        "selected_alternative_label": body.selected_alternative_label,
        "selected_alternative_rationale": body.selected_alternative_rationale,
        "action": body.action,
        "reason": body.reason,
        "synthetic": True,
    }
    policy_snapshot = {
        "human_initiated": True,
        "human_review_required": body.action == "defer" or body.decision_type in {"risk"},
        "data_classification": "synthetic_demo_only",
        "model_endpoint": model_scores.get("endpoint"),
        "governance": ["Unity Catalog", "MLflow", "Unity AI Gateway"],
    }
    action_evidence = {
        "model": model_scores,
        "recommendation_id": body.recommendation_id,
        "selected_alternative": {
            "key": body.selected_alternative,
            "label": body.selected_alternative_label,
            "rationale": body.selected_alternative_rationale,
        },
        "policy": policy_snapshot,
    }
    try:
        async with db.connection() as connection:
            async with connection.transaction():
                row = await connection.fetchrow(
                    """
                    INSERT INTO quilla_app.decision_requests (
                      request_id, country_code, decision_type, status, requested_by,
                      idempotency_key, request_payload, model_scores, policy_snapshot, decided_at
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,now())
                    ON CONFLICT (idempotency_key) DO UPDATE
                      SET idempotency_key = EXCLUDED.idempotency_key
                    RETURNING request_id, country_code, decision_type, status, requested_by, created_at, decided_at
                    """,
                    request_id, country_code, body.decision_type, status_by_action[body.action],
                    actor.actor_id, scoped_key, json.dumps(request_payload), json.dumps(model_scores),
                    json.dumps(policy_snapshot),
                )
                await connection.execute(
                    """
                    INSERT INTO quilla_app.decision_actions (
                      action_id, request_id, action_type, outcome, reason_code,
                      explanation, executed_by, evidence
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
                    ON CONFLICT (action_id) DO NOTHING
                    """,
                    action_id, request_id, body.action, status_by_action[body.action],
                    body.decision_type, body.reason, actor.actor_id, json.dumps(action_evidence),
                )
                await connection.execute(
                    """
                    INSERT INTO quilla_app.outbox_events (
                      event_id, aggregate_type, aggregate_id, event_type, payload
                    ) VALUES ($1,'decision',$2,'quilla.decision.recorded',$3::jsonb)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    uuid.uuid5(action_id, "outbox"), str(request_id),
                    json.dumps({
                        "action": body.action,
                        "selected_alternative": body.selected_alternative,
                        "country_code": country_code,
                    }),
                )
    except DatabaseUnavailable as exc:
        raise HTTPException(status_code=503, detail="Decision store unavailable") from exc
    return {
        **dict(row),
        "action_id": str(action_id),
        "action": body.action,
        "selected_alternative": body.selected_alternative,
        "model": model_scores,
        "policy": policy_snapshot,
        "persisted": True,
    }


def _job_state(payload: dict[str, Any]) -> dict[str, Any]:
    state = payload.get("state") or {}
    tasks = []
    for task in payload.get("tasks") or []:
        task_state = task.get("state") or {}
        tasks.append({
            "task_key": task.get("task_key"),
            "run_id": task.get("run_id"),
            "life_cycle_state": task_state.get("life_cycle_state") or "PENDING",
            "result_state": task_state.get("result_state"),
        })
    return {
        "run_id": payload.get("run_id"),
        "run_name": payload.get("run_name"),
        "life_cycle_state": state.get("life_cycle_state") or "PENDING",
        "result_state": state.get("result_state"),
        "state": state.get("result_state") or state.get("life_cycle_state") or "PENDING",
        "state_message": state.get("state_message"),
        "started_at": payload.get("start_time"),
        "completed_at": payload.get("end_time"),
        "tasks": tasks,
    }


@router.post("/realtime")
async def start_realtime(
    body: RealtimeRequest,
    _actor: ActorContext = Depends(trusted_actor),
):
    if not REALTIME_JOB_ID:
        raise HTTPException(status_code=503, detail="Realtime job is not bound to the App")
    try:
        payload = await _workspace_request(
            "POST",
            "/api/2.1/jobs/run-now",
            body={
                "job_id": int(REALTIME_JOB_ID),
                "job_parameters": {
                    "event_count": str(body.event_count),
                    "scenario": body.scenario,
                    "seed": "42",
                },
            },
            timeout=15.0,
        )
    except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=502, detail=f"Could not start realtime simulation: {type(exc).__name__}") from exc
    return {
        "run_id": payload.get("run_id"),
        "state": "PENDING",
        "life_cycle_state": "PENDING",
        "result_state": None,
        "tasks": [],
        "scenario": body.scenario,
        "event_count": body.event_count,
        "synthetic": True,
    }


@router.get("/realtime/{run_id}")
async def realtime_status(
    run_id: int,
    _actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
):
    if run_id <= 0:
        raise HTTPException(status_code=422, detail="Invalid run_id")
    try:
        payload = await _workspace_request("GET", f"/api/2.1/jobs/runs/get?run_id={run_id}", timeout=15.0)
        result = _job_state(payload)
        result["evidence"] = None
        if result["life_cycle_state"] == "TERMINATED":
            evidence_payload = await _statement(
                f"""
                SELECT simulation_id, scenario, expected_events, zerobus_rows,
                  zerobus_distinct_events, bronze_distinct_events, silver_distinct_events,
                  quarantine_distinct_events, reconciled_distinct_events, status, validated_at
                FROM {CATALOG}.{GOLD_SCHEMA}.realtime_demo_validation_runs
                WHERE simulation_id = '{run_id}'
                ORDER BY validated_at DESC LIMIT 1
                """,
                user_authorization,
            )
            evidence_rows = _rows(evidence_payload)
            result["evidence"] = evidence_rows[0] if evidence_rows else None
    except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=502, detail=f"Could not read realtime simulation: {type(exc).__name__}") from exc
    return result


@router.get("/operations")
async def operations_overview(
    _actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
):
    statement = f"""
      WITH models AS (
        SELECT COUNT(*) AS total_models,
          SUM(CASE WHEN status = 'PASSED' THEN 1 ELSE 0 END) AS validated_models
        FROM {CATALOG}.{ML_SCHEMA}.model_validation_evidence
      ), serving AS (
        SELECT COUNT(*) AS serving_endpoints,
          SUM(CASE WHEN status = 'READY' THEN 1 ELSE 0 END) AS ready_endpoints,
          MAX(validated_at) AS serving_validated_at
        FROM {CATALOG}.{ML_SCHEMA}.model_serving_evidence
      ), quality AS (
        SELECT MEASURE(datasets) AS datasets,
          100 * MEASURE(reconciliation_rate) AS reconciliation_rate_pct,
          MEASURE(quarantine_rows) AS quarantine_rows,
          100 * MEASURE(global_acceptance_rate) AS acceptance_rate_pct
        FROM {CATALOG}.{GOLD_SCHEMA}.metrics_data_quality
      ), realtime AS (
        SELECT simulation_id, scenario, expected_events, zerobus_distinct_events,
          bronze_distinct_events, silver_distinct_events, quarantine_distinct_events,
          reconciled_distinct_events, status AS realtime_status, validated_at AS realtime_validated_at
        FROM {CATALOG}.{GOLD_SCHEMA}.realtime_demo_validation_runs
        ORDER BY validated_at DESC LIMIT 1
      )
      SELECT * FROM models CROSS JOIN serving CROSS JOIN quality CROSS JOIN realtime
    """
    try:
        sql_payload, serving_payload = await asyncio.gather(
            _statement(statement, user_authorization),
            _workspace_request("GET", f"/api/2.0/serving-endpoints/{NEXT_BEST_ACTION_ENDPOINT}", timeout=10.0),
        )
        rows = _rows(sql_payload)
        if not rows:
            raise RuntimeError("No platform observability evidence")
        row = rows[0]
        endpoint_ready = ((serving_payload.get("state") or {}).get("ready") or "UNKNOWN").upper()
        host, _ = await asyncio.to_thread(_workspace_auth)
    except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=503, detail=f"Platform observability unavailable: {type(exc).__name__}") from exc

    models = {
        "total": int(row.get("total_models") or 0),
        "validated": int(row.get("validated_models") or 0),
        "serving_endpoints": int(row.get("serving_endpoints") or 0),
        "ready": int(row.get("ready_endpoints") or 0),
        "online_endpoint": endpoint_ready,
        "validated_at": row.get("serving_validated_at"),
    }
    checked_at = datetime.now(timezone.utc).isoformat()
    reconciliation_rate_pct = float(row.get("reconciliation_rate_pct") or 0)
    acceptance_rate_pct = float(row.get("acceptance_rate_pct") or 0)
    data_quality = {
        "datasets": int(row.get("datasets") or 0),
        "reconciled_datasets": round(
            int(row.get("datasets") or 0) * reconciliation_rate_pct / 100
        ),
        "reconciliation_rate_pct": reconciliation_rate_pct,
        "reconciliation_state": "operational" if reconciliation_rate_pct == 100 else "degraded",
        "quarantine_rows": int(row.get("quarantine_rows") or 0),
        "quarantine_scope": "current_materialized_snapshot",
        "acceptance_rate_pct": acceptance_rate_pct,
        "acceptance_state": "requires_review",
        "acceptance_target_pct": None,
        "acceptance_target_status": "not_defined",
        "metric_source": f"{CATALOG}.{GOLD_SCHEMA}.metrics_data_quality",
        "observed_at": checked_at,
    }
    realtime = {
        key: row.get(key)
        for key in (
            "simulation_id", "scenario", "expected_events", "zerobus_distinct_events",
            "bronze_distinct_events", "silver_distinct_events", "quarantine_distinct_events",
            "reconciled_distinct_events", "realtime_status", "realtime_validated_at",
        )
    }
    dashboards = [
        {"key": "executive", "title_pt": "Centro Executivo LATAM", "title_es": "Centro Ejecutivo LATAM", "url": _dashboard_url(host, DASHBOARD_IDS["executive"])},
        {"key": "growth", "title_pt": "Growth & Profitabilidade", "title_es": "Growth & Rentabilidad", "url": _dashboard_url(host, DASHBOARD_IDS["growth"])},
        {"key": "wealth", "title_pt": "Wealth & Macro Stress", "title_es": "Wealth & Estrés Macro", "url": _dashboard_url(host, DASHBOARD_IDS["wealth"])},
        {"key": "risk", "title_pt": "Risco, Crédito & Confiança", "title_es": "Riesgo, Crédito y Confianza", "url": _dashboard_url(host, DASHBOARD_IDS["risk"])},
    ]
    controls = [
        {"key": "mlflow", "state": "operational" if models["validated"] == models["total"] else "degraded", "evidence": f"{models['validated']}/{models['total']} modelos validados"},
        {"key": "serving", "state": "operational" if endpoint_ready == "READY" else "degraded", "evidence": f"{models['ready']}/{models['serving_endpoints']} endpoints · NBA {endpoint_ready}"},
        {
            "key": "dqx",
            "state": "requires_review" if data_quality["reconciliation_state"] == "operational" else "degraded",
            "evidence": (
                f"Reconciliação {reconciliation_rate_pct:.1f}% · aceitação {acceptance_rate_pct:.1f}% "
                "· meta contratual não definida"
            ),
        },
        {"key": "realtime", "state": "operational" if realtime["realtime_status"] == "PASSED" else "degraded", "evidence": f"Zerobus {realtime['simulation_id'] or 'sem evidência'}"},
        {"key": "lakeview", "state": "configured", "evidence": f"{len(dashboards)} dashboards publicados"},
        {"key": "lineage", "state": "configured", "evidence": "Unity Catalog: Bronze → Silver DQX → Gold → Metric Views"},
    ]
    return {
        "checked_at": checked_at,
        "source": f"{CATALOG}.{ML_SCHEMA} + {CATALOG}.{GOLD_SCHEMA}",
        "statement_id": sql_payload.get("statement_id"),
        "controls": controls,
        "models": models,
        "data_quality": data_quality,
        "realtime": realtime,
        "dashboards": dashboards,
        "synthetic": True,
    }
