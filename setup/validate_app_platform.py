#!/usr/bin/env python3
"""Valida as seis jornadas de plataforma diretamente pela API do App, sem Job validador."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
import urllib.error
import urllib.request
import uuid


def token_for(profile: str) -> str:
    result = subprocess.run(
        ["databricks", "auth", "token", "--profile", profile],
        check=True, capture_output=True, text=True,
    )
    return json.loads(result.stdout)["access_token"]


def request(url: str, token: str, *, method: str = "GET", body: dict | None = None,
            headers: dict | None = None, timeout: int = 80) -> dict:
    call = urllib.request.Request(
        url,
        data=None if body is None else json.dumps(body).encode(),
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            **({"Content-Type": "application/json"} if body is not None else {}),
            **(headers or {}),
        },
    )
    try:
        with urllib.request.urlopen(call, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {url}: HTTP {exc.code}: {exc.read().decode()[:1600]}") from exc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--url", required=True)
    parser.add_argument("--run-realtime", action="store_true")
    parser.add_argument("--realtime-run-id", type=int, help="Retoma um run existente sem iniciar outro.")
    parser.add_argument("--realtime-timeout", type=int, default=900)
    args = parser.parse_args()
    token = token_for(args.profile)
    base = args.url.rstrip("/")

    business = request(f"{base}/api/analytics/business?country_code=BR&locale=pt-BR", token)
    recommendations = request(f"{base}/api/analytics/recommendations?country_code=BR&locale=pt-BR", token)
    operations = request(f"{base}/api/platform/operations", token)

    assert business["kpis"]["gross_volume_usd"] > 0, business
    assert len(business["trend"]) > 0, business
    assert business["statement_ids"] and all(business["statement_ids"]), business
    assert len(operations["dashboards"]) == 4, operations
    assert operations["models"]["validated"] == operations["models"]["total"], operations
    assert operations["data_quality"]["datasets"] == operations["data_quality"]["reconciled_datasets"], operations
    assert operations["data_quality"]["reconciliation_state"] == "operational", operations
    assert operations["data_quality"]["acceptance_state"] == "requires_review", operations
    assert operations["data_quality"]["acceptance_target_status"] == "not_defined", operations

    candidate = recommendations["recommendations"][0]
    decision_key = f"platform-smoke-{uuid.uuid4()}"
    decision = request(
        f"{base}/api/platform/decisions", token, method="POST",
        headers={"X-Idempotency-Key": decision_key},
        body={
            "recommendation_id": candidate["recommendation_id"],
            "country_code": candidate["country_code"],
            "decision_type": candidate["category"],
            "selected_alternative": "offer",
            "selected_alternative_label": "Ofertar",
            "selected_alternative_rationale": "Validação da alternativa auditável.",
            "action": "approve",
            "reason": "Validação direta da jornada governada do App.",
        },
    )
    assert decision["persisted"] is True, decision
    assert decision["selected_alternative"] == "offer", decision
    assert decision["policy"]["human_initiated"] is True, decision
    assert decision["model"]["status"] == "scored", decision
    assert decision["model"].get("feature_snapshot"), decision

    realtime = None
    if args.run_realtime or args.realtime_run_id:
        if args.realtime_run_id:
            run_id = args.realtime_run_id
            realtime = {"run_id": run_id, "state": "RESUMED"}
        else:
            realtime = request(
                f"{base}/api/platform/realtime", token, method="POST",
                body={"event_count": 100, "scenario": "mixed"},
            )
            run_id = int(realtime["run_id"])
        deadline = time.monotonic() + args.realtime_timeout
        while time.monotonic() < deadline:
            realtime = request(f"{base}/api/platform/realtime/{run_id}", token)
            if realtime["life_cycle_state"] == "TERMINATED":
                break
            time.sleep(10)
        assert realtime["result_state"] == "SUCCESS", realtime
        assert realtime.get("evidence", {}).get("status") == "PASSED", realtime

    print(json.dumps({
        "status": "PASSED",
        "business": {
            "scope": business["scope"],
            "trend_points": len(business["trend"]),
            "statement_ids": business["statement_ids"],
        },
        "decision": {
            "request_id": decision["request_id"],
            "action_id": decision["action_id"],
            "model_status": decision["model"]["status"],
            "selected_alternative": decision["selected_alternative"],
            "persisted": decision["persisted"],
        },
        "operations": {
            "models": operations["models"],
            "data_quality": operations["data_quality"],
            "realtime": operations["realtime"],
            "dashboards": len(operations["dashboards"]),
        },
        "realtime_run": realtime,
    }, ensure_ascii=False, indent=2, default=str))


if __name__ == "__main__":
    main()
