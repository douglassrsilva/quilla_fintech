#!/usr/bin/env python3
"""Patch supervisor Serving Endpoint environment without registering a model."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quilla_supervisor.auth import DatabricksAuth


def api(auth: DatabricksAuth, method: str, path: str, body: dict | None = None) -> dict:
    request = Request(
        f"{auth.host()}{path}",
        data=json.dumps(body).encode("utf-8") if body is not None else None,
        headers={**auth.headers(), "Content-Type": "application/json"},
        method=method,
    )
    with urlopen(request, timeout=120) as response:
        raw = response.read()
        return json.loads(raw) if raw else {}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--endpoint", default="quilla-custom-supervisor")
    parser.add_argument("--you-timeout", default="90")
    parser.add_argument("--you-daily-limit", default="6")
    parser.add_argument("--wait-seconds", type=int, default=1800)
    args = parser.parse_args()

    auth = DatabricksAuth(profile=args.profile)
    current = api(auth, "GET", f"/api/2.0/serving-endpoints/{args.endpoint}")
    config = current.get("config") or {}
    entities = config.get("served_entities") or []
    if len(entities) != 1:
        raise RuntimeError(f"Expected one served entity, found {len(entities)}")
    source = entities[0]
    environment = dict(source.get("environment_vars") or {})
    update_in_progress = (current.get("state") or {}).get("config_update") == "IN_PROGRESS"
    already_configured = update_in_progress or (
        environment.get("QUILLA_YOU_FINANCIAL_TIMEOUT_SECONDS") == args.you_timeout
        and environment.get("QUILLA_YOU_FINANCIAL_DAILY_CALL_LIMIT")
        == args.you_daily_limit
    )
    environment.update(
        {
            "QUILLA_YOU_FINANCIAL_TIMEOUT_SECONDS": args.you_timeout,
            "QUILLA_YOU_FINANCIAL_DAILY_CALL_LIMIT": args.you_daily_limit,
        }
    )
    entity = {
        "name": source["name"],
        "entity_name": source["entity_name"],
        "entity_version": source["entity_version"],
        "workload_size": source.get("workload_size", "Small"),
        "scale_to_zero_enabled": source.get("scale_to_zero_enabled", False),
        "environment_vars": environment,
    }
    traffic = config.get("traffic_config") or {
        "routes": [
            {"served_model_name": source["name"], "traffic_percentage": 100}
        ]
    }
    if not already_configured:
        api(
            auth,
            "PUT",
            f"/api/2.0/serving-endpoints/{args.endpoint}/config",
            {"served_entities": [entity], "traffic_config": traffic},
        )

    deadline = time.monotonic() + args.wait_seconds
    last = {}
    while time.monotonic() < deadline:
        last = api(auth, "GET", f"/api/2.0/serving-endpoints/{args.endpoint}")
        state = last.get("state") or {}
        if state.get("config_update") == "UPDATE_FAILED":
            raise RuntimeError(json.dumps(last, ensure_ascii=False)[:5000])
        if state.get("ready") == "READY" and state.get("config_update") in {
            "NOT_UPDATING",
            None,
        }:
            break
        time.sleep(15)
    else:
        raise TimeoutError(f"Endpoint update did not finish: {last.get('state')}")

    print(
        json.dumps(
            {
                "status": "READY",
                "endpoint": args.endpoint,
                "model_version": source["entity_version"],
                "you_financial_timeout_seconds": environment[
                    "QUILLA_YOU_FINANCIAL_TIMEOUT_SECONDS"
                ],
                "you_financial_daily_call_limit": environment[
                    "QUILLA_YOU_FINANCIAL_DAILY_CALL_LIMIT"
                ],
                "submitted_update": not already_configured,
                "state": last.get("state"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
