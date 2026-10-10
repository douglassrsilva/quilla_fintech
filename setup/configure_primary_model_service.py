#!/usr/bin/env python3
"""Atomically configure GLM primary + Qwen fallback on the governed Model Service."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quilla_supervisor.model_service_config import update_config


def auth(profile: str) -> tuple[str, str]:
    def cli(*args: str) -> dict:
        result = subprocess.run(
            ["databricks", *args, "--profile", profile, "-o", "json"],
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(result.stdout)

    token = cli("auth", "token")["access_token"]
    host = cli("auth", "env").get("env", {}).get("DATABRICKS_HOST")
    if not host:
        raise RuntimeError("Databricks profile did not expose DATABRICKS_HOST")
    return host.rstrip("/"), token


def request(host: str, token: str, method: str, path: str, body: dict | None = None) -> dict:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"{host}{path}",
        data=payload,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=120) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"{method} {path} returned HTTP {exc.code}: {detail}") from exc


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument(
        "--service",
        default="example_catalog.example_ai.quilla-supervisor-primary",
    )
    parser.add_argument("--primary", default="databricks-glm-5-3")
    parser.add_argument("--fallback", default="databricks-qwen35-122b-a10b")
    args = parser.parse_args()

    host, token = auth(args.profile)
    base = f"/api/2.1/unity-catalog/model-services/{args.service}"
    before = request(host, token, "GET", base)
    policies_before = {item.get("name") for item in before.get("config", {}).get("service_policies", [])}
    fallback_models = (args.fallback,) if args.fallback else ()
    desired = update_config(before, args.primary, fallback_models)
    request(
        host,
        token,
        "PATCH",
        f"{base}?{urllib.parse.urlencode({'update_mask': 'config'})}",
        {"config": desired},
    )
    after = request(host, token, "GET", base)
    routing = after.get("config", {}).get("routing", {})
    primary = [
        item.get("pay_per_token_config", {}).get("model")
        for item in routing.get("destinations", [])
    ]
    fallbacks = [
        item.get("pay_per_token_config", {}).get("model")
        for item in routing.get("fallback", {}).get("destinations", [])
    ]
    policies_after = {item.get("name") for item in after.get("config", {}).get("service_policies", [])}
    expected_primary = [f"models/system.ai.{args.primary}"]
    expected_fallback = [f"models/system.ai.{model}" for model in fallback_models]
    if primary != expected_primary or fallbacks != expected_fallback:
        raise RuntimeError(f"Routing verification failed: primary={primary}, fallbacks={fallbacks}")
    if policies_after != policies_before:
        raise RuntimeError(
            f"Service policies changed unexpectedly: before={sorted(policies_before)}, after={sorted(policies_after)}"
        )
    print(
        json.dumps(
            {
                "status": "READY",
                "service": args.service,
                "primary": primary,
                "fallbacks": fallbacks,
                "service_policies": sorted(policies_after),
            },
            ensure_ascii=False,
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
