#!/usr/bin/env python3
"""Valida diretamente as APIs analíticas usadas pelo App; não cria Job."""

from __future__ import annotations

import argparse
import json
import subprocess
import urllib.request


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--url", required=True)
    args = parser.parse_args()
    token = json.loads(subprocess.run(
        ["databricks", "auth", "token", "--profile", args.profile],
        check=True, capture_output=True, text=True,
    ).stdout)["access_token"]

    def get(path: str):
        request = urllib.request.Request(
            f"{args.url}{path}",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=70) as response:
            return json.load(response)

    regional = get("/api/analytics/regional")
    recommendations = get("/api/analytics/recommendations?country_code=REG&locale=pt-BR")
    customers = get("/api/analytics/customers?country_code=REG&limit=12")
    trust = get("/api/analytics/trust")
    genie_history = get("/api/conversations?limit=50&experience=genie_research")
    assert len(regional["countries"]) == 12, regional
    assert regional["gross_volume_usd"] > 0, regional
    assert regional["statement_id"], regional
    assert recommendations["contract"] == "decision-candidate-v2", recommendations
    assert len(recommendations["recommendations"]) == 3, recommendations
    assert all(item["internal_source"] for item in recommendations["recommendations"]), recommendations
    assert all(item["external_signal"]["tool"] == "system.ai.web_search" for item in recommendations["recommendations"]), recommendations
    assert all(item["online_scoring"] is False for item in recommendations["recommendations"]), recommendations
    assert all(item["explanation"]["determining_signals"] for item in recommendations["recommendations"]), recommendations
    assert all(item["explanation"]["provenance"]["statement_id"] for item in recommendations["recommendations"]), recommendations
    assert all("causal" in " ".join(item["explanation"]["limitations"]).lower() for item in recommendations["recommendations"]), recommendations
    assert len(customers["customers"]) == 12, customers
    assert all(item["party_id"].startswith("QPTY-") for item in customers["customers"]), customers
    assert not any("email" in item or "phone" in item for item in customers["customers"]), customers
    states = {item["key"]: item["state"] for item in trust["controls"]}
    assert states["lakebase"] == "operational", states
    assert states["supervisor"] == "operational", states
    assert genie_history.get("conversations"), "No persisted Genie conversations found"
    print(json.dumps({
        "status": "PASSED",
        "regional_countries": len(regional["countries"]),
        "regional_statement_id": regional["statement_id"],
        "recommendation_statement_id": recommendations["statement_id"],
        "recommendation_candidates": len(recommendations["recommendations"]),
        "customer_360_profiles": len(customers["customers"]),
        "trust_states": states,
        "persisted_genie_conversations": len(genie_history["conversations"]),
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
