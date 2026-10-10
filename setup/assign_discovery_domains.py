#!/usr/bin/env python3
"""Assign Quilla assets to published Discovery domains through public APIs.

The operation is idempotent. It supports local execution with a Databricks CLI
profile and execution as a DAB job using workload identity from the workspace.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import quote

from databricks.sdk import WorkspaceClient


ROOT = "Quilla"
CLIENT = f"{ROOT}/Cliente 360 y Crecimiento"
PAYMENTS = f"{ROOT}/Pagos y Tiempo Real"
CREDIT = f"{ROOT}/Crédito y Precios"
RISK = f"{ROOT}/Riesgo, Fraude y Cumplimiento"
WEALTH = f"{ROOT}/Inversiones y Patrimonio"
EXTERNAL_AI = f"{ROOT}/Inteligencia Externa e IA"
PLATFORM = f"{ROOT}/Plataforma, Gobierno y Calidad"

DOMAIN_TAGS = (ROOT, CLIENT, PAYMENTS, CREDIT, RISK, WEALTH, EXTERNAL_AI, PLATFORM)


@dataclass(frozen=True)
class CatalogAsset:
    entity_type: str
    relative_name: str
    domains: tuple[str, ...]


@dataclass(frozen=True)
class WorkspaceAsset:
    entity_type: str
    entity_id: str
    domains: tuple[str, ...]


CATALOG_ASSETS = (
    # Cliente 360 y Crecimiento
    CatalogAsset("tables", "quilla_gold.mv_customer_360", (CLIENT,)),
    CatalogAsset("tables", "quilla_gold.vw_growth_customer_monthly", (CLIENT,)),
    CatalogAsset("tables", "quilla_gold.metrics_growth_customer", (CLIENT,)),
    CatalogAsset("tables", "quilla_ml.fs_customer_monthly", (CLIENT,)),
    CatalogAsset("tables", "quilla_ml.fs_interaction_context", (CLIENT,)),
    # Pagos y Tiempo Real: representative end-to-end Zerobus lineage.
    CatalogAsset("tables", "quilla_bronze.zerobus_financial_events", (PAYMENTS,)),
    CatalogAsset("tables", "quilla_bronze.bronze_financial_events", (PAYMENTS,)),
    CatalogAsset("tables", "quilla_silver.financial_events", (PAYMENTS,)),
    CatalogAsset("tables", "quilla_gold.mv_financial_performance_daily", (PAYMENTS,)),
    CatalogAsset("tables", "quilla_gold.metrics_financial_performance", (PAYMENTS,)),
    CatalogAsset("tables", "quilla_ml.fs_financial_daily", (PAYMENTS,)),
    # Crédito y Precios
    CatalogAsset("tables", "quilla_gold.mv_credit_portfolio_monthly", (CREDIT,)),
    CatalogAsset("tables", "quilla_gold.vw_pricing_performance_daily", (CREDIT,)),
    CatalogAsset("tables", "quilla_gold.metrics_credit_portfolio", (CREDIT,)),
    CatalogAsset("tables", "quilla_gold.metrics_pricing_profitability", (CREDIT,)),
    CatalogAsset("tables", "quilla_ml.fs_credit_performance_monthly", (CREDIT,)),
    CatalogAsset("tables", "quilla_ml.fs_pricing_offer", (CREDIT,)),
    # Riesgo, Fraude y Cumplimiento
    CatalogAsset("tables", "quilla_gold.mv_risk_operations_daily", (RISK,)),
    CatalogAsset("tables", "quilla_gold.vw_case_outcomes_daily", (RISK,)),
    CatalogAsset("tables", "quilla_gold.metrics_risk_operations", (RISK,)),
    CatalogAsset("tables", "quilla_gold.metrics_case_outcomes", (RISK,)),
    CatalogAsset("tables", "quilla_ml.fs_transaction_context", (RISK,)),
    CatalogAsset("tables", "quilla_ml.fs_security_session", (RISK,)),
    # Inversiones y Patrimonio
    CatalogAsset("tables", "quilla_gold.vw_wealth_performance_daily", (WEALTH,)),
    CatalogAsset("tables", "quilla_gold.metrics_wealth_engagement", (WEALTH,)),
    CatalogAsset("tables", "quilla_gold.metrics_investor_suitability", (WEALTH,)),
    CatalogAsset("tables", "quilla_gold.metrics_macro_stress", (WEALTH, EXTERNAL_AI)),
    CatalogAsset("tables", "quilla_ml.fs_investment_profile", (WEALTH,)),
    CatalogAsset("tables", "quilla_ml.fs_investment_event", (WEALTH,)),
    CatalogAsset("tables", "quilla_ml.fs_market_daily", (WEALTH,)),
    CatalogAsset("tables", "quilla_ml.fs_macro_country_daily", (WEALTH, EXTERNAL_AI)),
    # Inteligencia Externa e IA
    CatalogAsset("schemas", "example_ai", (EXTERNAL_AI,)),
    CatalogAsset("tables", "quilla_ai.regulatory_documents", (EXTERNAL_AI, RISK)),
    CatalogAsset("tables", "quilla_ai.regulatory_chunks", (EXTERNAL_AI, RISK)),
    CatalogAsset("tables", "quilla_ai.regulatory_documents_ai_prepared", (EXTERNAL_AI,)),
    CatalogAsset("tables", "quilla_ai.regulatory_knowledge_enriched", (EXTERNAL_AI, RISK)),
    CatalogAsset("tables", "quilla_ai.search_quality_metrics", (EXTERNAL_AI, PLATFORM)),
    CatalogAsset("tables", "quilla_ai.agent_registry", (EXTERNAL_AI,)),
    CatalogAsset("tables", "quilla_ai.agent_skill_registry", (EXTERNAL_AI,)),
    CatalogAsset("tables", "quilla_ai.agent_tool_registry", (EXTERNAL_AI,)),
    # Plataforma, Gobierno y Calidad
    CatalogAsset("schemas", "example_governance", (PLATFORM,)),
    CatalogAsset("schemas", "example_monitoring", (PLATFORM,)),
    CatalogAsset("tables", "quilla_gold.mv_data_quality_overview", (PLATFORM,)),
    CatalogAsset("tables", "quilla_gold.metrics_data_quality", (PLATFORM,)),
    CatalogAsset("tables", "quilla_governance.governance_validation_runs", (PLATFORM,)),
    CatalogAsset("tables", "quilla_ai.production_monitoring_manifest", (PLATFORM,)),
    CatalogAsset("tables", "quilla_ai.genai_governance_validation_runs", (PLATFORM,)),
    CatalogAsset("tables", "quilla_ml.model_validation_evidence", (PLATFORM,)),
)


def _encoded(value: str) -> str:
    return quote(value, safe="")


def _catalog_name(args: argparse.Namespace, asset: CatalogAsset) -> str:
    schema, separator, object_name = asset.relative_name.partition(".")
    schema_map = {
        "quilla_bronze": args.bronze_schema,
        "quilla_silver": args.silver_schema,
        "quilla_gold": args.gold_schema,
        "quilla_ml": args.ml_schema,
        "quilla_ai": args.ai_schema,
        "quilla_governance": args.governance_schema,
        "example_ai": args.ai_schema,
        "example_governance": args.governance_schema,
        "example_monitoring": args.monitoring_schema,
    }
    resolved_schema = schema_map.get(schema, schema)
    suffix = f".{object_name}" if separator else ""
    return f"{args.catalog}.{resolved_schema}{suffix}"


def _existing_catalog_tags(client: WorkspaceClient, entity_type: str, entity_name: str) -> set[str]:
    path = (
        "/api/2.1/unity-catalog/entity-tag-assignments/"
        f"{entity_type}/{_encoded(entity_name)}/tags"
    )
    response = client.api_client.do("GET", path, query={"max_results": 100})
    return {item["tag_key"] for item in response.get("tag_assignments", [])}


def _existing_workspace_tags(client: WorkspaceClient, entity_type: str, entity_id: str) -> set[str]:
    path = f"/api/2.0/entity-tag-assignments/{entity_type}/{_encoded(entity_id)}/tags"
    response = client.api_client.do("GET", path, query={"page_size": 20})
    return {item["tag_key"] for item in response.get("tag_assignments", [])}


def _validate_policies(client: WorkspaceClient) -> None:
    missing = []
    for tag_key in DOMAIN_TAGS:
        try:
            client.api_client.do("GET", f"/api/2.1/tag-policies/{_encoded(tag_key)}")
        except Exception:
            missing.append(tag_key)
    if missing:
        raise RuntimeError(f"Governed domain tags missing: {missing}")


def _workspace_assets(args: argparse.Namespace) -> Iterable[WorkspaceAsset]:
    return (
        WorkspaceAsset("apps", args.app_id, (ROOT,)),
        WorkspaceAsset("dashboards", args.dashboard_executive_id, (ROOT,)),
        WorkspaceAsset("geniespaces", args.genie_executive_id, (ROOT,)),
        WorkspaceAsset("dashboards", args.dashboard_growth_id, (CLIENT, CREDIT)),
        WorkspaceAsset("geniespaces", args.genie_customer_id, (CLIENT,)),
        WorkspaceAsset("geniespaces", args.genie_payments_id, (PAYMENTS,)),
        WorkspaceAsset("geniespaces", args.genie_credit_id, (CREDIT,)),
        WorkspaceAsset("dashboards", args.dashboard_risk_id, (RISK, CREDIT, PLATFORM)),
        WorkspaceAsset("geniespaces", args.genie_risk_id, (RISK,)),
        WorkspaceAsset("dashboards", args.dashboard_wealth_id, (WEALTH,)),
        WorkspaceAsset("geniespaces", args.genie_growth_wealth_id, (CLIENT, WEALTH)),
    )


def _ensure_catalog_assignment(
    client: WorkspaceClient,
    entity_type: str,
    entity_name: str,
    tag_key: str,
    dry_run: bool,
) -> str:
    if tag_key in _existing_catalog_tags(client, entity_type, entity_name):
        return "EXISTING"
    if dry_run:
        return "WOULD_CREATE"
    client.api_client.do(
        "POST",
        "/api/2.1/unity-catalog/entity-tag-assignments",
        body={"entity_type": entity_type, "entity_name": entity_name, "tag_key": tag_key},
    )
    return "CREATED"


def _ensure_workspace_assignment(
    client: WorkspaceClient,
    entity_type: str,
    entity_id: str,
    tag_key: str,
    dry_run: bool,
) -> str:
    if tag_key in _existing_workspace_tags(client, entity_type, entity_id):
        return "EXISTING"
    if dry_run:
        return "WOULD_CREATE"
    client.api_client.do(
        "POST",
        "/api/2.0/entity-tag-assignments",
        body={"entity_type": entity_type, "entity_id": entity_id, "tag_key": tag_key},
    )
    return "CREATED"


def _required(parser: argparse.ArgumentParser, name: str) -> None:
    parser.add_argument(f"--{name.replace('_', '-')}", dest=name, required=True)


def _profile_host(profile: str) -> str:
    """Read only the requested profile, tolerating malformed unrelated sections."""
    current_section = None
    values: dict[str, str] = {}
    for raw_line in Path.home().joinpath(".databrickscfg").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if line.startswith("[") and line.endswith("]"):
            current_section = line[1:-1].strip()
            continue
        if current_section != profile or not line or line.startswith(("#", ";")):
            continue
        if "=" in line:
            key, value = line.split("=", 1)
            values.setdefault(key.strip(), value.strip())
    host = values.get("host")
    if not host:
        raise RuntimeError(f"Profile {profile!r} has no host in ~/.databrickscfg")
    return host.rstrip("/")


def _workspace_client(profile: str | None) -> WorkspaceClient:
    if not profile:
        return WorkspaceClient()
    token_process = subprocess.run(
        ["databricks", "auth", "token", profile, "-o", "json"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if token_process.returncode != 0:
        raise RuntimeError(f"Unable to obtain CLI token for {profile}: {token_process.stderr.strip()}")
    access_token = json.loads(token_process.stdout)["access_token"]
    return WorkspaceClient(host=_profile_host(profile), token=access_token)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", help="CLI profile for local execution; omit inside a DAB job")
    parser.add_argument("--catalog", default="example_catalog")
    parser.add_argument("--bronze-schema", default="quilla_bronze")
    parser.add_argument("--silver-schema", default="quilla_silver")
    parser.add_argument("--gold-schema", default="quilla_gold")
    parser.add_argument("--ml-schema", default="quilla_ml")
    parser.add_argument("--ai-schema", default="quilla_ai")
    parser.add_argument("--governance-schema", default="quilla_governance")
    parser.add_argument("--monitoring-schema", default="quilla_monitoring")
    parser.add_argument("--dry-run", action="store_true")
    for argument in (
        "app_id",
        "dashboard_executive_id",
        "dashboard_growth_id",
        "dashboard_risk_id",
        "dashboard_wealth_id",
        "genie_executive_id",
        "genie_customer_id",
        "genie_payments_id",
        "genie_credit_id",
        "genie_risk_id",
        "genie_growth_wealth_id",
    ):
        _required(parser, argument)
    args = parser.parse_args()

    client = _workspace_client(args.profile)
    _validate_policies(client)

    outcomes = []
    failures = []
    for asset in CATALOG_ASSETS:
        entity_name = _catalog_name(args, asset)
        for tag_key in asset.domains:
            try:
                state = _ensure_catalog_assignment(
                    client, asset.entity_type, entity_name, tag_key, args.dry_run
                )
                outcomes.append(
                    {"scope": "catalog", "type": asset.entity_type, "id": entity_name, "tag": tag_key, "state": state}
                )
            except Exception as error:
                failures.append(
                    {"scope": "catalog", "type": asset.entity_type, "id": entity_name, "tag": tag_key, "error": str(error)}
                )

    for asset in _workspace_assets(args):
        for tag_key in asset.domains:
            try:
                state = _ensure_workspace_assignment(
                    client, asset.entity_type, asset.entity_id, tag_key, args.dry_run
                )
                outcomes.append(
                    {"scope": "workspace", "type": asset.entity_type, "id": asset.entity_id, "tag": tag_key, "state": state}
                )
            except Exception as error:
                failures.append(
                    {"scope": "workspace", "type": asset.entity_type, "id": asset.entity_id, "tag": tag_key, "error": str(error)}
                )

    summary = {
        "status": "PASSED" if not failures else "FAILED",
        "dry_run": args.dry_run,
        "assignments": len(outcomes),
        "created": sum(item["state"] == "CREATED" for item in outcomes),
        "existing": sum(item["state"] == "EXISTING" for item in outcomes),
        "would_create": sum(item["state"] == "WOULD_CREATE" for item in outcomes),
        "failures": failures,
        "outcomes": outcomes,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
