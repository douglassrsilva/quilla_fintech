# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Deploy idempotente do supervisor

# COMMAND ----------

import json
import time

from databricks.sdk import WorkspaceClient


def widget(name, default):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)
    except Exception:
        return default


catalog = widget("catalog", "example_catalog")
ai_schema = widget("ai_schema", "example_ai")
model_leaf = widget("supervisor_model_name", "quilla_custom_supervisor")
model_version = widget("model_version", "")
endpoint_name = widget("supervisor_endpoint_name", "quilla-custom-supervisor")
release = widget("supervisor_release", "2026-09-04-custom-supervisor-v23-long-financial-fallback")
web_search_timeout_seconds = widget("web_search_timeout_seconds", "45")
you_financial_timeout_seconds = widget("you_financial_timeout_seconds", "90")
you_financial_max_input_chars = widget("you_financial_max_input_chars", "1200")
you_financial_daily_call_limit = widget("you_financial_daily_call_limit", "6")
you_financial_total_call_limit = widget("you_financial_total_call_limit", "10")
you_financial_conversation_call_limit = widget("you_financial_conversation_call_limit", "2")
if not model_version.isdigit():
    raise ValueError(f"Invalid model_version: {model_version!r}")

registered_model = f"{catalog}.{ai_schema}.{model_leaf}"
client = WorkspaceClient()
lakebase_project = widget("lakebase_project", "quilla-ltap")
lakebase_branch = widget("lakebase_branch", "production")
lakebase_endpoint = widget("lakebase_endpoint", "primary")
lakebase_parent = f"projects/{lakebase_project}/branches/{lakebase_branch}"
lakebase_endpoint_name = f"{lakebase_parent}/endpoints/{lakebase_endpoint}"
lakebase_resource = next(
    item
    for item in client.postgres.list_endpoints(lakebase_parent)
    if item.name == lakebase_endpoint_name
)
if not lakebase_resource.status or not lakebase_resource.status.hosts:
    raise RuntimeError(f"Lakebase endpoint has no active host: {lakebase_endpoint_name}")
lakebase_host = lakebase_resource.status.hosts.host
served_entity = {
    "name": "quilla_supervisor",
    "entity_name": registered_model,
    "entity_version": model_version,
    "workload_size": "Small",
    # The interactive demo must not pay a cold-start penalty on its first turn.
    "scale_to_zero_enabled": False,
    "environment_vars": {
        "QUILLA_CATALOG": catalog,
        "QUILLA_AI_SCHEMA": ai_schema,
        "QUILLA_PRIMARY_MODEL": widget("primary_model", f"{catalog}.{ai_schema}.quilla-supervisor-primary"),
        "QUILLA_VERIFIER_MODEL": widget("verifier_model", f"{catalog}.{ai_schema}.quilla-supervisor-verifier"),
        "QUILLA_LONG_CONTEXT_MODEL": widget("long_context_model", f"{catalog}.{ai_schema}.quilla-supervisor-long-context"),
        "QUILLA_SUPERVISOR_RELEASE": release,
        "QUILLA_WEB_SEARCH_TIMEOUT_SECONDS": web_search_timeout_seconds,
        "QUILLA_YOU_FINANCIAL_ENABLED": "true",
        "QUILLA_YOU_FINANCIAL_TIMEOUT_SECONDS": you_financial_timeout_seconds,
        "QUILLA_YOU_FINANCIAL_MAX_INPUT_CHARS": you_financial_max_input_chars,
        "QUILLA_YOU_FINANCIAL_DAILY_CALL_LIMIT": you_financial_daily_call_limit,
        "QUILLA_YOU_FINANCIAL_TOTAL_CALL_LIMIT": you_financial_total_call_limit,
        "QUILLA_YOU_FINANCIAL_CONVERSATION_CALL_LIMIT": you_financial_conversation_call_limit,
        "QUILLA_MEMORY_ENABLED": "true",
        "QUILLA_MEMORY_REQUIRED": "true",
        "QUILLA_LAKEBASE_PROJECT": lakebase_project,
        "QUILLA_LAKEBASE_DATABASE": widget("lakebase_database", "quilla"),
        "QUILLA_LAKEBASE_BRANCH": lakebase_branch,
        "QUILLA_LAKEBASE_ENDPOINT": lakebase_endpoint,
        "QUILLA_LAKEBASE_HOST": lakebase_host,
        "QUILLA_GENIE_EXECUTIVE_REGIONAL": widget("genie_executive_regional", ""),
        "QUILLA_GENIE_PAYMENTS_PERFORMANCE": widget("genie_payments_performance", ""),
        "QUILLA_GENIE_CUSTOMER_OPERATIONS": widget("genie_customer_operations", ""),
        "QUILLA_GENIE_RISK_FRAUD": widget("genie_risk_fraud", ""),
        "QUILLA_GENIE_RESPONSIBLE_CREDIT": widget("genie_responsible_credit", ""),
        "QUILLA_GENIE_GROWTH_WEALTH": widget("genie_growth_wealth", ""),
    },
}

try:
    client.api_client.do("GET", f"/api/2.0/serving-endpoints/{endpoint_name}")
    client.api_client.do(
        "PUT",
        f"/api/2.0/serving-endpoints/{endpoint_name}/config",
        body={"served_entities": [served_entity], "traffic_config": {"routes": [{"served_model_name": "quilla_supervisor", "traffic_percentage": 100}]}},
    )
    action = "UPDATED"
except Exception as exc:
    if "RESOURCE_DOES_NOT_EXIST" not in str(exc) and "does not exist" not in str(exc).lower() and "404" not in str(exc):
        raise
    client.api_client.do(
        "POST",
        "/api/2.0/serving-endpoints",
        body={"name": endpoint_name, "config": {"served_entities": [served_entity]}},
    )
    action = "CREATED"

deadline = time.time() + 5400
last = {}
while time.time() < deadline:
    last = client.api_client.do("GET", f"/api/2.0/serving-endpoints/{endpoint_name}")
    state = last.get("state", {})
    ready = state.get("ready")
    update_state = state.get("config_update")
    if ready == "READY" and update_state in {"NOT_UPDATING", None}:
        break
    if update_state == "UPDATE_FAILED":
        raise RuntimeError(json.dumps(last, ensure_ascii=False))
    time.sleep(20)
else:
    raise TimeoutError(f"Endpoint {endpoint_name} did not become READY: {last}")

print(json.dumps({
    "status": "READY",
    "action": action,
    "endpoint": endpoint_name,
    "registered_model": registered_model,
    "model_version": model_version,
    "state": last.get("state"),
}, ensure_ascii=False))
