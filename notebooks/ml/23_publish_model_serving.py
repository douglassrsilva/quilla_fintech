# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

import time

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceDoesNotExist
from mlflow import MlflowClient

cfg = config()
client = MlflowClient(registry_uri="databricks-uc")
workspace = WorkspaceClient()

served_models = {
    "quilla-transaction-fraud": "transaction_fraud",
    "quilla-account-takeover": "account_takeover",
    "quilla-credit-pd": "credit_pd",
    "quilla-next-best-action": "next_best_action",
    "quilla-investment-suitability": "investment_suitability",
    "quilla-support-intent": "support_intent",
}


def get_endpoint(name: str):
    try:
        return workspace.api_client.do("GET", f"/api/2.0/serving-endpoints/{name}")
    except ResourceDoesNotExist:
        return None
    except Exception as exc:
        if "RESOURCE_DOES_NOT_EXIST" in str(exc) or "404" in str(exc) or "does not exist" in str(exc).lower():
            return None
        raise


def endpoint_ready(name: str) -> bool:
    endpoint = get_endpoint(name)
    if endpoint is None:
        return False
    state = endpoint.get("state", {})
    return state.get("ready") == "READY" and state.get("config_update") in (None, "NOT_UPDATING")


def endpoint_matches(endpoint, model_name: str, model_version: str, scale_to_zero_enabled: bool) -> bool:
    entities = (endpoint.get("config") or {}).get("served_entities") or []
    return any(
        entity.get("entity_name") == model_name
        and str(entity.get("entity_version")) == model_version
        and entity.get("scale_to_zero_enabled") is scale_to_zero_enabled
        for entity in entities
    )


published = []
for endpoint_name, model_key in served_models.items():
    model_name = f"{cfg['catalog']}.{cfg['ml_schema']}.quilla_{model_key}"
    champion = client.get_model_version_by_alias(model_name, "Champion")
    # The NBA endpoint is called synchronously in the App's human decision flow.
    # Keep it warm so a scale-from-zero transition cannot consume the UI timeout.
    scale_to_zero_enabled = model_key != "next_best_action"
    entity = {
        "name": model_key.replace("_", "-"),
        "entity_name": model_name,
        "entity_version": str(champion.version),
        "workload_size": "Small",
        "scale_to_zero_enabled": scale_to_zero_enabled,
    }
    config_payload = {
        "served_entities": [entity],
    }
    current = get_endpoint(endpoint_name)
    if current is None:
        workspace.api_client.do(
            "POST",
            "/api/2.0/serving-endpoints",
            body={"name": endpoint_name, "config": config_payload},
        )
    elif current.get("state", {}).get("config_update") == "IN_PROGRESS":
        print(json.dumps({"endpoint": endpoint_name, "status": "UPDATE_IN_PROGRESS", "action": "wait"}))
    elif not endpoint_matches(current, model_name, str(champion.version), scale_to_zero_enabled):
        workspace.api_client.do(
            "PUT",
            f"/api/2.0/serving-endpoints/{endpoint_name}/config",
            body=config_payload,
        )
    else:
        print(json.dumps({"endpoint": endpoint_name, "status": "CURRENT", "action": "none"}))
    published.append((endpoint_name, model_name, str(champion.version)))

deadline = time.time() + 3600
pending = {name for name, _, _ in published}
while pending and time.time() < deadline:
    for name in list(pending):
        if endpoint_ready(name):
            pending.remove(name)
    if pending:
        time.sleep(30)
if pending:
    raise TimeoutError(f"Model Serving endpoints not ready within one hour: {sorted(pending)}")

for endpoint_name, _, _ in published:
    workspace.api_client.do(
        "PUT",
        f"/api/2.0/serving-endpoints/{endpoint_name}/ai-gateway",
        body={
            "inference_table_config": {
                "catalog_name": cfg["catalog"],
                "schema_name": cfg["ml_schema"],
                "table_name_prefix": endpoint_name.replace("-", "_"),
                "enabled": True,
            }
        },
    )

gateway_deadline = time.time() + 1800
pending_gateway = {name for name, _, _ in published}
while pending_gateway and time.time() < gateway_deadline:
    for name in list(pending_gateway):
        endpoint = get_endpoint(name) or {}
        inference_config = (endpoint.get("ai_gateway") or {}).get("inference_table_config") or {}
        if (
            endpoint.get("state", {}).get("config_update") in (None, "NOT_UPDATING")
            and inference_config.get("enabled") is True
            and inference_config.get("catalog_name") == cfg["catalog"]
            and inference_config.get("schema_name") == cfg["ml_schema"]
        ):
            pending_gateway.remove(name)
    if pending_gateway:
        time.sleep(15)
if pending_gateway:
    raise TimeoutError(f"AI Gateway inference tables not ready within 30 minutes: {sorted(pending_gateway)}")

evidence = (
    spark.createDataFrame(published, "endpoint_name string, model_name string, model_version string")
    .withColumn("status", F.lit("READY"))
    .withColumn("validated_at", F.current_timestamp())
)
evidence.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ml_table(cfg, "model_serving_evidence"))

print(json.dumps({"status": "READY", "endpoints": published}))
