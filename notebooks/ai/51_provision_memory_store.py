# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Managed Memory por domínio
# MAGIC
# MAGIC Cria idempotentemente um Memory Store governado para um agente. O scope real será
# MAGIC sempre derivado do contexto autenticado; nenhum valor vindo do prompt é aceito.

# COMMAND ----------

# MAGIC %run ./_agent_common

# COMMAND ----------

import json

from databricks.sdk import WorkspaceClient

dbutils.widgets.text("agent_key", "supervisor")
agent_key = dbutils.widgets.get("agent_key")
cfg = agent_config()
spec = agent_by_key(agent_key)
full_name = f"{cfg['catalog']}.{cfg['ai_schema']}.{spec['memory_store']}"
client = WorkspaceClient()
base_path = "/api/2.1/unity-catalog/memory-stores"

listed = client.api_client.do(
    "GET",
    base_path,
    query={"catalog_name": cfg["catalog"], "schema_name": cfg["ai_schema"]},
) or {}
stores = listed.get("memory_stores", listed.get("stores", [])) if isinstance(listed, dict) else listed
existing_names = {
    item.get("full_name") or ".".join(filter(None, [item.get("catalog_name"), item.get("schema_name"), item.get("name")]))
    for item in stores
}

if full_name in existing_names:
    action = "REUSED"
else:
    response = client.api_client.do(
        "POST",
        base_path,
        body={
            "name": spec["memory_store"],
            "catalog_name": cfg["catalog"],
            "schema_name": cfg["ai_schema"],
            "description": (
                f"Quilla {spec['display_name']} managed memory. PT-BR/ES-LATAM; "
                "trusted scope only; synthetic demo; no secrets or raw financial identifiers."
            ),
        },
    )
    action = "CREATED"
    returned_name = response.get("full_name") if isinstance(response, dict) else None
    if returned_name and returned_name != full_name:
        raise AssertionError(f"Unexpected memory store created: {returned_name}")

verified = client.api_client.do("GET", f"{base_path}/{full_name}")
assert isinstance(verified, dict), verified
assert verified.get("full_name", full_name) == full_name, verified
print(json.dumps({
    "status": "PASSED",
    "action": action,
    "agent_key": agent_key,
    "memory_store": full_name,
    "scope_strategy": spec["scope_strategy"],
}, ensure_ascii=False))
