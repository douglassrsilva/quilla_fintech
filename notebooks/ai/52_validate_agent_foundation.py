# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Validação da fundação multiagente
# MAGIC
# MAGIC Reconciliamos os contratos UC, os oito Memory Stores e um ciclo efêmero de
# MAGIC entrada, busca e Conversation API. Nenhuma Agent Version é promovida.

# COMMAND ----------

# MAGIC %run ./_agent_common

# COMMAND ----------

import json
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from pyspark.sql import types as T

cfg = agent_config()
client = WorkspaceClient()
base_path = "/api/2.1/unity-catalog/memory-stores"

listed = client.api_client.do(
    "GET", base_path,
    query={"catalog_name": cfg["catalog"], "schema_name": cfg["ai_schema"]},
) or {}
stores = listed.get("memory_stores", listed.get("stores", [])) if isinstance(listed, dict) else listed
by_name = {
    item.get("full_name") or ".".join(filter(None, [item.get("catalog_name"), item.get("schema_name"), item.get("name")])): item
    for item in stores
}

expected_names = {
    f"{cfg['catalog']}.{cfg['ai_schema']}.{spec['memory_store']}" for spec in AGENT_SPECS
}
missing = expected_names - set(by_name)
assert not missing, f"Missing memory stores: {sorted(missing)}"

scope = "tenant:quilla-demo|country:BR|actor:bundle-validator"
path = f"/memories/foundation-check-{cfg['release']}.md"
probe_results = []


def _result_path(item):
    """Accept both direct entries and Beta search results wrapping memory_entry."""
    if not isinstance(item, dict):
        return None
    entry = item.get("memory_entry", item.get("entry", item))
    return entry.get("path") if isinstance(entry, dict) else None


for spec in AGENT_SPECS:
    full_name = f"{cfg['catalog']}.{cfg['ai_schema']}.{spec['memory_store']}"
    entry_path = f"{base_path}/{full_name}/entries"
    body = {
        "path": path,
        "contents": (
            "Validação sintética da fundação Quilla. Não contém PII, segredo, "
            "orientação financeira, jurídica ou regulatória."
        ),
        "description": f"Synthetic foundation probe for {spec['key']}",
    }
    try:
        client.api_client.do("POST", entry_path, query={"scope": scope}, body=body)
    except Exception as exc:
        message = str(exc).lower()
        if "already exists" not in message and "already_exists" not in message and "409" not in message:
            raise
        client.api_client.do(
            "PATCH", entry_path,
            body={
                "scope": scope,
                "path": path,
                "replace_all": {"contents": body["contents"]},
                "description": body["description"],
            },
        )

    loaded = client.api_client.do(
        "GET", f"{entry_path}:get", query={"scope": scope, "path": path}
    )
    assert loaded.get("path") == path, (full_name, loaded)

    # Search indexing is eventually consistent. Keep the original run reparable and
    # poll the Beta API instead of treating an immediate empty result as data loss.
    found = {}
    search_ready = False
    for delay_seconds in [0, 2, 4, 8, 12, 16]:
        if delay_seconds:
            time.sleep(delay_seconds)
        found = client.api_client.do(
            "POST", f"{entry_path}:search",
            body={
                "scope": scope,
                "query": "Synthetic foundation probe",
                "path_prefix": "/memories/",
                "top_k": 10,
            },
        ) or {}
        matches = found.get("entries", found.get("results", [])) if isinstance(found, dict) else found
        if any(_result_path(item) == path for item in matches):
            search_ready = True
            break
    assert search_ready, (full_name, found)
    client.api_client.do("DELETE", entry_path, query={"scope": scope, "path": path})
    probe_results.append((spec["key"], full_name, "READ_WRITE_SEARCH_PASSED"))

# Validate the server-side Conversation API against the supervisor store. The scope is
# assigned by trusted caller code and the ephemeral conversation is removed immediately.
supervisor = agent_by_key("supervisor")
supervisor_store = f"{cfg['catalog']}.{cfg['ai_schema']}.{supervisor['memory_store']}"
verified_user_id = str(client.current_user.me().id)
conversation = client.api_client.do(
    "POST", "/api/2.1/unity-catalog/conversations",
    body={
        "memory_store": {"name": supervisor_store},
        "scope": {"kind": "user", "value": verified_user_id},
        "metadata": {"project": "quilla", "release": cfg["release"], "synthetic": "true"},
    },
)
conversation_id = conversation.get("id")
assert conversation_id, conversation
loaded_conversation = client.api_client.do(
    "GET", f"/api/2.1/unity-catalog/conversations/{conversation_id}"
)
assert loaded_conversation.get("id") == conversation_id, loaded_conversation
client.api_client.do(
    "DELETE", f"/api/2.1/unity-catalog/conversations/{conversation_id}"
)

counts = spark.sql(f"""
SELECT
  (SELECT COUNT(*) FROM {agent_table(cfg, 'agent_registry')} WHERE agent_release = '{cfg['release']}') AS agents,
  (SELECT COUNT(*) FROM {agent_table(cfg, 'agent_skill_registry')} WHERE agent_release = '{cfg['release']}') AS skills,
  (SELECT COUNT(*) FROM {agent_table(cfg, 'agent_tool_registry')} WHERE agent_release = '{cfg['release']}') AS tools,
  (SELECT MAX(agent_version_registered) FROM {agent_table(cfg, 'agent_release_manifest')} WHERE agent_release = '{cfg['release']}') AS version_registered,
  (SELECT MAX(promotion_eligible) FROM {agent_table(cfg, 'agent_release_manifest')} WHERE agent_release = '{cfg['release']}') AS promotion_eligible
""").first()
assert counts.agents == len(AGENT_SPECS), counts
assert counts.skills == len(SKILL_SPECS), counts
assert counts.tools == len(TOOL_SPECS), counts
assert counts.version_registered is False, counts
assert counts.promotion_eligible is False, counts

validation_table = agent_table(cfg, "agent_foundation_validation_runs")
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {validation_table} (
  validated_at TIMESTAMP NOT NULL,
  agent_release STRING NOT NULL,
  status STRING NOT NULL,
  agent_count INT NOT NULL,
  specialist_count INT NOT NULL,
  skill_count INT NOT NULL,
  tool_count INT NOT NULL,
  memory_store_count INT NOT NULL,
  memory_probe_count INT NOT NULL,
  conversation_api_status STRING NOT NULL,
  agent_version_registered BOOLEAN NOT NULL,
  promotion_eligible BOOLEAN NOT NULL,
  synthetic_flag BOOLEAN NOT NULL
) USING DELTA
TBLPROPERTIES ('project'='quilla','managed_by'='dabs','data_classification'='synthetic')
""")
spark.sql(f"DELETE FROM {validation_table} WHERE agent_release = '{cfg['release']}'")
validation_schema = T.StructType([
    T.StructField("validated_at", T.TimestampType(), False),
    T.StructField("agent_release", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("agent_count", T.IntegerType(), False),
    T.StructField("specialist_count", T.IntegerType(), False),
    T.StructField("skill_count", T.IntegerType(), False),
    T.StructField("tool_count", T.IntegerType(), False),
    T.StructField("memory_store_count", T.IntegerType(), False),
    T.StructField("memory_probe_count", T.IntegerType(), False),
    T.StructField("conversation_api_status", T.StringType(), False),
    T.StructField("agent_version_registered", T.BooleanType(), False),
    T.StructField("promotion_eligible", T.BooleanType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
row = [(
    datetime.now(timezone.utc).replace(tzinfo=None), cfg["release"], "PASSED",
    len(AGENT_SPECS), len(AGENT_SPECS) - 1, len(SKILL_SPECS), len(TOOL_SPECS),
    len(expected_names), len(probe_results),
    "CREATE_GET_DELETE_PASSED", False, False, True,
)]
spark.createDataFrame(row, validation_schema).write.mode("append").saveAsTable(validation_table)

print(json.dumps({
    "status": "PASSED",
    "agent_release": cfg["release"],
    "agents": len(AGENT_SPECS),
    "specialists": len(AGENT_SPECS) - 1,
    "skills": len(SKILL_SPECS),
    "tools": len(TOOL_SPECS),
    "memory_stores": len(expected_names),
    "memory_probes": len(probe_results),
    "conversation_api": "PASSED",
    "agent_version_registered": False,
    "promotion_eligible": False,
}, ensure_ascii=False))
