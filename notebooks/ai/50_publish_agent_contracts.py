# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Contratos da fundação multiagente
# MAGIC
# MAGIC Publica agentes, skills e ferramentas como contratos governados no Unity Catalog.
# MAGIC Esta fase não registra nem promove uma Agent Version.

# COMMAND ----------

# MAGIC %run ./_agent_common

# COMMAND ----------

import json
from datetime import datetime, timezone

from pyspark.sql import types as T

cfg = agent_config()
now = datetime.now(timezone.utc).replace(tzinfo=None)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg['catalog']}.{cfg['ai_schema']}")

agent_registry = agent_table(cfg, "agent_registry")
skill_registry = agent_table(cfg, "agent_skill_registry")
tool_registry = agent_table(cfg, "agent_tool_registry")
release_manifest = agent_table(cfg, "agent_release_manifest")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {agent_registry} (
  published_at TIMESTAMP NOT NULL,
  agent_release STRING NOT NULL,
  agent_key STRING NOT NULL,
  display_name STRING NOT NULL,
  domain STRING NOT NULL,
  purpose_pt STRING NOT NULL,
  purpose_es STRING NOT NULL,
  model_endpoint STRING NOT NULL,
  prompt_name STRING NOT NULL,
  memory_store_name STRING NOT NULL,
  scope_strategy STRING NOT NULL,
  supported_countries STRING NOT NULL,
  supported_locales STRING NOT NULL,
  decision_authority STRING NOT NULL,
  high_risk_requires_human BOOLEAN NOT NULL,
  contract_sha256 STRING NOT NULL,
  synthetic_flag BOOLEAN NOT NULL
) USING DELTA
TBLPROPERTIES ('project'='quilla','managed_by'='dabs','data_classification'='synthetic')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {skill_registry} (
  published_at TIMESTAMP NOT NULL,
  agent_release STRING NOT NULL,
  skill_key STRING NOT NULL,
  agent_key STRING NOT NULL,
  skill_version STRING NOT NULL,
  relative_path STRING NOT NULL,
  intent_pt STRING NOT NULL,
  intent_es STRING NOT NULL,
  prohibited_actions STRING NOT NULL,
  human_review_rules STRING NOT NULL,
  contract_sha256 STRING NOT NULL,
  synthetic_flag BOOLEAN NOT NULL
) USING DELTA
TBLPROPERTIES ('project'='quilla','managed_by'='dabs','data_classification'='synthetic')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {tool_registry} (
  published_at TIMESTAMP NOT NULL,
  agent_release STRING NOT NULL,
  agent_key STRING NOT NULL,
  tool_name STRING NOT NULL,
  tool_type STRING NOT NULL,
  least_privilege STRING NOT NULL,
  purpose STRING NOT NULL,
  mcp_url_pattern STRING,
  contract_sha256 STRING NOT NULL,
  synthetic_flag BOOLEAN NOT NULL
) USING DELTA
TBLPROPERTIES ('project'='quilla','managed_by'='dabs','data_classification'='synthetic')
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {release_manifest} (
  published_at TIMESTAMP NOT NULL,
  agent_release STRING NOT NULL,
  status STRING NOT NULL,
  agent_count INT NOT NULL,
  specialist_count INT NOT NULL,
  skill_count INT NOT NULL,
  tool_count INT NOT NULL,
  evaluation_release STRING NOT NULL,
  evaluation_gate STRING NOT NULL,
  review_queue_gate STRING NOT NULL,
  agent_version_registered BOOLEAN NOT NULL,
  promotion_eligible BOOLEAN NOT NULL,
  notes STRING NOT NULL,
  synthetic_flag BOOLEAN NOT NULL
) USING DELTA
TBLPROPERTIES ('project'='quilla','managed_by'='dabs','data_classification'='synthetic')
""")

for table in [agent_registry, skill_registry, tool_registry, release_manifest]:
    spark.sql(f"DELETE FROM {table} WHERE agent_release = '{cfg['release']}'")

agent_schema = T.StructType([
    T.StructField("published_at", T.TimestampType(), False),
    T.StructField("agent_release", T.StringType(), False),
    T.StructField("agent_key", T.StringType(), False),
    T.StructField("display_name", T.StringType(), False),
    T.StructField("domain", T.StringType(), False),
    T.StructField("purpose_pt", T.StringType(), False),
    T.StructField("purpose_es", T.StringType(), False),
    T.StructField("model_endpoint", T.StringType(), False),
    T.StructField("prompt_name", T.StringType(), False),
    T.StructField("memory_store_name", T.StringType(), False),
    T.StructField("scope_strategy", T.StringType(), False),
    T.StructField("supported_countries", T.StringType(), False),
    T.StructField("supported_locales", T.StringType(), False),
    T.StructField("decision_authority", T.StringType(), False),
    T.StructField("high_risk_requires_human", T.BooleanType(), False),
    T.StructField("contract_sha256", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
agent_rows = []
for spec in AGENT_SPECS:
    prompt_name = f"{cfg['catalog']}.{cfg['ai_schema']}.quilla_{spec['prompt_key']}"
    full_store = f"{cfg['catalog']}.{cfg['ai_schema']}.{spec['memory_store']}"
    agent_rows.append((
        now, cfg["release"], spec["key"], spec["display_name"], spec["domain"],
        spec["purpose_pt"], spec["purpose_es"], cfg["model_endpoint"], prompt_name,
        full_store, spec["scope_strategy"], json.dumps(COUNTRIES),
        json.dumps(LOCALES, ensure_ascii=False), spec["decision_authority"],
        spec["human_review"], canonical_digest(spec), True,
    ))
spark.createDataFrame(agent_rows, agent_schema).write.mode("append").saveAsTable(agent_registry)

skill_schema = T.StructType([
    T.StructField("published_at", T.TimestampType(), False),
    T.StructField("agent_release", T.StringType(), False),
    T.StructField("skill_key", T.StringType(), False),
    T.StructField("agent_key", T.StringType(), False),
    T.StructField("skill_version", T.StringType(), False),
    T.StructField("relative_path", T.StringType(), False),
    T.StructField("intent_pt", T.StringType(), False),
    T.StructField("intent_es", T.StringType(), False),
    T.StructField("prohibited_actions", T.StringType(), False),
    T.StructField("human_review_rules", T.StringType(), False),
    T.StructField("contract_sha256", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
skill_rows = [(
    now, cfg["release"], spec["skill_key"], spec["agent_key"], spec["version"],
    spec["relative_path"], spec["intent_pt"], spec["intent_es"],
    spec["prohibited_actions"], spec["human_review_rules"], canonical_digest(spec), True,
) for spec in SKILL_SPECS]
spark.createDataFrame(skill_rows, skill_schema).write.mode("append").saveAsTable(skill_registry)

tool_schema = T.StructType([
    T.StructField("published_at", T.TimestampType(), False),
    T.StructField("agent_release", T.StringType(), False),
    T.StructField("agent_key", T.StringType(), False),
    T.StructField("tool_name", T.StringType(), False),
    T.StructField("tool_type", T.StringType(), False),
    T.StructField("least_privilege", T.StringType(), False),
    T.StructField("purpose", T.StringType(), False),
    T.StructField("mcp_url_pattern", T.StringType(), True),
    T.StructField("contract_sha256", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
tool_rows = []
for agent_key, tool_name, tool_type, privilege, purpose in TOOL_SPECS:
    mcp_url = None
    if tool_type == "AI_SEARCH_MCP":
        mcp_url = f"/api/2.0/mcp/ai-search/{cfg['catalog']}/{cfg['ai_schema']}/{tool_name}"
    payload = [agent_key, tool_name, tool_type, privilege, purpose, mcp_url]
    tool_rows.append((now, cfg["release"], *payload, canonical_digest(payload), True))
spark.createDataFrame(tool_rows, tool_schema).write.mode("append").saveAsTable(tool_registry)

release_schema = T.StructType([
    T.StructField("published_at", T.TimestampType(), False),
    T.StructField("agent_release", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("agent_count", T.IntegerType(), False),
    T.StructField("specialist_count", T.IntegerType(), False),
    T.StructField("skill_count", T.IntegerType(), False),
    T.StructField("tool_count", T.IntegerType(), False),
    T.StructField("evaluation_release", T.StringType(), False),
    T.StructField("evaluation_gate", T.StringType(), False),
    T.StructField("review_queue_gate", T.StringType(), False),
    T.StructField("agent_version_registered", T.BooleanType(), False),
    T.StructField("promotion_eligible", T.BooleanType(), False),
    T.StructField("notes", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
release_row = [(
    now, cfg["release"], "FOUNDATION_CONTRACTS_PUBLISHED", len(AGENT_SPECS),
    len(AGENT_SPECS) - 1, len(SKILL_SPECS), len(TOOL_SPECS),
    cfg["evaluation_release"], "APPROVED", "PENDING_HUMAN_REVIEW", False, False,
    "Agent Version intentionally not registered or promoted until the human Review Queue gate is complete.",
    True,
)]
spark.createDataFrame(release_row, release_schema).write.mode("append").saveAsTable(release_manifest)

assert len(AGENT_SPECS) == 8
assert len(SKILL_SPECS) == 10
assert {spec["key"] for spec in AGENT_SPECS} == {spec["agent_key"] for spec in SKILL_SPECS}
print(json.dumps({
    "status": "PASSED",
    "agent_release": cfg["release"],
    "agents": len(AGENT_SPECS),
    "specialists": len(AGENT_SPECS) - 1,
    "skills": len(SKILL_SPECS),
    "tools": len(TOOL_SPECS),
    "agent_version_registered": False,
}, ensure_ascii=False))
