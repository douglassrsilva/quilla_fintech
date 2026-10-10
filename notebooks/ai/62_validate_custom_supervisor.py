# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Prova conversacional do supervisor customizado

# COMMAND ----------

import json
import time
from datetime import datetime, timezone
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from databricks.sdk import WorkspaceClient
from pyspark.sql import types as T


def widget(name, default):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)
    except Exception:
        return default


catalog = widget("catalog", "example_catalog")
ai_schema = widget("ai_schema", "example_ai")
endpoint = widget("supervisor_endpoint_name", "quilla-custom-supervisor")
release = widget("supervisor_release", "2026-09-03-custom-supervisor-v11-app-obo")
model_version = widget("model_version", "")
client = WorkspaceClient()
validation_read_timeout_seconds = 45


def invoke_once(body):
    """Invoke once with an explicit read timeout and no SDK retry loop."""
    request = Request(
        f"{client.config.host.rstrip('/')}/serving-endpoints/{endpoint}/invocations",
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            **client.config.authenticate(),
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=validation_read_timeout_seconds) as response:
            return json.load(response)
    except HTTPError as exc:
        response_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Endpoint returned HTTP {exc.code}: {response_body}"
        ) from exc

turns = [
    ("pt-BR", "BR", "Quais os títulos que tiveram a maior alta na bolsa brasileira ontem?", "external_intelligence"),
]
history = []
results = []
memory_session_ids = []
conversation_id = f"quilla-validation-{release}"
for number, (locale, country, question, expected_route) in enumerate(turns, start=1):
    history.append({"role": "user", "content": [{"type": "input_text", "text": question}]})
    started_at = time.perf_counter()
    print(json.dumps({
        "event": "TURN_STARTED",
        "turn": number,
        "locale": locale,
        "country_code": country,
        "timeout_seconds": validation_read_timeout_seconds,
    }, ensure_ascii=False), flush=True)
    try:
        payload = invoke_once({
            "input": history,
            "context": {"user_id": "quilla-demo-validator"},
            "custom_inputs": {
                "conversation_id": conversation_id,
                "tenant_id": "quilla-demo",
                "country_code": country,
                "locale": locale,
                "demo": True,
            },
        })
    except Exception as exc:
        elapsed_seconds = round(time.perf_counter() - started_at, 3)
        print(json.dumps({
            "event": "TURN_FAILED",
            "turn": number,
            "elapsed_seconds": elapsed_seconds,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }, ensure_ascii=False), flush=True)
        raise
    elapsed_seconds = round(time.perf_counter() - started_at, 3)
    print(json.dumps({
        "event": "TURN_COMPLETED",
        "turn": number,
        "elapsed_seconds": elapsed_seconds,
    }, ensure_ascii=False), flush=True)
    output = payload.get("output", [])
    messages = [item for item in output if item.get("type") == "message"]
    if not messages:
        raise AssertionError(f"Turn {number} returned no message: {payload}")
    text = "\n".join(
        content.get("text", "")
        for item in messages
        for content in item.get("content", [])
        if content.get("text")
    )
    if not text.strip():
        raise AssertionError(f"Turn {number} returned empty text: {payload}")
    custom = payload.get("custom_outputs", {})
    if not custom.get("route") or not custom.get("trace_id"):
        raise AssertionError(f"Turn {number} missing supervisor evidence: {payload}")
    if custom["route"] != expected_route:
        raise AssertionError(f"Turn {number} expected route {expected_route}, got {custom['route']}")
    memory = custom.get("memory", {})
    if memory.get("status") != "PERSISTED" or not memory.get("session_id"):
        raise AssertionError(f"Turn {number} was not persisted in Lakebase: {payload}")
    memory_session_ids.append(memory["session_id"])
    if not custom.get("requires_human_review"):
        raise AssertionError(f"Turn {number} lost the high-impact review gate")
    web_evidence = [
        item for item in custom.get("tools", [])
        if item.get("server") == "web_search" and item.get("status") == "SUCCEEDED"
    ]
    if not web_evidence:
        raise AssertionError(f"Turn {number} did not execute governed Web Search: {payload}")
    source_evidence = json.dumps(web_evidence, ensure_ascii=False)
    has_verifiable_source = any(
        scheme in candidate
        for candidate in (text, source_evidence)
        for scheme in ("http://", "https://")
    )
    if not has_verifiable_source:
        raise AssertionError(f"Turn {number} did not preserve verifiable source evidence: {payload}")
    history.extend(output)
    results.append((number, locale, country, question, text[:2000], custom["route"], json.dumps(custom.get("models", [])), custom.get("trace_id"), bool(custom.get("requires_human_review"))))

table = f"{catalog}.{ai_schema}.custom_supervisor_validation_runs"
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {table} (
  validated_at TIMESTAMP NOT NULL,
  supervisor_release STRING NOT NULL,
  model_version STRING NOT NULL,
  endpoint_name STRING NOT NULL,
  turn_number INT NOT NULL,
  locale STRING NOT NULL,
  country_code STRING NOT NULL,
  question STRING NOT NULL,
  answer_preview STRING NOT NULL,
  route STRING NOT NULL,
  models STRING NOT NULL,
  trace_id STRING NOT NULL,
  requires_human_review BOOLEAN NOT NULL,
  status STRING NOT NULL,
  synthetic_flag BOOLEAN NOT NULL
) USING DELTA
TBLPROPERTIES ('project'='quilla','managed_by'='dabs','data_classification'='synthetic')
""")
spark.sql(f"DELETE FROM {table} WHERE supervisor_release = '{release}'")
schema = T.StructType([
    T.StructField("validated_at", T.TimestampType(), False), T.StructField("supervisor_release", T.StringType(), False),
    T.StructField("model_version", T.StringType(), False), T.StructField("endpoint_name", T.StringType(), False),
    T.StructField("turn_number", T.IntegerType(), False), T.StructField("locale", T.StringType(), False),
    T.StructField("country_code", T.StringType(), False), T.StructField("question", T.StringType(), False),
    T.StructField("answer_preview", T.StringType(), False), T.StructField("route", T.StringType(), False),
    T.StructField("models", T.StringType(), False), T.StructField("trace_id", T.StringType(), False),
    T.StructField("requires_human_review", T.BooleanType(), False), T.StructField("status", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
now = datetime.now(timezone.utc).replace(tzinfo=None)
rows = [(now, release, model_version, endpoint, *result, "PASSED", True) for result in results]
spark.createDataFrame(rows, schema).write.mode("append").saveAsTable(table)
if len(set(memory_session_ids)) != 1:
    raise AssertionError(f"Conversation was split across Lakebase sessions: {memory_session_ids}")
print(json.dumps({
    "status": "PASSED",
    "endpoint": endpoint,
    "model_version": model_version,
    "turns": len(results),
    "routes": [row[5] for row in results],
    "memory_store": "lakebase",
    "memory_session_id": memory_session_ids[0],
}, ensure_ascii=False))
