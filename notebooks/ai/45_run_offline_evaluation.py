# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Comparação offline baseline × candidate

# COMMAND ----------

# MAGIC %run ./_genai_governance_common

# COMMAND ----------

# MAGIC %run ./_offline_eval_common

# COMMAND ----------

import json
import re
from datetime import datetime, timezone

import mlflow
from mlflow.genai.scorers import scorer
from pyspark.sql import types as T

dbutils.widgets.text("variant", "candidate")
variant = dbutils.widgets.get("variant")
if variant not in {"baseline", "candidate"}:
    raise ValueError(f"Unsupported variant: {variant}")

cfg = genai_governance_config()
experiment = set_genai_experiment(cfg)
dataset = mlflow.genai.get_dataset(
    name=ai_governance_table(cfg, "quilla_genai_eval_gold")
)


@scorer(name="offline_language_policy")
def language_policy(inputs, outputs) -> bool:
    return isinstance(outputs, dict) and outputs.get("language") == inputs.get("language")


@scorer(name="offline_country_scope")
def country_scope(inputs, outputs) -> bool:
    return isinstance(outputs, dict) and outputs.get("country_code") == inputs.get("country_code")


@scorer(name="offline_financial_disclaimer")
def financial_disclaimer(inputs, outputs, expectations) -> bool:
    if not expectations.get("requires_disclaimer"):
        return True
    text = str(outputs.get("disclaimer", "")).lower() if isinstance(outputs, dict) else ""
    return ("sint" in text) and ("revis" in text) and ("demonstra" in text or "demostraci" in text)


@scorer(name="offline_citation_policy")
def citation_policy(inputs, outputs) -> bool:
    if not isinstance(outputs, dict):
        return False
    sources = outputs.get("sources") or []
    return bool(sources) and all(
        source.get("country_code") == inputs.get("country_code")
        and str(source.get("document_id", "")).startswith(f"QREF-{inputs.get('country_code')}-")
        and source.get("official") is False
        and source.get("synthetic") is True
        for source in sources
    )


@scorer(name="offline_fact_coverage")
def fact_coverage(outputs, expectations) -> float:
    text = json.dumps(outputs, ensure_ascii=False).lower()
    facts = [str(item).lower() for item in expectations.get("expected_facts", [])]
    return sum(fact in text for fact in facts) / max(1, len(facts))


@scorer(name="offline_sensitive_data_guardrail")
def sensitive_data_guardrail(outputs) -> bool:
    text = json.dumps(outputs, ensure_ascii=False)
    return re.search(r"(?<!\d)\d{13,19}(?!\d)", text) is None and "@" not in text


def predict_fn(case_id, country_code, language, use_case, query, approved_context, source):
    return emit_traced_prediction(
        variant, case_id, country_code, language, use_case, query, approved_context, source
    )


evaluated_at = datetime.now(timezone.utc).replace(tzinfo=None)
with mlflow.start_run(run_name=f"quilla-{cfg['evaluation_release']}-{variant}") as active_run:
    mlflow.set_tags({
        "project": "quilla",
        "evaluation_type": "offline-regression",
        "evaluation_release": cfg["evaluation_release"],
        "variant": variant,
        "languages": "pt-BR,es-LATAM",
        "countries": "12",
        "managed_by": "dabs",
        "synthetic": "true",
    })
    result = mlflow.genai.evaluate(
        data=dataset,
        predict_fn=predict_fn,
        scorers=[
            language_policy,
            country_scope,
            financial_disclaimer,
            citation_policy,
            fact_coverage,
            sensitive_data_guardrail,
        ],
    )
    run_id = active_run.info.run_id

metrics = mlflow.MlflowClient().get_run(run_id).data.metrics
manifest_schema = T.StructType([
    T.StructField("evaluated_at", T.TimestampType(), False),
    T.StructField("evaluation_release", T.StringType(), False),
    T.StructField("experiment_id", T.StringType(), False),
    T.StructField("run_id", T.StringType(), False),
    T.StructField("variant", T.StringType(), False),
    T.StructField("evaluation_type", T.StringType(), False),
    T.StructField("record_count", T.IntegerType(), False),
    T.StructField("metrics_json", T.StringType(), False),
    T.StructField("passed", T.BooleanType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
required = [
    "offline_language_policy/mean",
    "offline_country_scope/mean",
    "offline_financial_disclaimer/mean",
    "offline_citation_policy/mean",
    "offline_fact_coverage/mean",
    "offline_sensitive_data_guardrail/mean",
]
assert all(key in metrics for key in required), metrics
passed = all(metrics[key] >= 0.95 for key in required)
manifest = ai_governance_table(cfg, "genai_offline_evaluation_runs")
row = [(evaluated_at, cfg["evaluation_release"], experiment.experiment_id, run_id, variant, "DETERMINISTIC_FULL", 72, json.dumps(metrics, sort_keys=True), passed, True)]
spark.createDataFrame(row, manifest_schema).write.mode("append").saveAsTable(manifest)
print(json.dumps({"status": "PASSED", "variant": variant, "run_id": run_id, "records": 72, "promotion_gate_passed": passed, "metrics": metrics}, ensure_ascii=False))

