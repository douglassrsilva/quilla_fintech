# Databricks notebook source
"""Validate Prompt Registry, UC traces and MLflow Production Monitoring."""

# COMMAND ----------

# MAGIC %run ./_genai_governance_common

# COMMAND ----------

import json
from datetime import datetime, timezone

import mlflow
from mlflow.entities.trace_location import UnityCatalog
from pyspark.sql import types as T

cfg = genai_governance_config()
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
experiment = mlflow.set_experiment(
    experiment_name=cfg["experiment_path"],
    trace_location=UnityCatalog(
        catalog_name=cfg["catalog"],
        schema_name=cfg["ai_schema"],
        table_prefix=cfg["trace_table_prefix"],
    ),
)

prompt_keys = [
    "customer_service",
    "fraud_risk_explanation",
    "responsible_credit",
    "investment_suitability",
    "regulatory_summary",
    "next_best_action",
]
prompts = [
    mlflow.genai.load_prompt(
        f"prompts:/{prompt_name(cfg, key)}@production", link_to_model=False
    )
    for key in prompt_keys
]
assert len(prompts) == 6
assert all(prompt.template and int(prompt.version) >= 1 for prompt in prompts)

scorers = mlflow.genai.list_scorers(experiment_id=experiment.experiment_id)
managed_scorers = [item for item in scorers if item.name.startswith("quilla_")]
expected_scorers = {
    "quilla_response_contract",
    "quilla_language_policy",
    "quilla_financial_disclaimer",
    "quilla_sensitive_data_guardrail",
    "quilla_regulatory_citations",
    "quilla_country_scope",
    "quilla_builtin_safety",
    "quilla_builtin_relevance",
    "quilla_builtin_guidelines",
    "quilla_builtin_groundedness",
    "quilla_builtin_retrieval_relevance",
    "quilla_conversation_completeness",
    "quilla_user_frustration",
}
assert expected_scorers.issubset({item.name for item in managed_scorers})
assert all(item.sample_rate is not None and item.sample_rate > 0 for item in managed_scorers if item.name in expected_scorers)

trace_manifest = ai_governance_table(cfg, "genai_trace_manifest")
trace_stats = spark.sql(
    f"""
    SELECT COUNT(DISTINCT trace_id) AS traces,
           COUNT(DISTINCT country_code) AS countries,
           COUNT(DISTINCT use_case) AS use_cases,
           SUM(CASE WHEN synthetic_flag THEN 0 ELSE 1 END) AS non_synthetic
    FROM {trace_manifest}
    WHERE experiment_id = '{experiment.experiment_id}'
    """
).first()
assert trace_stats.traces >= 72
assert trace_stats.countries == 12
assert trace_stats.use_cases == 6
assert trace_stats.non_synthetic == 0

ui_assets = [
    (
        "CUSTOM_TRACE_VIEW",
        "Operações de risco | Operaciones de riesgo",
        "READY_FOR_UI",
        "Exiba país, idioma, caso de uso, entrada e resposta lado a lado; mostre latência, status, prompt URI, sinais autorizados e avaliações. Inclua aprovação, motivo de revisão e comentário livre.",
    ),
    (
        "CUSTOM_TRACE_VIEW",
        "Compliance regulatório | Cumplimiento regulatorio",
        "READY_FOR_UI",
        "Exiba pergunta, resposta, fontes QREF, país e idioma; destaque quilla_regulatory_citations, quilla_country_scope e disclaimer. Inclua aprovado/reprovado, categoria da falha e justificativa.",
    ),
    (
        "REVIEW_QUEUE",
        "Risco e fraude LATAM | Riesgo y fraude LATAM",
        "READY_FOR_UI",
        "Filtre use_case=fraud_risk_explanation. Pergunte: decisão explicável; ausência de informação explorável; ação segura; comentário do revisor.",
    ),
    (
        "REVIEW_QUEUE",
        "Regulação por país | Regulación por país",
        "READY_FOR_UI",
        "Filtre use_case=regulatory_summary. Pergunte: país correto; fonte QREF presente; separação fato/inferência; disclaimer e necessidade de validação jurídica local.",
    ),
]
ui_schema = T.StructType([
    T.StructField("asset_type", T.StringType(), False),
    T.StructField("asset_name", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("ui_definition", T.StringType(), False),
])
ui_table = ai_governance_table(cfg, "genai_ui_asset_specs")
spark.createDataFrame(ui_assets, ui_schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(ui_table)

validated_at = datetime.now(timezone.utc).replace(tzinfo=None)
result_schema = T.StructType([
    T.StructField("validated_at", T.TimestampType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("experiment_id", T.StringType(), False),
    T.StructField("experiment_path", T.StringType(), False),
    T.StructField("prompt_count", T.IntegerType(), False),
    T.StructField("trace_count", T.LongType(), False),
    T.StructField("country_count", T.LongType(), False),
    T.StructField("use_case_count", T.LongType(), False),
    T.StructField("active_scorer_count", T.IntegerType(), False),
    T.StructField("ui_only_asset_count", T.IntegerType(), False),
])
result_table = ai_governance_table(cfg, "genai_governance_validation_runs")
result = [(
    validated_at,
    "PASSED",
    experiment.experiment_id,
    cfg["experiment_path"],
    len(prompts),
    int(trace_stats.traces),
    int(trace_stats.countries),
    int(trace_stats.use_cases),
    len(expected_scorers),
    len(ui_assets),
)]
spark.createDataFrame(result, result_schema).write.mode("append").saveAsTable(result_table)
print(json.dumps({"status": "PASSED", "experiment_id": experiment.experiment_id, "experiment_path": cfg["experiment_path"], "prompts": len(prompts), "traces": int(trace_stats.traces), "countries": int(trace_stats.countries), "use_cases": int(trace_stats.use_cases), "active_scorers": len(expected_scorers), "ui_only_assets": len(ui_assets), "ui_asset_table": ui_table}, ensure_ascii=False))
