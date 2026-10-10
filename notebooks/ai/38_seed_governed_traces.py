# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Traces governados PT/ES
# MAGIC
# MAGIC Publica traces sintéticos com spans de prompt, retrieval e modelo para as seis jornadas.

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
trace_location = UnityCatalog(
    catalog_name=cfg["catalog"],
    schema_name=cfg["ai_schema"],
    table_prefix=cfg["trace_table_prefix"],
)
experiment = mlflow.set_experiment(
    experiment_name=cfg["experiment_path"], trace_location=trace_location
)

COUNTRY_LANGUAGE = [
    ("BR", "pt-BR"),
    ("MX", "es-MX"),
    ("CO", "es-CO"),
    ("AR", "es-AR"),
    ("CL", "es-CL"),
    ("PE", "es-PE"),
    ("EC", "es-EC"),
    ("GT", "es-GT"),
    ("DO", "es-DO"),
    ("CR", "es-CR"),
    ("PA", "es-PA"),
    ("UY", "es-UY"),
]
USE_CASES = [
    ("customer_service", "Como resolvo uma transferência pendente?", "¿Cómo resuelvo una transferencia pendiente?"),
    ("fraud_risk_explanation", "Por que esta compra entrou em revisão?", "¿Por qué esta compra entró en revisión?"),
    ("responsible_credit", "Quais fatores influenciam a análise de crédito?", "¿Qué factores influyen en el análisis de crédito?"),
    ("investment_suitability", "Como comparar opções adequadas ao meu perfil?", "¿Cómo comparo opciones adecuadas a mi perfil?"),
    ("regulatory_summary", "Quais controles de privacidade são relevantes?", "¿Qué controles de privacidad son relevantes?"),
    ("next_best_action", "Qual é a próxima ação mais adequada?", "¿Cuál es la próxima acción más adecuada?"),
]
FINANCIAL_CASES = {"responsible_credit", "investment_suitability", "next_best_action"}


def response_for(country, language, use_case):
    is_pt = language.startswith("pt")
    if is_pt:
        answer = {
            "customer_service": "A transferência permanece em análise. Verifique o status no aplicativo e, se necessário, abra atendimento para revisão humana.",
            "fraud_risk_explanation": "A operação foi encaminhada para revisão por uma combinação de sinais autorizados. A equipe pode confirmar a identidade sem revelar controles exploráveis.",
            "responsible_credit": "A análise considera capacidade de pagamento, histórico autorizado e contexto econômico. A decisão final depende das regras locais e de revisão humana.",
            "investment_suitability": "Compare risco, liquidez, custos e horizonte. Somente alternativas compatíveis com o perfil validado devem ser consideradas.",
            "regulatory_summary": "O corpus sintético indica controles de finalidade, consentimento, acesso mínimo e trilha de auditoria. A obrigação aplicável deve ser validada localmente.",
            "next_best_action": "A ação elegível prioriza utilidade, consentimento e adequação. Uma alternativa pode ser não realizar oferta quando os controles não forem satisfeitos.",
        }[use_case]
        disclaimer = "Demonstração com dados sintéticos; requer revisão humana e não constitui orientação financeira, jurídica ou regulatória."
    else:
        answer = {
            "customer_service": "La transferencia permanece en análisis. Verifica el estado en la aplicación y, si corresponde, solicita una revisión humana.",
            "fraud_risk_explanation": "La operación fue enviada a revisión por una combinación de señales autorizadas. El equipo puede confirmar la identidad sin revelar controles explotables.",
            "responsible_credit": "El análisis considera capacidad de pago, historial autorizado y contexto económico. La decisión final depende de reglas locales y revisión humana.",
            "investment_suitability": "Compara riesgo, liquidez, costos y horizonte. Solo deben considerarse alternativas compatibles con el perfil validado.",
            "regulatory_summary": "El corpus sintético indica controles de finalidad, consentimiento, acceso mínimo y trazabilidad. La obligación aplicable debe validarse localmente.",
            "next_best_action": "La acción elegible prioriza utilidad, consentimiento e idoneidad. No realizar una oferta es una alternativa cuando no se cumplen los controles.",
        }[use_case]
        disclaimer = "Demostración con datos sintéticos; requiere revisión humana y no constituye asesoría financiera, jurídica ni regulatoria."
    sources = []
    if use_case == "regulatory_summary":
        sources = [{"document_id": f"QREF-{country}-PRIV-001", "official": False, "synthetic": True}]
    return {
        "status": "DEMO_ONLY",
        "country_code": country,
        "language": language,
        "answer": answer,
        "disclaimer": disclaimer,
        "sources": sources,
    }


created_at = datetime.now(timezone.utc).replace(tzinfo=None)
trace_records = []
for country, language in COUNTRY_LANGUAGE:
    for use_case, query_pt, query_es in USE_CASES:
        query = query_pt if language.startswith("pt") else query_es
        inputs = {
            "country_code": country,
            "language": language,
            "use_case": use_case,
            "query": query,
            "customer_token": f"DEMO-{country}-CUSTOMER",
        }
        outputs = response_for(country, language, use_case)
        prompt = mlflow.genai.load_prompt(
            f"prompts:/{prompt_name(cfg, use_case)}@production", link_to_model=False
        )
        with mlflow.start_span(name=f"quilla_{use_case}", span_type="CHAIN") as root:
            root.set_inputs(inputs)
            mlflow.update_current_trace(
                tags={
                    "project": "quilla",
                    "environment": cfg["trace_table_prefix"].removeprefix("quilla_genai_"),
                    "country_code": country,
                    "language": language,
                    "use_case": use_case,
                    "synthetic": "true",
                    "review_status": "pending",
                },
                metadata={
                    "prompt_uri": prompt.uri,
                    "governance": "unity_catalog",
                    "data_classification": "synthetic",
                },
                session_id=f"QUILLA-DEMO-{country}-{language}",
                user=f"synthetic-reviewer-{country.lower()}",
                request_preview=query,
                response_preview=outputs["answer"][:180],
            )
            with mlflow.start_span(name="managed_prompt", span_type="PROMPT") as span:
                span.set_inputs({"prompt_uri": prompt.uri, "variables": inputs})
                span.set_outputs({"version": str(prompt.version), "aliases": list(prompt.aliases or [])})
            if use_case == "regulatory_summary":
                with mlflow.start_span(name="regulatory_retrieval", span_type="RETRIEVER") as span:
                    span.set_inputs({"country_code": country, "language": language, "query": query})
                    span.set_outputs(outputs["sources"])
            with mlflow.start_span(name="governed_response", span_type="CHAT_MODEL") as span:
                span.set_inputs({"prompt_uri": prompt.uri, "query": query})
                span.set_outputs(outputs)
                span.set_attribute("quilla.demo.synthetic", True)
                span.set_attribute("quilla.human_review_required", use_case in FINANCIAL_CASES)
            root.set_outputs(outputs)
            trace_id = root.trace_id
        trace_records.append((created_at, trace_id, experiment.experiment_id, country, language, use_case, prompt.uri, "PENDING_REVIEW", True))

mlflow.flush_trace_async_logging()
trace_schema = T.StructType([
    T.StructField("created_at", T.TimestampType(), False),
    T.StructField("trace_id", T.StringType(), False),
    T.StructField("experiment_id", T.StringType(), False),
    T.StructField("country_code", T.StringType(), False),
    T.StructField("language", T.StringType(), False),
    T.StructField("use_case", T.StringType(), False),
    T.StructField("prompt_uri", T.StringType(), False),
    T.StructField("review_status", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
manifest_table = ai_governance_table(cfg, "genai_trace_manifest")
spark.createDataFrame(trace_records, trace_schema).write.mode("append").saveAsTable(manifest_table)

assert len(trace_records) == 72
assert len({row[3] for row in trace_records}) == 12
assert len({row[5] for row in trace_records}) == 6
print(json.dumps({"status": "PASSED", "experiment_id": experiment.experiment_id, "trace_location": f"{cfg['catalog']}.{cfg['ai_schema']}.{cfg['trace_table_prefix']}", "traces": len(trace_records), "countries": 12, "use_cases": 6}, ensure_ascii=False))

