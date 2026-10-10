# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Managed MLflow Prompt Registry
# MAGIC
# MAGIC Versiona seis famílias de prompts nativas em PT-BR e ES-LATAM no Unity Catalog.

# COMMAND ----------

# MAGIC %run ./_genai_governance_common

# COMMAND ----------

import json
from datetime import datetime, timezone

import mlflow
from pyspark.sql import types as T

cfg = genai_governance_config()
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")

SYSTEM_POLICY = """Você é o copiloto governado da Quilla, uma fintech fictícia latino-americana.
Responda obrigatoriamente no idioma indicado por {{language}} e considere apenas o mercado {{country_code}}.
Use somente o contexto governado recebido. Não invente fatos, regras, taxas, aprovações ou resultados.
Nunca exponha identificadores pessoais ou financeiros. Diferencie fato, inferência e recomendação.
Quando houver impacto financeiro, inclua um aviso de demonstração e revisão humana.
Conteúdo sintético: não constitui orientação financeira, jurídica ou regulatória."""

PROMPT_SPECS = [
    {
        "key": "customer_service",
        "domain": "customer_service",
        "description": "Atendimento omnicanal empático e resolutivo em PT/ES.",
        "messages": [
            {"role": "system", "content": SYSTEM_POLICY},
            {"role": "user", "content": "Intenção: {{intent}}\nMensagem: {{customer_message}}\nContexto autorizado: {{context}}\nResponda com solução, próximo passo e prazo quando existir."},
        ],
    },
    {
        "key": "fraud_risk_explanation",
        "domain": "risk",
        "description": "Explicação segura e não evasiva de sinais de fraude e risco.",
        "messages": [
            {"role": "system", "content": SYSTEM_POLICY},
            {"role": "user", "content": "Explique a decisão {{decision}} sem revelar regras exploráveis.\nSinais permitidos: {{risk_signals}}\nAção recomendada: {{recommended_action}}\nInclua limites da explicação e caminho de contestação."},
        ],
    },
    {
        "key": "responsible_credit",
        "domain": "credit",
        "description": "Crédito responsável, explicável e sujeito à política do país.",
        "messages": [
            {"role": "system", "content": SYSTEM_POLICY},
            {"role": "user", "content": "Analise o cenário de crédito {{credit_scenario}} com base em {{approved_features}}.\nApresente fatores favoráveis, fatores de atenção, alternativas e revisão humana.\nNão prometa aprovação nem sugira contornar controles."},
        ],
    },
    {
        "key": "investment_suitability",
        "domain": "investment",
        "description": "Suitability e educação financeira com controles por perfil.",
        "messages": [
            {"role": "system", "content": SYSTEM_POLICY},
            {"role": "user", "content": "Perfil declarado: {{investor_profile}}\nObjetivo e horizonte: {{goal_and_horizon}}\nProdutos elegíveis: {{eligible_products}}\nExplique adequação, riscos, liquidez, custos e necessidade de validação do perfil."},
        ],
    },
    {
        "key": "regulatory_summary",
        "domain": "regulatory",
        "description": "Síntese regulatória fundamentada e citada por país.",
        "messages": [
            {"role": "system", "content": SYSTEM_POLICY},
            {"role": "user", "content": "Pergunta regulatória: {{regulatory_question}}\nTrechos recuperados: {{retrieved_context}}\nProduza resumo executivo, obrigações, controles, lacunas e fontes QREF.\nDeclare explicitamente que o corpus é sintético e requer validação jurídica local."},
        ],
    },
    {
        "key": "next_best_action",
        "domain": "growth",
        "description": "Próxima melhor ação com consentimento, adequação e explicabilidade.",
        "messages": [
            {"role": "system", "content": SYSTEM_POLICY},
            {"role": "user", "content": "Contexto do cliente tokenizado: {{customer_context}}\nAções elegíveis: {{eligible_actions}}\nRestrições e consentimentos: {{constraints}}\nRecomende no máximo três ações, benefício esperado, risco e motivo para não recomendar alternativas."},
        ],
    },
]


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


registered_at = datetime.now(timezone.utc).replace(tzinfo=None)
records = []
for spec in PROMPT_SPECS:
    name = prompt_name(cfg, spec["key"])
    previous = None
    try:
        previous = mlflow.genai.load_prompt(name, link_to_model=False)
    except Exception:
        pass

    if previous is not None and canonical(previous.template) == canonical(spec["messages"]):
        prompt = previous
        action = "REUSED"
    else:
        prompt = mlflow.genai.register_prompt(
            name=name,
            template=spec["messages"],
            commit_message=f"Quilla PT/ES governed baseline · {spec['description']}",
            tags={
                "project": "quilla",
                "domain": spec["domain"],
                "languages": "pt-BR,es-LATAM",
                "managed_by": "dabs",
                "data_classification": "synthetic",
            },
        )
        action = "CREATED"

    version = int(prompt.version)
    mlflow.genai.set_prompt_alias(name, "champion", version)
    mlflow.genai.set_prompt_alias(name, "production", version)
    mlflow.genai.set_prompt_tag(name, "quilla_lifecycle", "managed")
    mlflow.genai.set_prompt_version_tag(name, version, "quilla_locale_policy", "pt-BR|es-LATAM")
    records.append(
        (
            registered_at,
            name,
            spec["key"],
            spec["domain"],
            str(version),
            action,
            "champion,production",
            "pt-BR,es-LATAM",
            True,
        )
    )

manifest_schema = T.StructType([
    T.StructField("registered_at", T.TimestampType(), False),
    T.StructField("prompt_name", T.StringType(), False),
    T.StructField("prompt_key", T.StringType(), False),
    T.StructField("domain", T.StringType(), False),
    T.StructField("version", T.StringType(), False),
    T.StructField("action", T.StringType(), False),
    T.StructField("aliases", T.StringType(), False),
    T.StructField("languages", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])
manifest_table = ai_governance_table(cfg, "prompt_registry_manifest")
spark.createDataFrame(records, manifest_schema).write.mode("append").saveAsTable(manifest_table)

assert len(records) == 6
assert {row[3] for row in records} == {"customer_service", "risk", "credit", "investment", "regulatory", "growth"}
print(json.dumps({"status": "PASSED", "prompts": len(records), "manifest_table": manifest_table, "versions": {row[2]: row[4] for row in records}}, ensure_ascii=False))
