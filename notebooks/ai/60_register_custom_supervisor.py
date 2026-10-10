# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Registro do supervisor customizado
# MAGIC
# MAGIC Registra o runtime próprio como MLflow `ResponsesAgent` no Unity Catalog.

# COMMAND ----------

import json
import os
from pathlib import Path
import sys

import mlflow
from mlflow import MlflowClient
from mlflow.models.auth_policy import AuthPolicy, SystemAuthPolicy, UserAuthPolicy
from mlflow.models.resources import (
    DatabricksGenieSpace,
    DatabricksVectorSearchIndex,
)


def widget(name, default):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)
    except Exception:
        return default


catalog = widget("catalog", "example_catalog")
ai_schema = widget("ai_schema", "example_ai")
model_leaf = widget("supervisor_model_name", "quilla_custom_supervisor")
endpoint_name = widget("supervisor_endpoint_name", "quilla-custom-supervisor")
release = widget("supervisor_release", "2026-09-04-custom-supervisor-v23-long-financial-fallback")
web_search_timeout_seconds = widget("web_search_timeout_seconds", "45")
you_financial_timeout_seconds = widget("you_financial_timeout_seconds", "90")
you_financial_max_input_chars = widget("you_financial_max_input_chars", "1200")
you_financial_daily_call_limit = widget("you_financial_daily_call_limit", "6")
you_financial_total_call_limit = widget("you_financial_total_call_limit", "10")
you_financial_conversation_call_limit = widget("you_financial_conversation_call_limit", "2")
lakebase_project = widget("lakebase_project", "quilla-ltap")
lakebase_database = widget("lakebase_database", "quilla")
lakebase_branch = widget("lakebase_branch", "production")
lakebase_endpoint = widget("lakebase_endpoint", "primary")
primary_model = widget("primary_model", f"{catalog}.{ai_schema}.quilla-supervisor-primary")
verifier_model = widget("verifier_model", f"{catalog}.{ai_schema}.quilla-supervisor-verifier")
long_context_model = widget("long_context_model", f"{catalog}.{ai_schema}.quilla-supervisor-long-context")
genie_ids = [
    widget("genie_executive_regional", ""),
    widget("genie_payments_performance", ""),
    widget("genie_risk_fraud", ""),
    widget("genie_responsible_credit", ""),
    widget("genie_customer_operations", ""),
    widget("genie_growth_wealth", ""),
]

project_root = Path.cwd().parents[1]
sys.path.insert(0, str(project_root))
os.environ.update(
    {
        "QUILLA_CATALOG": catalog,
        "QUILLA_AI_SCHEMA": ai_schema,
        "QUILLA_PRIMARY_MODEL": primary_model,
        "QUILLA_VERIFIER_MODEL": verifier_model,
        "QUILLA_LONG_CONTEXT_MODEL": long_context_model,
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
        "QUILLA_LAKEBASE_DATABASE": lakebase_database,
        "QUILLA_LAKEBASE_BRANCH": lakebase_branch,
        "QUILLA_LAKEBASE_ENDPOINT": lakebase_endpoint,
        "QUILLA_GENIE_EXECUTIVE_REGIONAL": genie_ids[0],
        "QUILLA_GENIE_PAYMENTS_PERFORMANCE": genie_ids[1],
        "QUILLA_GENIE_CUSTOMER_OPERATIONS": genie_ids[4],
        "QUILLA_GENIE_RISK_FRAUD": genie_ids[2],
        "QUILLA_GENIE_RESPONSIBLE_CREDIT": genie_ids[3],
        "QUILLA_GENIE_GROWTH_WEALTH": genie_ids[5],
    }
)

from quilla_supervisor.model import QuillaResponsesAgent

registered_name = f"{catalog}.{ai_schema}.{model_leaf}"
mlflow.set_registry_uri("databricks-uc")
resources = [
    DatabricksVectorSearchIndex(index_name=f"{catalog}.{ai_schema}.regulatory_knowledge_hybrid_idx"),
    *[DatabricksGenieSpace(genie_space_id=value) for value in genie_ids if value],
]
auth_policy = AuthPolicy(
    # OBO makes MCP, Governed Skills, Genie and Model Services run with the
    # invoking user's UC permissions instead of an opaque endpoint identity.
    user_auth_policy=UserAuthPolicy(
        api_scopes=[
            "ai-gateway",
            "vector-search",
            "genie",
            "model-serving",
            "iam.current-user:read",
            "sql",
        ]
    ),
    system_auth_policy=SystemAuthPolicy(resources=resources),
)
input_example = {
    "input": [
        {
            "role": "user",
            "content": [{"type": "input_text", "text": "Como a inflação afeta o crédito responsável?"}],
        }
    ],
    "context": {"user_id": "quilla-demo-user"},
    "custom_inputs": {
        "conversation_id": "registration-example",
        "tenant_id": "quilla-demo",
        "country_code": "BR",
        "locale": "pt-BR",
        "demo": True,
        "registration_validation": True,
    },
}

with mlflow.start_run(run_name=f"quilla-custom-supervisor-{release}") as run:
    mlflow.set_tags(
        {
            "project": "quilla",
            "release": release,
            "agent_framework": "custom-responses-agent",
            "primary_model": primary_model,
            "verifier_model": verifier_model,
            "long_context_model": long_context_model,
            "synthetic_environment": "true",
        }
    )
    info = mlflow.pyfunc.log_model(
        name="quilla_supervisor",
        python_model=QuillaResponsesAgent(),
        code_paths=[str(project_root / "quilla_supervisor")],
        input_example=input_example,
        pip_requirements=[
            "mlflow>=3.15.0,<4",
            "openai-agents>=0.7,<1",
            "databricks-sdk>=0.60.0,<1",
            "psycopg[binary]>=3.2.9,<4",
        ],
        auth_policy=auth_policy,
        streamable=True,
        metadata={
            "release": release,
            "supported_locales": "pt-BR,es-LATAM",
            "managed_by": "dabs",
            "web_search_timeout_seconds": web_search_timeout_seconds,
            "you_financial_fallback": "enabled-deep-only",
            "you_financial_daily_call_limit": you_financial_daily_call_limit,
            "you_financial_total_call_limit": you_financial_total_call_limit,
        },
    )

    # O modelo UC já existe. Criar a versão diretamente evita que o MLflow
    # emita um create_registered_model redundante, importante em metastores
    # que já alcançaram a quota global de objetos registrados.
    created_version = MlflowClient().create_model_version(
        name=registered_name,
        source=info.model_uri,
        run_id=run.info.run_id,
        model_id=info.model_id,
        tags={"release": release, "managed_by": "dabs"},
    )

model_version = str(created_version.version)
dbutils.jobs.taskValues.set(key="model_version", value=model_version)
dbutils.jobs.taskValues.set(key="registered_model_name", value=registered_name)
print(json.dumps({
    "status": "REGISTERED",
    "run_id": run.info.run_id,
    "registered_model": registered_name,
    "model_version": model_version,
    "release": release,
    "endpoint": endpoint_name,
}, ensure_ascii=False))
