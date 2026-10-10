# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Unity AI Gateway governado
# MAGIC
# MAGIC Reconcilia Model Services do Unity Catalog e suas políticas governadas.
# MAGIC Políticas determinísticas permanecem bloqueantes. A verificação factual
# MAGIC é feita pelo Model Service verificador e pela skill governada de fontes:
# MAGIC o handler pós-chamada `block_hallucination` não enxerga de forma confiável
# MAGIC toda a evidência recuperada por ferramentas MCP.

# COMMAND ----------

import json
from pathlib import Path
import sys

from databricks.sdk import WorkspaceClient

project_root = Path.cwd().parents[1]
sys.path.insert(0, str(project_root))

from quilla_supervisor.model_service_config import service_config, update_config


def widget(name, default):
    try:
        dbutils.widgets.text(name, default)
        return dbutils.widgets.get(name)
    except Exception:
        return default


catalog = widget("catalog", "example_catalog")
ai_schema = widget("ai_schema", "example_ai")
judge_service = widget("judge_model_service", "quilla-guard-judge")
primary_service = widget("primary_model_service", "quilla-supervisor-primary")
verifier_service = widget("verifier_model_service", "quilla-supervisor-verifier")
long_context_service = widget("long_context_model_service", "quilla-supervisor-long-context")
primary_fallback_model = widget("primary_fallback_model", "databricks-qwen35-122b-a10b")

client = WorkspaceClient()
base = "/api/2.1/unity-catalog/model-services"


services = {
    # Nano occasionally emits malformed JSON; Qwen 3.5 122B-A10B exceeded the
    # synchronous policy deadline in validation. Mini is the validated balance
    # of structured-output reliability and hot-path latency. Open-weight models
    # remain in the supervisor, fallback and independent verifier layers.
    judge_service: ("databricks-gpt-5-mini", (), False),
    primary_service: ("databricks-glm-5-3", (primary_fallback_model,), True),
    verifier_service: ("databricks-deepseek-v4-pro-0813", (), True),
    long_context_service: ("databricks-kimi-k3", (), True),
}
judge_fq = f"{catalog}.{ai_schema}.{judge_service}"

competitor_policy_function = f"{catalog}.{ai_schema}.quilla_block_competitor_mentions"
retention_policy_function = f"{catalog}.{ai_schema}.quilla_block_churn_encouragement"
pii_policy_function = f"{catalog}.{ai_schema}.quilla_block_pii_output"
competitor_policy_handler = f"functions/{competitor_policy_function}"
retention_policy_handler = f"functions/{retention_policy_function}"
pii_policy_handler = f"functions/{pii_policy_function}"

spark.sql(f"""
CREATE OR REPLACE FUNCTION {pii_policy_function}(event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
RETURN CASE
  WHEN event:type::string = 'response' AND (
    lower(event:context.message::string) LIKE '%___@___.___%' OR
    lower(event:context.message::string) LIKE '%___.___.___-__%' OR
    contains(lower(event:context.message::string), 'cpf:') OR
    contains(lower(event:context.message::string), 'senha:') OR
    contains(lower(event:context.message::string), 'contraseña:') OR
    contains(lower(event:context.message::string), 'access token:') OR
    contains(lower(event:context.message::string), 'api key:') OR
    contains(lower(event:context.message::string), 'cartão:') OR
    contains(lower(event:context.message::string), 'numero do cartao:') OR
    contains(lower(event:context.message::string), 'número do cartão:') OR
    contains(lower(event:context.message::string), 'tarjeta:') OR
    contains(lower(event:context.message::string), 'card number:')
  ) THEN to_variant_object(named_struct(
    'result', 'DENY',
    'reason', 'A política de privacidade Quilla impede a exposição de PII ou credenciais na resposta.'
  ))
  ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {competitor_policy_function}(event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
RETURN CASE
  WHEN event:type::string = 'response' AND (
    contains(lower(event:context.message::string), 'nubank') OR
    contains(lower(event:context.message::string), 'mercado pago') OR
    contains(lower(event:context.message::string), 'picpay') OR
    contains(lower(event:context.message::string), 'banco inter') OR
    contains(lower(event:context.message::string), 'neon') OR
    contains(lower(event:context.message::string), 'ualá') OR
    contains(lower(event:context.message::string), 'uala') OR
    contains(lower(event:context.message::string), 'rappipay') OR
    contains(lower(event:context.message::string), 'pagbank') OR
    contains(lower(event:context.message::string), 'c6 bank')
  ) THEN to_variant_object(named_struct(
    'result', 'DENY',
    'reason', 'A política de marca Quilla não permite citar concorrentes em respostas ao cliente.'
  ))
  ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
END
""")

spark.sql(f"""
CREATE OR REPLACE FUNCTION {retention_policy_function}(event VARIANT)
RETURNS VARIANT
LANGUAGE SQL
RETURN CASE
  WHEN event:type::string = 'response' AND (
    contains(lower(event:context.message::string), 'recomendo fechar sua conta') OR
    contains(lower(event:context.message::string), 'recomendo encerrar sua conta') OR
    contains(lower(event:context.message::string), 'você deve fechar sua conta') OR
    contains(lower(event:context.message::string), 'voce deve fechar sua conta') OR
    contains(lower(event:context.message::string), 'deveria encerrar sua conta') OR
    contains(lower(event:context.message::string), 'recomiendo cerrar tu cuenta') OR
    contains(lower(event:context.message::string), 'debes cerrar tu cuenta') OR
    contains(lower(event:context.message::string), 'deberías cancelar tu cuenta') OR
    contains(lower(event:context.message::string), 'deberias cancelar tu cuenta')
  ) THEN to_variant_object(named_struct(
    'result', 'DENY',
    'reason', 'A política de relacionamento responsável proíbe incentivar o encerramento da conta.'
  ))
  ELSE to_variant_object(named_struct('result', 'ALLOW', 'reason', ''))
END
""")


def exists(fq_name):
    try:
        client.api_client.do("GET", f"{base}/{fq_name}")
        return True
    except Exception as exc:
        if "404" in str(exc) or "does not exist" in str(exc).lower() or "not found" in str(exc).lower():
            return False
        raise


results = []
for service_name, (model_name, fallback_models, governed) in services.items():
    fq_name = f"{catalog}.{ai_schema}.{service_name}"
    config = service_config(model_name, fallback_models)
    if exists(fq_name):
        action = "UPDATED"
        update_warnings = []
        updates = [
            (
                "config.routing.destinations",
                {"routing": {"destinations": config["routing"]["destinations"]}},
            ),
            ("config.rate_limits", {"rate_limits": config["rate_limits"]}),
            ("config.usage_tracking", {"usage_tracking": config["usage_tracking"]}),
        ]
        if fallback_models:
            updates.append(
                (
                    "config.routing.fallback",
                    {"routing": {"fallback": config["routing"]["fallback"]}},
                )
            )
        for update_mask, partial_config in updates:
            try:
                client.api_client.do(
                    "PATCH",
                    f"{base}/{fq_name}",
                    query={"update_mask": update_mask},
                    body={"config": partial_config},
                )
            except Exception as exc:
                # Preview fields can be immutable between API revisions. The
                # postcondition below remains authoritative and fails if the
                # destination or policy is not the requested one.
                update_warnings.append(f"{update_mask}: {str(exc)[:240]}")
        if fallback_models:
            current = client.api_client.do("GET", f"{base}/{fq_name}")
            current_fallbacks = (
                current.get("config", {})
                .get("routing", {})
                .get("fallback", {})
                .get("destinations", [])
            )
            if not current_fallbacks:
                client.api_client.do(
                    "PATCH",
                    f"{base}/{fq_name}",
                    query={"update_mask": "config"},
                    body={"config": update_config(current, model_name, fallback_models)},
                )
    else:
        action = "CREATED"
        update_warnings = []
        client.api_client.do(
            "POST",
            base,
            query={"parent": f"schemas/{catalog}.{ai_schema}", "model_service_id": service_name},
            body={"config": config},
        )

    expected_handlers = []
    if governed:
        def builtin(suffix, handler, rank, phases):
            return {
                "name": f"{service_name}-{suffix}",
                "policy_type": "POLICY_TYPE_BUILTIN",
                "handler": handler,
                "rank": rank,
                "options": {
                    "model_service": f"model-services/{judge_fq}",
                    "phases": phases,
                    "dry_run": "false",
                },
            }

        policies = [
            builtin("unsafe-input", "system.ai.block_unsafe_content", 2, "pre_call"),
            builtin("unsafe-output", "system.ai.block_unsafe_content", 3, "post_call"),
            {
                "name": f"{service_name}-pii-output",
                "policy_type": "POLICY_TYPE_CUSTOM",
                "handler": pii_policy_handler,
                "rank": 4,
                "options": {"phases": "post_call", "dry_run": "false"},
            },
        ]
        # Jailbreak evaluates user-originated traffic. The verifier receives an
        # internal critique prompt containing candidate text, which otherwise
        # creates false positives and prevents independent verification.
        if service_name in {primary_service, long_context_service}:
            policies.insert(0, builtin("jailbreak", "system.ai.block_jailbreak", 1, "pre_call"))
        expected_handlers = [item["handler"] for item in policies]
        if service_name in {primary_service, long_context_service}:
            policies.extend(
                [
                    {
                        "name": f"{service_name}-competitors",
                        "policy_type": "POLICY_TYPE_CUSTOM",
                        "handler": competitor_policy_handler,
                        "rank": 10,
                        "options": {"phases": "post_call", "dry_run": "false"},
                    },
                    {
                        "name": f"{service_name}-responsible-retention",
                        "policy_type": "POLICY_TYPE_CUSTOM",
                        "handler": retention_policy_handler,
                        "rank": 11,
                        "options": {"phases": "post_call", "dry_run": "false"},
                    },
                ]
            )
            expected_handlers.extend([competitor_policy_handler, retention_policy_handler])
        client.api_client.do(
            "PATCH",
            f"{base}/{fq_name}",
            query={"update_mask": "config.service_policies"},
            body={"config": {"service_policies": policies}},
        )

    actual = client.api_client.do("GET", f"{base}/{fq_name}")
    actual_destinations = actual.get("config", {}).get("routing", {}).get("destinations", [])
    expected_model = f"models/system.ai.{model_name}"
    if not any(item.get("pay_per_token_config", {}).get("model") == expected_model for item in actual_destinations):
        raise AssertionError(f"Destino incorreto no Model Service {fq_name}: {actual}")
    expected_fallbacks = [f"models/system.ai.{model}" for model in fallback_models]
    actual_fallbacks = [
        item.get("pay_per_token_config", {}).get("model")
        for item in actual.get("config", {})
        .get("routing", {})
        .get("fallback", {})
        .get("destinations", [])
    ]
    if actual_fallbacks != expected_fallbacks:
        raise AssertionError(
            f"Fallback incorreto no Model Service {fq_name}: "
            f"esperado={expected_fallbacks}, atual={actual_fallbacks}"
        )
    policies = actual.get("config", {}).get("service_policies", [])
    actual_handlers = {item.get("handler") for item in policies}
    missing_handlers = set(expected_handlers) - actual_handlers
    if governed and missing_handlers:
        raise AssertionError(f"Guardrails ausentes no Model Service {fq_name}: {sorted(missing_handlers)}")
    results.append(
        {
            "service": fq_name,
            "action": action,
            "model": model_name,
            "fallback_models": list(fallback_models),
            "policy_handlers": sorted(actual_handlers),
            "usage_tracking": actual.get("config", {}).get("usage_tracking", {}).get("enabled"),
            "update_warnings": update_warnings,
        }
    )

for key, value in {
    "judge_model": judge_fq,
    "primary_model": f"{catalog}.{ai_schema}.{primary_service}",
    "verifier_model": f"{catalog}.{ai_schema}.{verifier_service}",
    "long_context_model": f"{catalog}.{ai_schema}.{long_context_service}",
}.items():
    dbutils.jobs.taskValues.set(key=key, value=value)

print(json.dumps({"status": "READY", "services": results}, ensure_ascii=False))
