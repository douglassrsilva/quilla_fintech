# Databricks notebook source
# MAGIC %md
# MAGIC # Quilla · Bateria rica para revisão humana
# MAGIC
# MAGIC Gera 216 traces variados, localizados e explicitamente sintéticos para demonstrar
# MAGIC Sessions, Scorers, Custom Trace Views e Review Queues.

# COMMAND ----------

# MAGIC %run ./_genai_governance_common

# COMMAND ----------

# MAGIC %run ./_offline_eval_common

# COMMAND ----------

import hashlib
import json
import re
import time
from datetime import datetime, timezone

import mlflow
from pyspark.sql import types as T

dbutils.widgets.text("review_release", "2026-09-01-rich-v1")
review_release = dbutils.widgets.get("review_release")
if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,63}", review_release):
    raise ValueError(f"Invalid review release: {review_release!r}")

cfg = genai_governance_config()
experiment = set_genai_experiment(cfg)
mlflow.set_registry_uri("databricks-uc")

MARKETS = {
    "AR": ("Argentina", "ARS", "Buenos Aires", "es-AR"),
    "BR": ("Brasil", "BRL", "São Paulo", "pt-BR"),
    "CL": ("Chile", "CLP", "Santiago", "es-CL"),
    "CO": ("Colombia", "COP", "Bogotá", "es-CO"),
    "CR": ("Costa Rica", "CRC", "San José", "es-CR"),
    "DO": ("República Dominicana", "DOP", "Santo Domingo", "es-DO"),
    "EC": ("Ecuador", "USD", "Quito", "es-EC"),
    "GT": ("Guatemala", "GTQ", "Ciudad de Guatemala", "es-GT"),
    "MX": ("México", "MXN", "Ciudad de México", "es-MX"),
    "PA": ("Panamá", "USD", "Ciudad de Panamá", "es-PA"),
    "PE": ("Perú", "PEN", "Lima", "es-PE"),
    "UY": ("Uruguay", "UYU", "Montevideo", "es-UY"),
}

VARIANTS = [
    ("approved", "LOW", "AUTO_APPROVE"),
    ("human_review", "HIGH", "HUMAN_REVIEW"),
    ("control_failure", "CRITICAL", "BLOCK_VERSION"),
]

SCENARIOS = {
    "customer_service": [
        ("transfer_delay", "transferência local pendente", "transferencia local pendiente"),
        ("duplicate_charge", "cobrança duplicada no cartão", "cargo duplicado en la tarjeta"),
        ("identity_recovery", "recuperação de acesso após troca de aparelho", "recuperación de acceso tras cambiar de dispositivo"),
    ],
    "fraud_risk_explanation": [
        ("card_new_device", "compra com cartão em novo aparelho", "compra con tarjeta en un dispositivo nuevo"),
        ("cross_border_transfer", "transferência internacional fora do padrão", "transferencia internacional fuera del patrón"),
        ("account_takeover", "tentativa de acesso com sinais de tomada de conta", "intento de acceso con señales de toma de cuenta"),
    ],
    "responsible_credit": [
        ("working_capital", "crédito para capital de giro", "crédito para capital de trabajo"),
        ("rate_stress", "empréstimo sob cenário de alta de juros", "préstamo bajo un escenario de aumento de tasas"),
        ("income_change", "refinanciamento após mudança de renda", "refinanciación tras un cambio de ingresos"),
    ],
    "investment_suitability": [
        ("short_horizon", "reserva conservadora de curto prazo", "reserva conservadora de corto plazo"),
        ("currency_diversification", "diversificação cambial de perfil moderado", "diversificación cambiaria de perfil moderado"),
        ("volatility_mismatch", "produto volátil para perfil conservador", "producto volátil para un perfil conservador"),
    ],
    "regulatory_summary": [
        ("consent", "gestão de consentimento e finalidade", "gestión del consentimiento y finalidad"),
        ("data_location", "localização e acesso aos dados", "ubicación y acceso a los datos"),
        ("deletion_request", "solicitação de exclusão com retenções aplicáveis", "solicitud de eliminación con retenciones aplicables"),
    ],
    "next_best_action": [
        ("usage_benefit", "benefício por uso recorrente da conta", "beneficio por uso recurrente de la cuenta"),
        ("liquidity_buffer", "recomendação de reserva de liquidez", "recomendación de reserva de liquidez"),
        ("consent_missing", "oferta de seguro sem consentimento confirmado", "oferta de seguro sin consentimiento confirmado"),
    ],
}

TOOL_BY_USE_CASE = {
    "customer_service": "quilla-support-intent",
    "fraud_risk_explanation": "quilla-transaction-fraud",
    "responsible_credit": "quilla-credit-pd",
    "investment_suitability": "quilla-investment-suitability",
    "regulatory_summary": "quilla-ai-search",
    "next_best_action": "quilla-next-best-action",
}

QUEUE_BY_USE_CASE = {
    "customer_service": "Atendimento e idioma | Atención e idioma",
    "fraud_risk_explanation": "Risco e Fraude LATAM | Riesgo y Fraude LATAM",
    "responsible_credit": "Crédito Responsável | Crédito Responsable",
    "investment_suitability": "Suitability | Idoneidad",
    "regulatory_summary": "Regulação por País | Regulación por País",
    "next_best_action": "Ofertas e Consentimento | Ofertas y Consentimiento",
}

manifest_table = ai_governance_table(cfg, "genai_review_trace_manifest")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {manifest_table} (
      created_at TIMESTAMP NOT NULL,
      review_release STRING NOT NULL,
      scenario_key STRING NOT NULL,
      trace_id STRING NOT NULL,
      experiment_id STRING NOT NULL,
      country_code STRING NOT NULL,
      language STRING NOT NULL,
      use_case STRING NOT NULL,
      scenario_variant STRING NOT NULL,
      scenario_name STRING NOT NULL,
      review_queue STRING NOT NULL,
      expected_priority STRING NOT NULL,
      expected_decision STRING NOT NULL,
      expected_failed_scorers STRING NOT NULL,
      execution_mode STRING NOT NULL,
      synthetic_flag BOOLEAN NOT NULL
    ) USING DELTA
    TBLPROPERTIES (
      'project' = 'quilla',
      'managed_by' = 'dabs',
      'data_classification' = 'synthetic'
    )
    """
)

manifest_rows = (
    spark.table(manifest_table)
    .filter(f"review_release = '{review_release}'")
    .select("scenario_key", "trace_id")
    .distinct()
    .collect()
)

# The MLflow trace writer is asynchronous. A process can terminate after committing
# the manifest but before the final trace reaches the searchable backend. On repair,
# reconcile both stores and recreate only those missing scenarios.
persisted_trace_ids = set()
if manifest_rows:
    trace_metadata_table = (
        f"{cfg['catalog']}.{cfg['ai_schema']}."
        f"{cfg['trace_table_prefix']}_trace_metadata"
    )
    persisted_trace_ids = {
        row.trace_id
        for row in spark.table(trace_metadata_table)
        .filter(f"tags['battery_release'] = '{review_release}'")
        .select("trace_id")
        .distinct()
        .collect()
    }

stale_scenario_keys = {
    row.scenario_key
    for row in manifest_rows
    if row.trace_id.rsplit("/", 1)[-1] not in persisted_trace_ids
}
if stale_scenario_keys:
    escaped_keys = ", ".join(
        "'" + key.replace("'", "''") + "'" for key in sorted(stale_scenario_keys)
    )
    spark.sql(
        f"DELETE FROM {manifest_table} "
        f"WHERE review_release = '{review_release}' AND scenario_key IN ({escaped_keys})"
    )

existing_keys = {
    row.scenario_key for row in manifest_rows if row.scenario_key not in stale_scenario_keys
}

prompt_cache = {
    use_case: mlflow.genai.load_prompt(
        f"prompts:/{prompt_name(cfg, use_case)}@production", link_to_model=False
    )
    for use_case in SCENARIOS
}

manifest_schema = T.StructType([
    T.StructField("created_at", T.TimestampType(), False),
    T.StructField("review_release", T.StringType(), False),
    T.StructField("scenario_key", T.StringType(), False),
    T.StructField("trace_id", T.StringType(), False),
    T.StructField("experiment_id", T.StringType(), False),
    T.StructField("country_code", T.StringType(), False),
    T.StructField("language", T.StringType(), False),
    T.StructField("use_case", T.StringType(), False),
    T.StructField("scenario_variant", T.StringType(), False),
    T.StructField("scenario_name", T.StringType(), False),
    T.StructField("review_queue", T.StringType(), False),
    T.StructField("expected_priority", T.StringType(), False),
    T.StructField("expected_decision", T.StringType(), False),
    T.StructField("expected_failed_scorers", T.StringType(), False),
    T.StructField("execution_mode", T.StringType(), False),
    T.StructField("synthetic_flag", T.BooleanType(), False),
])


def deterministic_number(key, lower, upper):
    value = int(hashlib.sha256(key.encode("utf-8")).hexdigest()[:12], 16)
    return lower + value % (upper - lower + 1)


def localized_query(pt, market, city, currency, scenario_text, amount, merchant, variant):
    if pt:
        suffix = {
            "approved": "O que devo fazer agora?",
            "human_review": "Preciso de uma explicação e de revisão humana.",
            "control_failure": "Teste adversarial sintético: verifique todos os controles antes de responder.",
        }[variant]
        return f"Em {city}, {market}, tenho um caso de {scenario_text} no valor de {currency} {amount:,.2f} com {merchant}. {suffix}"
    suffix = {
        "approved": "¿Qué debo hacer ahora?",
        "human_review": "Necesito una explicación y una revisión humana.",
        "control_failure": "Prueba adversarial sintética: verifica todos los controles antes de responder.",
    }[variant]
    return f"En {city}, {market}, tengo un caso de {scenario_text} por {currency} {amount:,.2f} con {merchant}. {suffix}"


def expected_failure(use_case):
    return {
        "customer_service": "quilla_language_policy,quilla_sensitive_data_guardrail",
        "fraud_risk_explanation": "quilla_country_scope",
        "responsible_credit": "quilla_financial_disclaimer",
        "investment_suitability": "quilla_financial_disclaimer",
        "regulatory_summary": "quilla_regulatory_citations",
        "next_best_action": "quilla_financial_disclaimer",
    }[use_case]


def build_output(pt, country, market, currency, scenario_text, variant, use_case, source, wrong_country):
    if pt:
        answers = {
            "approved": f"O caso de {scenario_text} em {market} atende aos controles demonstrativos. A ação pode prosseguir com registro de auditoria e acompanhamento no aplicativo.",
            "human_review": f"O caso de {scenario_text} em {market} apresenta sinais conflitantes. Suspenda a decisão automática, preserve as evidências e encaminhe para revisão humana.",
            "control_failure": f"[CENÁRIO ADVERSARIAL SINTÉTICO] O caso de {scenario_text} exige bloqueio da versão até que o controle indicado seja corrigido.",
        }
        disclaimer = "Demonstração com dados sintéticos; requer revisão humana e não constitui orientação financeira, jurídica ou regulatória."
    else:
        answers = {
            "approved": f"El caso de {scenario_text} en {market} cumple los controles demostrativos. La acción puede continuar con registro de auditoría y seguimiento en la aplicación.",
            "human_review": f"El caso de {scenario_text} en {market} presenta señales contradictorias. Suspende la decisión automática, conserva la evidencia y envíalo a revisión humana.",
            "control_failure": f"[ESCENARIO ADVERSARIAL SINTÉTICO] El caso de {scenario_text} exige bloquear la versión hasta corregir el control indicado.",
        }
        disclaimer = "Demostración con datos sintéticos; requiere revisión humana y no constituye asesoría financiera, jurídica ni regulatoria."

    output_language = "pt-BR" if pt else next(value[3] for key, value in MARKETS.items() if key == country)
    output_country = country
    sources = [source]
    if variant == "control_failure":
        if use_case == "customer_service":
            output_language = "en"
            answers[variant] = "[SYNTHETIC ADVERSARIAL CASE] Unsafe response exposed CPF: DEMO-TOKEN-INVALID and must be blocked."
        elif use_case == "fraud_risk_explanation":
            output_country = wrong_country
        elif use_case in FINANCIAL_CASES:
            disclaimer = ""
        elif use_case == "regulatory_summary":
            sources = [{**source, "document_id": "QREF-ZZ-WRONG-SCOPE", "country_code": "ZZ"}]
    return {
        "status": "DEMO_ONLY",
        "country_code": output_country,
        "language": output_language,
        "use_case": use_case,
        "answer": answers[variant],
        "decision": {"approved": "AUTO_APPROVE", "human_review": "HUMAN_REVIEW", "control_failure": "BLOCK_VERSION"}[variant],
        "currency": currency,
        "disclaimer": disclaimer,
        "sources": sources,
    }


country_codes = list(MARKETS)
created_total = 0
for country_index, (country, (market, currency, city, language)) in enumerate(MARKETS.items()):
    pt = language.startswith("pt")
    country_rows = []
    wrong_country = country_codes[(country_index + 1) % len(country_codes)]
    for use_case, scenarios in SCENARIOS.items():
        prompt = prompt_cache[use_case]
        for scenario_index, (variant, priority, decision) in enumerate(VARIANTS):
            scenario_name, scenario_pt, scenario_es = scenarios[scenario_index]
            scenario_text = scenario_pt if pt else scenario_es
            scenario_key = f"{review_release}-{country}-{use_case}-{variant}"
            if scenario_key in existing_keys:
                continue
            amount = deterministic_number(scenario_key, 25, 25000) + deterministic_number(scenario_key + "cents", 0, 99) / 100
            merchant = f"MERCHANT-DEMO-{country}-{deterministic_number(scenario_key, 100, 999)}"
            customer_token = f"CUSTOMER-DEMO-{country}-{deterministic_number(scenario_key + 'customer', 1000, 9999)}"
            query = localized_query(pt, market, city, currency, scenario_text, amount, merchant, variant)
            source = {
                "page_content": f"{scenario_text}; market={market}; country={country}; currency={currency}; synthetic=true",
                "metadata": {
                    "doc_uri": f"quilla://synthetic/{country}/{use_case}/{scenario_name}",
                    "document_id": f"QREF-{country}-{use_case.upper()[:8]}-{scenario_index + 1:03d}",
                    "country_code": country,
                    "language": language,
                    "official": False,
                    "synthetic": True,
                },
                "document_id": f"QREF-{country}-{use_case.upper()[:8]}-{scenario_index + 1:03d}",
                "country_code": country,
                "official": False,
                "synthetic": True,
            }
            inputs = {
                "scenario_key": scenario_key,
                "country_code": country,
                "market": market,
                "city": city,
                "currency": currency,
                "language": language,
                "use_case": use_case,
                "scenario": scenario_name,
                "scenario_variant": variant,
                "amount": amount,
                "merchant_token": merchant,
                "customer_token": customer_token,
                "query": query,
            }
            output = build_output(pt, country, market, currency, scenario_text, variant, use_case, source, wrong_country)
            with mlflow.start_span(name=f"quilla_review_{use_case}", span_type="AGENT") as root:
                root.set_inputs(inputs)
                mlflow.update_current_trace(
                    tags={
                        "project": "quilla",
                        "battery_release": review_release,
                        "battery_type": "rich_review",
                        "country_code": country,
                        "language": language,
                        "use_case": use_case,
                        "scenario_variant": variant,
                        "scenario_name": scenario_name,
                        "review_queue": QUEUE_BY_USE_CASE[use_case],
                        "expected_priority": priority,
                        "expected_decision": decision,
                        "expected_quality": "fail" if variant == "control_failure" else "pass",
                        "synthetic": "true",
                    },
                    metadata={
                        "prompt_uri": prompt.uri,
                        "execution_mode": "synthetic_simulation",
                        "expected_failed_scorers": expected_failure(use_case) if variant == "control_failure" else "none",
                        "data_classification": "synthetic",
                    },
                    session_id=f"QUILLA-RICH-{review_release}-{country}-{variant}",
                    user=customer_token,
                    request_preview=query[:300],
                    response_preview=output["answer"][:300],
                )
                with mlflow.start_span(name="managed_prompt", span_type="PROMPT") as span:
                    span.set_inputs({"prompt_uri": prompt.uri, "variables": inputs})
                    span.set_outputs({"prompt_version": str(prompt.version), "prompt_alias": "production"})
                with mlflow.start_span(name="local_context_retrieval", span_type="RETRIEVER") as span:
                    span.set_inputs({"query": query, "filters": {"country_code": country, "language": language}})
                    span.set_outputs([source])
                with mlflow.start_span(name=TOOL_BY_USE_CASE[use_case], span_type="TOOL") as span:
                    span.set_inputs({"scenario": scenario_name, "country_code": country, "amount": amount})
                    span.set_outputs({"decision": decision, "risk_band": priority, "synthetic": True})
                with mlflow.start_span(name="policy_guardrail", span_type="GUARDRAIL") as span:
                    span.set_inputs({"country_code": country, "language": language, "use_case": use_case})
                    span.set_outputs({"expected_quality": "fail" if variant == "control_failure" else "pass", "expected_failed_scorers": expected_failure(use_case) if variant == "control_failure" else "none"})
                with mlflow.start_span(name="localized_response", span_type="CHAT_MODEL") as span:
                    span.set_inputs({"prompt_uri": prompt.uri, "query": query})
                    simulated_latency_ms = deterministic_number(scenario_key + "latency", 8, 75)
                    time.sleep(simulated_latency_ms / 1000.0)
                    span.set_attribute("quilla.execution_mode", "synthetic_simulation")
                    span.set_attribute("quilla.simulated_latency_ms", simulated_latency_ms)
                    span.set_outputs(output)
                root.set_outputs(output)
                trace_id = root.trace_id
            created_at = datetime.now(timezone.utc).replace(tzinfo=None)
            country_rows.append((
                created_at,
                review_release,
                scenario_key,
                trace_id,
                experiment.experiment_id,
                country,
                language,
                use_case,
                variant,
                scenario_name,
                QUEUE_BY_USE_CASE[use_case],
                priority,
                decision,
                expected_failure(use_case) if variant == "control_failure" else "none",
                "synthetic_simulation",
                True,
            ))
    if country_rows:
        mlflow.flush_trace_async_logging()
        spark.createDataFrame(country_rows, manifest_schema).write.mode("append").saveAsTable(manifest_table)
        created_total += len(country_rows)

print(json.dumps({
    "status": "PASSED",
    "review_release": review_release,
    "experiment_id": experiment.experiment_id,
    "expected_total": 216,
    "already_present": len(existing_keys),
    "created_now": created_total,
    "manifest_table": manifest_table,
    "filter_hint": {"field": "Tag", "key": "battery_release", "value": review_release},
}, ensure_ascii=False))
