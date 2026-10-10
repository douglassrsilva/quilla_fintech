# Databricks notebook source
"""Reusable golden records, traced predictors and evaluation helpers."""

import json

import mlflow
from mlflow.entities.trace_location import UnityCatalog


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
    "customer_service",
    "fraud_risk_explanation",
    "responsible_credit",
    "investment_suitability",
    "regulatory_summary",
    "next_best_action",
]

FINANCIAL_CASES = {"responsible_credit", "investment_suitability", "next_best_action"}


def set_genai_experiment(cfg):
    mlflow.set_tracking_uri("databricks")
    return mlflow.set_experiment(
        experiment_name=cfg["experiment_path"],
        trace_location=UnityCatalog(
            catalog_name=cfg["catalog"],
            schema_name=cfg["ai_schema"],
            table_prefix=cfg["trace_table_prefix"],
        ),
    )


def _scenario_text(country, language, use_case):
    pt = language.startswith("pt")
    query = {
        "customer_service": ("Como resolvo uma transferência pendente?", "¿Cómo resuelvo una transferencia pendiente?"),
        "fraud_risk_explanation": ("Por que esta compra entrou em revisão?", "¿Por qué esta compra entró en revisión?"),
        "responsible_credit": ("Quais fatores influenciam a análise de crédito?", "¿Qué factores influyen en el análisis de crédito?"),
        "investment_suitability": ("Como comparar opções adequadas ao meu perfil?", "¿Cómo comparo opciones adecuadas a mi perfil?"),
        "regulatory_summary": ("Quais controles de privacidade são relevantes?", "¿Qué controles de privacidad son relevantes?"),
        "next_best_action": ("Qual é a próxima ação mais adequada?", "¿Cuál es la próxima acción más adecuada?"),
    }[use_case][0 if pt else 1]
    answer = {
        "customer_service": (
            "A transferência permanece em análise; acompanhe o status e solicite revisão humana se o prazo informado for excedido.",
            "La transferencia permanece en análisis; revisa el estado y solicita revisión humana si se supera el plazo informado.",
        ),
        "fraud_risk_explanation": (
            "A operação foi encaminhada para revisão por sinais autorizados; confirme sua identidade pelo canal seguro sem expor controles antifraude.",
            "La operación fue enviada a revisión por señales autorizadas; confirma tu identidad por el canal seguro sin exponer controles antifraude.",
        ),
        "responsible_credit": (
            "A análise considera capacidade de pagamento, histórico autorizado e contexto econômico; a decisão exige revisão humana conforme as regras locais.",
            "El análisis considera capacidad de pago, historial autorizado y contexto económico; la decisión exige revisión humana según las reglas locales.",
        ),
        "investment_suitability": (
            "Compare risco, liquidez, custos e horizonte; apenas alternativas compatíveis com o perfil validado devem seguir para revisão humana.",
            "Compara riesgo, liquidez, costos y horizonte; solo las alternativas compatibles con el perfil validado deben pasar a revisión humana.",
        ),
        "regulatory_summary": (
            "O corpus sintético indica finalidade, consentimento, acesso mínimo e trilha de auditoria; valide a obrigação com assessoria jurídica local.",
            "El corpus sintético indica finalidad, consentimiento, acceso mínimo y trazabilidad; valida la obligación con asesoría jurídica local.",
        ),
        "next_best_action": (
            "A próxima ação deve respeitar consentimento, adequação e benefício demonstrável; não ofertar é a alternativa quando os controles falham.",
            "La próxima acción debe respetar consentimiento, idoneidad y beneficio demostrable; no ofertar es la alternativa cuando fallan los controles.",
        ),
    }[use_case][0 if pt else 1]
    terms = {
        "customer_service": ["revisão humana"] if pt else ["revisión humana"],
        "fraud_risk_explanation": ["canal seguro", "controles antifraude"],
        "responsible_credit": ["capacidade de pagamento", "revisão humana"] if pt else ["capacidad de pago", "revisión humana"],
        "investment_suitability": ["risco", "liquidez"] if pt else ["riesgo", "liquidez"],
        "regulatory_summary": ["corpus sintético", "jurídica local"],
        "next_best_action": ["consentimento", "não ofertar"] if pt else ["consentimiento", "no ofertar"],
    }[use_case]
    return query, answer, terms


def golden_records(release):
    records = []
    for country, language in COUNTRY_LANGUAGE:
        for use_case in USE_CASES:
            query, answer, terms = _scenario_text(country, language, use_case)
            source = {
                "document_id": f"QREF-{country}-{use_case.upper()[:8]}-001",
                "country_code": country,
                "language": language,
                "page_content": answer,
                "official": False,
                "synthetic": True,
            }
            records.append(
                {
                    "inputs": {
                        "case_id": f"{release}-{country}-{use_case}",
                        "country_code": country,
                        "language": language,
                        "use_case": use_case,
                        "query": query,
                        "approved_context": answer,
                        "source": source,
                    },
                    "expectations": {
                        "expected_response": answer,
                        "expected_facts": terms,
                        "expected_country": country,
                        "expected_language": language,
                        "requires_disclaimer": use_case in FINANCIAL_CASES or use_case == "regulatory_summary",
                    },
                    "tags": {
                        "project": "quilla",
                        "evaluation_release": release,
                        "country_code": country,
                        "language": language,
                        "use_case": use_case,
                        "synthetic": "true",
                    },
                }
            )
    return records


def semantic_sample(records, size):
    by_country = {country: [] for country, _ in COUNTRY_LANGUAGE}
    for record in records:
        by_country[record["inputs"]["country_code"]].append(record)
    selected = []
    for index, (country, _) in enumerate(COUNTRY_LANGUAGE):
        selected.append(by_country[country][index % len(USE_CASES)])
    if size <= len(selected):
        return selected[:size]
    remaining = [record for record in records if record not in selected]
    return (selected + remaining)[:size]


def governed_output(country_code, language, use_case, approved_context, source):
    pt = language.startswith("pt")
    disclaimer = (
        "Demonstração com dados sintéticos; requer revisão humana e não constitui orientação financeira, jurídica ou regulatória."
        if pt
        else "Demostración con datos sintéticos; requiere revisión humana y no constituye asesoría financiera, jurídica ni regulatoria."
    )
    return {
        "status": "DEMO_ONLY",
        "country_code": country_code,
        "language": language,
        "use_case": use_case,
        "answer": approved_context,
        "disclaimer": disclaimer,
        "sources": [source],
    }


def baseline_output(country_code, language, use_case, approved_context, source):
    return {
        "status": "UNCONTROLLED_BASELINE",
        "country_code": "LATAM",
        "language": "en",
        "use_case": use_case,
        "answer": "Your request was processed using a generic regional policy.",
        "disclaimer": "",
        "sources": [],
    }


def emit_traced_prediction(variant, case_id, country_code, language, use_case, query, approved_context, source):
    output_fn = governed_output if variant == "candidate" else baseline_output
    with mlflow.start_span(name=f"quilla_{variant}_agent", span_type="AGENT") as root:
        root.set_inputs({
            "case_id": case_id,
            "country_code": country_code,
            "language": language,
            "use_case": use_case,
            "query": query,
        })
        mlflow.update_current_trace(tags={
            "project": "quilla",
            "variant": variant,
            "country_code": country_code,
            "language": language,
            "use_case": use_case,
            "synthetic": "true",
        })
        with mlflow.start_span(name="governed_retrieval", span_type="RETRIEVER") as retrieval:
            retrieval.set_inputs({"query": query, "country_code": country_code, "language": language})
            retrieval.set_outputs([source])
        with mlflow.start_span(name="response_generation", span_type="CHAT_MODEL") as generation:
            generation.set_inputs({"approved_context": approved_context, "variant": variant})
            output = output_fn(country_code, language, use_case, approved_context, source)
            generation.set_outputs(output)
        root.set_outputs(output)
        return output


def metrics_json(run_id):
    run = mlflow.MlflowClient().get_run(run_id)
    return json.dumps(run.data.metrics, ensure_ascii=False, sort_keys=True)

