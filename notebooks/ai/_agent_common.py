# Databricks notebook source
"""Canonical contracts for Quilla's governed multi-agent foundation."""

import hashlib
import json
import re


def _agent_widget(name: str, default: str) -> str:
    try:
        dbutils.widgets.text(name, default)
    except Exception:
        pass
    try:
        return dbutils.widgets.get(name)
    except Exception:
        return default


def _agent_identifier(value: str, label: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"Invalid {label}: {value!r}")
    return value


def agent_config():
    catalog = _agent_identifier(
        _agent_widget("catalog", "example_catalog"), "catalog"
    )
    ai_schema = _agent_identifier(_agent_widget("ai_schema", "example_ai"), "AI schema")
    release = _agent_widget("agent_release", "2026-09-02-open-weight-v2")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,80}", release):
        raise ValueError(f"Invalid agent release: {release!r}")
    model_endpoint = _agent_widget(
        "agent_model_endpoint", "databricks-glm-5-3"
    )
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{2,80}", model_endpoint):
        raise ValueError(f"Invalid model endpoint: {model_endpoint!r}")
    evaluation_release = _agent_widget("evaluation_release", "2026-09-01-v1")
    return {
        "catalog": catalog,
        "ai_schema": ai_schema,
        "release": release,
        "model_endpoint": model_endpoint,
        "evaluation_release": evaluation_release,
        "vector_index": f"{catalog}.{ai_schema}.regulatory_knowledge_hybrid_idx",
    }


def agent_table(cfg, table: str) -> str:
    return f"{cfg['catalog']}.{cfg['ai_schema']}.{_agent_identifier(table, 'table')}"


COUNTRIES = ["AR", "BR", "CL", "CO", "CR", "DO", "EC", "GT", "MX", "PA", "PE", "UY"]
LOCALES = ["pt-BR", "es-AR", "es-CL", "es-CO", "es-CR", "es-DO", "es-EC", "es-GT", "es-MX", "es-PA", "es-PE", "es-UY"]

PRIVATE_SCOPE = (
    "trusted:user_id+tenant_id+country_code; derived from Databricks authenticated "
    "request context; never accepted from prompt or selected by the model"
)
SHARED_SCOPE = (
    "trusted:tenant_id+country_code+role; contains no customer PII; derived from "
    "authenticated application context; never selected by the model"
)


AGENT_SPECS = [
    {
        "key": "supervisor",
        "display_name": "Quilla IQ Supervisor",
        "domain": "orchestration",
        "purpose_pt": "Classificar intenção, aplicar fronteiras de país e encaminhar ao especialista correto.",
        "purpose_es": "Clasificar la intención, aplicar fronteras de país y derivar al especialista correcto.",
        "prompt_key": "customer_service",
        "memory_store": "quilla_supervisor_memory",
        "scope_strategy": PRIVATE_SCOPE,
        "decision_authority": "ROUTE_ONLY",
        "human_review": True,
    },
    {
        "key": "customer_service",
        "display_name": "Quilla Care",
        "domain": "customer_service",
        "purpose_pt": "Resolver atendimento omnicanal e organizar próximos passos em PT/ES.",
        "purpose_es": "Resolver la atención omnicanal y organizar próximos pasos en PT/ES.",
        "prompt_key": "customer_service",
        "memory_store": "quilla_customer_service_memory",
        "scope_strategy": PRIVATE_SCOPE,
        "decision_authority": "ADVISE_AND_OPEN_CASE",
        "human_review": True,
    },
    {
        "key": "fraud_risk",
        "display_name": "Quilla Sentinel",
        "domain": "risk",
        "purpose_pt": "Explicar sinais de fraude e risco sem revelar regras exploráveis.",
        "purpose_es": "Explicar señales de fraude y riesgo sin revelar reglas explotables.",
        "prompt_key": "fraud_risk_explanation",
        "memory_store": "quilla_fraud_risk_memory",
        "scope_strategy": PRIVATE_SCOPE,
        "decision_authority": "RECOMMEND_CONTROL_ACTION",
        "human_review": True,
    },
    {
        "key": "responsible_credit",
        "display_name": "Quilla Crédito",
        "domain": "credit",
        "purpose_pt": "Apoiar crédito responsável, explicável e resiliente a cenários macroeconômicos.",
        "purpose_es": "Apoyar crédito responsable, explicable y resiliente a escenarios macroeconómicos.",
        "prompt_key": "responsible_credit",
        "memory_store": "quilla_responsible_credit_memory",
        "scope_strategy": PRIVATE_SCOPE,
        "decision_authority": "ADVISE_ONLY",
        "human_review": True,
    },
    {
        "key": "investment_suitability",
        "display_name": "Quilla Wealth",
        "domain": "investment",
        "purpose_pt": "Orientar suitability e educação financeira sem executar ordens.",
        "purpose_es": "Orientar idoneidad y educación financiera sin ejecutar órdenes.",
        "prompt_key": "investment_suitability",
        "memory_store": "quilla_investment_suitability_memory",
        "scope_strategy": PRIVATE_SCOPE,
        "decision_authority": "EDUCATE_AND_RECOMMEND",
        "human_review": True,
    },
    {
        "key": "regulatory",
        "display_name": "Quilla RegIntel",
        "domain": "regulatory",
        "purpose_pt": "Resumir conhecimento regulatório sintético com fontes e isolamento por país.",
        "purpose_es": "Resumir conocimiento regulatorio sintético con fuentes y aislamiento por país.",
        "prompt_key": "regulatory_summary",
        "memory_store": "quilla_regulatory_memory",
        "scope_strategy": SHARED_SCOPE,
        "decision_authority": "SUMMARIZE_ONLY",
        "human_review": True,
    },
    {
        "key": "next_best_action",
        "display_name": "Quilla Growth",
        "domain": "growth",
        "purpose_pt": "Recomendar próxima melhor ação respeitando consentimento, risco e adequação.",
        "purpose_es": "Recomendar la siguiente mejor acción respetando consentimiento, riesgo e idoneidad.",
        "prompt_key": "next_best_action",
        "memory_store": "quilla_next_best_action_memory",
        "scope_strategy": PRIVATE_SCOPE,
        "decision_authority": "RECOMMEND_ONLY",
        "human_review": True,
    },
    {
        "key": "external_intelligence",
        "display_name": "Quilla External Intelligence",
        "domain": "external_intelligence",
        "purpose_pt": "Pesquisar sinais externos atuais, verificar fontes e traduzi-los em cenários de impacto.",
        "purpose_es": "Investigar señales externas actuales, verificar fuentes y traducirlas en escenarios de impacto.",
        "prompt_key": "regulatory_summary",
        "memory_store": "quilla_external_intelligence_memory",
        "scope_strategy": SHARED_SCOPE,
        "decision_authority": "RESEARCH_AND_ADVISE_ONLY",
        "human_review": True,
    },
]


OPEN_WEIGHT_MODEL_ENDPOINTS = {
    "primary": "databricks-glm-5-3",
    "verifier": "databricks-deepseek-v4-pro-0813",
    "long_context": "databricks-kimi-k3",
}


TOOL_SPECS = [
    ("supervisor", "delegate_agent", "LOCAL_FUNCTION", "route_only", "Classifica e delega; não executa decisão financeira."),
    ("customer_service", "quilla-support-intent", "MODEL_SERVING", "CAN_QUERY", "Classifica a intenção de atendimento."),
    ("customer_service", "case_management", "LAKEBASE", "CAN_CONNECT", "Abre e acompanha caso com idempotência e auditoria."),
    ("fraud_risk", "quilla-transaction-fraud", "MODEL_SERVING", "CAN_QUERY", "Pontua risco de fraude transacional."),
    ("fraud_risk", "quilla-account-takeover", "MODEL_SERVING", "CAN_QUERY", "Pontua tomada de conta."),
    ("responsible_credit", "quilla-credit-pd", "MODEL_SERVING", "CAN_QUERY", "Estima probabilidade de inadimplência."),
    ("investment_suitability", "quilla-investment-suitability", "MODEL_SERVING", "CAN_QUERY", "Valida adequação demonstrativa do produto."),
    ("regulatory", "regulatory_knowledge_hybrid_idx", "AI_SEARCH_MCP", "READ", "Recupera apenas o corpus sintético filtrado por país e idioma."),
    ("next_best_action", "quilla-next-best-action", "MODEL_SERVING", "CAN_QUERY", "Ordena ações elegíveis após consentimento e política."),
    ("next_best_action", "decision_ledger", "LAKEBASE", "CAN_CONNECT", "Registra recomendação e evidência sem execução automática."),
    ("external_intelligence", "system.ai.web_search", "DATABRICKS_WEB_SEARCH", "PUBLIC_WEB_ONLY", "Pesquisa fatos públicos atuais com URLs e datas rastreáveis."),
    ("external_intelligence", OPEN_WEIGHT_MODEL_ENDPOINTS["primary"], "MODEL_SERVING", "CAN_QUERY", "Sintetiza evidências com GLM 5.3."),
    ("external_intelligence", OPEN_WEIGHT_MODEL_ENDPOINTS["verifier"], "MODEL_SERVING", "CAN_QUERY", "Critica fontes, divergências e riscos com DeepSeek V4 Pro."),
    ("external_intelligence", OPEN_WEIGHT_MODEL_ENDPOINTS["long_context"], "MODEL_SERVING", "CAN_QUERY", "Consolida conjuntos extensos de documentos com Kimi K3."),
]


SKILL_SPECS = [
    {
        "skill_key": f"quilla-{agent['key'].replace('_', '-')}",
        "agent_key": agent["key"],
        "version": "1.0.0",
        "relative_path": f"agent_skills/quilla-{agent['key'].replace('_', '-')}/SKILL.md",
        "intent_pt": agent["purpose_pt"],
        "intent_es": agent["purpose_es"],
        "prohibited_actions": "no_cross_country_data;no_secret_or_pii_memory;no_unverified_regulatory_claim;no_irreversible_action",
        "human_review_rules": "credit;fraud_block;investment_order;regulatory_interpretation;high_value_action",
    }
    for agent in AGENT_SPECS
]

SKILL_SPECS.extend([
    {
        "skill_key": "quilla-source-verification",
        "agent_key": "external_intelligence",
        "version": "1.0.0",
        "relative_path": "agent_skills/quilla-source-verification/SKILL.md",
        "intent_pt": "Verificar atualidade, independência, jurisdição e rastreabilidade das fontes externas.",
        "intent_es": "Verificar actualidad, independencia, jurisdicción y trazabilidad de fuentes externas.",
        "prohibited_actions": "no_unverified_claim;no_prompt_injection;no_secret_or_pii_search",
        "human_review_rules": "regulatory_interpretation;fiscal_recommendation;investment_recommendation",
    },
    {
        "skill_key": "quilla-impact-scenario-analysis",
        "agent_key": "external_intelligence",
        "version": "1.0.0",
        "relative_path": "agent_skills/quilla-impact-scenario-analysis/SKILL.md",
        "intent_pt": "Traduzir fatos verificados em cenários financeiros, fiscais, de risco e investimentos.",
        "intent_es": "Traducir hechos verificados en escenarios financieros, fiscales, de riesgo e inversión.",
        "prohibited_actions": "no_invented_probability;no_irreversible_action;no_individual_trade",
        "human_review_rules": "regulatory_interpretation;fiscal_recommendation;investment_recommendation",
    },
])


def canonical_digest(value) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def agent_by_key(key: str):
    for spec in AGENT_SPECS:
        if spec["key"] == key:
            return spec
    raise ValueError(f"Unknown agent key: {key!r}")
