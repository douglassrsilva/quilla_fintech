"""Consultas analíticas governadas para as telas operacionais da Quilla."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import os
from typing import Any

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from .config import settings
from .db import db
from .security import ActorContext, trusted_actor, trusted_user_authorization


router = APIRouter(prefix="/api/analytics", tags=["analytics"])
CATALOG = os.getenv("QUILLA_CATALOG", "example_catalog")
GOLD_SCHEMA = os.getenv("QUILLA_GOLD_SCHEMA", "example_gold")
ML_SCHEMA = os.getenv("QUILLA_ML_SCHEMA", "example_ml")
WAREHOUSE_ID = os.getenv("QUILLA_SQL_WAREHOUSE_ID", "")
COUNTRY_CODES = {"REG", "AR", "BR", "CL", "CO", "CR", "DO", "EC", "GT", "MX", "PA", "PE", "UY"}
CURRENCIES = {
    "AR": "ARS", "BR": "BRL", "CL": "CLP", "CO": "COP", "CR": "CRC", "DO": "DOP",
    "EC": "USD", "GT": "GTQ", "MX": "MXN", "PA": "USD", "PE": "PEN", "UY": "UYU",
}


def _workspace_auth() -> tuple[str, str]:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    return client.config.host.rstrip("/"), client.config.authenticate().get("Authorization", "")


async def _statement(statement: str, user_authorization: str | None) -> dict[str, Any]:
    if not WAREHOUSE_ID:
        raise RuntimeError("SQL Warehouse not configured")
    host, service_authorization = await asyncio.to_thread(_workspace_auth)
    authorization = user_authorization or service_authorization
    async with httpx.AsyncClient(timeout=httpx.Timeout(55.0, connect=5.0)) as client:
        response = await client.post(
            f"{host}/api/2.0/sql/statements/",
            headers={"Authorization": authorization},
            json={
                "statement": statement,
                "warehouse_id": WAREHOUSE_ID,
                "format": "JSON_ARRAY",
                "wait_timeout": "50s",
            },
        )
        response.raise_for_status()
        payload = response.json()
    state = ((payload.get("status") or {}).get("state") or "UNKNOWN").upper()
    if state != "SUCCEEDED":
        raise RuntimeError(f"SQL statement ended in {state}")
    return payload


def _rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    columns = [item["name"] for item in (((payload.get("manifest") or {}).get("schema") or {}).get("columns") or [])]
    values = ((payload.get("result") or {}).get("data_array") or [])
    return [dict(zip(columns, row)) for row in values]


def _money(value: Any, locale: str) -> str:
    amount = float(value or 0)
    if amount >= 1_000_000:
        rendered = f"US$ {amount / 1_000_000:.1f}M"
    elif amount >= 1_000:
        rendered = f"US$ {amount / 1_000:.1f} mil" if locale.startswith("pt") else f"US$ {amount / 1_000:.1f} mil"
    else:
        rendered = f"US$ {amount:,.0f}"
    return rendered.replace(".", ",") if locale.startswith("pt") else rendered


def _recommendation_copy(kind: str, locale: str, row: dict[str, Any]) -> dict[str, str]:
    pt = locale.startswith("pt")
    country_code = str(row[f"{kind}_country"])
    country_names = {
        "AR": "Argentina", "BR": "Brasil", "CL": "Chile", "CO": "Colômbia" if pt else "Colombia",
        "CR": "Costa Rica", "DO": "República Dominicana", "EC": "Equador" if pt else "Ecuador",
        "GT": "Guatemala", "MX": "México", "PA": "Panamá", "PE": "Peru" if pt else "Perú", "UY": "Uruguai" if pt else "Uruguay",
    }
    country = country_names.get(country_code, country_code)
    observations = f"{int(row.get('offer_observations') or 0):,}".replace(",", ".")
    if kind == "offer":
        offer_names = {
            "personal_credit": ("crédito pessoal", "crédito personal"),
            "card_limit": ("aumento de limite", "aumento de límite"),
            "receivables": ("antecipação de recebíveis", "anticipo de cuentas por cobrar"),
            "insurance": ("proteção e seguros", "protección y seguros"),
            "investment": ("investimentos", "inversiones"),
            "fx": ("câmbio", "cambio"),
        }
        name = offer_names.get(str(row["offer_type"]), (str(row["offer_type"]), str(row["offer_type"])))[0 if pt else 1]
        return {
            "title": f"Priorize {name} em {country}" if pt else f"Prioriza {name} en {country}",
            "summary": (
                f"{observations} ofertas históricas sintéticas indicam a melhor margem observada por oferta elegível."
                if pt else
                f"{observations} ofertas históricas sintéticas indican el mejor margen observado por oferta elegible."
            ),
            "impact": f"{_money(row['offer_margin'], locale)} / oferta",
            "action": "Avaliar público elegível" if pt else "Evaluar público elegible",
            "internal_prompt": (
                f"Analise a oportunidade de {name} em {country}. Valide público elegível, margem, risco, política e alternativas usando dados governados."
                if pt else
                f"Analiza la oportunidad de {name} en {country}. Valida público elegible, margen, riesgo, política y alternativas con datos gobernados."
            ),
            "external_prompt": (
                f"Pesquise sinais externos atuais de {country} que possam confirmar ou invalidar uma campanha de {name}. Use web_search, cite fontes e datas e separe fatos de inferências."
                if pt else
                f"Investiga señales externas actuales de {country} que puedan confirmar o invalidar una campaña de {name}. Usa web_search, cita fuentes y fechas y separa hechos de inferencias."
            ),
        }
    if kind == "payments":
        declined_events = f"{int(row['declined_events'] or 0):,}".replace(",", ".")
        return {
            "title": f"Recupere pagamentos falhos em {country}" if pt else f"Recupera pagos fallidos en {country}",
            "summary": (
                f"{declined_events} eventos recusados foram identificados na camada Gold; priorize janela e canal antes de uma nova tentativa."
                if pt else
                f"Se identificaron {declined_events} eventos rechazados en la capa Gold; prioriza ventana y canal antes de un nuevo intento."
            ),
            "impact": f"{_money(row['declined_volume'], locale)} em volume recusado" if pt else f"{_money(row['declined_volume'], locale)} en volumen rechazado",
            "action": "Investigar recuperação" if pt else "Investigar recuperación",
            "internal_prompt": (
                f"Analise os pagamentos recusados em {country}, encontre padrões por produto, canal e horário e recomende uma estratégia segura de recuperação."
                if pt else
                f"Analiza los pagos rechazados en {country}, identifica patrones por producto, canal y horario y recomienda una estrategia segura de recuperación."
            ),
            "external_prompt": (
                f"Pesquise acontecimentos atuais em {country} que possam afetar aprovações e falhas de pagamento, incluindo indisponibilidades, feriados, regulação ou condições econômicas. Use web_search e cite fontes."
                if pt else
                f"Investiga acontecimientos actuales en {country} que puedan afectar aprobaciones y fallas de pago, incluyendo interrupciones, feriados, regulación o condiciones económicas. Usa web_search y cita fuentes."
            ),
        }
    return {
        "title": f"Reavalie risco e preços em {country}" if pt else f"Reevalúa riesgo y precios en {country}",
        "summary": (
            f"O último cenário sintético está classificado como {row['stress_regime']}; juros e inflação devem entrar na revisão de limites e preços."
            if pt else
            f"El último escenario sintético está clasificado como {row['stress_regime']}; tasas e inflación deben entrar en la revisión de límites y precios."
        ),
        "impact": (
            f"Juros {float(row['policy_rate_pct'] or 0):.1f}% · inflação {float(row['inflation_yoy_pct'] or 0):.1f}%"
            if pt else
            f"Tasa {float(row['policy_rate_pct'] or 0):.1f}% · inflación {float(row['inflation_yoy_pct'] or 0):.1f}%"
        ).replace(".", ",") if pt else f"Tasa {float(row['policy_rate_pct'] or 0):.1f}% · inflación {float(row['inflation_yoy_pct'] or 0):.1f}%",
        "action": "Executar análise de cenário" if pt else "Ejecutar análisis de escenario",
        "internal_prompt": (
            f"Execute uma análise de cenário para {country} usando juros, inflação, câmbio e regime macro. Quantifique impactos em crédito, margem, liquidez e risco."
            if pt else
            f"Ejecuta un análisis de escenario para {country} usando tasas, inflación, cambio y régimen macro. Cuantifica impactos en crédito, margen, liquidez y riesgo."
        ),
        "external_prompt": (
            f"Atualize o cenário econômico de {country} com informações públicas recentes. Use web_search, priorize fontes oficiais e avalie impactos em juros, inflação, câmbio, crédito e investimentos."
            if pt else
            f"Actualiza el escenario económico de {country} con información pública reciente. Usa web_search, prioriza fuentes oficiales y evalúa impactos en tasas, inflación, cambio, crédito e inversiones."
        ),
    }


def _model_identity(model: Any) -> tuple[str | None, str | None]:
    value = str(model or "").strip()
    if not value:
        return None, None
    name, separator, version = value.rpartition("@")
    return (name, version) if separator and version else (value, None)


def _recommendation_explanation(
    kind: str,
    locale: str,
    row: dict[str, Any],
    *,
    model: Any,
    source: str,
    statement_id: str | None,
    generated_at: str,
) -> dict[str, Any]:
    """Build explainability only from query evidence; never imply causality or SHAP."""
    pt = locale.startswith("pt")
    model_name, model_version = _model_identity(model)
    common_limitations = [
        (
            "Dados sintéticos e associações históricas; os sinais não demonstram causalidade."
            if pt else
            "Datos sintéticos y asociaciones históricas; las señales no demuestran causalidad."
        ),
        (
            "O contexto externo ainda não foi consultado e pode confirmar ou invalidar o candidato."
            if pt else
            "El contexto externo aún no fue consultado y puede confirmar o invalidar el candidato."
        ),
        (
            "A recomendação exige políticas aplicáveis e decisão humana antes de qualquer execução."
            if pt else
            "La recomendación requiere políticas aplicables y decisión humana antes de ejecutarse."
        ),
    ]
    alternatives = [
        {
            "key": "no_action",
            "label": "Não agir" if pt else "No actuar",
            "rationale": (
                "Preservar o estado atual quando evidência, consentimento ou benefício forem insuficientes."
                if pt else
                "Mantener el estado actual cuando la evidencia, el consentimiento o el beneficio sean insuficientes."
            ),
        },
        {
            "key": "educate",
            "label": "Educar" if pt else "Educar",
            "rationale": (
                "Explicar opções e riscos sem realizar oferta ou alterar condições."
                if pt else
                "Explicar opciones y riesgos sin realizar una oferta ni cambiar condiciones."
            ),
        },
        {
            "key": "protect",
            "label": "Proteger" if pt else "Proteger",
            "rationale": (
                "Priorizar prevenção, recuperação ou revisão de risco antes de crescimento."
                if pt else
                "Priorizar prevención, recuperación o revisión de riesgo antes del crecimiento."
            ),
        },
        {
            "key": "offer",
            "label": "Ofertar" if pt else "Ofrecer",
            "rationale": (
                "Prosseguir somente após elegibilidade, adequação, consentimento e políticas."
                if pt else
                "Continuar solo después de elegibilidad, idoneidad, consentimiento y políticas."
            ),
        },
    ]
    if kind == "offer":
        as_of = row.get("offers_as_of")
        why_now = (
            "Esta combinação de país e oferta lidera o ranking por margem média entre candidatos elegíveis, com volume histórico suficiente para investigação."
            if pt else
            "Esta combinación de país y oferta lidera el ranking por margen promedio entre candidatos elegibles, con volumen histórico suficiente para investigar."
        )
        signals = [
            {"label": "Observações históricas" if pt else "Observaciones históricas", "value": f"{int(row.get('offer_observations') or 0):,}".replace(",", "."), "numeric_value": int(row.get("offer_observations") or 0), "unit": "offers", "direction": "context", "source_field": "offer_observations"},
            {"label": "Elegibilidade média" if pt else "Elegibilidad promedio", "value": f"{100 * float(row.get('eligibility_score') or 0):.1f}%".replace(".", ",") if pt else f"{100 * float(row.get('eligibility_score') or 0):.1f}%", "numeric_value": float(row.get("eligibility_score") or 0), "unit": "ratio", "direction": "supports", "source_field": "eligibility_score"},
            {"label": "Aceitação histórica" if pt else "Aceptación histórica", "value": f"{100 * float(row.get('acceptance_rate') or 0):.1f}%".replace(".", ",") if pt else f"{100 * float(row.get('acceptance_rate') or 0):.1f}%", "numeric_value": float(row.get("acceptance_rate") or 0), "unit": "ratio", "direction": "context", "source_field": "accepted_flag"},
            {"label": "Margem média por oferta" if pt else "Margen promedio por oferta", "value": _money(row.get("offer_margin"), locale), "numeric_value": float(row.get("offer_margin") or 0), "unit": "USD", "direction": "supports", "source_field": "contribution_margin_usd_90d"},
        ]
        method = (
            "Ranking por margem média de contribuição em 90 dias; volume de observações como desempate."
            if pt else
            "Ranking por margen promedio de contribución a 90 días; volumen de observaciones como desempate."
        )
    elif kind == "payments":
        as_of = row.get("payments_as_of")
        why_now = (
            "Este mercado concentra o maior volume recusado no recorte consultado e merece investigação operacional antes de novas tentativas."
            if pt else
            "Este mercado concentra el mayor volumen rechazado del corte consultado y requiere investigación operativa antes de nuevos intentos."
        )
        signals = [
            {"label": "Eventos recusados" if pt else "Eventos rechazados", "value": f"{int(row.get('declined_events') or 0):,}".replace(",", "."), "numeric_value": int(row.get("declined_events") or 0), "unit": "events", "direction": "caution", "source_field": "declined_events"},
            {"label": "Volume recusado" if pt else "Volumen rechazado", "value": _money(row.get("declined_volume"), locale), "numeric_value": float(row.get("declined_volume") or 0), "unit": "USD", "direction": "caution", "source_field": "gross_volume_usd"},
            {"label": "Dados até" if pt else "Datos hasta", "value": str(as_of or "—"), "numeric_value": None, "unit": "date", "direction": "context", "source_field": "event_date"},
        ]
        method = (
            "Ranking por soma do volume de eventos recusados na visão financeira diária."
            if pt else
            "Ranking por suma del volumen de eventos rechazados en la vista financiera diaria."
        )
    else:
        as_of = row.get("observation_date")
        why_now = (
            "O regime macroeconômico mais recente combina sinais de juros, inflação e câmbio que podem alterar risco, preço e liquidez."
            if pt else
            "El régimen macroeconómico más reciente combina señales de tasas, inflación y cambio que pueden modificar riesgo, precio y liquidez."
        )
        signals = [
            {"label": "Regime de estresse" if pt else "Régimen de estrés", "value": str(row.get("stress_regime") or "—"), "numeric_value": None, "unit": "category", "direction": "caution", "source_field": "stress_regime"},
            {"label": "Taxa básica" if pt else "Tasa de política", "value": f"{float(row.get('policy_rate_pct') or 0):.1f}%".replace(".", ",") if pt else f"{float(row.get('policy_rate_pct') or 0):.1f}%", "numeric_value": float(row.get("policy_rate_pct") or 0), "unit": "percent", "direction": "context", "source_field": "policy_rate_pct"},
            {"label": "Inflação anual" if pt else "Inflación anual", "value": f"{float(row.get('inflation_yoy_pct') or 0):.1f}%".replace(".", ",") if pt else f"{float(row.get('inflation_yoy_pct') or 0):.1f}%", "numeric_value": float(row.get("inflation_yoy_pct") or 0), "unit": "percent", "direction": "caution", "source_field": "inflation_yoy_pct"},
            {"label": "Câmbio por USD" if pt else "Cambio por USD", "value": f"{float(row.get('fx_per_usd') or 0):,.2f}", "numeric_value": float(row.get("fx_per_usd") or 0), "unit": "local_currency_per_USD", "direction": "context", "source_field": "fx_per_usd"},
        ]
        method = (
            "Última observação por país, priorizada pela severidade do regime e pela inflação."
            if pt else
            "Última observación por país, priorizada por severidad del régimen e inflación."
        )
    return {
        "version": "recommendation-explanation-v1",
        "why_now": why_now,
        "method": method,
        "determining_signals": signals,
        "provenance": {
            "source": source,
            "statement_id": statement_id,
            "observed_at": str(as_of) if as_of is not None else None,
            "generated_at": generated_at,
            "synthetic": True,
        },
        "model": {
            "registered_name": model_name,
            "version": model_version,
            "registry": "Unity Catalog",
            "validation_status": "PASSED" if model_name else "UNAVAILABLE",
            "lifecycle_status": "validated_registered_candidate" if model_name else "unavailable",
            "online_scoring": False,
            "role": (
                "Modelo candidato; a evidência apresentada nesta etapa vem do ranking analítico."
                if pt else
                "Modelo candidato; la evidencia presentada en esta etapa proviene del ranking analítico."
            ),
        },
        "limitations": common_limitations,
        "alternatives": alternatives,
        "controls": [
            "Unity Catalog lineage",
            "Unity AI Gateway policies",
            "Human review before execution",
            "Synthetic data only",
        ],
    }


@router.get("/recommendations")
async def recommendations(
    country_code: str = Query(default="REG", min_length=2, max_length=3),
    locale: str = Query(default="pt-BR", min_length=2, max_length=16),
    _actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
):
    """Rankeia candidatos de decisão sem bloquear a tela em chamadas de IA generativa."""
    country_code = country_code.upper()
    if country_code not in COUNTRY_CODES:
        raise HTTPException(status_code=422, detail="Unsupported country_code")
    language = "pt-BR" if locale.lower().startswith("pt") else "es-419"
    filter_sql = "" if country_code == "REG" else f"WHERE country_code = '{country_code}'"
    statement = f"""
      WITH offer_ranked AS (
        SELECT country_code AS offer_country, offer_type,
          COUNT(*) AS offer_observations,
          AVG(eligibility_score) AS eligibility_score,
          AVG(CAST(accepted_flag AS DOUBLE)) AS acceptance_rate,
          AVG(contribution_margin_usd_90d) AS offer_margin,
          MAX(offer_ts) AS offers_as_of,
          ROW_NUMBER() OVER (
            ORDER BY AVG(contribution_margin_usd_90d) DESC, COUNT(*) DESC
          ) AS position
        FROM {CATALOG}.{ML_SCHEMA}.source_pricing_experiments
        {filter_sql}
        GROUP BY country_code, offer_type
      ), declined_ranked AS (
        SELECT country_code AS payments_country,
          SUM(declined_events) AS declined_events,
          SUM(gross_volume_usd) AS declined_volume,
          MAX(event_date) AS payments_as_of,
          ROW_NUMBER() OVER (ORDER BY SUM(gross_volume_usd) DESC) AS position
        FROM {CATALOG}.{GOLD_SCHEMA}.mv_financial_performance_daily
        WHERE event_status = 'declined'
          {"" if country_code == "REG" else f"AND country_code = '{country_code}'"}
        GROUP BY country_code
      ), latest_macro AS (
        SELECT country_code AS macro_country, observation_date, policy_rate_pct,
          inflation_yoy_pct, fx_per_usd, stress_regime,
          ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY observation_date DESC) AS freshness
        FROM {CATALOG}.{ML_SCHEMA}.fs_macro_country_daily
        {filter_sql}
      ), macro_ranked AS (
        SELECT *, ROW_NUMBER() OVER (
          ORDER BY CASE stress_regime WHEN 'recession' THEN 4 WHEN 'stagflation' THEN 3 WHEN 'inflation_shock' THEN 2 ELSE 1 END DESC,
          inflation_yoy_pct DESC
        ) AS position
        FROM latest_macro WHERE freshness = 1
      ), models AS (
        SELECT
          MAX(CASE WHEN model_key = 'next_best_action' THEN CONCAT(model_name, '@', model_version) END) AS offer_model,
          MAX(CASE WHEN model_key = 'cashflow_forecast' THEN CONCAT(model_name, '@', model_version) END) AS payments_model,
          MAX(CASE WHEN model_key = 'macro_regime' THEN CONCAT(model_name, '@', model_version) END) AS macro_model
        FROM {CATALOG}.{ML_SCHEMA}.model_validation_evidence
        WHERE status = 'PASSED'
      )
      SELECT offer_ranked.*, declined_ranked.*, macro_ranked.*, models.*
      FROM offer_ranked
      CROSS JOIN declined_ranked
      CROSS JOIN macro_ranked
      CROSS JOIN models
      WHERE offer_ranked.position = 1 AND declined_ranked.position = 1 AND macro_ranked.position = 1
    """
    try:
        payload = await _statement(statement, user_authorization)
        rows = _rows(payload)
        if not rows:
            raise RuntimeError("No recommendation candidates")
        row = rows[0]
    except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=503, detail=f"Recommendation analytics unavailable: {type(exc).__name__}") from exc

    source_map = {
        "offer": f"{CATALOG}.{ML_SCHEMA}.source_pricing_experiments",
        "payments": f"{CATALOG}.{GOLD_SCHEMA}.mv_financial_performance_daily",
        "macro": f"{CATALOG}.{ML_SCHEMA}.fs_macro_country_daily",
    }
    models = {"offer": row.get("offer_model"), "payments": row.get("payments_model"), "macro": row.get("macro_model")}
    definitions = (
        ("offer", "growth", "teal", "high"),
        ("payments", "payments", "blue", "high"),
        ("macro", "risk", "coral", "critical" if row.get("stress_regime") in {"recession", "stagflation"} else "medium"),
    )
    generated_at = datetime.now(timezone.utc).isoformat()
    statement_id = payload.get("statement_id")
    items = []
    for rank, (kind, category, tone, urgency) in enumerate(definitions, start=1):
        copy = _recommendation_copy(kind, language, row)
        items.append({
            "recommendation_id": f"{kind}-{row[f'{kind}_country']}-{payload.get('statement_id', '')[:8]}",
            "rank": rank,
            "category": category,
            "tone": tone,
            "urgency": urgency,
            "state": "candidate",
            **copy,
            "country_code": row[f"{kind}_country"],
            "model": models[kind],
            "model_status": "validated_candidate" if models[kind] else "unavailable",
            "online_scoring": False,
            "internal_source": source_map[kind],
            "external_signal": {
                "status": "on_demand",
                "tool": "system.ai.web_search",
                "prompt": copy["external_prompt"],
            },
            "explanation": _recommendation_explanation(
                kind,
                language,
                row,
                model=models[kind],
                source=source_map[kind],
                statement_id=statement_id,
                generated_at=generated_at,
            ),
        })
    return {
        "scope": country_code,
        "locale": language,
        "generated_at": generated_at,
        "statement_id": statement_id,
        "synthetic": True,
        "contract": "decision-candidate-v2",
        "recommendations": items,
    }


@router.get("/customers")
async def customer_profiles(
    country_code: str = Query(default="REG", min_length=2, max_length=3),
    limit: int = Query(default=12, ge=1, le=24),
    _actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
):
    """Retorna uma amostra mínima de Customer 360 sintético, sem contatos ou documentos."""
    country_code = country_code.upper()
    if country_code not in COUNTRY_CODES:
        raise HTTPException(status_code=422, detail="Unsupported country_code")
    base = f"""
      SELECT party_id, display_name, country_code, locale_code, preferred_language,
        customer_segment, risk_band, kyc_status, onboarding_ts, account_count,
        current_balance_local, available_balance_local, last_account_update_at,
        lifetime_event_count, lifetime_volume_usd, last_event_at,
        average_event_risk_score, risk_alert_count, open_risk_alert_count,
        last_alert_at, interaction_count, average_sentiment_score, last_interaction_at
      FROM {CATALOG}.{GOLD_SCHEMA}.mv_customer_360
      WHERE party_type = 'person' AND kyc_status = 'verified'
        AND display_name IS NOT NULL AND account_count > 0
        {"" if country_code == "REG" else f"AND country_code = '{country_code}'"}
    """
    statement = (
        f"""
        SELECT * EXCEPT (position) FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY country_code ORDER BY lifetime_volume_usd DESC, party_id
          ) AS position
          FROM ({base}) AS eligible
        ) WHERE position = 1
        ORDER BY lifetime_volume_usd DESC LIMIT {limit}
        """
        if country_code == "REG" else
        f"""{base} ORDER BY lifetime_volume_usd DESC, party_id LIMIT {limit}"""
    )
    try:
        payload = await _statement(statement, user_authorization)
        customers = []
        for row in _rows(payload):
            customers.append({
                "party_id": str(row["party_id"]),
                "display_name": str(row["display_name"]),
                "country_code": str(row["country_code"]),
                "currency_code": CURRENCIES.get(str(row["country_code"]), "USD"),
                "locale_code": str(row["locale_code"]),
                "preferred_language": str(row["preferred_language"]),
                "customer_segment": str(row["customer_segment"]),
                "risk_band": str(row["risk_band"]),
                "kyc_status": str(row["kyc_status"]),
                "onboarding_ts": row.get("onboarding_ts"),
                "account_count": int(row["account_count"] or 0),
                "current_balance_local": float(row["current_balance_local"] or 0),
                "available_balance_local": float(row["available_balance_local"] or 0),
                "last_account_update_at": row.get("last_account_update_at"),
                "lifetime_event_count": int(row["lifetime_event_count"] or 0),
                "lifetime_volume_usd": float(row["lifetime_volume_usd"] or 0),
                "last_event_at": row.get("last_event_at"),
                "average_event_risk_score": float(row["average_event_risk_score"] or 0),
                "risk_alert_count": int(row["risk_alert_count"] or 0),
                "open_risk_alert_count": int(row["open_risk_alert_count"] or 0),
                "last_alert_at": row.get("last_alert_at"),
                "interaction_count": int(row["interaction_count"] or 0),
                "average_sentiment_score": float(row["average_sentiment_score"] or 0),
                "last_interaction_at": row.get("last_interaction_at"),
            })
    except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=503, detail=f"Customer 360 unavailable: {type(exc).__name__}") from exc
    return {
        "scope": country_code,
        "source": f"{CATALOG}.{GOLD_SCHEMA}.mv_customer_360",
        "statement_id": payload.get("statement_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "synthetic": True,
        "customers": customers,
    }


@router.get("/regional")
async def regional_overview(
    _actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
):
    statement = f"""
      SELECT country_code,
        MEASURE(gross_volume_usd) AS gross_volume_usd,
        MEASURE(total_events) AS total_events,
        100 * MEASURE(completion_rate) AS completion_rate_pct,
        100 * MEASURE(fraud_rate) AS fraud_rate_pct
      FROM {CATALOG}.{GOLD_SCHEMA}.metrics_financial_performance
      GROUP BY country_code
      ORDER BY gross_volume_usd DESC
    """
    try:
        payload = await _statement(statement, user_authorization)
        countries = [
            {
                "country_code": str(row["country_code"]),
                "gross_volume_usd": float(row["gross_volume_usd"] or 0),
                "total_events": int(row["total_events"] or 0),
                "completion_rate_pct": float(row["completion_rate_pct"] or 0),
                "fraud_rate_pct": float(row["fraud_rate_pct"] or 0),
            }
            for row in _rows(payload)
        ]
    except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=503, detail=f"Regional analytics unavailable: {type(exc).__name__}") from exc
    return {
        "source": f"{CATALOG}.{GOLD_SCHEMA}.metrics_financial_performance",
        "statement_id": payload.get("statement_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "gross_volume_usd": sum(item["gross_volume_usd"] for item in countries),
        "total_events": sum(item["total_events"] for item in countries),
        "countries": countries,
    }


@router.get("/business")
async def business_overview(
    country_code: str = Query(default="REG", min_length=2, max_length=3),
    locale: str = Query(default="pt-BR", min_length=2, max_length=16),
    _actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
):
    """KPIs e séries empresariais live derivados exclusivamente de Metric Views Gold."""
    country_code = country_code.upper()
    if country_code not in COUNTRY_CODES:
        raise HTTPException(status_code=422, detail="Unsupported country_code")
    language = "pt-BR" if locale.lower().startswith("pt") else "es-419"
    finance_filter = "" if country_code == "REG" else f"WHERE country_code = '{country_code}'"
    growth_filter = "" if country_code == "REG" else f"WHERE country_code = '{country_code}'"
    declined_filter = "WHERE event_status = 'declined'"
    if country_code != "REG":
        declined_filter += f" AND country_code = '{country_code}'"

    kpi_sql = f"""
      WITH finance AS (
        SELECT MEASURE(gross_volume_usd) AS gross_volume_usd,
          MEASURE(net_flow_usd) AS net_flow_usd,
          100 * MEASURE(completion_rate) AS completion_rate_pct
        FROM {CATALOG}.{GOLD_SCHEMA}.metrics_financial_performance
        {finance_filter}
      ), declined AS (
        SELECT MEASURE(gross_volume_usd) AS declined_volume_usd
        FROM {CATALOG}.{GOLD_SCHEMA}.metrics_financial_performance
        {declined_filter}
      ), growth AS (
        SELECT MEASURE(revenue_usd_90d) AS revenue_usd_90d,
          MEASURE(contribution_margin_usd_90d) AS margin_usd_90d
        FROM {CATALOG}.{GOLD_SCHEMA}.metrics_growth_customer
        {growth_filter}
      )
      SELECT * FROM finance CROSS JOIN declined CROSS JOIN growth
    """
    trend_sql = f"""
      SELECT event_date,
        MEASURE(net_flow_usd) AS net_flow_usd,
        MEASURE(gross_volume_usd) AS gross_volume_usd
      FROM {CATALOG}.{GOLD_SCHEMA}.metrics_financial_performance
      {finance_filter}
      GROUP BY event_date
      ORDER BY event_date DESC
      LIMIT 90
    """
    receivables_sql = f"""
      SELECT country_code,
        MEASURE(gross_volume_usd) AS declined_volume_usd,
        MEASURE(total_events) AS declined_events
      FROM {CATALOG}.{GOLD_SCHEMA}.metrics_financial_performance
      {declined_filter}
      GROUP BY country_code
      ORDER BY declined_volume_usd DESC
      LIMIT 12
    """
    try:
        kpi_payload, trend_payload, receivables_payload = await asyncio.gather(
            _statement(kpi_sql, user_authorization),
            _statement(trend_sql, user_authorization),
            _statement(receivables_sql, user_authorization),
        )
        kpi_rows = _rows(kpi_payload)
        if not kpi_rows:
            raise RuntimeError("Business Metric Views returned no KPIs")
        raw_kpis = kpi_rows[0]
        kpis = {
            key: float(raw_kpis.get(key) or 0)
            for key in (
                "gross_volume_usd", "net_flow_usd", "completion_rate_pct",
                "declined_volume_usd", "revenue_usd_90d", "margin_usd_90d",
            )
        }
        trend = [
            {
                "event_date": str(row["event_date"]),
                "net_flow_usd": float(row["net_flow_usd"] or 0),
                "gross_volume_usd": float(row["gross_volume_usd"] or 0),
            }
            for row in reversed(_rows(trend_payload))
        ]
        receivables = [
            {
                "country_code": str(row["country_code"]),
                "declined_volume_usd": float(row["declined_volume_usd"] or 0),
                "declined_events": int(row["declined_events"] or 0),
            }
            for row in _rows(receivables_payload)
        ]
    except (httpx.HTTPError, RuntimeError, ValueError, TypeError) as exc:
        raise HTTPException(status_code=503, detail=f"Business analytics unavailable: {type(exc).__name__}") from exc
    return {
        "scope": country_code,
        "locale": language,
        "source": f"{CATALOG}.{GOLD_SCHEMA}.metrics_financial_performance + metrics_growth_customer",
        "statement_ids": [
            kpi_payload.get("statement_id"), trend_payload.get("statement_id"),
            receivables_payload.get("statement_id"),
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "synthetic": True,
        "kpis": kpis,
        "trend": trend,
        "receivables": receivables,
    }


async def _serving_state(user_authorization: str | None) -> tuple[str, str]:
    host, service_authorization = await asyncio.to_thread(_workspace_auth)
    authorization = user_authorization or service_authorization
    async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=4.0)) as client:
        response = await client.get(
            f"{host}/api/2.0/serving-endpoints/{settings.supervisor_endpoint}",
            headers={"Authorization": authorization},
        )
        response.raise_for_status()
        payload = response.json()
    ready = ((payload.get("state") or {}).get("ready") or "").upper()
    return ("operational" if ready == "READY" else "degraded", ready or "UNKNOWN")


@router.get("/trust")
async def trust_status(
    _actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
):
    lakebase_ok, serving_result = await asyncio.gather(
        db.ping(),
        _serving_state(user_authorization),
        return_exceptions=True,
    )
    serving_state, serving_evidence = (
        serving_result if isinstance(serving_result, tuple) else ("unavailable", type(serving_result).__name__)
    )
    genie_count = sum(bool(os.getenv(name, "").strip()) for name in (
        "QUILLA_GENIE_EXECUTIVE_REGIONAL", "QUILLA_GENIE_PAYMENTS_PERFORMANCE",
        "QUILLA_GENIE_RISK_FRAUD", "QUILLA_GENIE_RESPONSIBLE_CREDIT",
        "QUILLA_GENIE_CUSTOMER_OPERATIONS", "QUILLA_GENIE_GROWTH_WEALTH",
    ))
    gateway_services = [
        os.getenv("QUILLA_GATEWAY_PRIMARY_SERVICE", "").strip(),
        os.getenv("QUILLA_GATEWAY_GUARD_SERVICE", "").strip(),
    ]
    controls = [
        {"key": "lakebase", "state": "operational" if lakebase_ok else "unavailable", "evidence": "Lakebase readiness probe"},
        {"key": "supervisor", "state": serving_state, "evidence": f"{settings.supervisor_endpoint}: {serving_evidence}"},
        {"key": "genie", "state": "configured" if genie_count == 6 else "degraded", "evidence": f"{genie_count}/6 Genie Spaces vinculados"},
        {"key": "warehouse", "state": "configured" if WAREHOUSE_ID else "unavailable", "evidence": f"SQL Warehouse {WAREHOUSE_ID or 'ausente'}"},
        {"key": "gateway", "state": "configured" if all(gateway_services) else "unavailable", "evidence": ", ".join(filter(None, gateway_services)) or "Model Services ausentes"},
    ]
    return {"checked_at": datetime.now(timezone.utc).isoformat(), "controls": controls}
