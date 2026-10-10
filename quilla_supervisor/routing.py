"""Roteamento policy-first, previsível e explicável."""

from __future__ import annotations

import re

from .contracts import RouteDecision


INTENTS = {
    "payments_performance": (
        "pagamento", "pagamentos", "pago", "pagos", "pix", "transferência", "transferencia",
        "adquirência", "adquirencia", "merchant", "comerciante", "autorização", "autorizacion",
        "recusa", "recusado", "rechazo", "rechazado", "volume transacionado",
    ),
    "fraud_risk": (
        "fraude", "fraud*", "anômal*", "anomalo*", "sospechos*", "roubo", "robo", "phishing",
        "tomada de conta", "account takeover", "bloque*", "chargeback",
    ),
    "responsible_credit": (
        "crédito", "credito", "préstamo", "prestamo", "inadimpl*", "mora", "limite", "score",
        "default", "juros", "interés", "interes", "carteira de crédito",
    ),
    "investment_suitability": (
        "invest*", "carteira", "portfólio", "portafolio", "ativo*", "bono*", "ação", "ações",
        "accion*", "suitability", "adequação", "rentabilidade", "título*", "titulo*", "bolsa",
        "b3", "cotação", "cotacao", "ticker*", "ibovespa", "alta", "baixa",
    ),
    "regulatory": (
        "regula*", "norma*", "compliance", "lei", "ley", "lgpd", "proteção de dados",
        "proteccion de datos", "licença", "licencia", "política fiscal", "politica fiscal",
    ),
    "next_best_action": (
        "próxima melhor ação", "proxima mejor accion", "next best action", "cross-sell", "upsell",
        "campanha", "campaña", "oferta", "conversão", "conversion", "churn", "receita", "ventas",
    ),
    "executive": (
        "relatório executivo", "relatorio executivo", "informe ejecutivo", "briefing executivo",
        "resumo executivo", "comitê executivo", "comite executivo", "diretoria", "conselho",
    ),
    "external_intelligence": (
        "ontem", "ayer", "hoje", "hoy", "notícia", "noticia", "atual", "recente", "web",
        "internet", "banco central", "ministerio", "ministério", "inflação", "inflacion",
        "energia", "câmbio", "tipo de cambio", "crise", "crisis", "eleição", "eleccion",
    ),
    "customer_service": (
        "atendimento", "soporte", "ajuda", "ayuda", "conta", "cuenta", "cartão", "tarjeta",
        "reclama", "dúvida", "duvida", "consulta", "solicitação", "solicitud",
    ),
}

HIGH_IMPACT = (
    "aprovar", "aprobar", "negar", "rechazar", "bloquear", "encerrar", "cerrar", "demitir",
    "executar", "comprar", "vender", "ordem", "orden", "limite", "crédito", "credito",
    "fraude", "regulat*", "investimento", "inversión", "inversion", "invest*", "ação", "ações",
    "título*", "titulo*", "bolsa", "b3",
)

MARKET_TERMS = (
    "ação", "ações", "accion*", "ativo*", "título*", "titulo*", "bolsa", "b3", "cotação",
    "cotacao", "ticker*", "ibovespa", "mercado financeiro", "petróleo", "petroleo", "brent",
    "wti", "ouro", "gold", "commodity*", "commodities", "big tech*", "nasdaq", "s&p 500",
    "dow jones", "criptomoeda*", "bitcoin", "ethereum",
)

FRESHNESS_TERMS = (
    "hoje", "hoy", "ontem", "ayer", "agora", "atual*", "recente*", "esta semana",
    "essa semana", "nesta semana", "última semana", "ultima semana", "este mês", "este mes",
    "último mês", "ultimo mes", "últimos 30 dias", "ultimos 30 dias", "neste trimestre",
    "este trimestre",
)

PERFORMANCE_TERMS = (
    "alta*", "baixa*", "variação", "variacao", "desempenho", "performance", "retorno*",
    "preço*", "preco*", "valoriz*", "desvaloriz*", "subiu", "subiram", "caiu", "caíram",
    "cairam", "maiores altas", "maiores baixas",
)

MACRO_TERMS = (
    "economia", "econômico*", "economico*", "macroeconômico*", "macroeconomico*",
    "conjuntura", "pib", "inflação", "inflacion", "desemprego", "atividade econômica",
    "atividade economica", "balança comercial", "balanca comercial", "risco país", "risco pais",
)

SENSITIVE_DATA_TERMS = (
    "nome", "email", "e-mail", "telefone", "celular", "cpf", "documento", "endereço",
    "endereco", "contacto", "correo", "teléfono", "telefono",
)

DATA_EXPORT_TERMS = (
    "lista", "liste", "gere", "gerar", "export*", "extraia", "retorne", "mostre", "maiores clientes",
)


def _normalized(text: str) -> str:
    return re.sub(r"\s+", " ", text.lower()).strip()


def _matches(text: str, keyword: str) -> bool:
    """Match words/phrases explicitly; `*` is the only supported stem marker."""
    stem = keyword.endswith("*")
    token = re.escape(keyword[:-1] if stem else keyword)
    suffix = r"\w*" if stem else ""
    return re.search(rf"(?<!\w){token}{suffix}(?!\w)", text, flags=re.UNICODE) is not None


class IntentRouter:
    def route(self, text: str) -> RouteDecision:
        normalized = _normalized(text)
        scores = {
            intent: sum(1 for keyword in keywords if _matches(normalized, keyword))
            for intent, keywords in INTENTS.items()
        }
        ordered = sorted(scores, key=lambda key: (-scores[key], list(INTENTS).index(key)))
        primary = ordered[0] if scores[ordered[0]] else "customer_service"
        supporting = tuple(
            intent for intent in ordered[1:] if scores[intent] and scores[intent] >= scores[primary] - 1
        )[:2]
        has_freshness = any(_matches(normalized, term) for term in FRESHNESS_TERMS)
        has_market_subject = any(_matches(normalized, term) for term in MARKET_TERMS)
        has_performance_request = any(_matches(normalized, term) for term in PERFORMANCE_TERMS)
        has_macro_subject = any(_matches(normalized, term) for term in MACRO_TERMS)
        market_lookup = has_market_subject and (has_performance_request or has_freshness)
        macro_lookup = has_macro_subject and has_freshness and primary == "customer_service"
        time_sensitive_investment = primary == "investment_suitability" and has_freshness
        external = (
            scores["external_intelligence"] > 0
            or market_lookup
            or macro_lookup
            or time_sensitive_investment
        )
        if market_lookup or macro_lookup or time_sensitive_investment:
            previous_primary = primary
            primary = "external_intelligence"
            preferred_support = (
                ("investment_suitability", previous_primary, *supporting)
                if has_market_subject or previous_primary == "investment_suitability"
                else (previous_primary, *supporting)
            )
            supporting = tuple(
                key
                for key in dict.fromkeys(preferred_support)
                if key not in {"external_intelligence", "customer_service"}
            )[:2]
        if external and primary not in {"external_intelligence", "regulatory"}:
            supporting = tuple(dict.fromkeys((*supporting, "external_intelligence")))[:2]
        sensitive_export = (
            any(_matches(normalized, term) for term in SENSITIVE_DATA_TERMS)
            and any(_matches(normalized, term) for term in DATA_EXPORT_TERMS)
        )
        high_impact = sensitive_export or any(_matches(normalized, term) for term in HIGH_IMPACT)
        scored_reasons = tuple(
            f"{intent}:{scores[intent]}" for intent in (primary, *supporting) if scores[intent]
        )
        inferred_reasons = tuple(
            reason
            for condition, reason in (
                (market_lookup, "external:market-performance"),
                (macro_lookup, "external:macro-freshness"),
                (time_sensitive_investment, "external:investment-freshness"),
                (sensitive_export, "risk:sensitive-data-export"),
            )
            if condition
        )
        reasons = tuple(dict.fromkeys((*scored_reasons, *inferred_reasons))) or (
            "fallback:customer_service",
        )
        return RouteDecision(
            primary=primary,
            supporting=supporting,
            scores=scores,
            reasons=reasons,
            high_impact=high_impact,
            external_data=external or primary == "external_intelligence",
        )
