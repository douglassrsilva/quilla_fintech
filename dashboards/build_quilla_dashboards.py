#!/usr/bin/env python3
"""Generate deterministic Lakeview dashboards for the Quilla demo."""

import json
from pathlib import Path


CAT = "example_catalog"
GOLD = f"{CAT}.quilla_gold"
PALETTE = [
    "#5865F2",  # Quilla indigo
    "#20D3BC",  # Quilla teal
    "#FF7968",  # Quilla coral
    "#9B8CFF",
    "#63E6D2",
    "#FFB3A8",
    "#111827",
    "#CBD2FF",
]


def dataset(name, display_name, sql):
    # Lakeview's resource API currently validates dataset displayName as an
    # identifier (letters, numbers, hyphens and underscores only). Keep the
    # human-facing bilingual language in widget titles and use the stable
    # machine name here so DAB deployments are reproducible.
    return {"name": name, "displayName": name, "queryLines": [sql]}


def pos(x, y, width, height):
    return {"x": x, "y": y, "width": width, "height": height}


def counter(name, ds, field, title, position, number_format=None):
    value = {"fieldName": field, "displayName": title}
    if number_format:
        value["numberFormat"] = number_format
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {
                    "datasetName": ds,
                    "fields": [{"name": field, "expression": f"`{field}`"}],
                    "disaggregated": True,
                },
            }],
            "spec": {
                "version": 2,
                "widgetType": "counter",
                "encodings": {"value": value},
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": position,
    }


def bar(name, ds, x_field, y_field, title, position, x_title, y_title, color_field=None):
    fields = [
        {"name": x_field, "expression": f"`{x_field}`"},
        {"name": y_field, "expression": f"SUM(`{y_field}`)"},
    ]
    encodings = {
        "x": {
            "fieldName": x_field,
            "scale": {"type": "categorical", "sort": {"by": "y-reversed"}},
            "displayName": x_title,
        },
        "y": {
            "fieldName": y_field,
            "scale": {"type": "quantitative"},
            "displayName": y_title,
        },
        "label": {"show": True},
    }
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})
        encodings["color"] = {
            "fieldName": color_field,
            "scale": {"type": "categorical"},
            "displayName": color_field,
        }
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {"datasetName": ds, "fields": fields, "disaggregated": False},
            }],
            "spec": {
                "version": 3,
                "widgetType": "bar",
                "encodings": encodings,
                "frame": {"showTitle": True, "title": title},
                "mark": {"colors": PALETTE},
            },
        },
        "position": position,
    }


def line(name, ds, x_field, y_field, title, position, x_title, y_title, color_field=None):
    fields = [
        {"name": x_field, "expression": f"`{x_field}`"},
        {"name": y_field, "expression": f"SUM(`{y_field}`)"},
    ]
    encodings = {
        "x": {"fieldName": x_field, "scale": {"type": "temporal"}, "displayName": x_title},
        "y": {"fieldName": y_field, "scale": {"type": "quantitative"}, "displayName": y_title},
    }
    if color_field:
        fields.append({"name": color_field, "expression": f"`{color_field}`"})
        encodings["color"] = {
            "fieldName": color_field,
            "scale": {"type": "categorical"},
            "displayName": color_field,
        }
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {"datasetName": ds, "fields": fields, "disaggregated": False},
            }],
            "spec": {
                "version": 3,
                "widgetType": "line",
                "encodings": encodings,
                "frame": {"showTitle": True, "title": title},
                "mark": {"colors": PALETTE},
            },
        },
        "position": position,
    }


def table(name, ds, columns, title, position):
    fields = [{"name": c[0], "expression": f"`{c[0]}`"} for c in columns]
    encodings = []
    for order, (field, label, data_type, number_format) in enumerate(columns):
        numeric = data_type in {"float", "integer"}
        item = {
            "fieldName": field,
            "type": data_type,
            "displayAs": "number" if numeric else ("datetime" if data_type == "datetime" else "string"),
            "title": label,
            "displayName": label,
            "order": order,
            "alignContent": "right" if numeric or data_type == "datetime" else "left",
        }
        if number_format and numeric:
            item["numberFormat"] = number_format
        encodings.append(item)
    return {
        "widget": {
            "name": name,
            "queries": [{
                "name": "main_query",
                "query": {"datasetName": ds, "fields": fields, "disaggregated": True},
            }],
            "spec": {
                "version": 1,
                "widgetType": "table",
                "encodings": {"columns": encodings},
                "frame": {"showTitle": True, "title": title},
            },
        },
        "position": position,
    }


def dashboard(datasets, page_name, layout):
    return {
        "datasets": datasets,
        "pages": [{
            "name": page_name,
            "displayName": page_name,
            "pageType": "PAGE_TYPE_CANVAS",
            "layout": layout,
        }],
        "uiSettings": {
            "theme": {"widgetHeaderAlignment": "ALIGNMENT_LEFT"},
            "applyModeEnabled": False,
        },
    }


def executive():
    datasets = [
        dataset("exec_kpis", "KPIs executivos · KPIs ejecutivos", f"""
SELECT
  MEASURE(gross_volume_usd) AS gross_volume_usd,
  MEASURE(total_events) AS total_events,
  100 * MEASURE(completion_rate) AS completion_rate_pct,
  100 * MEASURE(fraud_rate) AS fraud_rate_pct,
  MEASURE(net_flow_usd) AS net_flow_usd,
  MEASURE(cross_border_volume_usd) AS cross_border_volume_usd
FROM {GOLD}.metrics_financial_performance
"""),
        dataset("exec_country", "Performance por país · Desempeño por país", f"""
SELECT country_code,
  MEASURE(gross_volume_usd) AS gross_volume_usd,
  100 * MEASURE(completion_rate) AS completion_rate_pct,
  100 * MEASURE(fraud_rate) AS fraud_rate_pct
FROM {GOLD}.metrics_financial_performance
GROUP BY country_code
"""),
        dataset("exec_month", "Tendência financeira · Tendencia financiera", f"""
SELECT DATE_TRUNC('MONTH', event_date) AS month,
  'Volume USD' AS series, MEASURE(gross_volume_usd) AS value
FROM {GOLD}.metrics_financial_performance GROUP BY ALL
UNION ALL
SELECT DATE_TRUNC('MONTH', event_date), 'Fluxo líquido USD', MEASURE(net_flow_usd)
FROM {GOLD}.metrics_financial_performance GROUP BY ALL
"""),
        dataset("exec_credit", "Crédito por país · Crédito por país", f"""
SELECT country_code,
  100 * MEASURE(approval_rate) AS approval_rate_pct,
  MEASURE(applications) AS applications,
  MEASURE(approved_amount_local) AS approved_amount_local
FROM {GOLD}.metrics_credit_portfolio GROUP BY country_code
"""),
        dataset("exec_risk", "Risco por país · Riesgo por país", f"""
SELECT country_code,
  MEASURE(total_alerts) AS total_alerts,
  100 * MEASURE(confirmation_rate) AS confirmation_rate_pct,
  100 * MEASURE(false_positive_rate) AS false_positive_rate_pct
FROM {GOLD}.metrics_risk_operations GROUP BY country_code
"""),
        dataset("exec_freshness", "Frescor realtime · Frescura realtime", f"""
SELECT MAX(event_date) AS max_event_date,
  SUM(CASE WHEN event_date = (SELECT MAX(event_date) FROM {GOLD}.mv_financial_performance_daily)
      THEN event_count ELSE 0 END) AS latest_day_events
FROM {GOLD}.mv_financial_performance_daily
"""),
        dataset("exec_quality", "Confiança dos dados · Confianza de datos", f"""
SELECT 100 * MEASURE(reconciliation_rate) AS reconciliation_rate_pct,
  100 * MEASURE(global_acceptance_rate) AS acceptance_rate_pct,
  MEASURE(quarantine_rows) AS quarantine_rows
FROM {GOLD}.metrics_data_quality
"""),
    ]
    layout = [
        counter("ex01", "exec_kpis", "gross_volume_usd", "Volume regional USD · Volumen regional", pos(0, 0, 1, 3), "$,.2f"),
        counter("ex02", "exec_kpis", "completion_rate_pct", "Conclusão · Finalización (%)", pos(1, 0, 1, 3), "0.00"),
        counter("ex03", "exec_kpis", "fraud_rate_pct", "Fraude sinalizada · Fraude (%)", pos(2, 0, 1, 3), "0.00"),
        counter("ex04", "exec_kpis", "net_flow_usd", "Fluxo líquido · Flujo neto USD", pos(3, 0, 1, 3), "$,.2f"),
        counter("ex05", "exec_quality", "reconciliation_rate_pct", "Reconciliação · Reconciliación (%)", pos(4, 0, 1, 3), "0.00"),
        counter("ex06", "exec_freshness", "latest_day_events", "Eventos no dia mais recente", pos(5, 0, 1, 3), "0"),
        bar("ex07", "exec_country", "country_code", "gross_volume_usd", "Volume por país · Volumen por país", pos(0, 3, 3, 6), "País", "USD"),
        line("ex08", "exec_month", "month", "value", "Volume e fluxo líquido · Volumen y flujo neto", pos(3, 3, 3, 6), "Mês · Mes", "USD", "series"),
        bar("ex09", "exec_credit", "country_code", "approval_rate_pct", "Aprovação de crédito · Aprobación", pos(0, 9, 3, 6), "País", "%"),
        bar("ex10", "exec_risk", "country_code", "confirmation_rate_pct", "Confirmação de alertas · Confirmación", pos(3, 9, 3, 6), "País", "%"),
        table("ex11", "exec_country", [
            ("country_code", "País", "string", None),
            ("gross_volume_usd", "Volume USD", "float", "$,.2f"),
            ("completion_rate_pct", "Conclusão (%)", "float", "0.00"),
            ("fraud_rate_pct", "Fraude (%)", "float", "0.00"),
        ], "Placar regional · Marcador regional", pos(0, 15, 6, 6)),
    ]
    return dashboard(datasets, "visao_regional", layout)


def growth():
    datasets = [
        dataset("growth_kpis", "KPIs Growth", f"""
SELECT MEASURE(revenue_usd_90d) AS revenue_usd,
  MEASURE(contribution_margin_usd_90d) AS margin_usd,
  100 * MEASURE(contribution_margin_rate) AS margin_rate_pct,
  100 * MEASURE(conversion_rate) AS conversion_rate_pct,
  100 * MEASURE(churn_rate) AS churn_rate_pct,
  MEASURE(weighted_average_clv_usd_12m) AS average_clv_usd
FROM {GOLD}.metrics_growth_customer
"""),
        dataset("growth_month", "Growth mensal · Growth mensual", f"""
SELECT observation_month AS month, 'Receita 90d' AS series, MEASURE(revenue_usd_90d) AS value
FROM {GOLD}.metrics_growth_customer GROUP BY ALL
UNION ALL
SELECT observation_month, 'Margem 90d', MEASURE(contribution_margin_usd_90d)
FROM {GOLD}.metrics_growth_customer GROUP BY ALL
"""),
        dataset("growth_country", "Growth por país", f"""
SELECT country_code,
  MEASURE(revenue_usd_90d) AS revenue_usd,
  MEASURE(contribution_margin_usd_90d) AS margin_usd,
  100 * MEASURE(conversion_rate) AS conversion_rate_pct,
  100 * MEASURE(churn_rate) AS churn_rate_pct,
  MEASURE(weighted_average_clv_usd_12m) AS average_clv_usd
FROM {GOLD}.metrics_growth_customer GROUP BY country_code
"""),
        dataset("pricing_offer", "Pricing por oferta", f"""
SELECT offer_type,
  MEASURE(offers) AS offers,
  100 * MEASURE(acceptance_rate) AS acceptance_rate_pct,
  MEASURE(revenue_usd_90d) AS revenue_usd,
  MEASURE(contribution_margin_usd_90d) AS margin_usd
FROM {GOLD}.metrics_pricing_profitability GROUP BY offer_type
"""),
        dataset("pricing_country", "Pricing por país", f"""
SELECT country_code,
  100 * MEASURE(acceptance_rate) AS acceptance_rate_pct,
  MEASURE(weighted_average_offered_apr_bps) / 100 AS offered_apr_pct,
  MEASURE(contribution_margin_usd_90d) AS margin_usd,
  100 * MEASURE(risk_adjusted_margin_rate) AS risk_adjusted_margin_pct
FROM {GOLD}.metrics_pricing_profitability GROUP BY country_code
"""),
    ]
    layout = [
        counter("gr01", "growth_kpis", "revenue_usd", "Receita 90d · Ingresos 90d", pos(0, 0, 1, 3), "$,.2f"),
        counter("gr02", "growth_kpis", "margin_usd", "Margem 90d · Margen 90d", pos(1, 0, 1, 3), "$,.2f"),
        counter("gr03", "growth_kpis", "margin_rate_pct", "Margem · Margen (%)", pos(2, 0, 1, 3), "0.00"),
        counter("gr04", "growth_kpis", "conversion_rate_pct", "Conversão · Conversión (%)", pos(3, 0, 1, 3), "0.00"),
        counter("gr05", "growth_kpis", "churn_rate_pct", "Churn (%)", pos(4, 0, 1, 3), "0.00"),
        counter("gr06", "growth_kpis", "average_clv_usd", "CLV médio 12m · CLV promedio", pos(5, 0, 1, 3), "$,.2f"),
        line("gr07", "growth_month", "month", "value", "Receita e margem · Ingresos y margen", pos(0, 3, 6, 6), "Mês · Mes", "USD", "series"),
        bar("gr08", "growth_country", "country_code", "margin_usd", "Margem por país · Margen por país", pos(0, 9, 3, 6), "País", "USD"),
        bar("gr09", "pricing_offer", "offer_type", "acceptance_rate_pct", "Aceite por oferta · Aceptación por oferta", pos(3, 9, 3, 6), "Oferta", "%"),
        table("gr10", "pricing_country", [
            ("country_code", "País", "string", None),
            ("acceptance_rate_pct", "Aceite (%)", "float", "0.00"),
            ("offered_apr_pct", "APR ofertada (%)", "float", "0.00"),
            ("margin_usd", "Margem ajustada USD", "float", "$,.2f"),
            ("risk_adjusted_margin_pct", "Margem/receita (%)", "float", "0.00"),
        ], "Pricing e retorno por país · Pricing y retorno", pos(0, 15, 6, 6)),
    ]
    return dashboard(datasets, "growth_profitabilidade", layout)


def wealth():
    datasets = [
        dataset("wealth_kpis", "KPIs Wealth", f"""
SELECT MEASURE(investment_volume_usd) AS investment_volume_usd,
  100 * MEASURE(recommendation_acceptance_rate) AS recommendation_acceptance_pct,
  100 * MEASURE(suitability_breach_rate) AS suitability_breach_pct,
  MEASURE(weighted_average_portfolio_value_usd) AS average_portfolio_value_usd
FROM {GOLD}.metrics_wealth_engagement
"""),
        dataset("suitability_kpis", "KPIs Suitability", f"""
SELECT MEASURE(profiles) AS profiles,
  100 * MEASURE(eligibility_rate) AS eligibility_rate_pct,
  MEASURE(restricted_profiles) AS restricted_profiles,
  MEASURE(investable_assets_usd) AS investable_assets_usd
FROM {GOLD}.metrics_investor_suitability
"""),
        dataset("wealth_asset", "Ativos · Activos", f"""
SELECT asset_class,
  MEASURE(investment_volume_usd) AS investment_volume_usd,
  100 * MEASURE(recommendation_acceptance_rate) AS recommendation_acceptance_pct,
  100 * MEASURE(suitability_breach_rate) AS suitability_breach_pct
FROM {GOLD}.metrics_wealth_engagement GROUP BY asset_class
"""),
        dataset("wealth_month", "Wealth mensal", f"""
SELECT DATE_TRUNC('MONTH', event_date) AS month, 'Compras' AS series, MEASURE(buy_volume_usd) AS value
FROM {GOLD}.metrics_wealth_engagement GROUP BY ALL
UNION ALL
SELECT DATE_TRUNC('MONTH', event_date), 'Vendas', MEASURE(sell_volume_usd)
FROM {GOLD}.metrics_wealth_engagement GROUP BY ALL
"""),
        dataset("suitability_objective", "Objetivos de investimento", f"""
SELECT investment_objective, MEASURE(profiles) AS profiles,
  100 * MEASURE(eligibility_rate) AS eligibility_rate_pct,
  MEASURE(average_loss_capacity_pct) AS average_loss_capacity_pct
FROM {GOLD}.metrics_investor_suitability GROUP BY investment_objective
"""),
        dataset("macro_latest", "Macro mais recente · Macro más reciente", f"""
SELECT country_code, stress_regime,
  MEASURE(average_policy_rate_pct) AS policy_rate_pct,
  MEASURE(average_inflation_yoy_pct) AS inflation_yoy_pct,
  MEASURE(average_unemployment_pct) AS unemployment_pct,
  MEASURE(average_gdp_nowcast_yoy_pct) AS gdp_nowcast_yoy_pct,
  MEASURE(average_energy_price_index) AS energy_price_index
FROM {GOLD}.metrics_macro_stress
WHERE observation_date = (SELECT MAX(observation_date) FROM {CAT}.quilla_ml.source_macroeconomic_daily)
GROUP BY ALL
"""),
        dataset("macro_trend", "Inflação e juros · Inflación y tasas", f"""
SELECT DATE_TRUNC('MONTH', observation_date) AS month, 'Inflação YoY' AS series,
  MEASURE(average_inflation_yoy_pct) AS value
FROM {GOLD}.metrics_macro_stress GROUP BY ALL
UNION ALL
SELECT DATE_TRUNC('MONTH', observation_date), 'Taxa básica', MEASURE(average_policy_rate_pct)
FROM {GOLD}.metrics_macro_stress GROUP BY ALL
"""),
    ]
    layout = [
        counter("we01", "wealth_kpis", "investment_volume_usd", "Volume investido · Volumen", pos(0, 0, 1, 3), "$,.2f"),
        counter("we02", "wealth_kpis", "recommendation_acceptance_pct", "Aceite de recomendações (%)", pos(1, 0, 1, 3), "0.00"),
        counter("we03", "wealth_kpis", "suitability_breach_pct", "Breaches suitability (%)", pos(2, 0, 1, 3), "0.00"),
        counter("we04", "suitability_kpis", "profiles", "Perfis avaliados · Perfiles", pos(3, 0, 1, 3), "0"),
        counter("we05", "suitability_kpis", "eligibility_rate_pct", "Elegibilidade · Elegibilidad (%)", pos(4, 0, 1, 3), "0.00"),
        counter("we06", "suitability_kpis", "investable_assets_usd", "Ativos investíveis · Activos USD", pos(5, 0, 1, 3), "$,.2f"),
        line("we07", "wealth_month", "month", "value", "Compras e vendas · Compras y ventas", pos(0, 3, 3, 6), "Mês · Mes", "USD", "series"),
        bar("we08", "wealth_asset", "asset_class", "investment_volume_usd", "Volume por classe de ativo · Clase", pos(3, 3, 3, 6), "Classe · Clase", "USD"),
        bar("we09", "suitability_objective", "investment_objective", "profiles", "Perfis por objetivo · Perfiles por objetivo", pos(0, 9, 3, 6), "Objetivo", "Perfis"),
        line("we10", "macro_trend", "month", "value", "Juros e inflação · Tasas e inflación", pos(3, 9, 3, 6), "Mês · Mes", "%", "series"),
        table("we11", "macro_latest", [
            ("country_code", "País", "string", None),
            ("stress_regime", "Regime", "string", None),
            ("policy_rate_pct", "Taxa básica (%)", "float", "0.00"),
            ("inflation_yoy_pct", "Inflação YoY (%)", "float", "0.00"),
            ("unemployment_pct", "Desemprego (%)", "float", "0.00"),
            ("gdp_nowcast_yoy_pct", "PIB nowcast (%)", "float", "0.00"),
            ("energy_price_index", "Índice energia", "float", "0.00"),
        ], "Radar macro dos 12 países · Radar macro", pos(0, 15, 6, 7)),
    ]
    return dashboard(datasets, "wealth_macro_stress", layout)


def risk_trust():
    datasets = [
        dataset("risk_kpis", "KPIs Risco", f"""
SELECT MEASURE(total_alerts) AS total_alerts,
  100 * MEASURE(confirmation_rate) AS confirmation_rate_pct,
  100 * MEASURE(false_positive_rate) AS false_positive_rate_pct
FROM {GOLD}.metrics_risk_operations
"""),
        dataset("case_kpis", "KPIs Casos", f"""
SELECT MEASURE(prevented_loss_usd) AS prevented_loss_usd,
  MEASURE(net_loss_usd) AS net_loss_usd,
  MEASURE(weighted_average_resolution_minutes) AS resolution_minutes
FROM {GOLD}.metrics_case_outcomes
"""),
        dataset("dq_kpis", "KPIs Qualidade", f"""
SELECT 100 * MEASURE(reconciliation_rate) AS reconciliation_rate_pct,
  100 * MEASURE(global_acceptance_rate) AS acceptance_rate_pct,
  MEASURE(quarantine_rows) AS quarantine_rows
FROM {GOLD}.metrics_data_quality
"""),
        dataset("risk_country", "Risco por país", f"""
SELECT country_code,
  MEASURE(total_alerts) AS total_alerts,
  100 * MEASURE(confirmation_rate) AS confirmation_rate_pct,
  100 * MEASURE(false_positive_rate) AS false_positive_rate_pct
FROM {GOLD}.metrics_risk_operations GROUP BY country_code
"""),
        dataset("risk_trend", "Risco diário", f"""
SELECT alert_date, severity, MEASURE(total_alerts) AS total_alerts
FROM {GOLD}.metrics_risk_operations GROUP BY alert_date, severity
"""),
        dataset("case_domain", "Outcomes por domínio", f"""
SELECT domain,
  MEASURE(cases) AS cases,
  MEASURE(prevented_loss_usd) AS prevented_loss_usd,
  MEASURE(net_loss_usd) AS net_loss_usd,
  100 * MEASURE(confirmation_rate) AS confirmation_rate_pct
FROM {GOLD}.metrics_case_outcomes GROUP BY domain
"""),
        dataset("credit_country", "Crédito responsável", f"""
SELECT country_code,
  MEASURE(applications) AS applications,
  100 * MEASURE(approval_rate) AS approval_rate_pct,
  MEASURE(weighted_average_annual_rate_bps) / 100 AS annual_rate_pct,
  MEASURE(weighted_average_risk_score) AS risk_score
FROM {GOLD}.metrics_credit_portfolio GROUP BY country_code
"""),
        dataset("dq_detail", "Qualidade por dataset", f"""
SELECT dataset_name,
  MEASURE(bronze_rows) AS bronze_rows,
  MEASURE(silver_rows) AS silver_rows,
  MEASURE(quarantine_rows) AS quarantine_rows,
  100 * MEASURE(global_acceptance_rate) AS acceptance_rate_pct,
  100 * MEASURE(reconciliation_rate) AS reconciliation_rate_pct
FROM {GOLD}.metrics_data_quality GROUP BY dataset_name
"""),
    ]
    layout = [
        counter("rt01", "risk_kpis", "total_alerts", "Alertas · Alertas", pos(0, 0, 1, 3), "0"),
        counter("rt02", "risk_kpis", "confirmation_rate_pct", "Confirmação · Confirmación (%)", pos(1, 0, 1, 3), "0.00"),
        counter("rt03", "risk_kpis", "false_positive_rate_pct", "Falso positivo (%)", pos(2, 0, 1, 3), "0.00"),
        counter("rt04", "case_kpis", "prevented_loss_usd", "Perda evitada · Pérdida evitada", pos(3, 0, 1, 3), "$,.2f"),
        counter("rt05", "dq_kpis", "reconciliation_rate_pct", "Reconciliação · Reconciliación (%)", pos(4, 0, 1, 3), "0.00"),
        counter("rt06", "dq_kpis", "quarantine_rows", "Quarentena DQX", pos(5, 0, 1, 3), "0"),
        line("rt07", "risk_trend", "alert_date", "total_alerts", "Alertas por severidade · Alertas por severidad", pos(0, 3, 3, 6), "Data · Fecha", "Alertas", "severity"),
        bar("rt08", "risk_country", "country_code", "total_alerts", "Alertas por país", pos(3, 3, 3, 6), "País", "Alertas"),
        bar("rt09", "case_domain", "domain", "prevented_loss_usd", "Perda evitada por domínio · Pérdida evitada", pos(0, 9, 3, 6), "Domínio", "USD"),
        bar("rt10", "credit_country", "country_code", "approval_rate_pct", "Crédito responsável por país", pos(3, 9, 3, 6), "País", "Aprovação (%)"),
        table("rt11", "dq_detail", [
            ("dataset_name", "Dataset", "string", None),
            ("bronze_rows", "Bronze", "integer", "0"),
            ("silver_rows", "Silver", "integer", "0"),
            ("quarantine_rows", "Quarentena", "integer", "0"),
            ("acceptance_rate_pct", "Aceitação (%)", "float", "0.00"),
            ("reconciliation_rate_pct", "Reconciliação (%)", "float", "0.00"),
        ], "Data Trust · Confianza de datos", pos(0, 15, 6, 7)),
    ]
    return dashboard(datasets, "risco_credito_confianca", layout)


def main():
    output_dir = Path(__file__).resolve().parent
    sql_dir = output_dir.parent / "sql"
    assets = {
        "executive_command_center.lvdash.json": executive(),
        "growth_profitability.lvdash.json": growth(),
        "wealth_macro.lvdash.json": wealth(),
        "risk_trust.lvdash.json": risk_trust(),
    }
    for filename, payload in assets.items():
        path = output_dir / filename
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        print(f"wrote {path.name}: {len(payload['datasets'])} datasets, {len(payload['pages'][0]['layout'])} widgets")

    validation_blocks = [
        "-- Generated by dashboards/build_quilla_dashboards.py. Do not edit manually.",
        "-- Executes every Lakeview dataset query and limits returned rows for a fast contract check.",
    ]
    for filename, payload in assets.items():
        for item in payload["datasets"]:
            query = "".join(item["queryLines"]).strip().rstrip(";")
            validation_blocks.extend([
                "",
                f"-- {filename} :: {item['name']}",
                f"{query}\nLIMIT 10;",
            ])
    validation_path = sql_dir / "validate_dashboard_datasets.sql"
    validation_path.write_text("\n".join(validation_blocks) + "\n", encoding="utf-8")
    print(f"wrote {validation_path.relative_to(output_dir.parent)}: "
          f"{sum(len(payload['datasets']) for payload in assets.values())} dataset probes")

    all_metrics_path = sql_dir / "create_all_metric_views.sql"
    all_metrics_sql = "\n\n".join([
        "-- Generated by dashboards/build_quilla_dashboards.py. Do not edit manually.",
        (sql_dir / "create_metric_views.sql").read_text(encoding="utf-8").strip(),
        (sql_dir / "create_business_metric_views.sql").read_text(encoding="utf-8").strip(),
    ])
    all_metrics_path.write_text(all_metrics_sql + "\n", encoding="utf-8")
    print(f"wrote {all_metrics_path.relative_to(output_dir.parent)}")


if __name__ == "__main__":
    main()
