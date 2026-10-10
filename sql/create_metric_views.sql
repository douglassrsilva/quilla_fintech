CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_financial_performance
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada de performance financeira da Quilla.'
AS $$
version: 1.1
source: example_catalog.example_gold.mv_financial_performance_daily
dimensions:
  - name: event_date
    expr: event_date
  - name: country_code
    expr: country_code
  - name: event_type
    expr: event_type
  - name: event_status
    expr: event_status
  - name: direction
    expr: direction
  - name: currency_code
    expr: currency_code
measures:
  - name: total_events
    expr: SUM(event_count)
  - name: gross_volume_usd
    expr: SUM(gross_volume_usd)
  - name: gross_volume_local
    expr: SUM(gross_volume_local)
  - name: net_flow_usd
    expr: SUM(CASE WHEN direction = 'credit' THEN gross_volume_usd ELSE -gross_volume_usd END)
  - name: cross_border_events
    expr: SUM(CASE WHEN event_type = 'cross_border_transfer' THEN event_count ELSE 0 END)
  - name: cross_border_volume_usd
    expr: SUM(CASE WHEN event_type = 'cross_border_transfer' THEN gross_volume_usd ELSE 0 END)
  - name: completed_events
    expr: SUM(completed_events)
  - name: completion_rate
    expr: SUM(completed_events) / NULLIF(SUM(event_count), 0)
  - name: decline_rate
    expr: SUM(declined_events) / NULLIF(SUM(event_count), 0)
  - name: fraud_rate
    expr: SUM(fraud_events) / NULLIF(SUM(event_count), 0)
  - name: weighted_average_risk_score
    expr: SUM(average_risk_score * event_count) / NULLIF(SUM(event_count), 0)
$$;

CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_risk_operations
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada de risco, fraude e AML da Quilla.'
AS $$
version: 1.1
source: example_catalog.example_gold.mv_risk_operations_daily
dimensions:
  - name: alert_date
    expr: alert_date
  - name: country_code
    expr: country_code
  - name: alert_type
    expr: alert_type
  - name: severity
    expr: severity
  - name: status
    expr: status
  - name: currency_code
    expr: currency_code
measures:
  - name: total_alerts
    expr: SUM(alert_count)
  - name: amount_at_risk_local
    expr: SUM(amount_at_risk_local)
  - name: confirmed_alerts
    expr: SUM(confirmed_alerts)
  - name: confirmation_rate
    expr: SUM(confirmed_alerts) / NULLIF(SUM(alert_count), 0)
  - name: false_positive_rate
    expr: SUM(false_positive_alerts) / NULLIF(SUM(alert_count), 0)
  - name: weighted_average_risk_score
    expr: SUM(average_risk_score * alert_count) / NULLIF(SUM(alert_count), 0)
$$;

CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_credit_portfolio
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada do portfólio de crédito da Quilla.'
AS $$
version: 1.1
source: example_catalog.example_gold.mv_credit_portfolio_monthly
dimensions:
  - name: request_month
    expr: request_month
  - name: country_code
    expr: country_code
  - name: product_id
    expr: product_id
  - name: decision
    expr: decision
  - name: policy_band
    expr: policy_band
  - name: currency_code
    expr: currency_code
measures:
  - name: applications
    expr: SUM(application_count)
  - name: applicants
    expr: SUM(applicant_count)
  - name: requested_amount_local
    expr: SUM(requested_amount_local)
  - name: approved_amount_local
    expr: SUM(approved_amount_local)
  - name: approval_rate
    expr: SUM(approved_applications) / NULLIF(SUM(application_count), 0)
  - name: weighted_average_risk_score
    expr: SUM(average_risk_score * application_count) / NULLIF(SUM(application_count), 0)
  - name: weighted_average_annual_rate_bps
    expr: SUM(average_annual_rate_bps * application_count) / NULLIF(SUM(application_count), 0)
$$;
