CREATE OR REPLACE VIEW example_catalog.example_gold.vw_growth_customer_monthly
COMMENT 'Gold semântico agregado de crescimento, retenção, receita e valor do cliente. Dados sintéticos.'
AS
SELECT
  observation_month,
  country_code,
  offer_exposed_code,
  treatment_group,
  COUNT(*) AS customer_observation_count,
  SUM(CASE WHEN converted_30d THEN 1 ELSE 0 END) AS converted_customer_observations,
  SUM(CASE WHEN churned_90d THEN 1 ELSE 0 END) AS churned_customer_observations,
  SUM(transaction_count_30d) AS transaction_count_30d,
  SUM(debit_volume_usd_30d) AS debit_volume_usd_30d,
  SUM(credit_volume_usd_30d) AS credit_volume_usd_30d,
  SUM(interaction_count_30d) AS interaction_count_30d,
  SUM(complaint_count_90d) AS complaint_count_90d,
  SUM(revenue_usd_90d) AS revenue_usd_90d,
  SUM(cost_to_serve_usd_90d) AS cost_to_serve_usd_90d,
  SUM(revenue_usd_90d - cost_to_serve_usd_90d) AS contribution_margin_usd_90d,
  AVG(clv_usd_12m) AS average_clv_usd_12m,
  AVG(digital_engagement_score) AS average_digital_engagement_score,
  AVG(product_count) AS average_product_count
FROM example_catalog.example_ml.source_customer_lifecycle_monthly
GROUP BY observation_month, country_code, offer_exposed_code, treatment_group;

ALTER VIEW example_catalog.example_gold.vw_growth_customer_monthly
SET TBLPROPERTIES ('quilla.synthetic'='true','quilla.asset.type'='semantic_gold_view','quilla.domain'='growth');

CREATE OR REPLACE VIEW example_catalog.example_gold.vw_pricing_performance_daily
COMMENT 'Gold semântico agregado de pricing, conversão, perda esperada e margem. Dados sintéticos.'
AS
SELECT
  TO_DATE(offer_ts) AS offer_date,
  country_code,
  product_id,
  offer_type,
  channel,
  treatment_group,
  COUNT(*) AS offer_count,
  SUM(CASE WHEN accepted_flag THEN 1 ELSE 0 END) AS accepted_offer_count,
  SUM(CASE WHEN activated_flag THEN 1 ELSE 0 END) AS activated_offer_count,
  SUM(revenue_usd_90d) AS revenue_usd_90d,
  SUM(expected_loss_usd_90d) AS expected_loss_usd_90d,
  SUM(contribution_margin_usd_90d) AS contribution_margin_usd_90d,
  AVG(reference_policy_rate_pct) AS average_policy_rate_pct,
  AVG(reference_apr_bps) AS average_reference_apr_bps,
  AVG(offered_apr_bps) AS average_offered_apr_bps,
  AVG(fee_bps) AS average_fee_bps,
  AVG(limit_local) AS average_limit_local,
  AVG(term_months) AS average_term_months,
  AVG(eligibility_score) AS average_eligibility_score,
  AVG(utilization_90d) AS average_utilization_90d
FROM example_catalog.example_ml.source_pricing_experiments
GROUP BY TO_DATE(offer_ts), country_code, product_id, offer_type, channel, treatment_group;

ALTER VIEW example_catalog.example_gold.vw_pricing_performance_daily
SET TBLPROPERTIES ('quilla.synthetic'='true','quilla.asset.type'='semantic_gold_view','quilla.domain'='pricing');

CREATE OR REPLACE VIEW example_catalog.example_gold.vw_wealth_performance_daily
COMMENT 'Gold semântico agregado de jornadas de investimento, suitability e produtos. Dados sintéticos.'
AS
SELECT
  TO_DATE(e.event_ts) AS event_date,
  e.country_code,
  e.event_type,
  e.channel,
  p.asset_class,
  p.risk_class,
  p.currency_code,
  r.investment_objective,
  r.liquidity_need,
  r.suitability_status,
  COUNT(*) AS investment_event_count,
  COUNT(DISTINCT e.profile_id) AS active_profile_count,
  SUM(e.amount_usd) AS investment_volume_usd,
  SUM(CASE WHEN e.event_type = 'buy' THEN e.amount_usd ELSE 0 END) AS buy_volume_usd,
  SUM(CASE WHEN e.event_type = 'sell' THEN e.amount_usd ELSE 0 END) AS sell_volume_usd,
  SUM(CASE WHEN e.recommendation_exposed THEN 1 ELSE 0 END) AS recommendation_exposure_count,
  SUM(CASE WHEN e.accepted_recommendation THEN 1 ELSE 0 END) AS accepted_recommendation_count,
  SUM(CASE WHEN e.suitability_breach_flag THEN 1 ELSE 0 END) AS suitability_breach_count,
  AVG(e.realized_return_90d_pct) AS average_realized_return_90d_pct,
  AVG(e.portfolio_value_after_usd) AS average_portfolio_value_usd,
  AVG(p.fee_bps) AS average_fee_bps,
  AVG(p.liquidity_days) AS average_liquidity_days,
  AVG(p.sustainability_score) AS average_sustainability_score
FROM example_catalog.example_ml.source_investment_events e
JOIN example_catalog.example_ml.source_investment_products p
  ON e.investment_product_id = p.investment_product_id
JOIN example_catalog.example_ml.source_investment_profiles r
  ON e.profile_id = r.profile_id
GROUP BY
  TO_DATE(e.event_ts), e.country_code, e.event_type, e.channel, p.asset_class,
  p.risk_class, p.currency_code, r.investment_objective, r.liquidity_need,
  r.suitability_status;

ALTER VIEW example_catalog.example_gold.vw_wealth_performance_daily
SET TBLPROPERTIES ('quilla.synthetic'='true','quilla.asset.type'='semantic_gold_view','quilla.domain'='wealth');

CREATE OR REPLACE VIEW example_catalog.example_gold.vw_case_outcomes_daily
COMMENT 'Gold semântico agregado de fraude, AML, seguros e eficiência investigativa. Dados sintéticos.'
AS
SELECT
  TO_DATE(opened_at) AS opened_date,
  country_code,
  domain,
  case_type,
  investigator_disposition,
  escalation_level,
  COUNT(*) AS case_count,
  SUM(CASE WHEN confirmed_flag THEN 1 ELSE 0 END) AS confirmed_case_count,
  SUM(CASE WHEN false_positive_flag THEN 1 ELSE 0 END) AS false_positive_case_count,
  SUM(loss_amount_usd) AS loss_amount_usd,
  SUM(prevented_loss_usd) AS prevented_loss_usd,
  SUM(recovery_amount_usd) AS recovery_amount_usd,
  AVG(investigation_minutes) AS average_investigation_minutes,
  AVG(CASE WHEN resolved_at IS NOT NULL THEN TIMESTAMPDIFF(MINUTE, opened_at, resolved_at) END) AS average_resolution_minutes
FROM example_catalog.example_ml.source_case_outcomes
GROUP BY TO_DATE(opened_at), country_code, domain, case_type, investigator_disposition, escalation_level;

ALTER VIEW example_catalog.example_gold.vw_case_outcomes_daily
SET TBLPROPERTIES ('quilla.synthetic'='true','quilla.asset.type'='semantic_gold_view','quilla.domain'='case-outcomes');

CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_growth_customer
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada de crescimento, retenção, receita e CLV da Quilla.'
AS $$
version: 1.1
source: example_catalog.example_gold.vw_growth_customer_monthly
dimensions:
  - name: observation_month
    expr: observation_month
  - name: country_code
    expr: country_code
  - name: offer_exposed_code
    expr: offer_exposed_code
  - name: treatment_group
    expr: treatment_group
measures:
  - name: customer_observations
    expr: SUM(customer_observation_count)
  - name: conversions
    expr: SUM(converted_customer_observations)
  - name: conversion_rate
    expr: SUM(converted_customer_observations) / NULLIF(SUM(customer_observation_count), 0)
  - name: churned_customer_observations
    expr: SUM(churned_customer_observations)
  - name: churn_rate
    expr: SUM(churned_customer_observations) / NULLIF(SUM(customer_observation_count), 0)
  - name: revenue_usd_90d
    expr: SUM(revenue_usd_90d)
  - name: cost_to_serve_usd_90d
    expr: SUM(cost_to_serve_usd_90d)
  - name: contribution_margin_usd_90d
    expr: SUM(contribution_margin_usd_90d)
  - name: contribution_margin_rate
    expr: SUM(contribution_margin_usd_90d) / NULLIF(SUM(revenue_usd_90d), 0)
  - name: weighted_average_clv_usd_12m
    expr: SUM(average_clv_usd_12m * customer_observation_count) / NULLIF(SUM(customer_observation_count), 0)
  - name: weighted_average_digital_engagement
    expr: SUM(average_digital_engagement_score * customer_observation_count) / NULLIF(SUM(customer_observation_count), 0)
  - name: transactions_30d
    expr: SUM(transaction_count_30d)
  - name: complaints_90d
    expr: SUM(complaint_count_90d)
$$;

CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_pricing_profitability
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada de pricing, conversão, perda esperada e margem da Quilla.'
AS $$
version: 1.1
source: example_catalog.example_gold.vw_pricing_performance_daily
dimensions:
  - name: offer_date
    expr: offer_date
  - name: country_code
    expr: country_code
  - name: product_id
    expr: product_id
  - name: offer_type
    expr: offer_type
  - name: channel
    expr: channel
  - name: treatment_group
    expr: treatment_group
measures:
  - name: offers
    expr: SUM(offer_count)
  - name: accepted_offers
    expr: SUM(accepted_offer_count)
  - name: acceptance_rate
    expr: SUM(accepted_offer_count) / NULLIF(SUM(offer_count), 0)
  - name: activated_offers
    expr: SUM(activated_offer_count)
  - name: activation_rate
    expr: SUM(activated_offer_count) / NULLIF(SUM(accepted_offer_count), 0)
  - name: revenue_usd_90d
    expr: SUM(revenue_usd_90d)
  - name: expected_loss_usd_90d
    expr: SUM(expected_loss_usd_90d)
  - name: contribution_margin_usd_90d
    expr: SUM(contribution_margin_usd_90d)
  - name: risk_adjusted_margin_rate
    expr: SUM(contribution_margin_usd_90d) / NULLIF(SUM(revenue_usd_90d), 0)
  - name: weighted_average_offered_apr_bps
    expr: SUM(average_offered_apr_bps * offer_count) / NULLIF(SUM(offer_count), 0)
  - name: weighted_average_fee_bps
    expr: SUM(average_fee_bps * offer_count) / NULLIF(SUM(offer_count), 0)
  - name: weighted_average_utilization_90d
    expr: SUM(average_utilization_90d * offer_count) / NULLIF(SUM(offer_count), 0)
$$;

CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_wealth_engagement
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada de investimento, recomendação e suitability da Quilla.'
AS $$
version: 1.1
source: example_catalog.example_gold.vw_wealth_performance_daily
dimensions:
  - name: event_date
    expr: event_date
  - name: country_code
    expr: country_code
  - name: event_type
    expr: event_type
  - name: channel
    expr: channel
  - name: asset_class
    expr: asset_class
  - name: risk_class
    expr: risk_class
  - name: currency_code
    expr: currency_code
  - name: investment_objective
    expr: investment_objective
  - name: liquidity_need
    expr: liquidity_need
  - name: suitability_status
    expr: suitability_status
measures:
  - name: investment_events
    expr: SUM(investment_event_count)
  - name: active_profile_observations
    expr: SUM(active_profile_count)
  - name: investment_volume_usd
    expr: SUM(investment_volume_usd)
  - name: buy_volume_usd
    expr: SUM(buy_volume_usd)
  - name: sell_volume_usd
    expr: SUM(sell_volume_usd)
  - name: recommendation_exposures
    expr: SUM(recommendation_exposure_count)
  - name: accepted_recommendations
    expr: SUM(accepted_recommendation_count)
  - name: recommendation_acceptance_rate
    expr: SUM(accepted_recommendation_count) / NULLIF(SUM(recommendation_exposure_count), 0)
  - name: suitability_breaches
    expr: SUM(suitability_breach_count)
  - name: suitability_breach_rate
    expr: SUM(suitability_breach_count) / NULLIF(SUM(investment_event_count), 0)
  - name: weighted_average_return_90d_pct
    expr: SUM(average_realized_return_90d_pct * investment_event_count) / NULLIF(SUM(investment_event_count), 0)
  - name: weighted_average_portfolio_value_usd
    expr: SUM(average_portfolio_value_usd * investment_event_count) / NULLIF(SUM(investment_event_count), 0)
$$;

CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_investor_suitability
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada de perfis, objetivos, capacidade de perda e elegibilidade demonstrativa.'
AS $$
version: 1.1
source: example_catalog.example_ml.source_investment_profiles
dimensions:
  - name: assessment_date
    expr: TO_DATE(assessment_ts)
  - name: country_code
    expr: country_code
  - name: locale_code
    expr: locale_code
  - name: liquidity_need
    expr: liquidity_need
  - name: sustainability_preference
    expr: sustainability_preference
  - name: investment_objective
    expr: investment_objective
  - name: declared_risk_tolerance
    expr: declared_risk_tolerance
  - name: modeled_risk_class
    expr: modeled_risk_class
  - name: suitability_status
    expr: suitability_status
measures:
  - name: profiles
    expr: COUNT(1)
  - name: eligible_profiles
    expr: SUM(CASE WHEN suitability_status = 'eligible' THEN 1 ELSE 0 END)
  - name: restricted_profiles
    expr: SUM(CASE WHEN suitability_status = 'restricted' THEN 1 ELSE 0 END)
  - name: education_required_profiles
    expr: SUM(CASE WHEN suitability_status = 'education_required' THEN 1 ELSE 0 END)
  - name: eligibility_rate
    expr: SUM(CASE WHEN suitability_status = 'eligible' THEN 1 ELSE 0 END) / NULLIF(COUNT(1), 0)
  - name: investable_assets_usd
    expr: SUM(investable_assets_usd)
  - name: average_investable_assets_usd
    expr: AVG(investable_assets_usd)
  - name: average_emergency_reserve_months
    expr: AVG(emergency_reserve_months)
  - name: average_horizon_months
    expr: AVG(horizon_months)
  - name: average_loss_capacity_pct
    expr: AVG(loss_capacity_pct)
  - name: average_investment_knowledge_score
    expr: AVG(investment_knowledge_score)
$$;

CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_macro_stress
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada de cenários macroeconômicos e stress dos 12 países.'
AS $$
version: 1.1
source: example_catalog.example_ml.source_macroeconomic_daily
dimensions:
  - name: observation_date
    expr: observation_date
  - name: country_code
    expr: country_code
  - name: currency_code
    expr: currency_code
  - name: stress_regime
    expr: stress_regime
  - name: vintage_date
    expr: vintage_date
measures:
  - name: observations
    expr: COUNT(1)
  - name: average_policy_rate_pct
    expr: AVG(policy_rate_pct)
  - name: average_inflation_yoy_pct
    expr: AVG(inflation_yoy_pct)
  - name: average_unemployment_pct
    expr: AVG(unemployment_pct)
  - name: average_gdp_nowcast_yoy_pct
    expr: AVG(gdp_nowcast_yoy_pct)
  - name: average_fx_per_usd
    expr: AVG(fx_per_usd)
  - name: average_energy_price_index
    expr: AVG(energy_price_index)
  - name: average_commodity_price_index
    expr: AVG(commodity_price_index)
  - name: average_consumer_confidence_index
    expr: AVG(consumer_confidence_index)
  - name: stress_observation_rate
    expr: SUM(CASE WHEN stress_regime <> 'baseline' THEN 1 ELSE 0 END) / NULLIF(COUNT(1), 0)
$$;

CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_case_outcomes
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada de perdas, prevenção, recuperação e eficiência de casos.'
AS $$
version: 1.1
source: example_catalog.example_gold.vw_case_outcomes_daily
dimensions:
  - name: opened_date
    expr: opened_date
  - name: country_code
    expr: country_code
  - name: domain
    expr: domain
  - name: case_type
    expr: case_type
  - name: investigator_disposition
    expr: investigator_disposition
  - name: escalation_level
    expr: escalation_level
measures:
  - name: cases
    expr: SUM(case_count)
  - name: confirmed_cases
    expr: SUM(confirmed_case_count)
  - name: confirmation_rate
    expr: SUM(confirmed_case_count) / NULLIF(SUM(case_count), 0)
  - name: false_positive_cases
    expr: SUM(false_positive_case_count)
  - name: false_positive_rate
    expr: SUM(false_positive_case_count) / NULLIF(SUM(case_count), 0)
  - name: loss_amount_usd
    expr: SUM(loss_amount_usd)
  - name: prevented_loss_usd
    expr: SUM(prevented_loss_usd)
  - name: recovery_amount_usd
    expr: SUM(recovery_amount_usd)
  - name: net_loss_usd
    expr: SUM(loss_amount_usd) - SUM(recovery_amount_usd)
  - name: weighted_average_investigation_minutes
    expr: SUM(average_investigation_minutes * case_count) / NULLIF(SUM(case_count), 0)
  - name: weighted_average_resolution_minutes
    expr: SUM(average_resolution_minutes * case_count) / NULLIF(SUM(case_count), 0)
$$;

CREATE OR REPLACE VIEW example_catalog.example_gold.metrics_data_quality
WITH METRICS
LANGUAGE YAML
COMMENT 'Camada semântica governada de reconciliação Bronze, Silver e quarentena DQX.'
AS $$
version: 1.1
source: example_catalog.example_gold.mv_data_quality_overview
dimensions:
  - name: dataset_name
    expr: dataset_name
  - name: reconciled
    expr: reconciled
measures:
  - name: datasets
    expr: COUNT(1)
  - name: bronze_rows
    expr: SUM(bronze_rows)
  - name: silver_rows
    expr: SUM(silver_rows)
  - name: quarantine_rows
    expr: SUM(quarantine_rows)
  - name: global_acceptance_rate
    expr: SUM(silver_rows) / NULLIF(SUM(bronze_rows), 0)
  - name: reconciliation_rate
    expr: SUM(CASE WHEN reconciled THEN 1 ELSE 0 END) / NULLIF(COUNT(1), 0)
$$;
