SELECT
  'growth_customer' AS semantic_asset,
  MEASURE(customer_observations) AS row_or_event_count,
  MEASURE(conversion_rate) AS primary_rate
FROM example_catalog.example_gold.metrics_growth_customer;

SELECT
  'pricing_profitability' AS semantic_asset,
  MEASURE(offers) AS row_or_event_count,
  MEASURE(acceptance_rate) AS primary_rate
FROM example_catalog.example_gold.metrics_pricing_profitability;

SELECT
  'wealth_engagement' AS semantic_asset,
  MEASURE(investment_events) AS row_or_event_count,
  MEASURE(recommendation_acceptance_rate) AS primary_rate
FROM example_catalog.example_gold.metrics_wealth_engagement;

SELECT
  'investor_suitability' AS semantic_asset,
  MEASURE(profiles) AS row_or_event_count,
  MEASURE(eligibility_rate) AS primary_rate
FROM example_catalog.example_gold.metrics_investor_suitability;

SELECT
  'macro_stress' AS semantic_asset,
  MEASURE(observations) AS row_or_event_count,
  MEASURE(stress_observation_rate) AS primary_rate
FROM example_catalog.example_gold.metrics_macro_stress;

SELECT
  'case_outcomes' AS semantic_asset,
  MEASURE(cases) AS row_or_event_count,
  MEASURE(confirmation_rate) AS primary_rate
FROM example_catalog.example_gold.metrics_case_outcomes;

SELECT
  'data_quality' AS semantic_asset,
  MEASURE(datasets) AS row_or_event_count,
  MEASURE(reconciliation_rate) AS primary_rate
FROM example_catalog.example_gold.metrics_data_quality;
