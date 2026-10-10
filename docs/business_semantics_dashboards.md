# Business Semantics e dashboards Quilla

Atualizado em: 2026-09-02

Todos os dados apresentados são sintéticos e destinados exclusivamente à demonstração.

## Camada semântica

O schema `example_catalog.example_gold` contém dez Metric Views:

- centrais: `metrics_financial_performance`, `metrics_risk_operations` e
  `metrics_credit_portfolio`;
- crescimento e margem: `metrics_growth_customer` e `metrics_pricing_profitability`;
- investimentos: `metrics_wealth_engagement` e `metrics_investor_suitability`;
- resiliência e confiança: `metrics_macro_stress`, `metrics_case_outcomes` e
  `metrics_data_quality`.

As medidas cobrem receita, margem, churn, conversão, crédito, risco, fraude, fluxo líquido,
investimentos, suitability, stress macroeconômico, perdas evitadas e reconciliação DQX.

## Dashboards publicados

- [Centro Executivo LATAM](https://<workspace-host>/dashboardsv3/<resource-id>/published?w=<resource-id>)
- [Growth & Profitabilidade](https://<workspace-host>/dashboardsv3/<resource-id>/published?w=<resource-id>)
- [Wealth & Macro Stress](https://<workspace-host>/dashboardsv3/<resource-id>/published?w=<resource-id>)
- [Risco, Crédito & Confiança](https://<workspace-host>/dashboardsv3/<resource-id>/published?w=<resource-id>)

Os quatro painéis somam 43 widgets e 27 datasets. O conteúdo é PT-BR/ES-LATAM e usa a
paleta Quilla: índigo `#5865F2`, teal `#20D3BC`, coral `#FF7968`, `#9B8CFF`, `#63E6D2`,
`#FFB3A8`, `#111827` e `#CBD2FF`. Lakeview não oferece CSS arbitrário; a identidade é
aplicada por títulos, terminologia, hierarquia visual e cores dos gráficos.

## Fonte de verdade e reprodução

- definições SQL: `sql/create_metric_views.sql` e `sql/create_business_metric_views.sql`;
- SQL consolidado gerado: `sql/create_all_metric_views.sql`;
- probes gerados: `sql/validate_dashboard_datasets.sql`;
- gerador: `dashboards/build_quilla_dashboards.py`;
- JSONs Lakeview: `dashboards/*.lvdash.json`;
- recursos DAB: `resources/semantic_dashboards.yml`.

```bash
cd .
python3 dashboards/build_quilla_dashboards.py
jq -e . dashboards/*.lvdash.json >/dev/null
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle validate -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo \
  quilla_business_semantics --profile DEFAULT
```

## Evidência

- Warehouse serverless: `<warehouse-id>`;
- job: `<resource-id>`;
- run: `<resource-id>`, reparado no mesmo histórico após o gate detectar uma medida ainda
  não atualizada no workspace;
- resultado final: `SUCCESS`;
- validação: todas as Metric Views e as 27 consultas de datasets passaram;
- publicação: os quatro endpoints `/published` foram confirmados com `embed_credentials=true`.
