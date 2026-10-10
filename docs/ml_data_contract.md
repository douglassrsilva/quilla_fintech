# Quilla — contrato de dados para ML

Atualizado em: 2026-09-01

## Escopo e segurança

Todos os registros são sintéticos, determinísticos (`seed=42`) e impróprios para decisões
financeiras reais. Os datasets desta etapa ficam separados do landing operacional em
`example_catalog.example_raw.ml_landing` e nunca alteram o snapshot raw de
13.758.648 eventos.

O snapshot de demonstração é `2026-08-31`; o smoke usa `smoke-2026-08-31`. Dados de
interação respeitam português para o Brasil e espanhol localizado para os outros 11 países.
Variáveis macroeconômicas e de mercado são cenários sintéticos, não observações oficiais.

## Datasets

| Dataset | Registros demo | Grão | Chave |
|---|---:|---|---|
| `macroeconomic_daily` | 8.760 | país/dia | `country_code`, `observation_date` |
| `credit_performance_monthly` | 1.440.000 | contrato/mês | `performance_id` |
| `customer_lifecycle_monthly` | 3.456.000 | cliente/mês | `lifecycle_id` |
| `security_sessions` | 1.200.000 | sessão | `session_id` |
| `case_outcomes` | 266.380 | caso investigado | `outcome_id` |
| `pricing_experiments` | 720.000 | oferta experimental | `experiment_id` |
| `investment_products` | 240 | produto | `investment_product_id` |
| `investment_profiles` | 72.000 | avaliação suitability | `profile_id` |
| `investment_events` | 1.200.000 | evento de investimento | `investment_event_id` |
| `market_daily` | 43.800 | instrumento/dia | `market_observation_id` |

Total esperado: **8.407.180 registros**.

## Temporalidade e leakage

- Features comportamentais são observadas antes dos respectivos outcomes;
- `label_available_at` registra quando o rótulo se torna conhecido;
- features macro possuem `vintage_date` e `available_at`;
- tabelas temporais do Feature Store usam chaves de tempo explícitas;
- labels permanecem em tabelas separadas das features sempre que há risco de vazamento;
- splits de avaliação são temporais quando há timestamp válido, com fallback determinístico.

## Controles de validação

O job exige contagem exata, unicidade das chaves, `synthetic_flag=true`, pelo menos duas
classes para rótulos binários e integridade entre eventos e perfis de investimento. Evidências
são persistidas em `<ml_schema>.ml_run_manifest`, sempre identificadas pelo snapshot.

## Publicação no Unity Catalog

As fontes Delta recebem o prefixo `source_`; Feature Tables governadas recebem `fs_`.
Features de transações, sessões, clientes, crédito, pricing, investimentos, mercado, fluxo
financeiro, lojistas e interações são publicadas com chaves primárias e, quando aplicável,
colunas de série temporal.
