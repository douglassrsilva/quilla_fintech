# Contrato de dados sintéticos raw

## Convenções

- Campos técnicos usam inglês e `snake_case`;
- Conteúdo humano usa `pt-BR` no Brasil e espanhol nos demais países;
- Referências exibíveis possuem versões `*_pt` e `*_es` quando compartilhadas regionalmente;
- Timestamps são armazenados em UTC e acompanhados pelo timezone do país quando relevante;
- Chaves e documentos são tokens sintéticos, não identificadores financeiros reais;
- Todos os datasets incluem `synthetic_flag=true` e/ou metadados equivalentes;
- Saída: `/Volumes/example_catalog/quilla_raw/synthetic_landing/snapshot=<snapshot>/<dataset>`.

## Volumes do target demo

| Dataset | Registros | Particionamento |
|---|---:|---|
| `countries` | 12 | — |
| `products` | 25 | — |
| `fx_rates` | 8.772 | `rate_month` |
| `parties` | 144.000 | `country_code` |
| `accounts` | 240.000 | `country_code` |
| `cards` | 160.000 | `country_code` |
| `merchants` | 20.000 | `country_code` |
| `devices` | 150.000 | `country_code` |
| `financial_events` | 13.758.648 | `country_code`, `event_month` |
| `receivables` | 300.000 | `country_code` |
| `credit_cases` | 120.000 | `country_code` |
| `insurance_cases` | 60.000 | `country_code` |
| `risk_alerts` | 206.380 | `country_code` |
| `consents` | 300.000 | `country_code` |
| `interactions` | 100.000 | `country_code` |
| `documents` | 25.000 | `country_code` |
| `quality_truth` | variável | `issue_code` |
| `generation_manifest` | uma linha por dataset validado | — |

`fx_rates` inclui 01/09/2026 como um dia adicional de cobertura de liquidação. Os eventos
financeiros permanecem estritamente entre 01/09/2024 e 31/08/2026.

## Distribuição de `financial_events`

| País | Código | Eventos |
|---|---|---:|
| Brasil | BR | 4.265.181 |
| México | MX | 2.889.316 |
| Colômbia | CO | 1.651.038 |
| Argentina | AR | 1.100.692 |
| Chile | CL | 963.105 |
| Peru | PE | 963.105 |
| Equador | EC | 481.553 |
| Guatemala | GT | 412.759 |
| República Dominicana | DO | 343.966 |
| Costa Rica | CR | 275.173 |
| Panamá | PA | 206.380 |
| Uruguai | UY | 206.380 |

Os pesos são uma premissa de demonstração e não representam market share.

## Colunas por dataset

### `countries`

`country_code`, `country_name_pt`, `country_name_es`, `locale_code`, `language_code`,
`currency_code`, `timezone`, `market_tier`, `data_residency_policy`, `is_active`,
`effective_from`, `effective_to`, `synthetic_flag`.

### `products`

`product_id`, `product_code`, `business_line`, `product_name_pt`, `product_name_es`,
`description_pt`, `description_es`, `customer_segment`, `availability_scope`,
`risk_class`, `is_active`, `effective_from`, `synthetic_flag`.

### `fx_rates`

`rate_date`, `rate_month`, `country_code`, `base_currency`, `quote_currency`,
`mid_rate`, `buy_rate`, `sell_rate`, `rate_source`, `observed_at`, `synthetic_flag`.

### `parties`

`party_id`, `party_type`, `country_code`, `locale_code`, `preferred_language`,
`legal_name`, `first_name`, `last_name`, `display_name`, `email`, `phone_masked`,
`city`, `customer_segment`, `kyc_status`, `risk_band`, `onboarding_ts`,
`source_system`, `synthetic_flag`, `ingested_at`.

### `accounts`

`account_id`, `party_id`, `product_id`, `country_code`, `account_type`,
`currency_code`, `status`, `opened_at`, `closed_at`, `available_balance`,
`current_balance`, `credit_limit`, `risk_band`, `source_system`, `record_version`,
`updated_at`, `synthetic_flag`, `ingested_at`.

### `cards`

`card_id`, `card_token`, `account_id`, `party_id`, `country_code`, `card_type`,
`network`, `form_factor`, `status`, `issued_at`, `expiry_ym`, `spending_limit`,
`available_limit`, `source_system`, `synthetic_flag`, `ingested_at`.

### `merchants`

`merchant_id`, `country_code`, `merchant_name`, `merchant_category`, `mcc`, `city`,
`onboarded_at`, `risk_band`, `status`, `source_system`, `synthetic_flag`, `ingested_at`.

### `devices`

`device_id`, `device_token`, `party_id`, `country_code`, `device_type`,
`operating_system`, `trust_status`, `first_seen_at`, `last_seen_at`,
`source_system`, `synthetic_flag`, `ingested_at`.

### `financial_events`

`source_record_id`, `event_id`, `country_code`, `party_id`, `account_id`, `card_id`,
`merchant_id`, `device_id`, `product_id`, `event_type`, `event_status`, `channel`,
`direction`, `amount_local`, `currency_code`, `amount_usd`, `event_ts_utc`,
`event_timezone`, `authorized_ts`, `settled_ts`, `ingested_at`, `is_cross_border`,
`counterparty_country_code`, `risk_score`, `fraud_label`, `description`,
`source_system`, `schema_version`, `payload_variant`, `event_month`, `synthetic_flag`.

### `receivables`

`receivable_id`, `party_id`, `account_id`, `country_code`, `invoice_token`,
`counterparty_name`, `issued_date`, `due_date`, `settled_date`, `currency_code`,
`face_value`, `outstanding_value`, `status`, `eligible_for_advance`,
`advance_rate`, `source_system`, `synthetic_flag`, `ingested_at`.

### `credit_cases`

`credit_case_id`, `party_id`, `account_id`, `country_code`, `product_id`,
`requested_at`, `decided_at`, `requested_amount`, `approved_amount`, `currency_code`,
`decision`, `risk_score`, `policy_band`, `annual_rate_bps`, `term_months`,
`decision_reason`, `source_system`, `synthetic_flag`, `ingested_at`.

### `insurance_cases`

`insurance_case_id`, `party_id`, `account_id`, `country_code`, `policy_token`,
`case_type`, `coverage_type`, `opened_at`, `resolved_at`, `currency_code`,
`claimed_amount`, `approved_amount`, `status`, `fraud_indicator`, `source_system`,
`synthetic_flag`, `ingested_at`.

### `risk_alerts`

`alert_id`, `event_id`, `party_id`, `account_id`, `country_code`, `alert_type`,
`severity`, `risk_score`, `created_at`, `status`, `disposition`, `assigned_queue`,
`rule_version`, `model_version`, `amount_at_risk`, `currency_code`, `source_system`,
`synthetic_flag`, `ingested_at`.

### `consents`

`consent_id`, `party_id`, `country_code`, `purpose_code`, `channel`, `status`,
`granted_at`, `expires_at`, `revoked_at`, `policy_version`, `locale_code`,
`source_system`, `synthetic_flag`, `ingested_at`.

### `interactions`

`interaction_id`, `party_id`, `country_code`, `interaction_ts`, `channel`,
`language_code`, `intent`, `message_text`, `sentiment_score`, `resolution_status`,
`agent_type`, `source_system`, `synthetic_flag`, `ingested_at`.

### `documents`

`document_id`, `country_code`, `document_type`, `language_code`, `title`, `body`,
`topic`, `effective_date`, `review_date`, `version`, `status`, `is_official`,
`disclaimer`, `source_system`, `synthetic_flag`, `ingested_at`.

### `quality_truth`

`truth_id`, `dataset_name`, `source_record_id`, `business_key`, `country_code`,
`issue_code`, `column_name`, `expected_value`, `observed_value`, `severity`,
`is_intentional`, `seed`, `created_at`.

### `generation_manifest`

`run_id`, `snapshot`, `dataset_name`, `volume_path`, `format`, `expected_rows`,
`actual_rows`, `validation_status`, `min_business_date`, `max_business_date`,
`schema_json`, `seed`, `generated_at`.

## Qualidade proposital

Em `financial_events`, sem alterar a contagem final:

- 0,20% de chaves de evento duplicadas;
- 2,00% de eventos com chegada entre 1 e 72 horas após a latência normal;
- 0,10% de `amount_local` ausente;
- 0,05% de incompatibilidade entre país e moeda;
- pequenas variações de caixa, whitespace e versão de payload.

Cada defeito material possui um registro correspondente em `quality_truth`.

