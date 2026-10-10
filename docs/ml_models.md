# Quilla — portfólio de modelos ML

Atualizado em: 2026-09-01

## Padrão operacional

Os modelos são treinados em jobs serverless, rastreados em experimentos MLflow isolados
por target no diretório do usuário e registrados como
`example_catalog.example_ml.quilla_<model_key>`. Cada treinamento registra
parâmetros, métricas, slices por país, assinatura, exemplo de entrada e um MLflow Trace de
inferência de validação. O modelo corrente recebe o alias `Champion`; o Champion anterior é
preservado como `Challenger`. O DAB executa uma task independente por modelo, permitindo
22 treinos paralelos e reparo isolado sem repetir modelos aprovados.

| Domínio | Modelo | Decisão de negócio |
|---|---|---|
| Risco realtime | `transaction_fraud` | bloquear ou desafiar transações suspeitas |
| Segurança | `account_takeover` | aplicar autenticação adaptativa |
| AML | `aml_anomaly` | priorizar padrões anômalos |
| Operações de risco | `alert_confirmation` | ordenar fila por chance de confirmação |
| Crédito | `credit_pd` | estimar probabilidade de default |
| Crédito | `credit_lgd` | estimar perda dado default |
| Crédito | `credit_ead` | estimar exposição no default |
| Cobrança | `delinquency_propensity` | antecipar atraso e ação preventiva |
| Tesouraria | `cashflow_forecast` | prever liquidez por país e produto |
| Macroeconomia | `macro_regime` | classificar regime econômico sintético |
| Stress testing | `portfolio_stress_loss` | estimar perda sob juros, inflação e crises |
| Pricing | `pricing_acceptance` | equilibrar conversão, preço e risco |
| Crescimento | `next_best_action` | selecionar próxima ação por cliente |
| Retenção | `customer_churn` | antecipar abandono |
| Valor | `customer_clv` | estimar valor do relacionamento |
| PMEs | `merchant_sales_forecast` | prever vendas e apoiar capital de giro |
| Seguros | `insurance_fraud` | priorizar sinistros suspeitos |
| Seguros | `insurance_severity` | estimar severidade financeira |
| CX PT/ES | `support_intent` | rotear atendimento nativo |
| Wealth | `investment_suitability` | classificar perfil compatível |
| Wealth | `investment_product_recommender` | recomendar classe de produto elegível |
| Wealth | `portfolio_risk_forecast` | antecipar volatilidade do portfólio |

## Model Serving

Seis Champions compõem a superfície realtime inicial: fraude transacional, account takeover,
PD de crédito, next best action, suitability e intenção de atendimento. Os endpoints usam
scale-to-zero e inference tables governadas pelo AI Gateway para observabilidade e posterior
monitoramento.

## Limites responsáveis

Resultados demonstram capacidades técnicas, não aprovação regulatória ou adequação para
produção. Antes de uso real seriam obrigatórios validação independente, explicabilidade,
fairness, calibração, thresholds por país, gestão de drift, revisão humana, políticas de
override e documentação regulatória local.
