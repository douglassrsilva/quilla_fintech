# Zerobus realtime — Quilla

## Objetivo

O fluxo demonstra ingestão direta de eventos financeiros em uma tabela Delta governada
pelo Unity Catalog, sem staging de arquivos e sem alterar o snapshot histórico. O Auto
Loader continua responsável pelo backfill Parquet; Zerobus é o caminho de eventos novos.

```text
Parquet histórico -> Auto Loader --+
                                  +-> Bronze canônica -> Silver DQX -> Gold -> Metric Views
Simulador PT/ES -> Zerobus -------+
```

A Bronze canônica de eventos usa dois `append_flow` nomeados no mesmo streaming table,
com checkpoints independentes para Auto Loader e Zerobus.

## Recursos DAB

- `quilla_zerobus_bootstrap`: cria/reutiliza o service principal M2M, a credencial no
  secret scope `quilla-zerobus`, a tabela `quilla_bronze.zerobus_financial_events` e os
  privilégios mínimos `USE CATALOG`, `USE SCHEMA`, `SELECT` e `MODIFY`;
- `simulate_realtime_events`: emite dados e executa o medallion incremental completo;
- `quilla_medallion`: também inclui o produtor Zerobus, tornando realtime parte integral
  da demonstração padrão;
- dependência pinada: `databricks-zerobus-ingest-sdk==1.7.0`.

Credenciais nunca são gravadas no repositório, passadas como parâmetros de task ou
exibidas no log. O produtor as resolve dentro do runtime e o segredo permanece no
Databricks Secret Scope. A criação é idempotente.

## Cenários

- `normal`: tráfego financeiro cotidiano;
- `fraud_burst`: pico de eventos de alto risco;
- `cross_border`: transferências e câmbio regionais;
- `dq_violations`: defeitos intencionais enviados à quarentena DQX;
- `mixed`: composição dos quatro cenários, padrão da demo.

Os eventos cobrem os 12 mercados, usam português no Brasil e espanhol nos demais países.
As chaves estrangeiras apontam para os domínios sintéticos históricos. Cada execução usa
`simulation_id={{job.run_id}}`; retries reapresentam as mesmas chaves e são absorvidos pela
deduplicação Silver.

## Operação

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT

# Bootstrap isolado, útil na primeira implantação
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo \
  quilla_zerobus_bootstrap --profile DEFAULT

# Demo completa com parâmetros padrão: 1.000 eventos no cenário mixed
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo \
  simulate_realtime_events --profile DEFAULT

# Exemplo: burst de fraude com 10.000 eventos
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo \
  simulate_realtime_events --params event_count=10000,scenario=fraud_burst \
  --profile DEFAULT
```

O produtor agrupa os registros, usa `ingest_records_offset`, aguarda confirmação durável
com `flush()` e só então libera as tasks Bronze, Silver e Gold. Colunas Delta
`TIMESTAMP` são codificadas como `int64` em microssegundos desde epoch, conforme o
contrato do Zerobus.

## Observabilidade e validação

As colunas `_producer_run_id`, `_producer_scenario`, `_producer_sequence`,
`_producer_language` e `_producer_ingested_at` permitem acompanhar cada demonstração. A
validação final deve confirmar a mesma execução em:

1. `quilla_bronze.zerobus_financial_events`;
2. `quilla_bronze.bronze_financial_events` com `_snapshot = 'realtime'`;
3. `quilla_silver.financial_events` ou `financial_events_quarantine`;
4. `quilla_gold.mv_financial_performance_daily`.

O notebook `09_validate_realtime.py` automatiza essas verificações e persiste o resultado
idempotente em `quilla_gold.realtime_demo_validation_runs`.

O endpoint regional está parametrizado por `zerobus_endpoint` em `databricks.yml`.
