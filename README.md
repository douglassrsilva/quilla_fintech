# Quilla — Financial Intelligence

Aplicação de referência da Quilla publicada em Databricks Apps. Os dados apresentados são
sintéticos e existem apenas para demonstração, mas o Quilla IQ usa o backend e os serviços reais.

Para instalar tudo em outro workspace, sem reutilizar IDs ou nomes do ambiente de origem,
consulte a [guía de despliegue reproducible en español](docs/GUIA_DESPLIEGUE_ES.md).

## Configuração segura do ambiente

O repositório não contém nomes reais de catálogos ou schemas, Warehouse IDs, resource IDs,
URLs de workspace/App ou credenciais. O DAB cria catálogo, schemas, Volumes, SQL Warehouse,
AI Search e Lakebase com defaults portáteis. Use `.env.local` somente para sobrescrever nomes
em workspaces compartilhados ou configurar o endpoint regional opcional do Zerobus.

```bash
python3 setup/bootstrap_portable.py \
  --profile DEFAULT --target smoke --validate-only
```

O `resources/app.yml` é a fonte canônica da configuração do Databricks App. Faça deploy pelo
DAB; um deploy isolado baseado apenas no `app.yaml` não injeta os recursos e IDs necessários.
Os assets renderizados ficam em `.generated/`, são ignorados pelo Git e devem ser recriados
sempre que catálogo ou schemas forem alterados.

## O que está incluído

- Primeiro acesso imersivo com animações orbitais;
- Centro de comando com feed governado de recomendações por país;
- Customer 360 individual ligado à Gold, com seletor de clientes reais da base sintética;
- Quilla IQ conectado ao Supervisor, Unity AI Gateway, MCPs governados e Lakebase;
- Empresas com KPIs e séries live sobre Metric Views Gold;
- Ciclo governado de recomendação com decisão humana, scoring online e persistência Lakebase;
- Control Room Zerobus com simulação sob demanda e evidência Bronze/Silver/Gold;
- Observabilidade operacional de MLflow, Model Serving, DQX, realtime e lineage;
- Launchpad para quatro dashboards Lakeview publicados;
- Visões Personal, Empresas, Regional e Confiança;
- Roteiro guiado de demonstração;
- Temas claro e escuro;
- Alternância PT/ES nos elementos globais;
- Layout responsivo para desktop, tablet e celular;
- BFF FastAPI com OBO, SSE, idempotência, memória conversacional e feedback por trace.

## Validar a identidade sem instalar nada

Abra `identity-preview.html` diretamente no navegador. Essa prévia é autocontida e não precisa
de npm, Python, servidor local ou conexão com a internet.

## Stack do protótipo

- React 19 + TypeScript;
- Vite 7;
- Lucide React;
- CSS responsivo próprio, sem runtime de framework visual;
- FastAPI + Uvicorn.

## Executar o frontend

```bash
cd frontend
npm install
npm run dev
```

Acesse `http://localhost:5173`.

## Executar no formato do Databricks App

```bash
cd frontend
npm run build
cd ..
python -m pip install -r requirements.txt
python -m uvicorn app:app --host 0.0.0.0 --port 8000
```

Acesse `http://localhost:8000`.

## Limites intencionais

O Centro de Comando e a visão Regional consultam dados governados reais da demonstração. As
recomendações são identificadas como `candidate`: combinam evidência interna e Champions
validados, mas não alegam scoring online. O contexto público atual é enriquecido sob demanda
pelo Quilla IQ com `system.ai.web_search`, evitando bloquear a abertura da tela. As demais
visões narrativas continuam explicitamente sintéticas.

O contrato e a jornada estão em [docs/recommendation_feed.md](docs/recommendation_feed.md).
O Customer 360 e suas regras de minimização estão em
[docs/customer_360.md](docs/customer_360.md).

As seis jornadas de plataforma podem ser validadas diretamente pela API do App, sem criar um
Job validador:

```bash
DATABRICKS_AUTH_STORAGE=plaintext python3 setup/validate_app_platform.py \
  --profile DEFAULT --url "$QUILLA_APP_URL" --run-realtime
```

## Dados sintéticos reproduzíveis

O DAB em `databricks.yml` cria Parquets PT/ES no catálogo, schema e Volume informados pelas
variáveis `catalog`, `schema` e `volume`.

```bash
# Validação rápida, com 12 mil eventos
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t smoke --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t smoke synthetic_raw_data --profile DEFAULT

# Snapshot final, com 13.758.648 eventos
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo synthetic_raw_data --profile DEFAULT
```

Em caso de falha depois de uma correção e novo deploy, preserve as tasks concluídas:

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks jobs repair-run <run-id> \
  --rerun-all-failed-tasks --rerun-dependent-tasks --timeout 2h \
  --profile DEFAULT
```

Veja `DEMO.md`, `TASKS.md` e `docs/raw_data_contract.md` para arquitetura, status e contrato.

## Bateria MLflow de revisão

O job DAB `quilla_genai_review_battery` publica 216 cenários canônicos, sintéticos e variados em PT/ES
para demonstrar Sessions, Scorers, Custom Trace Views e Review Queues. A cobertura, os filtros
da UI e o procedimento de reprodução estão em
[docs/genai_review_battery.md](docs/genai_review_battery.md).

## Lakeflow SDP — arquitetura medallion

O job `quilla_medallion` é independente da geração raw e orquestra:

1. `quilla_bronze_pipeline`: 17 Streaming Tables com Auto Loader sobre o snapshot Parquet;
2. `quilla_silver_pipeline`: contratos declarativos, DQX 0.16, quarentena e deduplicação;
3. `quilla_gold_pipeline`: cinco Materialized Views para finanças, risco, crédito, Customer 360 e qualidade;
4. criação de três Metric Views governadas no SQL Warehouse.

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo quilla_medallion --profile DEFAULT
```

Uma execução incremental usa `full_refresh: false`; em caso de falha, repare o run original para
preservar as camadas já concluídas. Os contratos vivem em `contracts/silver_contracts.yml`.

## Metric Views e dashboards executivos

O job `quilla_business_semantics` publica dez Metric Views governadas (três centrais e sete
de negócio) e valida as 27 consultas usadas por quatro dashboards Lakeview: Centro Executivo
LATAM, Growth & Profitabilidade, Wealth & Macro Stress e Risco, Crédito & Confiança.

```bash
python3 dashboards/build_quilla_dashboards.py
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_business_semantics --profile DEFAULT
```

Os JSONs Lakeview, o SQL semântico consolidado e os probes são gerados deterministicamente.
Identidade, medidas, URLs e reprodução estão em
[docs/business_semantics_dashboards.md](docs/business_semantics_dashboards.md).

## Zerobus — realtime integral

O job `simulate_realtime_events` envia eventos financeiros PT/ES diretamente para uma
tabela Delta governada pelo Unity Catalog e, após a confirmação durável do Zerobus, executa
Bronze, Silver DQX, Gold e Metric Views. O mesmo produtor também integra o job
`quilla_medallion`; nenhum dos dois recria o snapshot raw.

Consulte [docs/zerobus_realtime.md](docs/zerobus_realtime.md) para cenários, segurança,
execução e validação.

## MLflow e Feature Store

```bash
# 1) Outcomes sintéticos + Feature Store
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo quilla_ml_data --profile DEFAULT

# 2) 22 treinos paralelos, MLflow Traces e registro UC
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo quilla_ml_train --profile DEFAULT

# 3) Seis endpoints Champion com AI Gateway inference tables
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo quilla_ml_serving --profile DEFAULT
```

Os dados ML usam o Volume separado `quilla_raw.ml_landing`; o landing operacional de
13.758.648 eventos permanece imutável. Consulte [docs/ml_data_contract.md](docs/ml_data_contract.md)
e [docs/ml_models.md](docs/ml_models.md).

## Lakebase LTAP e AI Search híbrido

```bash
# 1) O DAB cria o projeto, branch, endpoint, role e database; este passo inicializa o schema SQL
DATABRICKS_AUTH_STORAGE=plaintext uv run --with 'psycopg[binary]==3.2.9' \
  python setup/lakebase_bootstrap.py \
  --profile DEFAULT --project quilla-ltap-demo --database quilla

# 2) Curadoria UC + Lakebase Search e Vector Search em paralelo + reranking
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_ltap_ai_search --profile DEFAULT
```

O fluxo cria o estado LTAP do App e uma base demonstrativa PT/ES com 25.000 documentos não
oficiais. O Supervisor persiste sessões, mensagens, rotas, evidências e traces nesse Lakebase;
o backend expõe recuperação isolada por usuário em `GET /api/conversations` e
`GET /api/conversations/{session_id}`. Consulte
[docs/ltap_ai_search.md](docs/ltap_ai_search.md) e
[docs/custom_supervisor.md](docs/custom_supervisor.md).

## Governança e avaliação GenAI

```bash
# Prompt Registry, traces UC e Production Monitoring
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_ai_governance --profile DEFAULT

# Golden datasets, baseline × candidate, juízes semânticos e promotion gate
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_genai_offline_evaluation --profile DEFAULT
```

Consulte [docs/genai_governance.md](docs/genai_governance.md),
[docs/genai_offline_evaluation.md](docs/genai_offline_evaluation.md) e o
[runbook integral](docs/reproduction_runbook.md).

## Fundação multiagente e Managed Memory

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_agent_foundation --profile DEFAULT
```

O job publica o supervisor e seis especialistas PT/ES, cria sete Managed Memory Stores em
paralelo e valida a Conversation API. A promoção de Agent Versions permanece bloqueada até a
Review Queue humana. Consulte [docs/agent_foundation.md](docs/agent_foundation.md).

## Genie Agents

Seis Genie Spaces bilíngues são criados e atualizados pelo próprio deploy do DAB:

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy \
  -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle summary \
  -t demo --profile DEFAULT
```

Eles cobrem Executivo/Regional, Pagamentos, Risco/Fraude, Crédito Responsável,
Cliente/Operações e Growth/Wealth. Consulte [docs/genie_agents.md](docs/genie_agents.md).
