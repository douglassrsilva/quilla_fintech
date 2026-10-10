# Runbook de reprodução integral da Quilla

Atualizado em: 2026-09-02

## Supervisor multiagente customizado

O runtime, registro MLflow, endpoint e teste conversacional são reproduzidos pelo job
`quilla_custom_supervisor`. Consulte `docs/custom_supervisor.md` para contratos,
guardrails e evidências.

## Princípio

O repositório é a fonte de verdade. Recursos automatizáveis vivem no DAB, notebooks,
pipelines, contratos e scripts versionados. Recursos Beta que ainda não possuem API pública
são mantidos como especificações governadas e checklist manual; nunca são descritos como
implantados automaticamente.

## Ambiente

- perfil: `DEFAULT`;
- workspace: `<workspace-host>`;
- catálogo: `example_catalog`;
- targets: `smoke` e `demo`;
- compute: serverless sempre que suportado;
- governança: Unity Catalog e namespaces de três níveis;
- idiomas: PT-BR e ES-LATAM.

## Validação e deployment comuns

```bash
cd .
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle validate -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
```

## Ordem de construção greenfield

1. `synthetic_raw_data`: landing Parquet sintético no Volume;
2. `quilla_zerobus_bootstrap`: identidade e destino realtime;
3. `quilla_medallion`: Bronze Auto Loader, Silver DQX e Gold/Metrics;
4. `simulate_realtime_events`: fluxo realtime Zerobus end-to-end;
5. `quilla_ml_data`: outcomes, cenários e Feature Store;
6. `quilla_ml_train`: 22 modelos em paralelo, MLflow e registro UC;
7. `quilla_ml_serving`: seis endpoints, AI Gateway e inference tables;
8. `setup/lakebase_bootstrap.py`: Lakebase Autoscaling idempotente;
9. `quilla_ltap_ai_search`: conhecimento PT/ES, Lakebase Search, Vector Search e reranker;
10. `quilla_ai_quality`: AI Prep/Enrich e benchmark de search;
11. `quilla_governance_bootstrap`: tags, classificação e DQ Monitoring;
12. `quilla_ai_governance`: Prompt Registry, traces e Production Monitoring;
13. `quilla_genai_offline_evaluation`: datasets e Evaluation Runs;
14. `quilla_genai_review_battery`: bateria variada para revisão humana;
15. `quilla_agent_foundation`: contratos, oito Managed Memory Stores e Conversation API;
16. `quilla_governed_skills`: três Governed Skills nativas do Unity AI Gateway;
17. `setup/external_intelligence_bootstrap.py`: Supervisor, Web Search e GLM/DeepSeek/Kimi;
18. deploy DAB: seis Genie Spaces PT/ES sobre Gold, Metrics e Feature Store governada.
19. `quilla_business_semantics`: dez Metric Views, quatro dashboards e 27 probes Lakeview.
20. `quilla_custom_supervisor`: quatro Model Services UC com policies do AI Gateway,
    modelo MLflow com OBO, endpoint e prova conversacional PT/ES com Web Search obrigatório.

No ambiente atual, o snapshot raw já foi concluído e **não deve ser regenerado**. Jobs das
fases seguintes são independentes do gerador raw.

## Jobs DAB

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo <job-key> \
  --no-wait --profile DEFAULT
```

As definições ficam em `resources/*.yml`. Parâmetros e volumes ficam em `databricks.yml`.
Contratos Silver ficam em `contracts/silver_contracts.yml`.

## Recuperação sem reprocessamento integral

Após corrigir e fazer deploy, reparar sempre o run original:

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks jobs repair-run <run-id> \
  --rerun-all-failed-tasks --no-wait --profile DEFAULT
```

Tasks concluídas são preservadas. Não iniciar outro job completo para contornar uma falha.

## Passos ainda UI-only

1. 23 Custom Classifiers, especificados em `quilla_governance.custom_classifier_specs`;
2. quatro unit tests nativos, executados pelo Lakeflow Pipelines Editor;
3. duas Custom Trace Views, especificadas em `quilla_ai.genai_ui_asset_specs`;
4. duas MLflow Review Queues, especificadas na mesma tabela.

Os passos detalhados estão em `docs/governance_quality.md` e
`docs/genai_governance.md`.

## Bateria MLflow para revisão humana

Execute `quilla_genai_review_battery` após `quilla_ai_governance`. A release fica em
`genai_review_release` e deve ser imutável. O job reconcilia o manifesto com o backend de
traces em reparos e preserva cenários válidos; consulte `docs/genai_review_battery.md`.

## Lakebase

Lakebase Autoscaling ainda é provisionado pelo script idempotente
`setup/lakebase_bootstrap.py`, pois o recurso não está disponível como declaração DAB neste
projeto. O script é versionado, parametrizado e faz parte formal deste runbook.

O mesmo bootstrap cria e migra o ledger de memória conversacional (`app_sessions`,
`app_messages`, `agent_runs` e feedback), o ledger de decisões humanas (`decision_requests`,
`decision_actions`) e o outbox transacional (`outbox_events`). Valide persistência,
restauração e isolamento antes de promover
uma nova versão do Supervisor:

```bash
DATABRICKS_AUTH_STORAGE=plaintext uv run --with 'psycopg[binary]==3.2.9' \
  python setup/validate_conversation_memory.py --profile DEFAULT
python setup/validate_app_conversation_api.py
```

## Fundação multiagente

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo \
  quilla_agent_foundation --profile DEFAULT
```

O job cria os oito Memory Stores em paralelo. Em falha da validação, repare o run original;
não recrie stores já concluídos. Managed Memory e Omnigent estão em Beta. A sessão Omnigent
em Sandbox é iniciada pela interface `/omnigent`; não existe recurso DAB para materializar uma
sessão interativa. As skills e os contratos consumidos por ela permanecem versionados.

O Supervisor Agent Beta ainda não é recurso declarativo DAB. Reconcile-o idempotentemente:

```bash
python3 setup/external_intelligence_bootstrap.py --profile DEFAULT
```

As Governed Skills possuem um job DAB independente e idempotente. Execute-o após o deploy:

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo \
  quilla_governed_skills --profile DEFAULT
```

Como alternativa local, inclusive para reconciliação conjunta pelo bootstrap do Supervisor:

```bash
python3 setup/register_governed_skills.py --profile DEFAULT
```

O job `quilla_custom_supervisor` registra e implanta a versão ResponsesAgent. Ele resolve e
injeta no Serving apenas identificadores e o hostname não secreto do projeto Lakebase; a
conexão TLS usa diretamente a credencial OBO temporária do usuário. Esse caminho evita a API
de controle do Lakebase Autoscaling, porque o scope `postgres` ainda não pertence à allowlist
do `UserAuthPolicy` do Agent Serving. O validador executa quatro turnos com o mesmo
`conversation_id` e só aprova quando todos retornam `memory.status=PERSISTED`. Em falha de
registro, deploy ou validação, repare o run original em vez de começar uma nova execução.

Execute a prova pelo protocolo Responses sem perder campos que a CLI tipada ainda ignora:

```bash
python3 setup/query_external_intelligence.py --profile DEFAULT
python3 setup/test_external_intelligence_conversation.py --profile DEFAULT
```

O backend atual do Supervisor não habilita `skill` como tool e não aceita inference tables no
endpoint MAS. As três skills externas estão registradas como Governed Skills nativas no schema
`quilla_ai`, além dos contratos UC e das instruções do Supervisor; GLM 5.3, DeepSeek V4 Pro e
Kimi K3 são endpoints open-weight do Unity AI Gateway anexados como tools.

## Genie Agents

Os seis Spaces são recursos declarativos em `resources/genie.yml`, com corpos serializados em
`genie_spaces/*.geniespace.json`. Não existe job para executar: o próprio `bundle deploy`
cria ou atualiza os recursos. Após o deploy, use `databricks bundle summary -t demo` para
obter os IDs do ambiente e valide uma pergunta PT e outra ES pela Conversation API. Consulte
`docs/genie_agents.md` para os IDs atuais, guardrails e integração managed MCP.

## Business semantics e dashboards

Gere os artefatos determinísticos antes do deploy e execute o gate semântico:

```bash
python3 dashboards/build_quilla_dashboards.py
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo \
  quilla_business_semantics --profile DEFAULT
```

O job recria as três Metric Views centrais e as sete de negócio, executa os probes de medidas
e depois todas as 27 consultas Lakeview. O próprio recurso DAB publica os quatro dashboards
com credenciais embutidas do Warehouse. Consulte `docs/business_semantics_dashboards.md`.

## Estado e evidência

- `TASKS.md`: checklist e IDs de execução;
- `DEMO.md`: narrativa e arquitetura funcional;
- `docs/raw_data_contract.md`: contrato raw;
- `docs/ml_data_contract.md` e `docs/ml_models.md`: ML e serving;
- `docs/ltap_ai_search.md`: LTAP e retrieval;
- `docs/governance_quality.md`: governança e qualidade;
- `docs/genai_governance.md`: prompts, traces e monitoring;
- `docs/genai_offline_evaluation.md`: datasets, Evaluation Runs e gates.
- `docs/genai_review_battery.md`: traces variados, filtros de UI e evidência de revisão.
- `docs/agent_foundation.md`: agentes, Agent Skills, memória, MCP e gates de promoção;
- `docs/genie_agents.md`: Genie Spaces PT/ES, IDs, probes e integração MCP.
- `docs/business_semantics_dashboards.md`: Metric Views, dashboards, identidade e evidência.
### Databricks App backend

O App, seu endpoint, Lakebase e Warehouse são declarados em `resources/app.yml`.
Depois do primeiro deploy, reconcilie as tabelas e os grants do role federado do App:

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext python3 setup/lakebase_bootstrap.py \
  --profile DEFAULT --project quilla-ltap --database quilla \
  --app-principal <service_principal_client_id-do-app>
```

O bootstrap é idempotente. O principal recebe somente conexão, uso do schema e
`SELECT`, `INSERT` e `UPDATE`; não recebe `DELETE`, `CREATE ROLE` ou superusuário.

O gate funcional oficial é o smoke direto contra a API publicada do App. Ele valida health,
readiness/Lakebase, chat, replay idempotente, histórico, arquivamento e feedback no mesmo caminho
que será consumido pelo frontend:

```bash
DATABRICKS_AUTH_STORAGE=plaintext python3 setup/validate_app_backend.py --profile DEFAULT
```

Não execute um Databricks Job para essa validação. O Job `quilla_custom_supervisor` termina no
deploy do ResponsesAgent; a experiência online é validada exclusivamente pela API do App, sem
provisionamento de compute e sem a política de retries do Jobs.
