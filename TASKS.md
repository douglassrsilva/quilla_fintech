# Quilla — Plano de execução

Atualizado em: 2026-09-03

## Dados sintéticos raw

- [x] Definir mercados, idiomas, janela temporal e seed;
- [x] Aprovar catálogo, schema, Volume e volume total de eventos;
- [x] Definir contrato lógico e defeitos de qualidade intencionais;
- [x] Criar DAB com targets `smoke` e `demo`;
- [x] Criar notebooks parametrizados de setup, geração e validação;
- [x] Validar sintaxe local e configuração DAB;
- [x] Implantar e executar `smoke`;
- [x] Verificar contratos, contagens, FKs, locales e manifesto do `smoke`;
- [x] Implantar e executar `demo` com 13.758.648 eventos;
- [x] Verificar o snapshot final no Unity Catalog Volume.

### Operação de falhas

Autorização operacional registrada: nesta etapa, inspeções, correções, deployments,
execuções, cancelamentos necessários, reparos e validações podem prosseguir sem nova
confirmação. O snapshot raw concluído nunca deve ser regenerado.

Após corrigir e fazer deploy de uma falha, usar `databricks jobs repair-run` no run
original. Não iniciar uma nova execução completa: as tasks concluídas devem ser preservadas
e somente a task com falha e suas dependentes devem ser reprocessadas.

## Lakeflow SDP e qualidade

- [x] Criar schemas Bronze, Silver e Gold governados no Unity Catalog;
- [x] Definir 17 Streaming Tables Bronze com Auto Loader;
- [x] Versionar contratos Silver por dataset;
- [x] Aplicar DQX 0.16 com regras de erro e warning;
- [x] Criar quarentenas e deduplicação determinística;
- [x] Criar cinco Materialized Views Gold;
- [x] Criar três Metric Views governadas;
- [x] Orquestrar o fluxo em DAB sem dependência da geração raw;
- [x] Confirmar execução e reconciliação final no target `demo` (`run <resource-id>`, `SUCCESS`).

## Zerobus realtime

- [x] Definir tabela Delta de ingestão direta governada no Unity Catalog;
- [x] Criar bootstrap idempotente de identidade M2M, Secret Scope e privilégios mínimos;
- [x] Criar simulador PT/ES para cinco cenários e 12 países;
- [x] Convergir Auto Loader e Zerobus na Bronze canônica;
- [x] Orquestrar Zerobus, Bronze, Silver, Gold e Metric Views via DAB;
- [x] Confirmar ingestão Zerobus e propagação até Gold no target `demo` (`run <resource-id>`, `PASSED`).

### Evidência realtime

- Cenário: `mixed`;
- Eventos esperados/Zerobus/Bronze: `1.000 / 1.000 / 1.000`;
- Silver aceita: `963`;
- Quarentena DQX intencional: `37`;
- Silver + quarentena reconciliados: `1.000`;
- Evidência persistida em `quilla_gold.realtime_demo_validation_runs`.

## MLflow, Feature Store e Model Serving

- [x] Definir dez datasets de outcome e cenário em Volume separado;
- [x] Implementar geração PT/ES, contratos e validações anti-leakage;
- [x] Publicar fontes Delta e Feature Tables governadas no Unity Catalog;
- [x] Definir 22 modelos orientados a receita, risco, crescimento, CX e wealth;
- [x] Implementar tracking MLflow, slices por país, Traces e aliases Champion/Challenger;
- [x] Orquestrar dados, 22 treinos paralelos e serving em DAB serverless com reparo por modelo;
- [x] Validar dados e Feature Store em smoke (`run <resource-id>`, reparo `<resource-id>`, `SUCCESS`);
- [x] Treinar e validar os 22 modelos em smoke (`run <resource-id>`, `SUCCESS`);
- [x] Executar dados e Feature Store no target demo (`run <resource-id>`, `SUCCESS`);
- [x] Treinar 22 modelos em paralelo e validar Champions/Traces no target demo (`run <resource-id>`, `SUCCESS`);
- [x] Publicar seis endpoints Model Serving com AI Gateway inference tables (`run <resource-id>`, reparo `<resource-id>`, `SUCCESS`);

## Próximas fases

- [x] Concluir e validar a camada LTAP com Lakebase Autoscaling;
- [x] Concluir a base PT/ES, Lakebase Search, AI Search híbrido e reranking;
- [x] Aplicar AI Enrich e AI Prep Search ao conhecimento e dados da demo;
- [x] Criar avaliação de qualidade do AI Search por idioma, país e intenção;
- [x] Criar governed tags, amostras sintéticas e 23 especificações PT/ES para classificadores PII financeiros dos 12 países;
- [ ] Criar os 23 Custom Classifiers na interface Beta, que ainda não expõe API pública de criação;
- [x] Adicionar Data Quality Monitoring com profile, drift, slices e checks customizados sem substituir contratos DQX;
- [x] Versionar prompts PT/ES/país no Managed MLflow Prompt Registry;
- [x] Criar traces governados em Unity Catalog e Production Monitoring no MLflow;
- [ ] Criar duas Custom Trace Views e duas Review Queues pela interface Beta, usando as definições publicadas em `quilla_ai.genai_ui_asset_specs`;
- [x] Criar golden datasets PT/ES e Evaluation Runs comparativos no MLflow;
- [x] Aplicar cinco juízes semânticos e gate reproduzível de promoção;
- [x] Criar bateria rica com 216 traces PT/ES variados para Sessions, Scorers e Review Queues;
- [x] Definir supervisor e seis especialistas PT/ES, com contratos e Agent Skills versionadas;
- [x] Registrar e finalizar as três skills de inteligência externa como Governed Skills nativas no Unity AI Gateway;
- [x] Provisionar sete Managed Memory Stores e validar entries/search/Conversation API;
- [x] Promover e validar a memória conversacional transacional do Supervisor no Lakebase
  (sessões, mensagens, runs, restauração por `conversation_id` e APIs read-only do App);
- [x] Criar seis Genie Spaces PT/ES por domínio, implantá-los por DAB e validar a Conversation API;
- [x] Migrar os seis Genie Spaces para as novas Metric Views Gold e remover fontes ML do Space Growth & Wealth;
- [ ] Conectar os seis Genie Spaces ao supervisor pelo managed MCP no runtime do App;
- [x] Criar sete Metric Views adicionais para Growth, pricing, wealth, suitability, macro stress, casos e DQ;
- [x] Criar e publicar quatro dashboards Lakeview com identidade Quilla e validar seus 27 datasets;
- [ ] Implementar o runtime único do App com MLflow ResponsesAgent, OpenAI Agents SDK e MCPs governados;
- [ ] Configurar uma sessão demonstrativa Omnigent em Databricks Sandbox usando as skills versionadas;
- [ ] Registrar e promover Agent Versions somente após evaluation gate e Review Queue;
- [ ] Usar Databricks Sandbox para simulações e execução isolada de ações geradas;
- [ ] Expor Predictive AI Functions para risco, liquidez, receita e stress macro;
- [ ] Criar experiências Genie Code/One com web search, citações e guardrails;
- [x] Versionar quatro unit tests nativos para o pipeline Silver e configurar o canal `PREVIEW`;
- [ ] Executar `Run tests in file` no Lakeflow Pipelines Editor, única superfície suportada pela Beta;
- [x] Integrar o Quilla IQ ao BFF real com SSE, histórico Lakebase, fontes e feedback por trace.

### Evidência LTAP e AI Search

- Lakebase Autoscaling: projeto `quilla-ltap`, banco `quilla`, schema `quilla_app`, 11 tabelas;
- Conhecimento UC/Lakebase Search: 25.000 documentos PT/ES, 12 países, zero documentos oficiais;
- AI Search: endpoint `quilla-ai-search`, índice Delta Sync `regulatory_knowledge_hybrid_idx`;
- Índice: `HYBRID`, Qwen3 multilingual embeddings, writeback table e 25.000/25.000 chunks;
- Retenção da fonte: `delta.deletedFileRetentionDuration = interval 30 days`;
- Retrieval: filtros por país/idioma e `DatabricksReranker` validados;
- Execução demo: run original `<resource-id>`, reparo final `<resource-id>`, `SUCCESS`.

### Evidência da memória conversacional

- Supervisor: endpoint `quilla-custom-supervisor`, model version `13`, release
  `2026-09-03-custom-supervisor-v10-lakebase-memory`;
- Run original `<resource-id>`, reparo final `<resource-id>`, `SUCCESS`;
- Gate: quatro turnos PT/ES, mesma sessão Lakebase, 8 mensagens, 4 agent runs e 4 traces;
- Sessão validada: `<resource-id>`;
- API do App: listagem `200`, recuperação `200` e tentativa cross-user `404`;
- Regressão local: 25 testes `OK`; bundle demo `Validation OK`.

### Evidência AI Enrich, AI Prep Search e qualidade

- AI Prep Search: 360 documentos estratificados e 360 chunks embedding-ready;
- AI Enrich: 72 linhas, cobertura de 12 países/6 temas, grounding no índice e zero erros;
- Benchmark: 144 consultas PT/ES, 576 buscas e quatro estratégias;
- Consultas naturais ES: HYBRID + reranker obteve precision@10 `0,9227`, topic recall@10
  `1,0000`, MRR `0,9469` e latência média `502,61 ms`;
- FULL_TEXT em consultas naturais ES priorizou latência (`96,83 ms`) com menor precision@10
  (`0,1667`), evidenciando o trade-off esperado;
- Consultas por palavra-chave: todas as estratégias obtiveram precision@10 `1,0000`;
- Run original `<resource-id>`, reparo final `<resource-id>`, `SUCCESS`.

### Evidência de governança e qualidade contínua

- Governed tags: `quilla_national_identifier`, `quilla_financial_identifier` e
  `quilla_sensitive_context`;
- Custom Classifiers preparados: 23, sendo 12 específicos por país, seis financeiros e cinco
  contextuais, todos com descrição PT/ES e sample column sintética;
- Data Classification: oito schemas no escopo, preservando a configuração anterior do catálogo;
- Data Quality Monitor: `quilla_silver.financial_events`, status `ACTIVE`, janelas diárias,
  slices por país/tipo/status e três métricas customizadas;
- Outputs: `quilla_monitoring.financial_events_profile_metrics`,
  `financial_events_drift_metrics` e dashboard `<resource-id>`;
- Refresh `<resource-id>`: `SUCCESS`;
- Pipeline Silver no canal `PREVIEW` e quatro testes nativos editor-ready;
- Run original `<resource-id>`, reparo `<resource-id>`, `SUCCESS`.

### Evidência de governança GenAI

- Managed Prompt Registry: seis famílias PT/ES, aliases `champion` e `production`;
- MLflow Experiment `<resource-id>` com traces armazenados no Unity Catalog;
- Traces: 72, cobrindo 12 países e seis jornadas;
- Production Monitoring ativo por DAB com 13 scorers: seis determinísticos, cinco Built-in
  Judges single-turn e dois multi-turn; controles críticos avaliados em 100%;
- Especificações UI-only: duas Custom Trace Views e duas Review Queues publicadas em
  `quilla_ai.genai_ui_asset_specs`;
- Run original `<resource-id>`, repair `<resource-id>`, `SUCCESS`;
- Validação SQL `<resource-id>`: `PASSED`.

### Evidência de avaliação offline GenAI

- Golden datasets UC: 72 casos completos e 12 casos semânticos, PT/ES e 12 países;
- Evaluation Runs: baseline, candidate e semantic; os IDs são gerados no workspace de destino
  e registrados nas tabelas de evidência do próprio deploy;
- Candidate melhorou cinco controles e recebeu decisão `APPROVED`;
- Run original `<resource-id>`, repair `<resource-id>`, `SUCCESS`.

### Evidência da bateria rica de revisão GenAI

- Release `2026-09-01-rich-v1`: 216 traces canônicos, 12 países/locales, seis jornadas e três variantes;
- Distribuição: 72 `approved`, 72 `human_review` e 72 `control_failure`;
- Seis filas lógicas, respostas localizadas e spans de agente, prompt, retrieval, tool,
  guardrail e modelo;
- Job `<resource-id>`, run `<resource-id>`, repair final `<resource-id>`, `SUCCESS`;
- Validação `PASSED` e statement `<resource-id>`, `SUCCEEDED`;
- Manifesto e validação no schema `quilla_ai`; detalhes em `docs/genai_review_battery.md`.

### Evidência da fundação multiagente

- Release candidata `2026-09-02-foundation-v1`: um supervisor e seis especialistas PT/ES;
- Sete Agent Skills versionadas e dez contratos de ferramentas;
- Sete Managed Memory Stores governados no schema `quilla_ai`, um por domínio;
- Probes de create/get/search/delete: `7/7`; Conversation API: `CREATE_GET_DELETE_PASSED`;
- Agent Version registrada: `false`; elegível para promoção: `false`, aguardando Review Queue;
- Job `<resource-id>`, run original `<resource-id>`, repair final `<resource-id>`, `SUCCESS`;
- Validação SQL `<resource-id>`, `SUCCEEDED`;
- Contratos e limites Beta em `docs/agent_foundation.md`.

### Evidência dos Genie Agents

- Seis Spaces DAB: Executivo, Pagamentos, Risco, Crédito, Cliente e Growth & Wealth;
- Warehouse serverless `<warehouse-id>` e fontes governadas no Unity Catalog;
- 24 perguntas iniciais bilíngues e guardrails de moeda, data, PII, percentuais e aconselhamento;
- Oito probes funcionais com SQL gerado e respostas PT/ES, incluindo Growth e Wealth sobre Metric Views;
- Growth & Wealth sem fontes `source_*` ou `fs_*`; cinco Metric Views Gold confirmadas no workspace;
- Fronteira realtime validada: `MAX(event_date) = 2026-09-01`, posterior ao snapshot de 2026-08-31;
- IDs, fontes e reprodução em `docs/genie_agents.md`.

### Evidência de Business Semantics e dashboards

- Dez Metric Views governadas no schema `quilla_gold`, sendo três centrais e sete adicionais;
- quatro dashboards Lakeview publicados, com 43 widgets e paleta Quilla PT/ES;
- 27 consultas de datasets validadas no Warehouse serverless `<warehouse-id>`;
- job `quilla_business_semantics` `<resource-id>`;
- run original `<resource-id>`, reparado no próprio histórico e finalizado `SUCCESS`;
- IDs, URLs e reprodução em `docs/business_semantics_dashboards.md`.

## Backend do Databricks App

### Jornadas Ultimate App

- [x] Empresas consumindo KPIs, tendência e recusas diretamente de Metric Views Gold;
- [x] Recomendações com explicabilidade, ação humana, scoring online e ledger Lakebase;
- [x] Control Room Zerobus com start/status pela API e evidência Medallion;
- [x] SSE com aceite imediato, heartbeat, etapa observável e tempo decorrido PT/ES;
- [x] Observabilidade de MLflow, Serving, DQX, Zerobus, Lakeview e lineage;
- [x] Quatro dashboards Lakeview publicados acessíveis pelo App;
- [x] Testes locais dos novos contratos e validador remoto direto, sem Job validador.
- [x] Isolar o renderer do Genie Research com validação de schema, valores nulos e Error Boundary;
- [x] Regressão `npm run test:genie` cobrindo troca de índice métrico e células ausentes.

- [x] Implementar BFF com identidade confiável/OBO, SSE, heartbeat, cancelamento e timeout de 45s;
- [x] Implementar Lakebase assíncrono, paginação, arquivamento, idempotência e feedback por trace;
- [x] Publicar o App com bindings DAB para Supervisor, Lakebase e SQL Warehouse;
- [x] Validar 33 testes locais;
- [x] Validar a jornada completa diretamente pela API real do App;
- [x] Remover do Job o validador pós-deploy do ResponsesAgent.
- [x] Conectar o frontend PT/ES ao chat SSE e ao ciclo de vida das conversas;
- [x] Publicar a experiência responsiva com seleção dos 12 países.

### Gate oficial do backend

Validação direta, sem Databricks Job:
`DATABRICKS_AUTH_STORAGE=plaintext python3 setup/validate_app_backend.py --profile DEFAULT`.

Último smoke aprovado: live, readiness/Lakebase, chat, replay idempotente, histórico,
arquivamento e feedback com HTTP `200`. Trace:
`<resource-id>`.
