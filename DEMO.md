# Quilla — Financial Intelligence

## Propósito

Aplicação de referência para demonstrar uma fintech latino-americana orientada por dados,
IA e decisões em tempo real. A Quilla é fictícia e todo dado produzido por este projeto é
sintético, rastreável e impróprio para qualquer decisão financeira real.

## Experiência e marca

- Idiomas nativos: português do Brasil e espanhol latino-americano localizado;
- Tom: claro, confiante, humano e tecnologicamente sofisticado;
- Cores principais: índigo `#5865F2`, teal `#20D3BC` e coral `#FF7968`;
- Temas claro e escuro e layout responsivo;
- A interface deve sempre identificar explicitamente o ambiente de demonstração.

## Mercados

Brasil, México, Colômbia, Argentina, Chile, Peru, Equador, Guatemala, República
Dominicana, Costa Rica, Panamá e Uruguai. Brasil usa `pt-BR`; os demais mercados usam
espanhol com locale local quando disponível e fallback `es`.

## Casos de uso da demonstração

1. Visão financeira regional e LTAP;
2. Pagamentos e movimentação de dinheiro em tempo real;
3. Risco, fraude, AML e qualidade de dados;
4. Crédito, recebíveis e seguros;
5. Atendimento e inteligência conversacional PT/ES;
6. Governança, residência de dados e políticas por país;
7. IA governada e recomendações explicáveis.

## Arquitetura-alvo conceitual

O landing sintético em Parquet e o Zerobus alimentam uma Bronze canônica nos pipelines
Lakeflow SDP serverless. Unity Catalog é o plano de governança. Auto Loader cobre o
histórico; Zerobus recebe eventos novos diretamente em Delta; Silver aplica contratos DQX,
quarentena e deduplicação; Gold publica Materialized Views e Metric Views. Lakebase compõe
o plano LTAP para estado operacional do App, enquanto MLflow, Model Serving, AI Gateway,
Lakebase Search, AI Search híbrido e reranking formam a camada de inteligência governada.

## Contrato da camada raw

- Catálogo: `example_catalog`;
- Schema: `quilla_raw`;
- Volume: `synthetic_landing`;
- Formato: Parquet;
- Snapshot final: `snapshot=2026-08-31`;
- Janela de eventos: 2024-09-01 a 2026-08-31;
- Seed: 42;
- Identificadores são tokens sintéticos, e-mails usam `example.test` e não há PAN, CPF
  ou documento nacional válido;
- Registros de documentos e políticas são rotulados como conteúdo demonstrativo e não
  reproduzem textos regulatórios oficiais.

O contrato detalhado está em `docs/raw_data_contract.md`.

## Medallion implementado

- Bronze: schema `quilla_bronze`, com uma Streaming Table por dataset raw;
- Silver: schema `quilla_silver`, com dados aceitos e tabelas `<dataset>_quarantine`;
- Gold: schema `quilla_gold`, com MVs de finanças, risco, crédito, Customer 360 e qualidade;
- Semântica: dez Metric Views governadas para finanças, risco, crédito, Growth, pricing,
  wealth, suitability, macro stress, outcomes de casos e qualidade;
- Contratos: `contracts/silver_contracts.yml`;
- Realtime: produtor Zerobus PT/ES com cenários normal, fraude, cross-border e DQ;
- Orquestração reproduzível: jobs DAB `quilla_medallion` e `simulate_realtime_events`.

## Business semantics e Lakeview

Quatro dashboards Lakeview publicados formam a camada executiva da demo: Centro Executivo
LATAM, Growth & Profitabilidade, Wealth & Macro Stress e Risco, Crédito & Confiança. Eles
consomem somente ativos governados no Unity Catalog e um SQL Warehouse serverless.

A identidade usa índigo `#5865F2`, teal `#20D3BC`, coral `#FF7968` e tons complementares;
títulos e rótulos são PT-BR/ES-LATAM, e todos os dados permanecem explicitamente sintéticos.
O job `quilla_business_semantics` recria as dez Metric Views e executa 27 probes de dataset
antes de considerar a entrega válida. Detalhes em `docs/business_semantics_dashboards.md`.

## Princípios WAF aplicados

- Segurança e governança: Unity Catalog, namespace de três níveis, Volume UC e identidade
  M2M com privilégios mínimos;
- Confiabilidade: geração determinística, manifesto, contagens esperadas, validação de FKs,
  confirmação durável Zerobus e reconciliação end-to-end;
- Excelência operacional: DAB, notebooks parametrizados, retries, timeouts e tags;
- Eficiência de performance: serverless, particionamento controlado e geração Spark;
- Otimização de custos: target `smoke` antes da carga completa e ausência de clusters ociosos;
- Sustentabilidade: snapshots reproduzíveis e execução sob demanda.

## ML e decisões inteligentes

O schema `quilla_ml` reúne dez fontes de outcomes e cenários, Feature Tables governadas,
evidências de treinamento e 22 modelos registrados no Unity Catalog. Os 22 treinos são tasks
independentes e paralelas. MLflow captura métricas, slices por país, assinaturas, aliases
`Champion`/`Challenger` e Traces de inferência. Seis Champions são publicados em Model
Serving com inference tables governadas pelo AI Gateway.

Os jobs DAB são separados para preservar tarefas concluídas em reparos:

1. `quilla_ml_data`: geração, contrato e Feature Store;
2. `quilla_ml_train`: treinamento e validação dos 22 modelos;
3. `quilla_ml_serving`: publicação dos seis endpoints realtime.

Contratos e catálogo de decisões estão em `docs/ml_data_contract.md` e `docs/ml_models.md`.

## LTAP e conhecimento regulatório demonstrativo

O projeto Lakebase Autoscaling `quilla-ltap` mantém estado de aplicação, memória de
conversas, decisões, casos, auditoria e cache. O schema UC `quilla_ai` cura os 25.000
documentos Silver em PT/ES e os publica no endpoint `quilla-ai-search`, em um índice Delta Sync `HYBRID`, usando embeddings
Qwen3 multilíngues e `DatabricksReranker`. Em paralelo, um espelho PostgreSQL com GIN e
`tsvector` oferece busca textual de baixa latência ao futuro App.

Nenhum documento é oficial: todos preservam `is_official=false`, `synthetic_flag=true` e
disclaimer explícito. Detalhes em `docs/ltap_ai_search.md`.

## Enriquecimento e qualidade do AI Search

AI Prep Search cria chunks semanticamente preparados em uma amostra estratificada e
parametrizável, sem alterar o corpus principal. AI Enrich adiciona metadados operacionais,
justificativas e fontes com grounding no índice governado. Um benchmark PT/ES de 144
consultas cobre os 12 países e compara ANN, híbrida, full-text e híbrida com reranker,
persistindo métricas e latência no Unity Catalog. Detalhes em
`docs/ai_enrichment_quality.md`.

## Governança e qualidade contínua

O schema `quilla_governance` publica amostras estritamente sintéticas, três governed tags e
23 especificações PT/ES de Custom Classifiers: 12 identificadores nacionais, seis
identificadores financeiros e cinco contextos regulatórios. Data Classification cobre os
schemas raw, Bronze, Silver, Gold, ML, AI e governança sem remover o escopo já existente no
catálogo.

Data Quality Monitoring observa `quilla_silver.financial_events` em janelas diárias e slices
por país, tipo e status. O monitor está ativo, produz tabelas de profile/drift em
`quilla_monitoring`, um dashboard gerenciado e métricas de score inválido, atraso de chegada
e incompatibilidade país–moeda. DQX permanece como controle determinístico inline.

O pipeline Silver usa canal `PREVIEW` e possui quatro testes unitários nativos editor-ready.
Custom Classifiers e testes nativos são Betas com criação/execução restritas à interface web;
os artefatos e instruções estão em `docs/governance_quality.md`.

## Governança GenAI

O Managed MLflow Prompt Registry versiona seis famílias de prompts governados, nativos em
PT-BR e ES-LATAM, com aliases `champion` e `production`. Um experimento separado armazena
traces no Unity Catalog e cobre seis jornadas em cada um dos 12 países, com spans de prompt,
retrieval e resposta.

MLflow Production Monitoring combina seis scorers determinísticos com sete Built-in Judges
para segurança, relevância, guidelines Quilla, grounding, retrieval e qualidade multi-turn.
Custom Trace Views e Review Queues permanecem UI-only; suas definições bilíngues são
publicadas como dados governados e documentadas em `docs/genai_governance.md`.

Evidência demo: experimento `<resource-id>`, 13 scorers ativos e run
`<resource-id>`, com todas as quatro tarefas em `SUCCESS`.

## Avaliação offline e promoção de agentes

Dois MLflow Evaluation Datasets governados cobrem 72 casos PT/ES e uma amostra semântica de
12 casos. Evaluation Runs comparam baseline e candidate usando seis controles determinísticos;
o fluxo agora define sete juízes gerenciados para safety, guidelines, relevância à pergunta,
correctness, groundedness, relevância e suficiência do retrieval. A evidência histórica da
release `2026-09-01-v1`, anterior a essa ampliação, melhorou cinco controles e recebeu decisão
`APPROVED`; uma nova execução materializa as duas métricas adicionais.

Esse gate será obrigatório para futuras Agent Versions, seguido de Review Queue humana antes
da promoção. Detalhes em `docs/genai_offline_evaluation.md` e o procedimento integral de
reconstrução está em `docs/reproduction_runbook.md`.

## Bateria rica de revisão humana

Uma release separada adiciona 216 cenários PT/ES realmente distintos para demonstrar MLflow
Sessions, Scorers, Custom Trace Views e Review Queues: 12 países, seis jornadas e três
variantes (`approved`, `human_review` e `control_failure`). Entradas, respostas, tokens,
valores, fontes e spans variam por cenário; 72 casos contêm falhas controladas conhecidas.

Use o filtro `Tag / battery_release / = / 2026-09-01-rich-v1`. O job DAB
`quilla_genai_review_battery` e as evidências estão descritos em
`docs/genai_review_battery.md`.

## Fundação multiagente governada

A release candidata `2026-09-02-foundation-v1` define o Quilla IQ Supervisor e seis
especialistas: atendimento, fraude/risco, crédito responsável, investimentos/suitability,
regulação e next-best-action. Sete Agent Skills PT/ES e dez contratos de ferramentas ficam
versionados e publicados no Unity Catalog.

Cada domínio possui seu próprio Managed Memory Store. O scope privado deriva de identidade,
tenant e país autenticados; o modelo nunca escolhe store ou scope. O job DAB executa as sete
criações em paralelo e valida entries, busca eventual e Conversation API. Agent Versions
continuam bloqueadas até a conclusão da Review Queue humana.

O runtime seguinte será um único Databricks App com MLflow ResponsesAgent e OpenAI Agents SDK,
com os especialistas in-process e ferramentas via Model Serving, Lakebase e managed MCP. O
Omnigent em Sandbox será uma superfície de desenvolvimento/demonstração, não o supervisor
transacional. Detalhes em `docs/agent_foundation.md`.

## Genie Agents governados

Seis Genie Spaces bilíngues foram implantados declarativamente pelo DAB e validados pela
Conversation API: Executivo & Regional, Pagamentos & Performance, Risco & Fraude, Crédito
Responsável, Cliente & Operações e Growth & Wealth. São 24 perguntas iniciais PT/ES com
guardrails de país, moeda, data, PII, percentuais e aconselhamento.

Os seis Spaces usam Gold e Metric Views. Growth & Wealth consulta exclusivamente as cinco
Metric Views específicas de Growth, pricing, wealth, suitability e macro stress, sem expor
Feature Tables ou outcomes de treinamento na camada executiva. O runtime do Quilla IQ
consumirá os seis via managed MCP. IDs e evidências estão em `docs/genie_agents.md`.
