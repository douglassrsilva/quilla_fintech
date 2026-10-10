# Quilla Custom Multi-Agent Supervisor

## Objetivo

O runtime em `quilla_supervisor/` substitui o Agent Bricks MAS como orquestrador
principal. O endpoint gerenciado anterior permanece somente como fallback operacional.
O contrato público é MLflow `ResponsesAgent`, portanto o consumidor não depende do
framework interno de orquestração.

## Fluxo de uma solicitação

1. valida o contexto confiável (`user_id`, tenant, país e locale);
2. envia toda inferência por Model Services do Unity AI Gateway, com `block_jailbreak` em `pre_call`;
3. preserva o domínio em perguntas de continuidade explícita;
4. seleciona um especialista e, quando necessário, especialistas de apoio;
5. descobre ferramentas MCP em runtime;
6. remove ferramentas mutáveis antes de apresentá-las ao modelo;
7. executa GLM 5.3 ou Kimi K3 por serviços UC governados, dependendo do tamanho do contexto;
8. usa DeepSeek V4 Pro como verificador independente em casos externos, regulatórios ou de alto impacto;
9. registra spans MLflow e devolve rota, modelos, ferramentas, avisos e `trace_id` em `custom_outputs`;
10. restaura o histórico da sessão no Lakebase antes da inferência;
11. persiste de forma transacional as mensagens, a rota, os modelos, as ferramentas, as
    evidências, o `trace_id` e o resultado da execução;
12. exige revisão humana para crédito, fraude, investimento e decisões regulatórias.

Políticas de conteúdo não são codificadas no runtime. O código local valida somente
contexto/limites, isola tenant e país, controla a allowlist de ferramentas e impede
operações mutáveis. Detecção de jailbreak é uma Service Policy governada e auditável no
Unity AI Gateway. As policies reconciliadas são:

- `block_jailbreak` em requests;
- `block_unsafe_content` em requests e responses;
- custom PII output policy em responses, evitando vazamento sem bloquear a entrada financeira legítima;
- `block_hallucination` em responses;
- custom policy de marca que impede menções a concorrentes na resposta;
- custom policy de retenção responsável que impede recomendar o encerramento da conta,
  mas não bloqueia instruções neutras quando o cliente solicita exercer esse direito.

As três custom policies são SQL UDFs UC (`VARIANT -> VARIANT`) e retornam decisões
`ALLOW`/`DENY`; portanto também são objetos governados, auditáveis e reproduzíveis.
O handler built-in `system.ai.block_pii` não está disponível neste workspace; por isso PII
é implementado como Service Policy custom no Gateway, e não como regex no runtime do agente.

Os Model Services reconciliados pelo DAB são:

- `quilla-guard-judge`;
- `quilla-supervisor-primary`;
- `quilla-supervisor-verifier`;
- `quilla-supervisor-long-context`.

## MCPs e Skills

- Web Search: `/ai-gateway/mcp-services/system.ai.web_search`;
- You.com Finance fallback: `/ai-gateway/mcp-services/example_catalog.example_ai.you_financial_mcp`;
- Governed Skills: `/ai-gateway/skills/?schema=<catalog>.<ai_schema>`;
- AI Search regulatório: índice `regulatory_knowledge_hybrid_idx`;
- Genie: seis spaces declarados em `resources/genie.yml`.

O cliente implementa MCP Streamable HTTP. No servidor de Governed Skills, somente as
skills Quilla e as operações `list/load/get` são expostas. Operações administrativas
`create/update/delete` são ocultadas e também bloqueadas no guardrail de execução.

O Web Search usa `QUILLA_WEB_SEARCH_TIMEOUT_SECONDS=45`. Esse limite cobre somente a
execução do MCP. O App usa um teto de transporte de 180 segundos para acomodar o caminho
degradado de pesquisa financeira longa, além de roteamento,
busca, síntese e verificação, mantendo heartbeats SSE a cada cinco segundos. Esse teto não
é um retry nem uma meta de latência: o SLO conversacional continua abaixo de 30 segundos.
Não há retry longo dentro da mesma interação; uma falha é devolvida como lacuna explícita.

O MCP `you_financial_mcp` é um fallback pago, invisível ao modelo e acionado somente quando o
Web Search primário falha por timeout, transporte, HTTP 429 ou HTTP 5xx. O envelope nominal
`isError=false` também é classificado como falha quando carrega uma mensagem curta de
indisponibilidade, como `Web search timed out`. O supervisor sempre
força `research_effort=deep`; `exhaustive` não é permitido. Os limites fail-closed são uma
chamada por solicitação, duas por conversa, seis por dia e dez no total da demo, persistidos
atomicamente em `quilla_app.external_tool_budget` e auditados em
`quilla_app.external_tool_usage`. Como o MCP não informa custo monetário por chamada, a
proteção é expressa em chamadas e não apresenta uma estimativa fictícia em dólares.

## Memória conversacional no Lakebase

O ledger de conversas usa Lakebase Autoscaling (`quilla-ltap`, branch `production`, banco
`quilla`) como hot state da experiência LTAP. O escopo de cada sessão é derivado do contexto
confiável e nunca do texto do prompt:

`tenant_id + actor_id + conversation_id`

O runtime gera um `conversation_id` quando o consumidor não o envia e o devolve em
`custom_outputs`. Ao reutilizar esse ID, o agente recupera o histórico antes de processar o
novo turno. Com `QUILLA_MEMORY_REQUIRED=true`, falhas de leitura ou escrita são explícitas e
impedem uma resposta que aparentaria ter sido persistida.

As tabelas principais são:

- `quilla_app.app_sessions`: identidade externa, tenant, ator, título, país, locale e soft delete;
- `quilla_app.app_messages`: sequência idempotente, papel, conteúdo e metadados por mensagem;
- `quilla_app.agent_runs`: rota, modelos, ferramentas, evidências, latência, status e trace MLflow.

O App pode recuperar somente as conversas do usuário autenticado por:

- `GET /api/conversations`;
- `GET /api/conversations/{session_id}`.

Uma sessão pertencente a outro `tenant_id` ou `actor_id` retorna `404`, evitando confirmar a
existência do recurso. O bootstrap e as migrações permanecem idempotentes em
`setup/lakebase_bootstrap.py`.

## Registro e deploy reproduzível

```bash
databricks bundle validate -t demo --profile DEFAULT
databricks bundle deploy -t demo --profile DEFAULT
databricks bundle run -t demo quilla_custom_supervisor --profile DEFAULT
databricks bundle run -t demo quilla_ai_gateway_policies --profile DEFAULT
```

O job reconcilia os quatro Model Services e suas policies, registra o modelo UC com
autenticação OBO e scopes mínimos permitidos pelo Agent Serving. Durante o deployment, o job
resolve o hostname não secreto do Lakebase e o injeta no endpoint; em runtime, a conexão TLS
usa diretamente o token OBO do usuário como credencial PostgreSQL. Isso evita chamar as APIs
de controle `postgres`, cujo scope ainda não é aceito pelo `UserAuthPolicy`, e mantém a
identidade individual no banco. O job então atualiza idempotentemente o endpoint
`quilla-custom-supervisor`, executa quatro turnos PT/ES com o mesmo `conversation_id` e persiste evidências em
`quilla_ai.custom_supervisor_validation_runs`.

O gate exige `custom_outputs.memory.status == PERSISTED` em todos os turnos. A validação
direta do contrato Lakebase e a validação read-only do App podem ser repetidas com:

```bash
DATABRICKS_AUTH_STORAGE=plaintext uv run --with 'psycopg[binary]==3.2.9' \
  python setup/validate_conversation_memory.py --profile DEFAULT
python setup/validate_app_conversation_api.py
```

O teste de regressão começa com a pergunta real sobre as maiores altas da bolsa brasileira
no dia anterior. O gate exige rota `external_intelligence`, execução bem-sucedida do MCP
Web Search e ao menos uma URL verificável na resposta.

O exemplo usado pelo `mlflow.pyfunc.log_model` ativa `registration_validation`, que
valida o contrato sem consumir LLMs nem ferramentas durante o registro.

## Limites desta fase

- o frontend Quilla IQ consome o BFF por SSE, permite cancelamento e exibe rota, fontes e estado;
- o histórico Lakebase pode ser listado e retomado no App, com arquivamento e feedback por trace;
- a memória operacional é o ledger Lakebase; Managed Memory continua reservado à memória
  semântica dos agentes, sem substituir o histórico transacional;
- DSPy fica reservado para otimização offline, nunca como dependência obrigatória do caminho online.
