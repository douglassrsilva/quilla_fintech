# Fundação multiagente governada da Quilla

Atualizado em: 2026-09-02

## Resultado desta fase

A release `2026-09-02-open-weight-v2` define um supervisor e sete especialistas nativos em
PT-BR/ES-LATAM: atendimento, fraude/risco, crédito responsável, investimentos/suitability,
regulação, next-best-action e inteligência externa. Cada domínio possui um Managed Memory Store separado no Unity
Catalog e uma Agent Skill versionada.

Esta fase é propositalmente anterior ao registro de Agent Versions. O gate de avaliação
offline está aprovado, mas a Review Queue humana continua pendente; portanto
`agent_version_registered=false` e `promotion_eligible=false` são controles verificáveis.

## Arquitetura correta

- Runtime futuro: Databricks App com MLflow `ResponsesAgent` e OpenAI Agents SDK;
- modelos exclusivamente open-weight no Unity AI Gateway:
  `databricks-glm-5-3` para síntese, `databricks-deepseek-v4-pro-0813` para
  verificação crítica e `databricks-kimi-k3` para contexto longo;
- Ferramentas: Model Serving, Lakebase e managed MCP de AI Search;
- Memória: Managed Memory e Conversation API, governadas pelo Unity Catalog;
- Desenvolvimento/demonstração: Omnigent em Databricks Sandbox;
- Observabilidade: MLflow Tracing, Evaluation e Production Monitoring já existentes.

Omnigent é um meta-harness para sessões de desenvolvimento. Ele não substitui o supervisor
transacional do App. As skills ensinam o comportamento; os managed MCP servers fornecem as
ferramentas governadas. As três skills de inteligência externa também são securables nativos
`catalog.schema.skill` no Unity Catalog, além de permanecerem versionadas no repositório.

## Isolamento de memória

O scope nunca é aceito do prompt ou escolhido pelo modelo. Para memória privada ele é derivado
de `user_id + tenant_id + country_code` verificados. A memória regulatória compartilhada usa
`tenant_id + country_code + role` e não admite PII. A service principal continua tecnicamente
capaz de acessar os scopes concedidos; seus privilégios devem permanecer mínimos.

O workspace atual demonstra isolamento lógico em um ambiente sintético. Residência física e
transferências internacionais de dados exigirão workspaces/controles regionais por grupo de
países antes de uso real.

## Ativos reproduzíveis

- Job DAB: `quilla_agent_foundation`;
- contratos: `quilla_ai.agent_registry`, `agent_skill_registry`, `agent_tool_registry`;
- gate: `quilla_ai.agent_release_manifest`;
- evidência: `quilla_ai.agent_foundation_validation_runs`;
- skills: `agent_skills/*/SKILL.md`;
- Memory Stores: oito securables `example_catalog.example_ai.quilla_*_memory`.

O job publica contratos, cria os oito stores em paralelo e executa probes efêmeros de
create/search/delete, além de create/get/delete na Conversation API.

## Quilla External Intelligence

- Supervisor Agent: `<resource-id>`;
- endpoint: `<legacy-supervisor-endpoint>`, tarefa `agent/v1/responses`;
- tools: Databricks Web Search, GLM 5.3, DeepSeek V4 Pro e Kimi K3;
- bootstrap idempotente: `setup/external_intelligence_bootstrap.py`;
- skills: pesquisa externa, verificação de fontes e análise de cenários.

Em 2026-09-02, as três skills foram publicadas e finalizadas em
`example_catalog.example_ai` pelo catálogo nativo de Governed Skills do Unity
AI Gateway. O tipo `skill` ainda não pode ser anexado como tool deste backend específico de
Supervisor; por isso, as mesmas políticas também permanecem incorporadas às instruções do
Supervisor. Registro governado e anexação ao Supervisor são capacidades distintas.

| Governed Skill | ID |
|---|---|
| `quilla-external-intelligence` | `<resource-id>` |
| `quilla-source-verification` | `<resource-id>` |
| `quilla-impact-scenario-analysis` | `<resource-id>` |

O job DAB `<resource-id>` comprovou a reconciliação idempotente no run
`<resource-id>` (`SUCCESS`), preservando os três IDs e atualizando os bundles finalizados.

O endpoint MAS também recusou `put-ai-gateway` para inference table, apesar do help genérico
indicar suporte a agent endpoints. As chamadas aos três modelos passam por endpoints
Unity AI Gateway já ativos; a observabilidade do Supervisor permanece no experimento MLflow
`<resource-id>` até esse suporte ser liberado para MAS.

## Reprodução

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle validate -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_agent_foundation --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_governed_skills --profile DEFAULT
python3 setup/external_intelligence_bootstrap.py --profile DEFAULT
python3 setup/query_external_intelligence.py --profile DEFAULT
python3 setup/test_external_intelligence_conversation.py --profile DEFAULT
```

### Prova conversacional de 2026-09-02

O teste de três perguntas consecutivas foi concluído com `PASSED` no endpoint
`<legacy-supervisor-endpoint>`, preservando 18 itens de histórico:

- turno 1: `resp_70d951e027174819a2cbdb0c328431ba`, duas aprovações Web Search;
- turno 2: `resp_c1f4c196dc604f578e5f0544d03354c4`, nenhuma nova busca e contexto preservado;
- turno 3: `resp_2b47a78fa0a34e0d818adbb1c04a3a35`, duas novas buscas para México e Chile.

O cliente deve intercalar cada `mcp_approval_response` imediatamente após o respectivo
`mcp_approval_request`. Enviar aprovações em bloco ou usar apenas `previous_response_id`
produziu os erros `unexpected position` e `approval response ID does not match`.

Continuidade técnica foi aprovada. Qualidade factual ainda exige o gate de Source Verification:
o terceiro turno alternou indevidamente para espanhol e algumas alegações usaram páginas
institucionais genéricas em vez do documento primário específico.

Em falha, corrija, faça deploy e repare o run original. Não registre nem promova uma Agent
Version antes de concluir a Review Queue humana.

## Limites Beta

Managed Memory e Omnigent estão em Beta; Managed MCP está em Public Preview. Omnigent é aberto
na rota `/omnigent` e o host Sandbox é selecionado pela interface. Essa sessão interativa não
é um recurso declarativo DAB; os contratos, skills e dependências usados por ela permanecem
versionados e reproduzíveis no projeto.

## Fontes oficiais consultadas em 2026-09-02

- https://docs.databricks.com/aws/en/agents/custom-agents/author-agent
- https://docs.databricks.com/aws/en/agents/agent-memory/managed-memory
- https://docs.databricks.com/aws/en/agents/agent-memory/memory-store-api
- https://docs.databricks.com/aws/en/agents/mcp-tools/managed-mcp
- https://docs.databricks.com/aws/en/ai-gateway/govern-skills
- https://docs.databricks.com/aws/en/agents/uc-skills/create-share-uc-skills
- https://docs.databricks.com/aws/en/omnigent/quickstart
- https://docs.databricks.com/aws/en/agent-skills/
