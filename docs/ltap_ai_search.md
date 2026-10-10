# LTAP, Lakebase Search e AI Search

## Escopo

Esta fase combina o histórico governado do Lakehouse com estado transacional de baixa
latência. O corpus contém somente documentos sintéticos PT/ES e nunca deve ser apresentado
como legislação, orientação jurídica ou conteúdo oficial.

## Componentes

- Lakebase Autoscaling: projeto `quilla-ltap`, branch `production`, endpoint `primary`,
  banco `quilla` e schema PostgreSQL `quilla_app`;
- LTAP: sessões e mensagens, execuções de agentes/MLflow Trace IDs, solicitações e ações de
  decisão, casos/work items, auditoria/cache de busca e outbox de eventos;
- Lakebase Search: espelho de 25.000 documentos e índice PostgreSQL multilíngue
  `tsvector`/GIN, com configurações `portuguese` e `spanish`;
- Unity Catalog: schema `quilla_ai`, documentos curados, chunks e evidências de validação;
- AI Search: endpoint `quilla-ai-search` e índice Delta Sync
  `regulatory_knowledge_hybrid_idx`, subtipo `HYBRID`, com embeddings gerenciados
  persistidos na writeback table do índice;
- Embeddings: `databricks-qwen3-embedding-0-6b` para recuperação multilíngue;
- Reranking: `DatabricksReranker` sobre `chunk_text`, com filtros por país e idioma.

O Lakebase serve o hot state do futuro Databricks App. O Lakehouse permanece como sistema
analítico e histórico governado. O mesmo documento mantém `is_official=false`,
`synthetic_flag=true`, `knowledge_scope=synthetic_demo_only` e disclaimer explícito nas
duas superfícies.

A fonte Delta `quilla_ai.regulatory_chunks` mantém
`delta.deletedFileRetentionDuration = interval 30 days`. A janela cobre a cadência
sob demanda do índice `TRIGGERED` e evita falhas caso o intervalo entre sincronizações
ultrapasse os sete dias padrão.

## Reprodutibilidade

Lakebase Autoscaling ainda não é um recurso declarativo do DAB. O bootstrap idempotente
versionado cobre essa lacuna; todos os schemas, notebooks e jobs de dados são implantados
pelo bundle.

```bash
DATABRICKS_AUTH_STORAGE=plaintext uv run --with 'psycopg[binary]==3.2.9' \
  python setup/lakebase_bootstrap.py --profile DEFAULT

DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy \
  -t demo --profile DEFAULT

DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_ltap_ai_search --profile DEFAULT
```

O job executa:

1. curadoria e chunking no Unity Catalog;
2. em paralelo, sincronização do Lakebase Search e criação/sync do índice híbrido;
3. consulta híbrida reranqueada e reconciliação entre UC, Lakebase e Vector Search;
4. persistência da evidência em `quilla_ai.search_validation_runs`.

Em caso de falha, faça o deploy da correção e repare o run original com
`--rerun-all-failed-tasks --rerun-dependent-tasks`; não refaça tasks concluídas.

## Próxima camada de agentes

Omnigent será o meta-harness gerenciado para compor agentes e alternar/combinar harnesses
como Codex e Claude sem duplicar prompts, skills e políticas. As sessões executadas em
Databricks Sandbox usarão obrigatoriamente modelos pelo Unity AI Gateway. O Gateway será o
plano único para permissões UC, model/MCP services, políticas de serviço, rate limits,
budgets, traffic splitting/fallback, inference tables e observabilidade de custo/uso.

Fontes oficiais consultadas em 2026-09-01:

- https://docs.databricks.com/aws/en/omnigent/
- https://docs.databricks.com/aws/en/ai-gateway/
