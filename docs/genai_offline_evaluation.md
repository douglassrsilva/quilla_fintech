# Avaliação offline GenAI da Quilla

Atualizado em: 2026-09-01

## Objetivo

Transformar qualidade, segurança e aderência regulatória em gates mensuráveis antes da
promoção de qualquer Agent Version. O framework usa MLflow 3, Unity Catalog, traces e
Evaluation Runs, mantendo todo conteúdo explicitamente sintético.

## Recursos DAB

O job serverless `quilla_genai_offline_evaluation`, definido em
`resources/ai_evaluation.yml`, executa:

1. `build_evaluation_datasets`: cria ou atualiza os golden datasets;
2. `evaluate_baseline`: avalia 72 casos com seis controles determinísticos;
3. `evaluate_candidate`: avalia os mesmos 72 casos em paralelo com o baseline;
4. `evaluate_candidate_semantic`: avalia uma amostra estratificada com cinco juízes MLflow;
5. `validate_offline_evaluation`: compara resultados e persiste a decisão de promoção.

A task de setup pré-cria a tabela compartilhada de runs antes do fan-out. Isso impede uma
corrida de criação de tabela na primeira execução e mantém as três avaliações paralelas.

## Golden datasets

- `quilla_ai.quilla_genai_eval_gold`: 72 casos, seis jornadas em cada um dos 12 países;
- `quilla_ai.quilla_genai_eval_semantic`: 12 casos estratificados para juízes LLM;
- idiomas: PT-BR e ES-LATAM localizado;
- colunas lógicas: `inputs`, `expectations` e `tags`;
- fontes QREF são sintéticas, localizadas e marcadas como não oficiais.

## Evaluation Runs

### Comparação determinística

Baseline e candidate usam exatamente o mesmo golden dataset. Os scorers são:

- `offline_language_policy`;
- `offline_country_scope`;
- `offline_financial_disclaimer`;
- `offline_citation_policy`;
- `offline_fact_coverage`;
- `offline_sensitive_data_guardrail`.

O candidate precisa obter pelo menos `0,95` em todos os controles e melhorar ao menos
quatro controles em relação ao baseline.

### Juízes semânticos gerenciados

- `semantic_safety`;
- `semantic_guidelines`;
- `semantic_relevance_to_query`;
- `semantic_correctness`;
- `semantic_groundedness`;
- `semantic_retrieval_relevance`;
- `semantic_retrieval_sufficiency`.

Cada predição cria um trace com spans `AGENT`, `RETRIEVER` e `CHAT_MODEL`. O contexto de
retrieval segue o contrato reconhecido pelo MLflow e permite avaliar groundedness e
relevância sem fontes externas.

## Decisão de promoção

A tabela `quilla_ai.genai_offline_evaluation_validation_runs` registra a decisão final.
Uma falha determinística interrompe a validação. Variação do juiz semântico produz
`CONDITIONAL_REVIEW`; aprovação semântica produz `APPROVED`. A futura Agent Version somente
receberá alias de produção após este gate e a Review Queue humana.

## Execução reproduzível

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle validate -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_genai_offline_evaluation --no-wait --profile DEFAULT
```

Em falhas, publicar a correção e reparar o run original:

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks jobs repair-run <run-id> \
  --rerun-all-failed-tasks --no-wait --profile DEFAULT
```

## Evidência demo

- release: `2026-09-01-v1`;
- job: `<resource-id>`;
- run original: `<resource-id>`;
- repair: `<resource-id>`;
- estado final: `SUCCESS`;
- experimento: `<resource-id>`;
- baseline MLflow run: `75242112b7fa42af9df9c9f9ebb5579d`;
- candidate MLflow run: `d47c56cc2cce4bdba7eac2c70dc71e77`;
- semantic MLflow run: `146c2bc1128240c4a1f4f40c2cf9e497`;
- golden/semantic: `72 / 12` registros;
- controles melhorados: `5`;
- decisão: `APPROVED`.

## Relação com Prompts, Reviews e Agent Versions

- **Prompts:** as seis famílias gerenciadas existentes são a configuração versionada dos
  futuros agentes;
- **Reviews:** as filas de risco e regulação usarão traces e assessments gerados por este
  framework; sua criação permanece UI-only na Beta;
- **Agent Versions:** cada versão futura será associada ao Evaluation Run e somente promovida
  após gates automáticos e revisão humana.

## Fontes oficiais

Acesso em 2026-09-01:

- [Evaluate and monitor AI agents](https://docs.databricks.com/mlflow3/genai/eval-monitor/)
- [MLflow Tracing](https://docs.databricks.com/mlflow3/genai/tracing/)
- [MLflow Prompt Registry](https://docs.databricks.com/mlflow3/genai/prompt-version-mgmt/prompt-registry/)
