# Bateria rica de revisão GenAI

Atualizado em: 2026-09-01

## Objetivo

Esta bateria substitui os seeds técnicos repetitivos como base da demonstração visual de
MLflow Tracing, Sessions, Scorers, Custom Trace Views e Review Queues. Todo o conteúdo é
sintético e identificado como `DEMO_ONLY`; nenhuma resposta deve orientar uma decisão real.

## Cobertura

A release `2026-09-01-rich-v1` contém 216 cenários canônicos:

- 12 países e 12 locales, com PT-BR no Brasil e espanhol localizado nos demais mercados;
- seis jornadas: atendimento, fraude/risco, crédito responsável, suitability, regulação e
  next-best-action;
- três variantes por país e jornada: `approved`, `human_review` e `control_failure`;
- 72 casos aprováveis, 72 para revisão humana e 72 falhas intencionais de controle;
- entradas distintas por cidade, moeda, valor, cenário, merchant token e customer token;
- respostas localizadas e spans `AGENT`, `PROMPT`, `RETRIEVER`, `TOOL`, `GUARDRAIL` e
  `CHAT_MODEL`;
- sessões separadas por país e variante para navegação conversacional.

As falhas controladas cobrem idioma e identificador sintético, escopo de país, disclaimer
financeiro e citações regulatórias. O resultado esperado fica registrado nas tags e no
manifesto, permitindo comparar o julgamento humano com o controle automatizado.

## Filtrar na interface

No experimento `/Users/user@example.com/quilla-genai-demo`, use:

1. Field: `Tag`;
2. Key: `battery_release`;
3. Operator: `=`;
4. Value: `2026-09-01-rich-v1`.

Para a fila de risco, acrescente `use_case = fraud_risk_explanation`. Outros filtros úteis são
`country_code`, `scenario_variant`, `review_queue`, `expected_priority`,
`expected_decision` e `expected_quality`. Na UI, informe apenas o nome da tag, sem o prefixo
`tags.`.

## Recursos reproduzíveis

- job DAB: `quilla_genai_review_battery`;
- notebooks: `48_seed_rich_review_traces.py` e `49_validate_rich_review_traces.py`;
- manifesto: `example_catalog.example_ai.genai_review_trace_manifest`;
- evidência: `example_catalog.example_ai.genai_review_battery_validation_runs`.

O gerador é idempotente. Em um repair, ele normaliza e reconcilia as URIs do manifesto com os
IDs persistidos no Unity Catalog, remove somente referências órfãs e recria apenas os cenários ausentes. A
validação cruza o manifesto canônico com a tabela governada de trace metadata no Unity
Catalog, evitando falsos negativos causados por atraso da API genérica de busca.

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle validate -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run -t demo \
  quilla_genai_review_battery --profile DEFAULT
```

Em caso de falha, repare o run original; não dispare a bateria completa novamente.

## Evidência demo

- job: `<resource-id>`;
- run original: `<resource-id>`;
- repairs: `<resource-id>`, `<resource-id>` e `<resource-id>` (final);
- estado final: `SUCCESS`;
- release: `2026-09-01-rich-v1`;
- validação UC: `PASSED`, 216 traces canônicos, 12 países/locales, seis jornadas, três
  variantes, seis filas lógicas e 72 falhas intencionais;
- statement de evidência: `<resource-id>`, `SUCCEEDED`.

O backend físico mantém 217 traces sob a tag da release: o cenário
`UY/next_best_action/control_failure` possui uma cópia histórica que chegou após a
reconciliação. O manifesto seleciona exatamente uma versão canônica e preserva a outra como
evidência de recuperação; as contagens de validação e promoção usam sempre o manifesto.
