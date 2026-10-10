# AI Enrich, AI Prep Search e qualidade de retrieval

## Escopo

Esta fase usa as funções Beta `ai_prep_search` e `ai_enrich` sobre o corpus
demonstrativo, sintético e não oficial da Quilla. Nenhum resultado pode ser interpretado
como orientação jurídica ou regulatória.

`ai_prep_search` produz uma vitrine estratificada de chunks semanticamente enriquecidos.
O tamanho padrão é de cinco documentos para cada combinação de país, idioma e tema:
`12 × 6 × 5 = 360` documentos. O corpus principal de 25 mil documentos e seu índice não
são reduzidos ou substituídos. O parâmetro DAB permite ampliar a amostra quando necessário.

`ai_enrich` processa ao menos uma linha de cada um dos 72 segmentos e usa o índice
`regulatory_knowledge_hybrid_idx` como fonte de grounding. O resultado preserva o VARIANT
com valores, justificativas e fontes, além de colunas normalizadas para consumo pelo App.

## Avaliação reproduzível

O conjunto dourado contém duas consultas determinísticas por segmento — linguagem natural
e palavra-chave — totalizando 144 consultas PT/ES. Cada consulta é executada com:

1. ANN;
2. HYBRID;
3. FULL_TEXT (Beta);
4. HYBRID com `DatabricksReranker`.

Filtros obrigatórios de país e idioma impedem mistura de jurisdição. Os resultados e as
métricas DCG@k, NDCG@k, precision@k, topic recall@k, MRR, relevância média, percentual não
relevante e latência são persistidos no schema `quilla_ai`.

A avaliação nativa do AI Search também está habilitada, mas a documentação pública oferece
o início da execução somente pelo botão **Evaluate search quality** na página do índice.
Por isso, o DAB mantém uma avaliação programática equivalente e auditável; a avaliação
nativa pode ser iniciada na interface para gerar o dashboard comparativo gerenciado.

## Tabelas

- `regulatory_documents_ai_prepared`;
- `regulatory_chunks_ai_prepared`;
- `regulatory_knowledge_enriched`;
- `search_quality_queries`;
- `search_quality_query_results`;
- `search_quality_metrics`;
- `search_quality_validation_runs`.

## Execução

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy \
  -t demo --profile DEFAULT

DATABRICKS_AUTH_STORAGE=plaintext databricks bundle run \
  -t demo quilla_ai_quality --profile DEFAULT
```

Fontes oficiais consultadas em 2026-09-01:

- https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_prep_search
- https://docs.databricks.com/aws/en/sql/language-manual/functions/ai_enrich
- https://docs.databricks.com/aws/en/ai-search/retrieval-quality
- https://docs.databricks.com/aws/en/ai-search/retrieval-quality-eval
- https://docs.databricks.com/aws/en/ai-search/query-ai-search

## Evidência executada

O run DAB `<resource-id>` terminou em `SUCCESS`; o reparo final
`<resource-id>` reexecutou somente o benchmark após endurecer as paráfrases naturais.
Foram produzidos 360 chunks via AI Prep Search e 72 enriquecimentos com zero erros.

Nas consultas naturais em espanhol, HYBRID com reranker alcançou precision@10 `0,9227`,
topic recall@10 `1,0000` e MRR `0,9469`, com latência média de `502,61 ms`. FULL_TEXT
foi mais rápido (`96,83 ms`), mas obteve precision@10 `0,1667`. O resultado demonstra
que a estratégia deve ser escolhida por intenção e orçamento de latência, e não por uma
configuração única para todas as experiências.
