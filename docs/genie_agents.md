# Genie Agents da Quilla

Atualizado em: 2026-09-02

## Resultado

Seis Genie Spaces bilíngues foram declarados no DAB, implantados no target `demo` e
validados pela Conversation API com perguntas reais em português e espanhol. Todos usam o
SQL Warehouse serverless `<warehouse-id>` e dados governados pelo Unity Catalog.

| Space | ID `demo` | Fontes principais |
|---|---|---|
| Executivo & Regional | `<resource-id>` | Dez Metric Views e cinco Gold MVs |
| Pagamentos & Performance | `<resource-id>` | Finanças, DQ e série Gold |
| Risco & Fraude | `<resource-id>` | Risco, casos, DQ e Customer 360 |
| Crédito Responsável | `<resource-id>` | Crédito, pricing, macro stress e série Gold |
| Cliente & Operações | `<resource-id>` | Growth, DQ e Customer 360 |
| Growth & Wealth | `<resource-id>` | Cinco Metric Views Gold do domínio |

Regulação não foi duplicada em um Genie estruturado. O agente regulatório usa AI Search
híbrido, filtros por país/idioma e reranker, que são a superfície correta para documentos,
políticas e citações.

## Guardrails

- responder no idioma da pergunta, em PT-BR ou ES-LATAM;
- identificar todo conteúdo como sintético e demonstrativo;
- distinguir o snapshot até 2026-08-31 dos eventos Zerobus posteriores;
- derivar a data máxima da própria fonte consultada;
- consolidar países somente com valores em USD;
- formatar razões como percentuais e pontos-base corretamente;
- impedir exposição de identificadores e linhas individuais;
- não transformar scores em prova de fraude ou decisão adversa;
- não produzir aconselhamento financeiro, jurídico ou regulatório;
- declarar ausência de dados em vez de estimar ou inventar valores.

## Evidência funcional

Os seis probes produziram SQL executável e resposta no idioma solicitado. Foram exercitados
volume regional em USD, tendência de pagamentos, alertas críticos, aprovação de crédito,
reconciliação de qualidade e perfis de investimento.

A validação temporal confirmou `MAX(event_date) = 2026-09-01`, classificada corretamente
como evento Zerobus posterior ao snapshot. O probe final de crédito apresentou `63,25%`, e
não `0,6325`, e incluiu o disclaimer obrigatório.

Após a ampliação da camada semântica, os seis Spaces foram atualizados e suas fontes
efetivas reconfirmadas no workspace. Growth & Wealth não consulta mais `source_*` nem
Feature Tables: usa exclusivamente `metrics_growth_customer`,
`metrics_pricing_profitability`, `metrics_wealth_engagement`,
`metrics_investor_suitability` e `metrics_macro_stress`.

Os probes adicionais validaram:

- PT: conversão e churn por país em `metrics_growth_customer`, conversa
  `<resource-id>`;
- ES: volume e aceite de recomendações por classe de ativo em
  `metrics_wealth_engagement`, conversa `<resource-id>`.

## Reprodução

As definições ficam em `genie_spaces/*.geniespace.json`; o recurso DAB fica em
`resources/genie.yml`.

```bash
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle validate \
  -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle deploy \
  -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle summary \
  -t demo --profile DEFAULT
```

O JSON externo do Genie não resolve variáveis DAB nesta versão do CLI. Por isso os
identificadores UC de três níveis materializam o catálogo aprovado
`example_catalog`; warehouse e parent path continuam parametrizados pelo
bundle.

## Integração seguinte

O runtime do Quilla IQ deverá consumir cada Space pelo managed MCP
`/api/2.0/mcp/genie/{space_id}`. O supervisor selecionará o domínio e aplicará contexto de
identidade, país e finalidade antes da chamada. O Space Growth & Wealth já usa suas Metric
Views Gold próprias. Feature Tables permanecem isoladas para treinamento e
inferência; a experiência analítica executiva usa a camada Gold semântica.
