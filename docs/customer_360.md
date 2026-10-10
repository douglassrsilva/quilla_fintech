# Customer 360 governado

## Fonte e contrato

A visão Pessoas consome diretamente:

`example_catalog.example_gold.mv_customer_360`

O backend expõe `GET /api/analytics/customers`, autenticado pelo Databricks App. A resposta
contém somente clientes sintéticos verificados e com conta, sem e-mail, telefone, documentos
ou identificadores de conta.

No escopo regional, a API seleciona um perfil por país. Ao escolher um país no topo do App,
ela devolve os perfis daquele mercado. O seletor usa `party_id` existente na Gold; personas
inventadas no frontend não são permitidas.

## Experiência

- saldo atual e disponível na moeda local;
- volume histórico em USD e quantidade de contas;
- segmento, KYC, faixa e sinal médio de risco;
- alertas, sentimento e liquidez disponível;
- datas reais de onboarding, último evento e última interação;
- investigações de comportamento, risco e próxima melhor ação.

## Genie individual seguro

O Space Cliente & Operações permanece agregado por padrão. Uma análise individual só é
permitida quando o App envia exatamente um `party_id` sintético completo. O Genie deve usar
esse identificador como filtro exato, não procurar por nome, não listar outras pessoas e não
expor contatos, documentos ou IDs de conta.

Sinais de risco e sentimento nunca justificam isoladamente uma decisão. Próximas ações são
hipóteses sujeitas a elegibilidade, políticas do Unity AI Gateway e revisão humana.
