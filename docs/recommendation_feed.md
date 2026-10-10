# Feed governado de recomendações

## Objetivo

O Centro de Comando apresenta candidatos de decisão derivados de dados sintéticos governados,
com linhagem de dados, modelo candidato, investigação interna e enriquecimento externo. A tela
não bloqueia sua abertura aguardando IA generativa.

## Contrato

`GET /api/analytics/recommendations?country_code=REG&locale=pt-BR`

O contrato `decision-candidate-v2` devolve três candidatos:

1. crescimento e oferta, a partir de `quilla_ml.source_pricing_experiments`;
2. recuperação de pagamentos, a partir de `quilla_gold.mv_financial_performance_daily`;
3. revisão macro de risco e preços, a partir de `quilla_ml.fs_macro_country_daily`.

Cada item contém origem interna, impacto observado, modelo registrado e validado no Unity Catalog,
prompt do Genie e prompt externo para `system.ai.web_search`. `online_scoring=false` é
intencional: a aplicação não apresenta um Champion disponível como se uma inferência online
já tivesse sido executada.

Cada candidato também contém `explanation`, com sinais quantitativos, método de priorização,
fonte, SQL statement ID, timestamp observado, identidade e versão do modelo, alternativas de ação,
limitações e controles. A explicação descreve associação histórica, nunca causalidade ou SHAP
quando esse tipo de evidência não foi calculado.

As alternativas não são decorativas: o usuário deve selecionar `não agir`, `educar`, `proteger`
ou `ofertar` antes de aprovar, rejeitar ou adiar. O App envia a chave, o rótulo e a justificativa
da alternativa; o backend persiste esse snapshot no pedido, na evidência e no evento de outbox
do Lakebase. O scoring online de next-best-action só ocorre quando a alternativa `ofertar` é
explicitamente aprovada.

O endpoint `quilla-next-best-action` permanece aquecido porque participa de uma decisão humana
síncrona. Ao aprovar uma recomendação de crescimento, a resposta inclui a predição online,
latência e o snapshot sintético de features sem PII usado no scoring.

O cliente faz duas novas tentativas curtas somente para HTTP 502, 503 e 504. Durante uma
atualização ou falha transitória, a última leitura válida para o mesmo país e idioma continua
visível e pode ser atualizada manualmente.

## Jornada

- O seletor regional refaz o ranking para o país escolhido.
- “Entender e agir” abre a explicabilidade e a linhagem da recomendação.
- A escolha de alternativa habilita a decisão humana e permanece visível no comprovante.
- “Investigar no Genie” consulta dados internos governados.
- “Atualizar contexto externo” abre uma nova conversa no Quilla IQ e executa a pesquisa
  pública governada no idioma ativo.
- Políticas do Unity AI Gateway, fontes e revisão humana permanecem no caminho de decisão.

## Validação

```bash
cd frontend && npm run build
cd ..
uv run --with-requirements requirements-dev.txt python -m pytest tests -q
DATABRICKS_AUTH_STORAGE=plaintext databricks bundle validate -t demo --profile DEFAULT
DATABRICKS_AUTH_STORAGE=plaintext python3 setup/validate_app_analytics.py --profile DEFAULT
```

A última validação chama APIs diretamente; nenhum Job é criado para testar o App.
