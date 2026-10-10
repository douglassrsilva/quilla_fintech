# Governança, classificação e qualidade contínua

Atualizado em 2026-09-01.

Esta fase complementa os contratos DQX da Silver. DQX continua responsável por validação
determinística, deduplicação e quarentena; Data Quality Monitoring mede distribuição, drift,
atraso e mudanças por país ao longo do tempo.

## Data Classification

O job `quilla_governance_bootstrap` cria três governed tags e 23 especificações PT/ES:

- 12 identificadores nacionais/tributários: BR, MX, CO, AR, CL, PE, EC, GT, DO, CR, PA e UY;
- seis identificadores financeiros: cliente, conta, cartão, dispositivo, apólice e fatura;
- cinco contextos sensíveis: KYC/AML, crédito, fraude, consentimento e sinistro.

As amostras são sintéticas e ficam em
`example_catalog.example_governance.classifier_sample_values`. As descrições,
tags, valores e sample columns ficam em `custom_classifier_specs`.

Custom Classifiers está em Beta e sua API pública ainda não permite criação: a documentação
oficial exige `Manage custom classifiers` na interface de Data Classification. O bundle deixa
cada definição com estado `READY_FOR_CUSTOM_CLASSIFIER_UI`, pronta para criação sem decisões
adicionais. O classificador se aplica a todos os catálogos habilitados no metastore; não há
escopo por schema.

## Data Quality Monitoring

O monitor de `quilla_silver.financial_events` usa janelas diárias e slices independentes por
país, tipo e status. Além do perfil e drift nativos, publica três métricas de negócio:

- taxa de score fora de 0–1;
- taxa de chegada após cinco minutos;
- taxa de incompatibilidade país–moeda.

O monitor usa compute serverless e grava profile/drift em `quilla_monitoring`. O monitor não
substitui DQX e não modifica a tabela observada.

Para permitir um deploy realmente novo, o monitor não é criado durante o primeiro
`bundle deploy`: a tabela Silver ainda não existe nesse momento. O job idempotente
`quilla_governance_bootstrap`, executado depois de `quilla_medallion`, cria ou atualiza o
monitor usando o Warehouse e os schemas que o próprio DAB provisionou.

## Testes unitários nativos de Lakeflow Pipelines

O pipeline Silver usa canal `PREVIEW`, requisito da Beta. O arquivo
`pipelines/silver/tests/test_financial_events_quality.py` cobre aceitação, quarentena,
deduplicação e normalização de warning.

A execução é deliberadamente restrita ao Lakeflow Pipelines Editor pela própria plataforma.
No editor do pipeline Silver, adicionar o arquivo como **Test**, sem incluí-lo como source do
pipeline, e executar **Run tests in file**. Os testes usam somente tabelas por nome e mockam a
Bronze, preservando o isolamento `redirecting_<id>`; não executam Auto Loader nem acessam o
Volume raw.

## Fontes oficiais

Acesso em 2026-09-01:

- https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-classification-custom-classifiers
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring/
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring/anomaly-detection/
- https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-quality-monitoring/data-profiling/custom-metrics
- https://docs.databricks.com/aws/en/ldp/unit-testing
