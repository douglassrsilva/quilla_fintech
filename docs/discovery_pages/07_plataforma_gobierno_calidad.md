# Page: Confianza y calidad de datos

**Subdominio:** `Quilla Quilla/Plataforma, Gobierno y Calidad`

**Sinónimos:** Data Trust; Confianza de datos; Confiança de dados; Data Quality; Calidad de datos

**Descripción:** Evidencia gobernada de aceptación, cuarentena, reconciliación y aptitud de los datos para su uso.

## Definición oficial

La confianza de datos combina contratos, completitud, validez, unicidad,
consistencia, frescura, deduplicación y reconciliación. En la arquitectura
Quilla, cada dataset Bronze se reconcilia con registros Silver aceptados y
registros en cuarentena.

La tasa de aceptación es `silver_rows / bronze_rows`. Un dataset está
reconciliado cuando `bronze_rows = silver_rows + quarantine_rows`. Una alta tasa
de aceptación no demuestra por sí sola exactitud de negocio; deben considerarse
los contratos, linaje, observabilidad y finalidad del análisis.

Los datos que incumplen reglas críticas permanecen en cuarentena y no deben
entrar silenciosamente en decisiones, modelos o métricas Gold.

## Related assets

- `example_catalog.example_gold.metrics_data_quality`
- `example_catalog.example_gold.mv_data_quality_overview`
- Dashboard `[demo] Quilla | Risco, Crédito & Confiança`
- Quality Monitor de `example_catalog.example_silver.financial_events`

## Sources

- `example_catalog.example_governance.governance_validation_runs`
- `example_catalog.example_ai.production_monitoring_manifest`
- `example_catalog.example_ml.model_validation_evidence`

