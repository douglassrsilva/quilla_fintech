# Page: Volumen financiero bruto y volumen completado

**Subdominio:** `Quilla Quilla/Pagos y Tiempo Real`

**Sinónimos:** Volumen procesado; Volume processado; Gross Financial Volume; TPV; GMV

**Descripción:** Definición gobernada de los volúmenes financieros de Quilla y de su consolidación regional.

## Definición oficial

El volumen financiero bruto es la suma de `gross_volume_usd` para el alcance
seleccionado en `metrics_financial_performance`. Incluye todos los estados cuando
no se aplica un filtro de estado. El volumen completado requiere filtrar
`event_status = 'completed'`.

La consolidación entre países debe usar valores convertidos a USD. Los valores
locales solo pueden agregarse dentro de la misma `currency_code`. El volumen es
una métrica operativa sintética y no equivale a ingresos contables de Quilla.

## Related assets

- `example_catalog.example_gold.metrics_financial_performance`
- `example_catalog.example_gold.mv_financial_performance_daily`
- Genie Space `[demo] Quilla | Pagamentos & Performance`
- Dashboard `[demo] Quilla | Centro Executivo LATAM`

## Sources

- `example_catalog.example_silver.financial_events`

---

# Page: Tasa de finalización de pagos

**Subdominio:** `Quilla Quilla/Pagos y Tiempo Real`

**Sinónimos:** Tasa de aprobación; Taxa de conclusão; Completion rate; Approval rate

**Descripción:** Porcentaje de eventos financieros completados dentro del alcance analizado.

## Definición oficial

La tasa de finalización se calcula como `completed_events / total_events` dentro
del período y dimensiones seleccionados. Se almacena como razón entre 0 y 1 y se
presenta como porcentaje.

No debe confundirse con aprobación de crédito. Una reducción puede estar
relacionada con fraude, fallas operativas, reglas de autorización, liquidez,
canal o comportamiento del cliente; la métrica por sí sola no prueba causalidad.

## Related assets

- `example_catalog.example_gold.metrics_financial_performance`
- Genie Space `[demo] Quilla | Pagamentos & Performance`

## Sources

- `example_catalog.example_gold.mv_financial_performance_daily`

---

# Page: Evento financiero en tiempo real

**Subdominio:** `Quilla Quilla/Pagos y Tiempo Real`

**Sinónimos:** Evento realtime; Evento em tempo real; Real-time financial event; Zerobus event

**Descripción:** Evento financiero sintético incorporado por Zerobus y procesado incrementalmente por la arquitectura medallion.

## Definición oficial

Un evento financiero en tiempo real es un registro sintético emitido por el
simulador Zerobus. Se incorpora a la tabla Bronze canónica junto con el backfill
Parquet, pasa por contratos y deduplicación DQX en Silver y actualiza productos
Gold y métricas semánticas.

Tiempo real describe el transporte y procesamiento incremental de la demo; no
garantiza por sí solo un SLA empresarial. La frescura debe calcularse con las
marcas de tiempo de producción, ingestión y procesamiento.

## Related assets

- `example_catalog.example_bronze.zerobus_financial_events`
- `example_catalog.example_bronze.bronze_financial_events`
- `example_catalog.example_silver.financial_events`
- `example_catalog.example_gold.mv_financial_performance_daily`

## Sources

- Job `[demo] Quilla | Simulate Realtime Events`
- Pipeline `[demo] Quilla | 01 Bronze Auto Loader`
- Pipeline `[demo] Quilla | 02 Silver DQX`
- Pipeline `[demo] Quilla | 03 Gold Data Products`

