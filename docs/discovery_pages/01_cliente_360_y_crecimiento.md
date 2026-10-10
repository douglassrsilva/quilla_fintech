# Page: Cliente 360

**Subdominio:** `Quilla Quilla/Cliente 360 y Crecimiento`

**Sinónimos:** Vista 360 del cliente; Visão 360 do cliente; Customer 360; C360

**Descripción:** Vista gobernada que reúne relación, actividad, saldos, riesgo y atención de un cliente sintético.

## Definición oficial

Cliente 360 es la representación analítica consolidada de una persona o empresa
sintética dentro de Quilla. Integra país, idioma, segmento, estado KYC, banda de
riesgo, cuentas, saldos, actividad financiera, alertas e interacciones.

La vista no es un sistema maestro de identidad ni autoriza decisiones adversas.
Los identificadores y datos de contacto no deben exponerse en análisis agregados
o conversaciones sin finalidad autorizada. Las cantidades `active_parties` y
`active_accounts` son distintas dentro de cada grupo y no deben sumarse como si
fueran únicas globalmente.

## Related assets

- `example_catalog.example_gold.mv_customer_360`
- `example_catalog.example_gold.metrics_growth_customer`
- Genie Space `[demo] Quilla | Cliente & Operações`
- Dashboard `[demo] Quilla | Growth & Profitabilidade`

## Sources

- `example_catalog.example_silver.parties`
- `example_catalog.example_silver.accounts`
- `example_catalog.example_silver.financial_events`
- `example_catalog.example_silver.risk_alerts`
- `example_catalog.example_silver.interactions`

---

# Page: Próxima mejor acción

**Subdominio:** `Quilla Quilla/Cliente 360 y Crecimiento`

**Sinónimos:** Próxima melhor ação; Next Best Action; NBA; Recomendación contextual

**Descripción:** Recomendación explicable y gobernada de la acción comercial o de servicio más adecuada para un contexto de cliente.

## Definición oficial

La próxima mejor acción es una recomendación priorizada a partir del perfil,
comportamiento, relación, riesgo, elegibilidad, contexto regional y señales
externas verificables. Puede proponer una oferta, contenido educativo, atención,
retención, protección o no realizar ninguna acción.

La recomendación debe mostrar objetivo, señales utilizadas, factores a favor y
en contra, nivel de confianza, alternativas, restricciones y resultado esperado.
No aprueba crédito, no ejecuta inversiones, no modifica límites y no contacta al
cliente automáticamente. Las acciones sensibles exigen revisión humana.

## Related assets

- Modelo `example_catalog.example_ml.quilla_next_best_action`
- `example_catalog.example_gold.mv_customer_360`
- `example_catalog.example_gold.metrics_growth_customer`
- App `quilla-demo`

## Sources

- `example_catalog.example_ml.fs_customer_monthly`
- `example_catalog.example_ml.fs_interaction_context`
- `example_catalog.example_gold.vw_growth_customer_monthly`

