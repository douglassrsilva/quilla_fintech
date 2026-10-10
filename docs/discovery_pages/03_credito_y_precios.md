# Page: Probabilidad de incumplimiento — PD

**Subdominio:** `Quilla Quilla/Crédito y Precios`

**Sinónimos:** Probabilidad de default; Probabilidade de inadimplência; Probability of Default; PD

**Descripción:** Probabilidad estimada de que una exposición de crédito incumpla durante un horizonte definido.

## Definición oficial

PD es una estimación probabilística de incumplimiento para un horizonte y una
definición de default explícitos. Debe interpretarse junto con la fecha del
modelo, población, país, producto, condiciones macroeconómicas y calibración.

Una PD alta no constituye prueba de incumplimiento ni puede ser la única base de
una decisión adversa. Toda decisión individual requiere política local,
explicabilidad, controles de sesgo y revisión humana.

## Related assets

- Modelo `example_catalog.example_ml.quilla_credit_pd`
- `example_catalog.example_gold.metrics_credit_portfolio`
- Genie Space `[demo] Quilla | Crédito Responsável`

## Sources

- `example_catalog.example_ml.fs_credit_performance_monthly`
- `example_catalog.example_gold.mv_credit_portfolio_monthly`

---

# Page: Pérdida esperada — PD, LGD y EAD

**Subdominio:** `Quilla Quilla/Crédito y Precios`

**Sinónimos:** Perda esperada; Expected Loss; EL; PD × LGD × EAD

**Descripción:** Estimación de pérdida crediticia derivada de probabilidad de incumplimiento, severidad y exposición.

## Definición oficial

La pérdida esperada se expresa conceptualmente como `PD × LGD × EAD`, usando
horizontes, moneda, fecha de referencia y escenarios compatibles. PD representa
probabilidad de incumplimiento; LGD, la fracción de pérdida condicionada al
incumplimiento; EAD, la exposición esperada en ese momento.

El resultado es una estimación bajo supuestos y no una pérdida observada. Para
comparar escenarios de inflación, tasas, energía o crisis económica deben
documentarse shocks, dependencias y fecha de los datos macroeconómicos.

## Related assets

- Modelo `example_catalog.example_ml.quilla_credit_pd`
- Modelo `example_catalog.example_ml.quilla_credit_lgd`
- Modelo `example_catalog.example_ml.quilla_credit_ead`
- Modelo `example_catalog.example_ml.quilla_portfolio_stress_loss`

## Sources

- `example_catalog.example_ml.fs_credit_performance_monthly`
- `example_catalog.example_gold.metrics_macro_stress`

---

# Page: Precio y crédito responsable

**Subdominio:** `Quilla Quilla/Crédito y Precios`

**Sinónimos:** Precificación responsável; Responsible pricing; APR responsable; Risk-based pricing

**Descripción:** Principios para definir ofertas de crédito sostenibles, explicables y compatibles con el riesgo y la jurisdicción.

## Definición oficial

El precio responsable combina costo de fondeo, pérdida esperada, costos de
servicio, plazo, límites, margen y restricciones locales sin explotar la
vulnerabilidad del cliente. APR y comisiones almacenadas en puntos base deben
convertirse correctamente: 100 bps equivalen a un punto porcentual.

La aceptación estimada y el margen no sustituyen capacidad de pago, idoneidad,
equidad ni requisitos regulatorios. No debe suponerse que una oferta es válida
de la misma forma en los doce países.

## Related assets

- `example_catalog.example_gold.metrics_pricing_profitability`
- Modelo `example_catalog.example_ml.quilla_pricing_acceptance`
- Dashboard `[demo] Quilla | Growth & Profitabilidade`

## Sources

- `example_catalog.example_gold.vw_pricing_performance_daily`
- `example_catalog.example_ml.fs_pricing_offer`

