# Page: Alerta confirmada y falso positivo

**Subdominio:** `Quilla Quilla/Riesgo, Fraude y Cumplimiento`

**Sinónimos:** Alerta confirmada; Falso positivo; Confirmed alert; False positive; Disposição de alerta

**Descripción:** Resultado gobernado de la investigación de una señal de riesgo o fraude.

## Definición oficial

Una alerta es una señal de priorización, no una prueba de fraude, lavado de
dinero u otro ilícito. Una alerta confirmada es aquella cuya investigación
registró evidencia suficiente según el proceso de la demo. Un falso positivo es
una alerta investigada que no confirmó la hipótesis señalada.

La tasa de confirmación es `confirmed_alerts / total_alerts`; la tasa de falsos
positivos es `false_positive_alerts / total_alerts`. Ambas se presentan como
porcentajes y deben segmentarse por país, tipo, severidad y período. Ningún score
debe provocar bloqueo automático sin política, explicabilidad y revisión.

## Related assets

- `example_catalog.example_gold.metrics_risk_operations`
- `example_catalog.example_gold.metrics_case_outcomes`
- Modelo `example_catalog.example_ml.quilla_transaction_fraud`
- Modelo `example_catalog.example_ml.quilla_alert_confirmation`
- Dashboard `[demo] Quilla | Risco, Crédito & Confiança`
- Genie Space `[demo] Quilla | Risco & Fraude`

## Sources

- `example_catalog.example_gold.mv_risk_operations_daily`
- `example_catalog.example_gold.vw_case_outcomes_daily`

