# Guía de despliegue reproducible de Quilla

Esta guía permite crear Quilla en otro workspace sin reutilizar nombres, URLs,
IDs ni credenciales del ambiente original. Los datos generados son sintéticos.

## 1. Qué crea el bundle

El Databricks Asset Bundle administra los siguientes recursos:

- catálogo de Unity Catalog y schemas Raw, Bronze, Silver, Gold, ML, AI,
  Governance y Monitoring;
- volúmenes administrados para el landing Parquet y los datasets de ML;
- SQL Warehouse serverless;
- pipelines Lakeflow Declarative Pipelines para Bronze, Silver/DQX y Gold;
- jobs de generación, MLflow, Feature Store, GenAI, gobierno y realtime;
- endpoint de AI Search, índices, Genie Spaces y dashboards AI/BI;
- proyecto, branch, endpoint, role y database de Lakebase Autoscaling;
- Databricks App con sus resource bindings;
- artefactos del supervisor, Model Serving y Unity AI Gateway mediante jobs
  idempotentes versionados en el mismo bundle.

Los IDs de Warehouse, App, jobs, dashboards y Genie Spaces se obtienen de
referencias `${resources...}` durante el deploy. No deben copiarse desde otro
workspace.

## 2. Prerrequisitos del workspace

Use un workspace con Unity Catalog y serverless habilitados. El perfil completo
de la demo también requiere acceso a Databricks Apps, Lakebase Autoscaling,
AI Search, Genie, Model Serving, Unity AI Gateway y los modelos Foundation
Model API configurados en `databricks.yml`.

Algunas capacidades son Beta o Preview y pueden requerir habilitación por
workspace o región. El bundle no puede conceder un entitlement que la cuenta no
posee. Esto es un prerrequisito de plataforma, no una dependencia del ambiente
original.

La identidad que despliega necesita permiso para crear catálogo, Warehouse,
jobs, pipelines, Apps, endpoints de AI Search/Model Serving y recursos
Lakebase. Para Discovery Hub también necesita administrar governed tags.

Instale localmente:

- Databricks CLI 1.19.0 o posterior;
- Python 3.11 o posterior;
- `uv` para inicializar el esquema PostgreSQL de Lakebase;
- Git. Node.js solo es necesario si desea recompilar el frontend.

## 3. Autenticación y clonación

```bash
databricks auth login --profile MI_PERFIL
git clone https://github.com/douglassrsilva/quilla_fintech.git
cd quilla_fintech
```

Compruebe la identidad antes de crear recursos:

```bash
databricks current-user me --profile MI_PERFIL
```

## 4. Nombres portátiles y sobrescrituras

No es obligatorio crear `.env.local`. Los targets incluyen nombres genéricos y
separados:

| Recurso | `smoke` | `demo` |
|---|---|---|
| Catálogo | `quilla_fintech_smoke` | `quilla_fintech_demo` |
| Lakebase project | `quilla-ltap-smoke` | `quilla-ltap-demo` |
| AI Search endpoint | `quilla-ai-search-smoke` | `quilla-ai-search-demo` |

En un workspace compartido, copie el ejemplo y defina un prefijo propio para
evitar colisiones:

```bash
cp .env.example .env.local
```

Ejemplo:

```bash
DATABRICKS_BUNDLE_VAR_catalog=equipo_a_quilla_demo
DATABRICKS_BUNDLE_VAR_lakebase_project=equipo-a-quilla-ltap
DATABRICKS_BUNDLE_VAR_vector_search_endpoint=equipo-a-quilla-ai-search
```

`.env.local` está ignorado por Git. Nunca almacene tokens, secrets ni URLs
privadas en el repositorio.

## 5. Validación sin crear recursos

El bootstrap carga `.env.local`, renderiza SQL/dashboards/Genie y valida el DAB:

```bash
python3 setup/bootstrap_portable.py \
  --profile MI_PERFIL \
  --target smoke \
  --validate-only
```

Los archivos renderizados quedan en `.generated/` y no se publican en Git,
porque contienen los nombres elegidos para el workspace de destino.

## 6. Deploy de infraestructura

Para una prueba pequeña:

```bash
python3 setup/bootstrap_portable.py \
  --profile MI_PERFIL \
  --target smoke
```

Para la demo integral:

```bash
python3 setup/bootstrap_portable.py \
  --profile MI_PERFIL \
  --target demo
```

El comando ejecuta, en orden: autenticación, render, `bundle validate`,
`bundle deploy`, inicialización idempotente del esquema LTAP y
`bundle summary`.

También puede ejecutar cada paso manualmente:

```bash
python3 setup/render_bundle_assets.py --target demo
databricks bundle validate -t demo --profile MI_PERFIL
databricks bundle deploy -t demo --profile MI_PERFIL
uv run --with 'psycopg[binary]==3.2.9' \
  python setup/lakebase_bootstrap.py \
  --profile MI_PERFIL --project quilla-ltap-demo --database quilla
```

## 7. Orden recomendado de materialización

Después del deploy, ejecute los workloads por dependencia:

```bash
# Datos sintéticos raw
databricks bundle run -t demo synthetic_raw_data --profile MI_PERFIL

# Bronze Auto Loader, Silver DQX, Gold y Metric Views
databricks bundle run -t demo quilla_medallion --profile MI_PERFIL

# Feature Store y modelos
databricks bundle run -t demo quilla_ml_data --profile MI_PERFIL
databricks bundle run -t demo quilla_ml_train --profile MI_PERFIL
databricks bundle run -t demo quilla_ml_serving --profile MI_PERFIL

# Semántica, dashboards y búsqueda
databricks bundle run -t demo quilla_business_semantics --profile MI_PERFIL
databricks bundle run -t demo quilla_ltap_ai_search --profile MI_PERFIL
databricks bundle run -t demo quilla_governance_bootstrap --profile MI_PERFIL

# GenAI gobernada y supervisor
databricks bundle run -t demo quilla_governed_skills --profile MI_PERFIL
databricks bundle run -t demo quilla_ai_gateway_policies --profile MI_PERFIL
databricks bundle run -t demo quilla_custom_supervisor --profile MI_PERFIL
databricks bundle run -t demo quilla_ai_governance --profile MI_PERFIL
databricks bundle run -t demo quilla_genai_offline_evaluation --profile MI_PERFIL
```

Use `smoke` en lugar de `demo` para validar con volúmenes reducidos.

## 8. Componentes opcionales o condicionados

### Zerobus

El deploy no exige un endpoint Zerobus. Para ejecutar el simulador realtime,
configure el endpoint regional del workspace en `.env.local`, renderice y haga
un nuevo deploy:

```bash
DATABRICKS_BUNDLE_VAR_zerobus_endpoint=https://<endpoint-regional>
```

Luego ejecute `quilla_zerobus_bootstrap` y `simulate_realtime_events`.

### Modelos Foundation y Unity AI Gateway

Los nombres de GLM, Qwen, DeepSeek y Kimi son variables del bundle. Si un modelo
no está disponible en la región, reemplace la variable por un endpoint
habilitado que cumpla el mismo contrato. Los jobs de AI Gateway reconcilian las
políticas y no reutilizan IDs de otro workspace.

### MCP financiero externo

`system.ai.web_search` es una capacidad administrada. El fallback financiero de
You.com requiere que el MCP `you_financial_mcp` sea creado en el catálogo y
schema AI del nuevo ambiente y que su secret sea configurado localmente. El
repositorio nunca contiene la API key. Sin ese MCP, el supervisor continúa con
Web Search e informa la falta del fallback pago.

### Discovery Hub

La publicación de dominios puede requerir interfaz según la versión del
workspace. Cree la jerarquía con raíz `Quilla` y ejecute después el job
`quilla_discovery_domain_associations`. Las asociaciones usan los IDs creados
por el bundle y los schemas configurados en el nuevo ambiente.

## 9. Verificación

```bash
uv run --with-requirements requirements.txt --with-requirements requirements-dev.txt \
  python -m pytest -q tests
cd frontend && npm install && npm run build && cd ..
python3 setup/validate_portability.py
databricks bundle summary -t demo --profile MI_PERFIL
```

La validación de portabilidad falla si encuentra host, ID de workspace,
catálogo privado, nombre personal, IDs hardcoded o variables obligatorias de
infraestructura que deberían ser creadas por el DAB.

## 10. Solución de problemas

- `PERMISSION_DENIED`: solicite el privilegio específico indicado; no copie un
  ID perteneciente a otra persona.
- feature no habilitada: active el Preview/Beta correspondiente o use un
  workspace/región compatible.
- conflicto de nombre: sobrescriba catálogo, Lakebase project y AI Search
  endpoint en `.env.local`.
- error después de varios minutos: repare el run existente con
  `databricks jobs repair-run`; no reinicie las tasks ya concluidas.
- assets desactualizados: vuelva a ejecutar `render_bundle_assets.py` para el
  mismo target antes del deploy.
