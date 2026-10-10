# Databricks notebook source
"""Bootstrap idempotente do produtor Zerobus da demonstração Quilla."""

import uuid
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import ResourceAlreadyExists


# COMMAND ----------

dbutils.widgets.text("catalog", "example_catalog")
dbutils.widgets.text("bronze_schema", "example_bronze")
dbutils.widgets.text("secret_scope", "quilla-zerobus")
dbutils.widgets.text("service_principal_name", "quilla-zerobus-demo")

CATALOG = dbutils.widgets.get("catalog")
BRONZE_SCHEMA = dbutils.widgets.get("bronze_schema")
SECRET_SCOPE = dbutils.widgets.get("secret_scope")
SERVICE_PRINCIPAL_NAME = dbutils.widgets.get("service_principal_name")

for value, label in ((CATALOG, "catalog"), (BRONZE_SCHEMA, "bronze_schema")):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{label} inválido: {value!r}")
if not re.fullmatch(r"[A-Za-z0-9_.-]+", SECRET_SCOPE):
    raise ValueError(f"secret_scope inválido: {SECRET_SCOPE!r}")

TARGET_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.zerobus_financial_events"

w = WorkspaceClient()


# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{BRONZE_SCHEMA}`")
spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
      source_record_id STRING,
      event_id STRING,
      country_code STRING,
      party_id STRING,
      account_id STRING,
      card_id STRING,
      merchant_id STRING,
      device_id STRING,
      product_id INT,
      event_type STRING,
      event_status STRING,
      channel STRING,
      direction STRING,
      amount_local DOUBLE,
      currency_code STRING,
      amount_usd DOUBLE,
      event_ts_utc TIMESTAMP,
      event_timezone STRING,
      authorized_ts TIMESTAMP,
      settled_ts TIMESTAMP,
      ingested_at TIMESTAMP,
      is_cross_border BOOLEAN,
      counterparty_country_code STRING,
      risk_score DOUBLE,
      fraud_label BOOLEAN,
      description STRING,
      source_system STRING,
      schema_version STRING,
      payload_variant STRING,
      event_month STRING,
      synthetic_flag BOOLEAN,
      _producer_run_id STRING,
      _producer_scenario STRING,
      _producer_sequence LONG,
      _producer_language STRING,
      _producer_ingested_at TIMESTAMP
    )
    USING DELTA
    COMMENT 'Landing governado de eventos financeiros sintéticos recebidos pelo Zerobus.'
    TBLPROPERTIES (
      'delta.enableChangeDataFeed' = 'true',
      'quilla.synthetic' = 'true',
      'quilla.transport' = 'zerobus',
      'quality' = 'bronze-source'
    )
    """
)


# COMMAND ----------

principals = list(
    w.service_principals.list(filter=f'displayName eq "{SERVICE_PRINCIPAL_NAME}"')
)
if len(principals) > 1:
    raise RuntimeError(f"Mais de um service principal chamado {SERVICE_PRINCIPAL_NAME}")

if principals:
    principal = principals[0]
else:
    application_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"databricks://{spark.conf.get('spark.databricks.workspaceUrl')}/{SERVICE_PRINCIPAL_NAME}",
        )
    )
    principal = w.service_principals.create(
        application_id=application_id,
        display_name=SERVICE_PRINCIPAL_NAME,
        active=True,
    )

if not principal.id or not principal.application_id:
    raise RuntimeError("Service principal criado sem id ou application_id")


# COMMAND ----------

try:
    w.secrets.create_scope(SECRET_SCOPE)
except ResourceAlreadyExists:
    pass

secret_keys = {item.key for item in w.secrets.list_secrets(SECRET_SCOPE)}
stored_client_id = None
if "client_id" in secret_keys:
    try:
        stored_client_id = dbutils.secrets.get(SECRET_SCOPE, "client_id")
    except Exception:
        stored_client_id = None

credential_ready = (
    stored_client_id == principal.application_id and "client_secret" in secret_keys
)

if not credential_ready:
    old_secrets = list(w.service_principal_secrets_proxy.list(principal.id))
    credential = w.service_principal_secrets_proxy.create(
        principal.id,
        lifetime="2592000s",
    )
    if not credential.secret:
        raise RuntimeError("A API não retornou o segredo M2M recém-criado")

    w.secrets.put_secret(SECRET_SCOPE, "client_id", string_value=principal.application_id)
    w.secrets.put_secret(SECRET_SCOPE, "client_secret", string_value=credential.secret)

    for old_secret in old_secrets:
        if old_secret.id and old_secret.id != credential.id:
            w.service_principal_secrets_proxy.delete(principal.id, old_secret.id)


# COMMAND ----------

principal_sql = principal.application_id.replace("`", "``")
spark.sql(f"GRANT USE CATALOG ON CATALOG `{CATALOG}` TO `{principal_sql}`")
spark.sql(
    f"GRANT USE SCHEMA ON SCHEMA `{CATALOG}`.`{BRONZE_SCHEMA}` TO `{principal_sql}`"
)
spark.sql(f"GRANT SELECT, MODIFY ON TABLE {TARGET_TABLE} TO `{principal_sql}`")

print(
    {
        "status": "READY",
        "target_table": TARGET_TABLE,
        "service_principal": SERVICE_PRINCIPAL_NAME,
        "client_id": principal.application_id,
        "secret_scope": SECRET_SCOPE,
        "credential_reused": credential_ready,
    }
)
