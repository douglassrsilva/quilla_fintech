# Databricks notebook source
"""Prepare governed LATAM financial classifiers and Data Classification scope."""

import json
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.dataclassification import (
    CatalogConfig,
    CatalogConfigSchemaNames,
    FieldMask,
)
from pyspark.sql import types as T


for name, default in {
    "catalog": "example_catalog",
    "raw_schema": "example_raw",
    "bronze_schema": "example_bronze",
    "silver_schema": "example_silver",
    "gold_schema": "example_gold",
    "ml_schema": "example_ml",
    "ai_schema": "example_ai",
    "governance_schema": "example_governance",
}.items():
    dbutils.widgets.text(name, default)

CATALOG = dbutils.widgets.get("catalog")
GOVERNANCE_SCHEMA = dbutils.widgets.get("governance_schema")
CLASSIFICATION_SCHEMAS = sorted(
    {
        dbutils.widgets.get("raw_schema"),
        dbutils.widgets.get("bronze_schema"),
        dbutils.widgets.get("silver_schema"),
        dbutils.widgets.get("gold_schema"),
        dbutils.widgets.get("ml_schema"),
        dbutils.widgets.get("ai_schema"),
        GOVERNANCE_SCHEMA,
    }
)

COUNTRIES = {
    "BR": ("CPF/CNPJ", "Cadastro de Pessoa Física ou Jurídica"),
    "MX": ("CURP/RFC", "Clave Única de Registro de Población o RFC"),
    "CO": ("CC/NIT", "Cédula de Ciudadanía o NIT"),
    "AR": ("DNI/CUIT", "Documento Nacional de Identidad o CUIT"),
    "CL": ("RUN/RUT", "Rol Único Nacional o Tributario"),
    "PE": ("DNI/RUC", "Documento Nacional de Identidad o RUC"),
    "EC": ("Cédula/RUC", "Cédula de identidad o RUC"),
    "GT": ("CUI/NIT", "Código Único de Identificación o NIT"),
    "DO": ("Cédula/RNC", "Cédula de identidad o RNC"),
    "CR": ("Cédula física/jurídica", "Cédula de persona física o jurídica"),
    "PA": ("Cédula/RUC", "Cédula de identidad o RUC"),
    "UY": ("CI/RUT", "Cédula de identidad o RUT"),
}

TAG_DEFINITIONS = {
    "quilla_national_identifier": {
        "description": "Identificador nacional ou tributário sintético dos 12 mercados Quilla.",
        "values": list(COUNTRIES),
    },
    "quilla_financial_identifier": {
        "description": "Identificador financeiro ou token operacional da Quilla.",
        "values": [
            "customer_id",
            "bank_account",
            "card_token",
            "device_token",
            "policy_token",
            "invoice_token",
        ],
    },
    "quilla_sensitive_context": {
        "description": "Domínio financeiro sensível sujeito a controles regulatórios.",
        "values": [
            "kyc_aml",
            "credit_decision",
            "fraud_risk",
            "privacy_consent",
            "insurance_claim",
        ],
    },
}


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def create_governed_tag(tag: str, definition: dict) -> str:
    values = ", ".join(sql_literal(value) for value in definition["values"])
    statement = (
        f"CREATE GOVERNED TAG {tag} "
        f"DESCRIPTION {sql_literal(definition['description'])} VALUES ({values})"
    )
    try:
        spark.sql(statement)
        return "CREATED"
    except Exception as exc:
        message = str(exc).upper()
        if "ALREADY_EXISTS" in message or "ALREADY EXISTS" in message:
            return "EXISTING"
        raise


tag_states = {tag: create_governed_tag(tag, definition) for tag, definition in TAG_DEFINITIONS.items()}

sample_fields = [
    T.StructField("country_code", T.StringType(), False),
    T.StructField("identifier_family", T.StringType(), False),
]
sample_fields.extend(
    T.StructField(f"national_id_{country.lower()}", T.StringType(), True)
    for country in COUNTRIES
)
sample_fields.extend(
    [
        T.StructField("party_id", T.StringType(), False),
        T.StructField("account_id", T.StringType(), False),
        T.StructField("card_token", T.StringType(), False),
        T.StructField("device_token", T.StringType(), False),
        T.StructField("policy_token", T.StringType(), False),
        T.StructField("invoice_token", T.StringType(), False),
        T.StructField("context_text_pt", T.StringType(), False),
        T.StructField("context_text_es", T.StringType(), False),
        T.StructField("synthetic_flag", T.BooleanType(), False),
    ]
)

sample_rows = []
for position, (country, (identifier_family, description)) in enumerate(COUNTRIES.items(), start=1):
    national_values = {
        f"national_id_{code.lower()}": (
            f"SYNTHETIC-{code}-{identifier_family.replace('/', '-')}-{position:06d}"
            if code == country
            else None
        )
        for code in COUNTRIES
    }
    sample_rows.append(
        {
            "country_code": country,
            "identifier_family": identifier_family,
            **national_values,
            "party_id": f"PTY-SYNTH-{country}-{position:06d}",
            "account_id": f"ACC-SYNTH-{country}-{position:06d}",
            "card_token": f"CARD-TKN-SYNTH-{country}-{position:06d}",
            "device_token": f"DEVICE-TKN-SYNTH-{country}-{position:06d}",
            "policy_token": f"POLICY-TKN-SYNTH-{country}-{position:06d}",
            "invoice_token": f"INVOICE-TKN-SYNTH-{country}-{position:06d}",
            "context_text_pt": f"Amostra sintética de {description} para validação de governança.",
            "context_text_es": f"Muestra sintética de {description} para validación de gobierno.",
            "synthetic_flag": True,
        }
    )

samples_table = f"{CATALOG}.{GOVERNANCE_SCHEMA}.classifier_sample_values"
spark.createDataFrame(sample_rows, T.StructType(sample_fields)).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(samples_table)

specs = []
for country, (identifier_family, description) in COUNTRIES.items():
    specs.append(
        {
            "classifier_key": f"national_identifier_{country.lower()}",
            "governed_tag": "quilla_national_identifier",
            "tag_value": country,
            "description_pt": (
                f"Detectar colunas com {description} ({identifier_family}) de clientes do mercado {country}; "
                "não classificar códigos internos sem significado civil ou tributário."
            ),
            "description_es": (
                f"Detectar columnas con {description} ({identifier_family}) de clientes del mercado {country}; "
                "no clasificar códigos internos sin significado civil o tributario."
            ),
            "sample_column": f"{samples_table}.national_id_{country.lower()}",
            "countries": [country],
            "ui_state": "READY_FOR_CUSTOM_CLASSIFIER_UI",
        }
    )

financial_specs = {
    "customer_id": ("party_id", "identificador interno de cliente", "identificador interno de cliente"),
    "bank_account": ("account_id", "identificador de conta bancária", "identificador de cuenta bancaria"),
    "card_token": ("card_token", "token de cartão", "token de tarjeta"),
    "device_token": ("device_token", "token de dispositivo", "token de dispositivo"),
    "policy_token": ("policy_token", "token de apólice", "token de póliza"),
    "invoice_token": ("invoice_token", "token de fatura ou recebível", "token de factura o cuenta por cobrar"),
}
for tag_value, (column, label_pt, label_es) in financial_specs.items():
    specs.append(
        {
            "classifier_key": f"financial_{tag_value}",
            "governed_tag": "quilla_financial_identifier",
            "tag_value": tag_value,
            "description_pt": f"Detectar colunas que armazenam {label_pt} da Quilla em qualquer um dos 12 países.",
            "description_es": f"Detectar columnas que almacenan {label_es} de Quilla en cualquiera de los 12 países.",
            "sample_column": f"{samples_table}.{column}",
            "countries": list(COUNTRIES),
            "ui_state": "READY_FOR_CUSTOM_CLASSIFIER_UI",
        }
    )

context_specs = {
    "kyc_aml": "dados de KYC, AML e verificação de identidade",
    "credit_decision": "decisões, políticas e motivos de crédito",
    "fraud_risk": "scores, alertas e evidências de fraude",
    "privacy_consent": "consentimentos, finalidades e preferências de privacidade",
    "insurance_claim": "sinistros, coberturas e decisões de seguro",
}
for tag_value, label in context_specs.items():
    specs.append(
        {
            "classifier_key": f"sensitive_context_{tag_value}",
            "governed_tag": "quilla_sensitive_context",
            "tag_value": tag_value,
            "description_pt": f"Detectar colunas com {label}; excluir métricas agregadas sem vínculo individual.",
            "description_es": f"Detectar columnas con {label}; excluir métricas agregadas sin vínculo individual.",
            "sample_column": f"{samples_table}.context_text_pt",
            "countries": list(COUNTRIES),
            "ui_state": "READY_FOR_CUSTOM_CLASSIFIER_UI",
        }
    )

spec_schema = T.StructType(
    [
        T.StructField("classifier_key", T.StringType(), False),
        T.StructField("governed_tag", T.StringType(), False),
        T.StructField("tag_value", T.StringType(), False),
        T.StructField("description_pt", T.StringType(), False),
        T.StructField("description_es", T.StringType(), False),
        T.StructField("sample_column", T.StringType(), False),
        T.StructField("countries", T.ArrayType(T.StringType(), False), False),
        T.StructField("ui_state", T.StringType(), False),
    ]
)
specs_table = f"{CATALOG}.{GOVERNANCE_SCHEMA}.custom_classifier_specs"
spark.createDataFrame(specs, spec_schema).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(specs_table)

workspace = WorkspaceClient()
config_name = f"catalogs/{CATALOG}/config"
try:
    current_config = workspace.data_classification.get_catalog_config(config_name)
    existing_schemas = set(current_config.included_schemas.names if current_config.included_schemas else [])
    desired_schemas = sorted(existing_schemas.union(CLASSIFICATION_SCHEMAS))
    updated = workspace.data_classification.update_catalog_config(
        config_name,
        CatalogConfig(included_schemas=CatalogConfigSchemaNames(names=desired_schemas)),
        FieldMask(field_mask=["included_schemas"]),
    )
except NotFound:
    desired_schemas = CLASSIFICATION_SCHEMAS
    updated = workspace.data_classification.create_catalog_config(
        f"catalogs/{CATALOG}",
        CatalogConfig(
            name=config_name,
            included_schemas=CatalogConfigSchemaNames(names=desired_schemas),
        ),
    )

run_at = datetime.now(timezone.utc).isoformat()
print(
    json.dumps(
        {
            "status": "PREPARED",
            "run_at": run_at,
            "catalog": CATALOG,
            "governed_tags": tag_states,
            "classifier_specs": len(specs),
            "countries": len(COUNTRIES),
            "classification_schemas": updated.included_schemas.names,
            "custom_classifier_execution": "UI_ONLY_BETA",
        },
        ensure_ascii=False,
    )
)
