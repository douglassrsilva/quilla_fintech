"""Quilla Silver: contract-driven DQX validation, quarantine and deduplication."""

from pathlib import Path

import yaml
from databricks.labs.dqx.config import ExtraParams
from databricks.labs.dqx.engine import DQEngine
from databricks.sdk import WorkspaceClient
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


CATALOG = spark.conf.get("quilla.catalog")
BRONZE_SCHEMA = spark.conf.get("quilla.bronze_schema")
SNAPSHOT = spark.conf.get("quilla.snapshot")
CONTRACT_PATH = Path(spark.conf.get("quilla.contract_path"))

with CONTRACT_PATH.open("r", encoding="utf-8") as contract_file:
    CONTRACT_DOCUMENT = yaml.safe_load(contract_file)

CONTRACTS = CONTRACT_DOCUMENT["datasets"]
STANDARDS = CONTRACT_DOCUMENT["standards"]

DQ_ENGINE = DQEngine(
    WorkspaceClient(),
    spark=spark,
    extra_params=ExtraParams(
        run_time_overwrite="2026-08-31T23:59:59+00:00",
        run_id_overwrite=f"quilla-silver-{SNAPSHOT}",
        user_metadata={"project": "quilla", "snapshot": SNAPSHOT},
    ),
)


def _quoted(values: list[str]) -> str:
    return ",".join(f"'{value}'" for value in values)


COUNTRY_CURRENCY_EXPRESSION = " OR ".join(
    [
        "(country_code = 'BR' AND {column} = 'BRL')",
        "(country_code = 'MX' AND {column} = 'MXN')",
        "(country_code = 'CO' AND {column} = 'COP')",
        "(country_code = 'AR' AND {column} = 'ARS')",
        "(country_code = 'CL' AND {column} = 'CLP')",
        "(country_code = 'PE' AND {column} = 'PEN')",
        "(country_code = 'EC' AND {column} = 'USD')",
        "(country_code = 'GT' AND {column} = 'GTQ')",
        "(country_code = 'DO' AND {column} = 'DOP')",
        "(country_code = 'CR' AND {column} = 'CRC')",
        "(country_code = 'PA' AND {column} = 'PAB')",
        "(country_code = 'UY' AND {column} = 'UYU')",
    ]
)


def _sql_check(name: str, expression: str, message: str, criticality: str) -> dict:
    return {
        "name": name,
        "criticality": criticality,
        "check": {
            "function": "sql_expression",
            "arguments": {"expression": expression, "msg": message},
        },
    }


def _compile_checks(dataset_name: str, contract: dict) -> list[dict]:
    checks = [
        _sql_check(
            "schema_rescue_empty",
            "_rescued_data IS NULL",
            "Colunas incompatíveis foram capturadas pelo Auto Loader",
            "error",
        )
    ]

    checks.extend(
        {
            "name": f"{column}_required",
            "criticality": "error",
            "check": {"function": "is_not_null", "arguments": {"column": column}},
        }
        for column in contract.get("required", [])
    )

    standards = set(contract.get("standards", []))
    if "country" in standards:
        checks.append(
            _sql_check(
                "country_code_iso",
                f"country_code IN ({_quoted(STANDARDS['country_codes'])})",
                "Código de país fora dos 12 mercados Quilla",
                "error",
            )
        )
    if "currency" in standards:
        checks.append(
            _sql_check(
                "currency_code_iso",
                f"currency_code IN ({_quoted(STANDARDS['currency_codes'])})",
                "Código de moeda fora do domínio regional",
                "error",
            )
        )
    if "language" in standards:
        checks.append(
            _sql_check(
                "language_code_iso",
                f"language_code IN ({_quoted(STANDARDS['language_codes'])})",
                "Idioma deve ser português ou espanhol",
                "error",
            )
        )
    if "synthetic" in standards:
        checks.append(
            _sql_check(
                "synthetic_data_guardrail",
                "synthetic_flag = true",
                "A demo aceita apenas dados explicitamente sintéticos",
                "error",
            )
        )
    if "country_currency" in standards:
        checks.append(
            _sql_check(
                "country_currency_consistent",
                COUNTRY_CURRENCY_EXPRESSION.format(column="currency_code"),
                "Moeda incompatível com o país",
                "error",
            )
        )
    if "country_base_currency" in standards:
        checks.append(
            _sql_check(
                "country_base_currency_consistent",
                COUNTRY_CURRENCY_EXPRESSION.format(column="base_currency"),
                "Moeda-base incompatível com o país",
                "error",
            )
        )

    checks.extend(
        _sql_check(
            rule["name"],
            rule["expression"],
            rule["message"],
            rule["criticality"],
        )
        for rule in contract.get("checks", [])
    )
    checks.append(
        _sql_check(
            "business_key_unique",
            "_dq_duplicate_count = 1",
            f"Chave de negócio duplicada em {dataset_name}",
            "warn",
        )
    )
    return checks


def _checked_dataframe(dataset_name: str, contract: dict):
    bronze_table = f"{CATALOG}.{BRONZE_SCHEMA}.bronze_{dataset_name}"
    primary_key = contract["primary_key"]
    key_window = Window.partitionBy(*primary_key)

    source = spark.read.table(bronze_table).withColumn(
        "_dq_duplicate_count", F.count(F.lit(1)).over(key_window)
    )
    checked = DQ_ENGINE.apply_checks_by_metadata(
        source,
        _compile_checks(dataset_name, contract),
    )
    checked = checked.withColumn(
        # Some runtime configurations implement size(NULL) as -1. DQX uses a
        # null error array for clean records, so normalize explicitly to zero;
        # otherwise valid rows fall between the valid and quarantine filters.
        "_dq_error_count",
        F.when(F.col("_errors").isNull(), F.lit(0)).otherwise(
            F.size(F.col("_errors"))
        ),
    )

    tie_breakers = contract.get("tie_breakers", [])
    ordering = [
        F.col("_dq_error_count").asc(),
        F.col("_source_file_modification_time").desc_nulls_last(),
        F.col("_source_file").desc_nulls_last(),
    ]
    ordering.extend(F.col(column).desc_nulls_last() for column in tie_breakers)
    dedup_window = Window.partitionBy(*primary_key).orderBy(*ordering)
    return checked.withColumn("_dedup_rank", F.row_number().over(dedup_window))


def _normalize_valid(df, contract: dict):
    normalized = df
    for column, operation in contract.get("normalizations", {}).items():
        if operation == "lower_trim":
            normalized = normalized.withColumn(column, F.lower(F.trim(F.col(column))))
        else:
            raise ValueError(f"Unsupported normalization {operation!r} for {column!r}")

    return (
        normalized.withColumnRenamed("_warnings", "_dq_warnings")
        .withColumn("_silver_processed_at", F.to_timestamp(F.lit("2026-08-31 23:59:59")))
        .drop("_errors", "_dq_error_count", "_dq_duplicate_count", "_dedup_rank")
    )


def _register_silver_assets(dataset_name: str, contract: dict) -> None:
    checked_view_name = f"_{dataset_name}_quality_checked"
    quarantine_table_name = f"{dataset_name}_quarantine"

    def quality_checked(dataset_name=dataset_name, contract=contract):
        return _checked_dataframe(dataset_name, contract)
    quality_checked.__name__ = f"{dataset_name}_quality_checked_definition"
    dp.temporary_view(name=checked_view_name)(quality_checked)

    def silver_valid(checked_view_name=checked_view_name, contract=contract):
        checked = spark.read.table(checked_view_name)
        return _normalize_valid(
            checked.filter((F.col("_dq_error_count") == 0) & (F.col("_dedup_rank") == 1)),
            contract,
        )
    silver_valid.__name__ = f"{dataset_name}_silver_definition"
    dp.materialized_view(
        name=dataset_name,
        comment=f"Quilla Silver {dataset_name}: validado por contrato DQX e deduplicado.",
        table_properties={
            "quality": "silver",
            "delta.enableChangeDataFeed": "true",
            "quilla.contract.version": str(CONTRACT_DOCUMENT["version"]),
        },
    )(silver_valid)

    def silver_quarantine(checked_view_name=checked_view_name):
        checked = spark.read.table(checked_view_name)
        return (
            checked.filter((F.col("_dq_error_count") > 0) | (F.col("_dedup_rank") > 1))
            .withColumn(
                "_quarantine_reason",
                F.when(
                    (F.col("_dq_error_count") > 0) & (F.col("_dedup_rank") > 1),
                    F.lit("CONTRACT_AND_DUPLICATE"),
                )
                .when(F.col("_dq_error_count") > 0, F.lit("CONTRACT_VIOLATION"))
                .otherwise(F.lit("DUPLICATE_LOSER")),
            )
            .withColumnRenamed("_errors", "_dq_errors")
            .withColumnRenamed("_warnings", "_dq_warnings")
            .withColumn("_quarantined_at", F.to_timestamp(F.lit("2026-08-31 23:59:59")))
        )
    silver_quarantine.__name__ = f"{dataset_name}_quarantine_definition"
    dp.materialized_view(
        name=quarantine_table_name,
        comment=f"Quarentena DQX e duplicatas rejeitadas de {dataset_name}.",
        table_properties={"quality": "quarantine", "quilla.synthetic": "true"},
    )(silver_quarantine)

for _dataset_name, _contract in CONTRACTS.items():
    _register_silver_assets(_dataset_name, _contract)
