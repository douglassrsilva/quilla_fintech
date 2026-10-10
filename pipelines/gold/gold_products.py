"""Quilla Gold: governed materialized data products for analytics and AI."""

from functools import reduce

from pyspark import pipelines as dp
from pyspark.sql import functions as F


CATALOG = spark.conf.get("quilla.catalog")
BRONZE_SCHEMA = spark.conf.get("quilla.bronze_schema")
SILVER_SCHEMA = spark.conf.get("quilla.silver_schema")

DATASETS = (
    "countries",
    "products",
    "fx_rates",
    "parties",
    "accounts",
    "cards",
    "merchants",
    "devices",
    "financial_events",
    "receivables",
    "credit_cases",
    "insurance_cases",
    "risk_alerts",
    "consents",
    "interactions",
    "documents",
    "quality_truth",
)


def silver(table_name: str):
    return spark.read.table(f"{CATALOG}.{SILVER_SCHEMA}.{table_name}")


@dp.materialized_view(
    name="mv_financial_performance_daily",
    comment="Performance financeira diária por mercado, produto e status.",
    table_properties={"quality": "gold", "quilla.data_product": "financial-performance"},
)
def financial_performance_daily():
    events = silver("financial_events").withColumn(
        "event_date",
        F.to_date(
            F.from_utc_timestamp("event_ts_utc", F.col("event_timezone"))
        ),
    )
    return events.groupBy(
        "event_date",
        "country_code",
        "event_type",
        "event_status",
        "direction",
        "currency_code",
    ).agg(
        F.count(F.lit(1)).alias("event_count"),
        F.countDistinct("party_id").alias("active_parties"),
        F.countDistinct("account_id").alias("active_accounts"),
        F.sum("amount_local").alias("gross_volume_local"),
        F.sum("amount_usd").alias("gross_volume_usd"),
        F.sum(F.when(F.col("event_status") == "completed", 1).otherwise(0)).alias("completed_events"),
        F.sum(F.when(F.col("event_status") == "declined", 1).otherwise(0)).alias("declined_events"),
        F.sum(F.when(F.col("fraud_label"), 1).otherwise(0)).alias("fraud_events"),
        F.avg("risk_score").alias("average_risk_score"),
    )


@dp.materialized_view(
    name="mv_risk_operations_daily",
    comment="Operação diária de fraude e risco por país, tipo e severidade.",
    table_properties={"quality": "gold", "quilla.data_product": "risk-operations"},
)
def risk_operations_daily():
    alerts = silver("risk_alerts").withColumn("alert_date", F.to_date("created_at"))
    return alerts.groupBy(
        "alert_date", "country_code", "alert_type", "severity", "status", "currency_code"
    ).agg(
        F.count(F.lit(1)).alias("alert_count"),
        F.countDistinct("party_id").alias("parties_alerted"),
        F.sum("amount_at_risk").alias("amount_at_risk_local"),
        F.sum(F.when(F.col("disposition") == "confirmed", 1).otherwise(0)).alias("confirmed_alerts"),
        F.sum(F.when(F.col("disposition") == "false_positive", 1).otherwise(0)).alias("false_positive_alerts"),
        F.avg("risk_score").alias("average_risk_score"),
    )


@dp.materialized_view(
    name="mv_credit_portfolio_monthly",
    comment="Funil mensal de crédito por mercado, produto, decisão e política.",
    table_properties={"quality": "gold", "quilla.data_product": "credit-portfolio"},
)
def credit_portfolio_monthly():
    cases = silver("credit_cases").withColumn("request_month", F.date_trunc("month", "requested_at"))
    return cases.groupBy(
        "request_month", "country_code", "product_id", "decision", "policy_band", "currency_code"
    ).agg(
        F.count(F.lit(1)).alias("application_count"),
        F.countDistinct("party_id").alias("applicant_count"),
        F.sum("requested_amount").alias("requested_amount_local"),
        F.sum(F.coalesce("approved_amount", F.lit(0.0))).alias("approved_amount_local"),
        F.sum(F.when(F.col("decision") == "approved", 1).otherwise(0)).alias("approved_applications"),
        F.avg("risk_score").alias("average_risk_score"),
        F.avg("annual_rate_bps").alias("average_annual_rate_bps"),
    )


@dp.materialized_view(
    name="mv_customer_360",
    comment="Customer 360 regional com relacionamento, saldos, atividade, risco e atendimento.",
    table_properties={"quality": "gold", "quilla.data_product": "customer-360"},
)
def customer_360():
    parties = silver("parties").alias("p")
    accounts = silver("accounts").groupBy("party_id").agg(
        F.count(F.lit(1)).alias("account_count"),
        F.sum("current_balance").alias("current_balance_local"),
        F.sum("available_balance").alias("available_balance_local"),
        F.max("updated_at").alias("last_account_update_at"),
    )
    events = silver("financial_events").groupBy("party_id").agg(
        F.count(F.lit(1)).alias("lifetime_event_count"),
        F.sum("amount_usd").alias("lifetime_volume_usd"),
        F.max("event_ts_utc").alias("last_event_at"),
        F.avg("risk_score").alias("average_event_risk_score"),
        F.sum(F.when(F.col("fraud_label"), 1).otherwise(0)).alias("fraud_event_count"),
    )
    alerts = silver("risk_alerts").groupBy("party_id").agg(
        F.count(F.lit(1)).alias("risk_alert_count"),
        F.sum(F.when(F.col("status") != "closed", 1).otherwise(0)).alias("open_risk_alert_count"),
        F.max("created_at").alias("last_alert_at"),
    )
    interactions = silver("interactions").groupBy("party_id").agg(
        F.count(F.lit(1)).alias("interaction_count"),
        F.avg("sentiment_score").alias("average_sentiment_score"),
        F.max("interaction_ts").alias("last_interaction_at"),
    )

    return (
        parties.join(accounts, "party_id", "left")
        .join(events, "party_id", "left")
        .join(alerts, "party_id", "left")
        .join(interactions, "party_id", "left")
        .select(
            "party_id",
            "p.party_type",
            "p.country_code",
            "p.locale_code",
            "p.preferred_language",
            "p.display_name",
            "p.customer_segment",
            "p.kyc_status",
            "p.risk_band",
            "p.onboarding_ts",
            F.coalesce("account_count", F.lit(0)).alias("account_count"),
            F.coalesce("current_balance_local", F.lit(0.0)).alias("current_balance_local"),
            F.coalesce("available_balance_local", F.lit(0.0)).alias("available_balance_local"),
            "last_account_update_at",
            F.coalesce("lifetime_event_count", F.lit(0)).alias("lifetime_event_count"),
            F.coalesce("lifetime_volume_usd", F.lit(0.0)).alias("lifetime_volume_usd"),
            "last_event_at",
            "average_event_risk_score",
            F.coalesce("fraud_event_count", F.lit(0)).alias("fraud_event_count"),
            F.coalesce("risk_alert_count", F.lit(0)).alias("risk_alert_count"),
            F.coalesce("open_risk_alert_count", F.lit(0)).alias("open_risk_alert_count"),
            "last_alert_at",
            F.coalesce("interaction_count", F.lit(0)).alias("interaction_count"),
            "average_sentiment_score",
            "last_interaction_at",
        )
    )


@dp.materialized_view(
    name="mv_data_quality_overview",
    comment="Reconciliação Bronze, Silver e quarentena para todos os contratos DQX.",
    table_properties={"quality": "gold", "quilla.data_product": "data-quality"},
)
def data_quality_overview():
    summaries = []
    for dataset_name in DATASETS:
        bronze = spark.read.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_{dataset_name}").agg(
            F.count(F.lit(1)).alias("bronze_rows")
        )
        accepted = silver(dataset_name).agg(F.count(F.lit(1)).alias("silver_rows"))
        quarantined = silver(f"{dataset_name}_quarantine").agg(
            F.count(F.lit(1)).alias("quarantine_rows")
        )
        summaries.append(
            bronze.crossJoin(accepted)
            .crossJoin(quarantined)
            .withColumn("dataset_name", F.lit(dataset_name))
            .withColumn(
                "acceptance_rate",
                F.when(
                    F.col("bronze_rows") > 0,
                    F.col("silver_rows") / F.col("bronze_rows"),
                ).otherwise(F.lit(1.0)),
            )
            .withColumn("reconciled", F.col("bronze_rows") == F.col("silver_rows") + F.col("quarantine_rows"))
            .select(
                "dataset_name",
                "bronze_rows",
                "silver_rows",
                "quarantine_rows",
                "acceptance_rate",
                "reconciled",
            )
        )
    return reduce(lambda left, right: left.unionByName(right), summaries)
