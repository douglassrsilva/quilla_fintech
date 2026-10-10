# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------

from functools import reduce

cfg = load_config({
    "event_rows": 13_758_648,
    "party_rows": 144_000,
    "account_rows": 240_000,
    "risk_alert_rows": 206_380,
    "consent_rows": 300_000,
    "interaction_rows": 100_000,
    "document_rows": 25_000,
})


def engagement_backbone(rows: int, seed_offset: int):
    return (
        spark.range(rows, numPartitions=max(4, rows // 100_000))
        .withColumnRenamed("id", "row_id")
        .withColumn("country_index", F.pmod("row_id", F.lit(12)).cast("int"))
        .withColumn("country_code", F.element_at(country_array("code"), (F.col("country_index") + 1).cast("int")))
        .withColumn("h", stable_hash("row_id", seed=cfg["seed"] + seed_offset))
        .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
    )


# COMMAND ----------

event_country_counts = event_counts(cfg["event_rows"])
alert_country_counts = event_counts(cfg["risk_alert_rows"])
alert_frames = []
alert_offset = 0
for country_index, country in enumerate(COUNTRIES):
    code = country["code"]
    rows = alert_country_counts[code]
    available_events = event_country_counts[code]
    alerts = (
        spark.range(rows, numPartitions=max(2, rows // 100_000))
        .withColumnRenamed("id", "local_id")
        .withColumn("country_code", F.lit(code))
        .withColumn("country_index", F.lit(country_index))
        .withColumn("h", stable_hash("local_id", F.lit(code), seed=cfg["seed"] + 701))
        .withColumn("alert_id", F.format_string("QALT-%012d", F.col("local_id") + F.lit(alert_offset) + 1))
        .withColumn("candidate_event", F.pmod(F.col("h"), F.lit(available_events)))
        .withColumn("event_local_id", F.when(F.pmod(F.col("candidate_event"), F.lit(500)) == 499, F.col("candidate_event") - 1).otherwise(F.col("candidate_event")))
        .withColumn("event_id", F.format_string(f"QEVT-{code}-%012d", F.col("event_local_id") + 1))
        .withColumn("party_id", same_country_id(F.col("h"), F.col("country_index"), cfg["party_rows"], "QPTY"))
        .withColumn("account_id", same_country_id(F.col("h"), F.col("country_index"), cfg["account_rows"], "QACC"))
        .withColumn("alert_type", F.element_at(F.array(*[F.lit(v) for v in ["velocity_spike", "geographic_anomaly", "amount_anomaly", "account_takeover", "aml_pattern", "device_risk"]]), (F.pmod(F.col("h"), F.lit(6)) + 1).cast("int")))
        .withColumn("risk_score", F.round(F.lit(0.50) + F.pmod(F.col("h"), F.lit(5000)) / 10000.0, 4))
        .withColumn("severity", F.when(F.col("risk_score") >= 0.90, "critical").when(F.col("risk_score") >= 0.78, "high").when(F.col("risk_score") >= 0.64, "medium").otherwise("low"))
        .withColumn("created_at", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit("2024-09-01 00:00:00")) + F.pmod(F.col("h"), F.lit(63_071_999)))))
        .withColumn("status", F.when(F.pmod(F.col("h"), F.lit(100)) < 26, "open").when(F.pmod(F.col("h"), F.lit(100)) < 54, "under_review").otherwise("closed"))
        .withColumn("disposition", F.when(F.col("status") != "closed", F.lit(None).cast("string")).when(F.pmod(F.col("h"), F.lit(100)) < 18, "confirmed").otherwise("false_positive"))
        .withColumn("assigned_queue", F.concat(F.lit("RISK_"), F.col("country_code"), F.lit("_L"), (F.pmod(F.col("h"), F.lit(3)) + 1).cast("string")))
        .withColumn("rule_version", F.lit("rules-2026.08"))
        .withColumn("model_version", F.when(F.col("alert_type").isin("account_takeover", "device_risk"), "fraud-model-7").otherwise("aml-model-4"))
        .withColumn("amount_at_risk", F.round(10 + F.pmod(F.col("h"), F.lit(10_000_000)) / 100.0, 2))
        .withColumn("currency_code", F.lit(country["currency"]))
        .withColumn("source_system", F.concat(F.lit("QUILLA_RISK_"), F.col("country_code")))
        .withColumn("synthetic_flag", F.lit(True))
        .withColumn("ingested_at", F.col("created_at") + F.expr("INTERVAL 30 SECONDS"))
        .select("alert_id", "event_id", "party_id", "account_id", "country_code", "alert_type", "severity", "risk_score", "created_at", "status", "disposition", "assigned_queue", "rule_version", "model_version", "amount_at_risk", "currency_code", "source_system", "synthetic_flag", "ingested_at")
    )
    alert_frames.append(alerts)
    alert_offset += rows

risk_alerts_df = reduce(lambda left, right: left.unionByName(right), alert_frames)
write_parquet(risk_alerts_df, cfg, "risk_alerts", ["country_code"])

# COMMAND ----------

consent_base = engagement_backbone(cfg["consent_rows"], 809)
granted_at = F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit("2024-09-01 00:00:00")) + F.pmod(F.col("h"), F.lit(63_071_999))))
consent_status = F.when(F.pmod(F.col("h"), F.lit(100)) < 78, "active").when(F.pmod(F.col("h"), F.lit(100)) < 91, "revoked").otherwise("expired")
consents_df = (
    consent_base
    .withColumn("consent_id", F.format_string("QCNS-%012d", F.col("row_id") + 1))
    .withColumn("purpose_code", F.element_at(F.array(*[F.lit(v) for v in ["account_aggregation", "credit_analysis", "fraud_prevention", "personalization", "marketing"]]), (F.pmod(F.col("h"), F.lit(5)) + 1).cast("int")))
    .withColumn("channel", F.element_at(F.array(F.lit("mobile"), F.lit("web"), F.lit("api")), (F.pmod(F.col("h"), F.lit(3)) + 1).cast("int")))
    .withColumn("status", consent_status)
    .withColumn("granted_at", granted_at)
    .withColumn("expires_at", F.add_months(F.col("granted_at"), 12))
    .withColumn("revoked_at", F.when(F.col("status") == "revoked", F.least(utc_now_literal(), F.col("granted_at") + F.expr("INTERVAL 90 DAYS"))))
    .withColumn("policy_version", F.concat(F.lit("CONSENT-"), F.col("country_code"), F.lit("-2026.1")))
    .withColumn("locale_code", country_value(F.col("country_code"), "locale"))
    .withColumn("source_system", F.concat(F.lit("QUILLA_CONSENT_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select("consent_id", "party_id", "country_code", "purpose_code", "channel", "status", "granted_at", "expires_at", "revoked_at", "policy_version", "locale_code", "source_system", "synthetic_flag", "ingested_at")
)
write_parquet(consents_df, cfg, "consents", ["country_code"])

# COMMAND ----------

interaction_base = engagement_backbone(cfg["interaction_rows"], 907)
intent = F.element_at(F.array(*[F.lit(v) for v in ["cash_flow", "payment_status", "card_support", "credit_offer", "fraud_report", "fx_quote", "insurance_help"]]), (F.pmod(F.col("h"), F.lit(7)) + 1).cast("int"))
message_pt = F.element_at(F.array(*[F.lit(v) for v in ["Quero entender meu fluxo de caixa.", "Onde está meu pagamento?", "Preciso de ajuda com meu cartão.", "Quais opções de crédito tenho?", "Não reconheço esta transação.", "Qual é a cotação de câmbio?", "Preciso de ajuda com meu seguro."]]), (F.pmod(F.col("h"), F.lit(7)) + 1).cast("int"))
message_es = F.element_at(F.array(*[F.lit(v) for v in ["Quiero entender mi flujo de caja.", "¿Dónde está mi pago?", "Necesito ayuda con mi tarjeta.", "¿Qué opciones de crédito tengo?", "No reconozco esta transacción.", "¿Cuál es la cotización de cambio?", "Necesito ayuda con mi seguro."]]), (F.pmod(F.col("h"), F.lit(7)) + 1).cast("int"))
interactions_df = (
    interaction_base
    .withColumn("interaction_id", F.format_string("QINT-%012d", F.col("row_id") + 1))
    .withColumn("interaction_ts", F.to_timestamp(F.from_unixtime(F.unix_timestamp(F.lit("2024-09-01 00:00:00")) + F.pmod(F.col("h"), F.lit(63_071_999)))))
    .withColumn("channel", F.element_at(F.array(*[F.lit(v) for v in ["in_app_chat", "whatsapp_demo", "email", "voice", "web"]]), (F.pmod(F.col("h"), F.lit(5)) + 1).cast("int")))
    .withColumn("language_code", country_value(F.col("country_code"), "language"))
    .withColumn("intent", intent)
    .withColumn("message_text", F.when(F.col("language_code") == "pt", message_pt).otherwise(message_es))
    .withColumn("sentiment_score", F.round((F.pmod(F.col("h"), F.lit(2001)) - 1000) / 1000.0, 3))
    .withColumn("resolution_status", F.when(F.pmod(F.col("h"), F.lit(100)) < 74, "resolved").when(F.pmod(F.col("h"), F.lit(100)) < 92, "escalated").otherwise("open"))
    .withColumn("agent_type", F.when(F.pmod(F.col("h"), F.lit(100)) < 68, "ai_assistant").otherwise("human"))
    .withColumn("source_system", F.concat(F.lit("QUILLA_CX_"), "country_code"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", F.col("interaction_ts") + F.expr("INTERVAL 10 SECONDS"))
    .select("interaction_id", "party_id", "country_code", "interaction_ts", "channel", "language_code", "intent", "message_text", "sentiment_score", "resolution_status", "agent_type", "source_system", "synthetic_flag", "ingested_at")
)
write_parquet(interactions_df, cfg, "interactions", ["country_code"])

# COMMAND ----------

document_base = engagement_backbone(cfg["document_rows"], 1009)
topic_pt = F.element_at(F.array(*[F.lit(v) for v in ["Privacidade", "Crédito responsável", "Prevenção à fraude", "Residência de dados", "Atendimento", "Segurança"]]), (F.pmod(F.col("h"), F.lit(6)) + 1).cast("int"))
topic_es = F.element_at(F.array(*[F.lit(v) for v in ["Privacidad", "Crédito responsable", "Prevención de fraude", "Residencia de datos", "Atención", "Seguridad"]]), (F.pmod(F.col("h"), F.lit(6)) + 1).cast("int"))
documents_df = (
    document_base
    .withColumn("document_id", F.format_string("QDOC-%012d", F.col("row_id") + 1))
    .withColumn("document_type", F.element_at(F.array(F.lit("demo_policy"), F.lit("demo_procedure"), F.lit("demo_control"), F.lit("demo_faq")), (F.pmod(F.col("h"), F.lit(4)) + 1).cast("int")))
    .withColumn("language_code", country_value(F.col("country_code"), "language"))
    .withColumn("topic", F.when(F.col("language_code") == "pt", topic_pt).otherwise(topic_es))
    .withColumn("title", F.when(F.col("language_code") == "pt", F.concat(F.lit("Conteúdo demonstrativo — "), F.col("topic"))).otherwise(F.concat(F.lit("Contenido demostrativo — "), F.col("topic"))))
    .withColumn("body", F.when(F.col("language_code") == "pt", F.concat(F.lit("Exemplo sintético para validar busca, governança e aplicação de políticas da Quilla no mercado "), F.col("country_code"), F.lit(". Não constitui orientação regulatória."))).otherwise(F.concat(F.lit("Ejemplo sintético para validar búsqueda, gobierno y aplicación de políticas de Quilla en el mercado "), F.col("country_code"), F.lit(". No constituye orientación regulatoria."))))
    .withColumn("effective_date", F.date_add(F.to_date(F.lit(EVENT_START)), F.pmod(F.col("h"), F.lit(700)).cast("int")))
    .withColumn("review_date", F.add_months(F.col("effective_date"), 6))
    .withColumn("version", F.concat(F.lit("demo-"), (F.pmod(F.col("h"), F.lit(5)) + 1).cast("string"), F.lit(".0")))
    .withColumn("status", F.when(F.pmod(F.col("h"), F.lit(100)) < 88, "active").otherwise("draft"))
    .withColumn("is_official", F.lit(False))
    .withColumn("disclaimer", F.when(F.col("language_code") == "pt", "DOCUMENTO SINTÉTICO — NÃO OFICIAL").otherwise("DOCUMENTO SINTÉTICO — NO OFICIAL"))
    .withColumn("source_system", F.lit("QUILLA_DEMO_KNOWLEDGE"))
    .withColumn("synthetic_flag", F.lit(True))
    .withColumn("ingested_at", utc_now_literal())
    .select("document_id", "country_code", "document_type", "language_code", "title", "body", "topic", "effective_date", "review_date", "version", "status", "is_official", "disclaimer", "source_system", "synthetic_flag", "ingested_at")
)
write_parquet(documents_df, cfg, "documents", ["country_code"])
