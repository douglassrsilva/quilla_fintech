# Databricks notebook source
# MAGIC %run ./_common

# COMMAND ----------
# MAGIC %run ./_training

# COMMAND ----------

cfg = config()
experiment_path = widget("experiment_path", "/Shared/quilla/ml")
model_key = widget("model_key", "transaction_fraud")
allowed_models = {"transaction_fraud", "account_takeover", "aml_anomaly", "alert_confirmation"}
if model_key not in allowed_models:
    raise ValueError(f"Unsupported realtime-risk model_key: {model_key!r}")
results = []

if model_key == "transaction_fraud":
    transactions = (
        spark.read.table(ml_table(cfg, "fs_transaction_context"))
        .join(spark.read.table(ml_table(cfg, "labels_transaction_fraud")), ["event_id", "country_code", "event_ts_utc"])
        .withColumn("is_cross_border_num", F.col("is_cross_border").cast("double"))
    )
    results.append(
        train_tabular(
            cfg, model_key, transactions, "fraud_label", "binary",
            ["amount_usd", "amount_log", "event_hour", "event_day_of_week", "product_id", "is_cross_border_num"],
            ["country_code", "event_type", "event_status", "channel", "direction", "party_risk_band", "customer_segment", "device_trust_status", "device_type", "operating_system", "merchant_risk_band", "merchant_category", "mcc"],
            time_col="event_ts_utc", max_rows=350_000, sample_fraction=0.15, experiment_path=experiment_path,
        )
    )
elif model_key == "account_takeover":
    security = (
        spark.read.table(ml_table(cfg, "source_security_sessions"))
        .withColumn("new_device_num", F.col("new_device").cast("double"))
        .withColumn("impossible_travel_num", F.col("impossible_travel").cast("double"))
        .withColumn("credential_reset_num", F.col("credential_reset_24h").cast("double"))
    )
    results.append(
        train_tabular(
            cfg, model_key, security, "account_takeover_label", "binary",
            ["geo_distance_km", "minutes_since_previous_session", "failed_logins_1h", "sessions_10m", "device_trust_score", "new_device_num", "impossible_travel_num", "credential_reset_num"],
            ["country_code", "channel", "network_type", "mfa_method", "mfa_result"],
            time_col="session_ts", max_rows=300_000, sample_fraction=0.5, experiment_path=experiment_path,
        )
    )
else:
    alert_scope = (
        spark.read.table(ml_table(cfg, "source_case_outcomes"))
        .filter(F.col("alert_id").isNotNull())
        .select("alert_id", "confirmed_flag")
    )
    alerts = spark.read.table(source_table(cfg, "risk_alerts")).join(
        alert_scope.select("alert_id"), "alert_id", "inner"
    )
    if model_key == "aml_anomaly":
        results.append(
            train_anomaly_model(
                cfg, model_key, alerts.filter(F.col("alert_type") == "aml_pattern"),
                ["risk_score", "amount_at_risk"], max_rows=200_000, experiment_path=experiment_path,
            )
        )
    else:
        alert_training = alerts.join(alert_scope, "alert_id")
        results.append(
            train_tabular(
                cfg, model_key, alert_training, "confirmed_flag", "binary",
                ["risk_score", "amount_at_risk"],
                ["country_code", "alert_type", "severity", "assigned_queue"],
                time_col="created_at", max_rows=220_000, experiment_path=experiment_path,
            )
        )

print(json.dumps({"status": "complete", "family": "realtime_risk", "model_key": model_key, "models": results}, default=str))
