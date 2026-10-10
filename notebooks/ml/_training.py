# Databricks notebook source
"""Reusable MLflow training, evaluation and UC registration helpers."""

import json
from datetime import datetime, timezone

import mlflow
import mlflow.sklearn
import numpy as np
import pandas as pd
from mlflow import MlflowClient
from mlflow.models import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingClassifier, HistGradientBoostingRegressor, IsolationForest
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    average_precision_score,
    f1_score,
    mean_absolute_error,
    mean_squared_error,
    r2_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder, StandardScaler


def _registered_name(cfg, model_key: str) -> str:
    return f"{cfg['catalog']}.{cfg['ml_schema']}.quilla_{model_key}"


def _prepare_pandas(frame, columns, label, max_rows, seed, sample_fraction=1.0):
    selected = frame.select(*columns, label).filter(F.col(label).isNotNull())
    if sample_fraction < 1.0:
        selected = selected.sample(False, sample_fraction, seed)
    pdf = selected.limit(max_rows).toPandas()
    if len(pdf) < 100:
        raise ValueError(f"Insufficient training rows for {label}: {len(pdf)}")
    return pdf


def _temporal_split(pdf, time_col, seed=42):
    if time_col and time_col in pdf.columns:
        times = pd.to_datetime(pdf[time_col], errors="coerce")
        cutoff = times.quantile(0.8)
        train_mask = times <= cutoff
        test_mask = times > cutoff
        if train_mask.sum() >= 50 and test_mask.sum() >= 20:
            return train_mask.to_numpy(), test_mask.to_numpy(), str(cutoff)
    rng = np.random.default_rng(seed)
    indices = rng.permutation(len(pdf))
    boundary = max(1, int(len(pdf) * 0.8))
    train_mask = np.zeros(len(pdf), dtype=bool)
    train_mask[indices[:boundary]] = True
    return train_mask, ~train_mask, "deterministic_random_80_20"


def _tabular_pipeline(numeric_features, categorical_features, kind):
    numeric = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
    ])
    categorical = Pipeline([
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("encoder", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1)),
    ])
    preprocessor = ColumnTransformer(
        [("numeric", numeric, numeric_features), ("categorical", categorical, categorical_features)],
        remainder="drop",
        sparse_threshold=0,
    )
    if kind in ("binary", "multiclass"):
        estimator = HistGradientBoostingClassifier(
            learning_rate=0.08,
            max_iter=120,
            max_leaf_nodes=31,
            min_samples_leaf=30,
            l2_regularization=0.2,
            class_weight="balanced" if kind == "binary" else None,
            random_state=42,
        )
    elif kind == "regression":
        estimator = HistGradientBoostingRegressor(
            learning_rate=0.07,
            max_iter=140,
            max_leaf_nodes=31,
            min_samples_leaf=30,
            l2_regularization=0.2,
            random_state=42,
        )
    else:
        raise ValueError(f"Unsupported kind: {kind}")
    return Pipeline([("preprocessor", preprocessor), ("model", estimator)])


def _metrics(kind, y_true, predictions, probabilities=None):
    if kind == "binary":
        result = {
            "f1": float(f1_score(y_true, predictions, zero_division=0)),
            "recall": float(recall_score(y_true, predictions, zero_division=0)),
            "accuracy": float(accuracy_score(y_true, predictions)),
        }
        if len(np.unique(y_true)) > 1 and probabilities is not None:
            result["roc_auc"] = float(roc_auc_score(y_true, probabilities))
            result["pr_auc"] = float(average_precision_score(y_true, probabilities))
        return result
    if kind == "multiclass":
        return {
            "accuracy": float(accuracy_score(y_true, predictions)),
            "macro_f1": float(f1_score(y_true, predictions, average="macro", zero_division=0)),
        }
    return {
        "mae": float(mean_absolute_error(y_true, predictions)),
        "rmse": float(mean_squared_error(y_true, predictions) ** 0.5),
        "r2": float(r2_score(y_true, predictions)),
    }


def _country_slices(kind, country_values, y_true, predictions, probabilities=None):
    if country_values is None:
        return {}
    result = {}
    country_values = np.asarray(country_values)
    y_true = np.asarray(y_true)
    predictions = np.asarray(predictions)
    probabilities = np.asarray(probabilities) if probabilities is not None else None
    for country in sorted(pd.Series(country_values).dropna().unique()):
        mask = country_values == country
        if mask.sum() < 10:
            continue
        country_probabilities = probabilities[mask] if probabilities is not None else None
        result[str(country)] = {"rows": int(mask.sum()), **_metrics(kind, y_true[mask], predictions[mask], country_probabilities)}
    return result


def _promote_alias(model_name, version):
    client = MlflowClient()
    try:
        previous = client.get_model_version_by_alias(model_name, "Champion")
        if str(previous.version) != str(version):
            client.set_registered_model_alias(model_name, "Challenger", previous.version)
    except Exception:
        pass
    client.set_registered_model_alias(model_name, "Champion", version)


def _record_result(cfg, model_key, kind, run_id, version, metrics, rows, trace_id=None):
    payload = [(
        cfg["snapshot"],
        model_key,
        kind,
        run_id,
        str(version),
        trace_id,
        int(rows),
        json.dumps(metrics, sort_keys=True),
        datetime.now(timezone.utc),
    )]
    result = spark.createDataFrame(
        payload,
        "snapshot string, model_key string, model_kind string, mlflow_run_id string, model_version string, trace_id string, training_rows long, metrics_json string, recorded_at timestamp",
    )
    result.write.mode("append").option("mergeSchema", "true").saveAsTable(ml_table(cfg, "model_training_results"))


def train_tabular(
    cfg,
    model_key,
    frame,
    label,
    kind,
    numeric_features,
    categorical_features,
    time_col=None,
    country_col="country_code",
    max_rows=250_000,
    sample_fraction=1.0,
    experiment_path="/Shared/quilla/ml",
):
    feature_columns = list(numeric_features) + list(categorical_features)
    auxiliary = [column for column in [time_col, country_col] if column and column not in feature_columns]
    pdf = _prepare_pandas(frame, feature_columns + auxiliary, label, max_rows, cfg["seed"], sample_fraction)
    train_mask, test_mask, split_description = _temporal_split(pdf, time_col, cfg["seed"])

    X = pdf[feature_columns]
    y = pdf[label]
    if kind == "binary":
        y = y.astype(int)
    model = _tabular_pipeline(numeric_features, categorical_features, kind)

    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment_path)
    model_name = _registered_name(cfg, model_key)
    with mlflow.start_run(run_name=model_key) as run:
        mlflow.set_tags({
            "project": "quilla",
            "model_family": model_key,
            "data_classification": "synthetic",
            "governance": "unity_catalog",
        })
        mlflow.log_params({
            "model_kind": kind,
            "split": split_description,
            "training_rows": int(train_mask.sum()),
            "validation_rows": int(test_mask.sum()),
            "feature_count": len(feature_columns),
        })
        model.fit(X.loc[train_mask], y.loc[train_mask])
        with mlflow.start_span(name="validation_batch_inference", span_type="MODEL") as span:
            span.set_inputs({"model_key": model_key, "rows": int(test_mask.sum()), "feature_count": len(feature_columns)})
            predictions = model.predict(X.loc[test_mask])
            probabilities = None
            if kind == "binary":
                probabilities = model.predict_proba(X.loc[test_mask])[:, 1]
            span.set_outputs({"prediction_rows": len(predictions), "prediction_sample": np.asarray(predictions[:5]).tolist()})
            trace_id = getattr(span, "trace_id", None)
        metrics = _metrics(kind, y.loc[test_mask], predictions, probabilities)
        mlflow.log_metrics(metrics)
        country_values = pdf.loc[test_mask, country_col].to_numpy() if country_col in pdf.columns else None
        slices = _country_slices(kind, country_values, y.loc[test_mask].to_numpy(), predictions, probabilities)
        mlflow.log_dict(slices, "evaluation/country_slices.json")
        sample = X.loc[train_mask].head(20)
        signature = infer_signature(sample, model.predict(sample))
        model_info = mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            registered_model_name=model_name,
            signature=signature,
            input_example=sample.head(5),
            await_registration_for=600,
        )
        version = model_info.registered_model_version
        _promote_alias(model_name, version)
        _record_result(cfg, model_key, kind, run.info.run_id, version, metrics, len(pdf), trace_id)
        return {"model": model_name, "version": str(version), "run_id": run.info.run_id, "metrics": metrics}


def train_text_classifier(
    cfg,
    model_key,
    frame,
    label,
    text_col,
    categorical_features,
    time_col=None,
    country_col="country_code",
    max_rows=150_000,
    experiment_path="/Shared/quilla/ml",
):
    features = [text_col] + list(categorical_features)
    auxiliary = [column for column in [time_col, country_col] if column and column not in features]
    pdf = _prepare_pandas(frame, features + auxiliary, label, max_rows, cfg["seed"])
    train_mask, test_mask, split_description = _temporal_split(pdf, time_col, cfg["seed"])
    preprocessor = ColumnTransformer(
        [
            ("text", TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=2, max_features=30_000), text_col),
            ("categorical", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), categorical_features),
        ]
    )
    model = Pipeline([
        ("preprocessor", preprocessor),
        ("model", LogisticRegression(max_iter=400, class_weight="balanced", random_state=42)),
    ])
    y = pdf[label].astype(str)
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment_path)
    model_name = _registered_name(cfg, model_key)
    with mlflow.start_run(run_name=model_key) as run:
        mlflow.set_tags({"project": "quilla", "model_family": model_key, "language": "pt-es", "data_classification": "synthetic"})
        mlflow.log_params({"model_kind": "multiclass_text", "split": split_description, "training_rows": int(train_mask.sum())})
        model.fit(pdf.loc[train_mask, features], y.loc[train_mask])
        with mlflow.start_span(name="validation_batch_inference", span_type="MODEL") as span:
            span.set_inputs({"model_key": model_key, "rows": int(test_mask.sum()), "language": "pt-es"})
            predictions = model.predict(pdf.loc[test_mask, features])
            span.set_outputs({"prediction_rows": len(predictions), "prediction_sample": np.asarray(predictions[:5]).tolist()})
            trace_id = getattr(span, "trace_id", None)
        metrics = _metrics("multiclass", y.loc[test_mask], predictions)
        mlflow.log_metrics(metrics)
        sample = pdf.loc[train_mask, features].head(20)
        signature = infer_signature(sample, model.predict(sample))
        model_info = mlflow.sklearn.log_model(
            sk_model=model,
            name="model",
            registered_model_name=model_name,
            signature=signature,
            input_example=sample.head(5),
            await_registration_for=600,
        )
        version = model_info.registered_model_version
        _promote_alias(model_name, version)
        _record_result(cfg, model_key, "multiclass_text", run.info.run_id, version, metrics, len(pdf), trace_id)
        return {"model": model_name, "version": str(version), "run_id": run.info.run_id, "metrics": metrics}


def train_anomaly_model(
    cfg,
    model_key,
    frame,
    numeric_features,
    max_rows=200_000,
    sample_fraction=1.0,
    experiment_path="/Shared/quilla/ml",
):
    pdf = frame.select(*numeric_features).dropna().sample(False, sample_fraction, cfg["seed"]).limit(max_rows).toPandas()
    pipeline = Pipeline([
        ("imputer", SimpleImputer(strategy="median")),
        ("scaler", StandardScaler()),
        ("model", IsolationForest(n_estimators=180, contamination=0.03, random_state=42, n_jobs=-1)),
    ])
    mlflow.set_registry_uri("databricks-uc")
    mlflow.set_experiment(experiment_path)
    model_name = _registered_name(cfg, model_key)
    with mlflow.start_run(run_name=model_key) as run:
        pipeline.fit(pdf)
        with mlflow.start_span(name="validation_batch_inference", span_type="MODEL") as span:
            span.set_inputs({"model_key": model_key, "rows": len(pdf), "feature_count": len(numeric_features)})
            predictions = pipeline.predict(pdf)
            span.set_outputs({"prediction_rows": len(predictions), "prediction_sample": np.asarray(predictions[:5]).tolist()})
            trace_id = getattr(span, "trace_id", None)
        anomaly_rate = float((predictions == -1).mean())
        metrics = {"anomaly_rate": anomaly_rate}
        mlflow.log_metrics(metrics)
        mlflow.set_tags({"project": "quilla", "model_family": model_key, "model_kind": "unsupervised_anomaly"})
        signature = infer_signature(pdf.head(20), pipeline.predict(pdf.head(20)))
        model_info = mlflow.sklearn.log_model(
            sk_model=pipeline,
            name="model",
            registered_model_name=model_name,
            signature=signature,
            input_example=pdf.head(5),
            await_registration_for=600,
        )
        version = model_info.registered_model_version
        _promote_alias(model_name, version)
        _record_result(cfg, model_key, "anomaly", run.info.run_id, version, metrics, len(pdf), trace_id)
        return {"model": model_name, "version": str(version), "run_id": run.info.run_id, "metrics": metrics}
