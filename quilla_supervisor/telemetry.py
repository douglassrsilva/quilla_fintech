"""Tracing MLflow opcional para que testes puros não dependam do pacote."""

from __future__ import annotations

from contextlib import contextmanager


@contextmanager
def span(name: str, span_type: str = "CHAIN"):
    try:
        import mlflow
    except ImportError:
        mlflow = None
    if mlflow is None:
        yield None
        return
    with mlflow.start_span(name=name, span_type=span_type) as active:
        yield active


def attributes(active, values: dict) -> None:
    if active is not None:
        active.set_attributes(values)
