"""Configuração tipada do backend Quilla, local e Databricks Apps."""

from __future__ import annotations

from dataclasses import dataclass
import os


SUPPORTED_COUNTRIES = frozenset(
    {"AR", "BO", "BR", "CL", "CO", "CR", "EC", "MX", "PA", "PE", "PY", "UY"}
)
SUPPORTED_LOCALES = frozenset(
    {"pt-BR", "es-AR", "es-BO", "es-CL", "es-CO", "es-CR", "es-EC", "es-MX", "es-PA", "es-PE", "es-PY", "es-UY"}
)
DEFAULT_LOCALE_BY_COUNTRY = {country: f"es-{country}" for country in SUPPORTED_COUNTRIES}
DEFAULT_LOCALE_BY_COUNTRY["BR"] = "pt-BR"


def _boolean(name: str, default: bool = False) -> bool:
    return os.getenv(name, str(default)).strip().lower() in {"1", "true", "yes", "on"}


def _integer(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    tenant_id: str
    supervisor_endpoint: str
    request_timeout_seconds: int
    heartbeat_seconds: int
    allow_demo_context: bool
    demo_actor: str
    demo_roles: tuple[str, ...]
    demo_mode: bool
    default_country: str
    database_required: bool
    lakebase_host: str
    lakebase_port: int
    lakebase_database: str
    lakebase_user: str
    db_pool_min_size: int
    db_pool_max_size: int
    token_refresh_seconds: int
    review_queue_name: str
    is_databricks_app: bool
    runtime_mode: str
    obo_required: bool

    @classmethod
    def from_env(cls) -> "Settings":
        country = os.getenv("QUILLA_DEFAULT_COUNTRY", "BR").upper()
        if country not in SUPPORTED_COUNTRIES:
            country = "BR"
        return cls(
            tenant_id=os.getenv("QUILLA_TENANT_ID", "quilla-demo"),
            supervisor_endpoint=os.getenv("QUILLA_SUPERVISOR_ENDPOINT", "quilla-custom-supervisor"),
            request_timeout_seconds=max(5, _integer("QUILLA_CHAT_TIMEOUT_SECONDS", 180)),
            heartbeat_seconds=max(1, _integer("QUILLA_HEARTBEAT_SECONDS", 5)),
            allow_demo_context=_boolean("QUILLA_ALLOW_DEMO_CONTEXT"),
            demo_actor=os.getenv("QUILLA_DEMO_ACTOR", "quilla-demo-user"),
            demo_roles=tuple(filter(None, os.getenv("QUILLA_DEMO_ROLES", "demo-user").split(","))),
            demo_mode=_boolean("QUILLA_DEMO_MODE", True),
            default_country=country,
            database_required=_boolean("QUILLA_DATABASE_REQUIRED", True),
            lakebase_host=os.getenv("PGHOST", os.getenv("QUILLA_LAKEBASE_HOST", "")),
            lakebase_port=_integer("PGPORT", 5432),
            lakebase_database=os.getenv("PGDATABASE", os.getenv("QUILLA_LAKEBASE_DATABASE", "quilla")),
            lakebase_user=os.getenv("PGUSER", ""),
            db_pool_min_size=max(1, _integer("QUILLA_DB_POOL_MIN_SIZE", 1)),
            db_pool_max_size=max(1, _integer("QUILLA_DB_POOL_MAX_SIZE", 5)),
            token_refresh_seconds=max(300, _integer("QUILLA_DB_TOKEN_REFRESH_SECONDS", 2700)),
            review_queue_name=os.getenv("QUILLA_REVIEW_QUEUE", "Quilla Human Review"),
            is_databricks_app=bool(os.getenv("DATABRICKS_APP_NAME")),
            runtime_mode=os.getenv("QUILLA_RUNTIME_MODE", "in_process").strip().lower(),
            obo_required=_boolean("QUILLA_OBO_REQUIRED", True),
        )


settings = Settings.from_env()
