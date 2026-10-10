"""Persistência conversacional LTAP no Lakebase Autoscaling.

O Lakebase mantém o histórico recuperável pelo App. Managed Memory continua
responsável por lembranças semânticas de longo prazo; os dois papéis não se
misturam.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
import subprocess
from typing import Any, Iterable
import uuid

from .contracts import SupervisorResult, TrustedContext


SESSION_NAMESPACE = uuid.UUID("f250df8e-418f-4c4c-86d1-6f30f6f17e21")


def session_uuid(context: TrustedContext) -> uuid.UUID:
    """Deriva uma chave estável sem aceitar o ID externo como autorização."""
    if not context.conversation_id:
        raise ValueError("conversation_id is required for Lakebase persistence")
    material = "\x1f".join(
        (context.tenant_id, context.user_id, context.conversation_id)
    )
    return uuid.uuid5(SESSION_NAMESPACE, material)


def message_text(item: dict[str, Any]) -> str:
    content = item.get("content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            str(part.get("text", ""))
            for part in content
            if isinstance(part, dict) and part.get("text")
        )
    return str(content) if content is not None else ""


def merge_history(
    stored: list[dict[str, Any]], incoming: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Restaura histórico quando o cliente envia somente o novo turno."""
    if not stored or len(incoming) != 1:
        return incoming
    current = incoming[0]
    if current.get("role") != "user":
        return incoming
    if (
        stored[-1].get("role") == "user"
        and message_text(stored[-1]).strip() == message_text(current).strip()
    ):
        return stored
    return [*stored, current]


@dataclass(frozen=True)
class MemoryReceipt:
    session_id: str
    persisted_messages: int
    status: str = "PERSISTED"

    def as_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "session_id": self.session_id,
            "persisted_messages": self.persisted_messages,
            "store": "lakebase",
        }


class NullConversationStore:
    enabled = False
    required = False

    def restore(
        self, context: TrustedContext, incoming: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        return incoming

    def save_exchange(
        self,
        context: TrustedContext,
        history: list[dict[str, Any]],
        result: SupervisorResult,
    ) -> MemoryReceipt | None:
        return None


class LakebaseConversationStore:
    """Repositório síncrono e curto, adequado ao contrato MLflow pyfunc."""

    enabled = True

    def __init__(
        self,
        *,
        project: str,
        branch: str,
        endpoint_id: str,
        database: str,
        profile: str | None = None,
        required: bool = True,
        explicit_host: str | None = None,
        explicit_user: str | None = None,
        explicit_authorization: str | None = None,
    ):
        self.project = project
        self.branch = branch
        self.endpoint_id = endpoint_id
        self.database = database
        self.profile = profile
        self.required = required
        self.explicit_host = (explicit_host or "").strip()
        self.explicit_user = (explicit_user or "").strip()
        self.explicit_authorization = (explicit_authorization or "").strip()

    @classmethod
    def from_env(cls):
        if os.getenv("QUILLA_MEMORY_ENABLED", "false").lower() != "true":
            return NullConversationStore()
        return cls(
            project=os.getenv("QUILLA_LAKEBASE_PROJECT", "quilla-ltap"),
            branch=os.getenv("QUILLA_LAKEBASE_BRANCH", "production"),
            endpoint_id=os.getenv("QUILLA_LAKEBASE_ENDPOINT", "primary"),
            database=os.getenv("QUILLA_LAKEBASE_DATABASE", "quilla"),
            profile=os.getenv("DATABRICKS_CONFIG_PROFILE")
            or os.getenv("DATABRICKS_PROFILE"),
            required=os.getenv("QUILLA_MEMORY_REQUIRED", "true").lower() == "true",
        )

    @classmethod
    def for_app(cls, *, user_id: str, authorization: str):
        """Build a request-scoped store from Databricks Apps trusted OBO context."""
        if os.getenv("QUILLA_MEMORY_ENABLED", "false").lower() != "true":
            return NullConversationStore()
        return cls(
            project=os.getenv("QUILLA_LAKEBASE_PROJECT", "quilla-ltap"),
            branch=os.getenv("QUILLA_LAKEBASE_BRANCH", "production"),
            endpoint_id=os.getenv("QUILLA_LAKEBASE_ENDPOINT", "primary"),
            database=os.getenv("PGDATABASE", os.getenv("QUILLA_LAKEBASE_DATABASE", "quilla")),
            required=os.getenv("QUILLA_MEMORY_REQUIRED", "true").lower() == "true",
            explicit_host=os.getenv("PGHOST", os.getenv("QUILLA_LAKEBASE_HOST", "")),
            explicit_user=user_id,
            explicit_authorization=authorization,
        )

    @property
    def endpoint_name(self) -> str:
        return (
            f"projects/{self.project}/branches/{self.branch}/"
            f"endpoints/{self.endpoint_id}"
        )

    def _workspace_client(self):
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.credentials_provider import ModelServingUserCredentials

        return (
            WorkspaceClient(profile=self.profile)
            if self.profile
            else WorkspaceClient(credentials_strategy=ModelServingUserCredentials())
        )

    def _connection(self):
        import psycopg

        parent = f"projects/{self.project}/branches/{self.branch}"
        if self.explicit_authorization:
            host = self.explicit_host or os.getenv("PGHOST", "").strip() or os.getenv(
                "QUILLA_LAKEBASE_HOST", ""
            ).strip()
            user = self.explicit_user
            token = (
                self.explicit_authorization.split(" ", 1)[1].strip()
                if self.explicit_authorization.lower().startswith("bearer ")
                else self.explicit_authorization
            )
            if not host or not user or not token:
                raise RuntimeError("Explicit Lakebase OBO identity is incomplete")
        elif self.profile:
            def cli(*arguments: str):
                completed = subprocess.run(
                    ["databricks", *arguments, "--profile", self.profile, "-o", "json"],
                    check=True,
                    capture_output=True,
                    text=True,
                    env={**os.environ, "DATABRICKS_AUTH_STORAGE": "plaintext"},
                )
                return json.loads(completed.stdout)

            endpoints = cli("postgres", "list-endpoints", parent)
            endpoint = next(item for item in endpoints if item["name"] == self.endpoint_name)
            credential = cli(
                "postgres", "generate-database-credential", self.endpoint_name
            )
            principal = cli("current-user", "me")
            host = endpoint["status"]["hosts"]["host"]
            token = credential["token"]
            user = principal["userName"]
        else:
            client = self._workspace_client()
            principal = client.current_user.me()
            user = principal.user_name or principal.display_name
            host = os.getenv("QUILLA_LAKEBASE_HOST", "").strip()
            authorization = client.config.authenticate().get("Authorization", "")
            token = authorization.removeprefix("Bearer ").strip()
            if not user or not host or not token:
                raise RuntimeError(
                    "Lakebase host or OBO authenticated principal is incomplete"
                )
        return psycopg.connect(
            host=host,
            port=5432,
            dbname=self.database,
            user=user,
            password=token,
            sslmode="require",
            connect_timeout=20,
        )

    @staticmethod
    def _language(locale: str) -> str:
        return "pt" if locale.startswith("pt") else "es"

    def restore(
        self, context: TrustedContext, incoming: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        session_id = session_uuid(context)
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT m.role, m.content
                FROM quilla_app.app_messages m
                JOIN quilla_app.app_sessions s ON s.session_id = m.session_id
                WHERE s.session_id = %s AND s.tenant_id = %s AND s.actor_id = %s
                  AND s.deleted_at IS NULL
                ORDER BY m.sequence_number
                LIMIT 100
                """,
                (session_id, context.tenant_id, context.user_id),
            )
            stored = [
                {"role": role, "content": content}
                for role, content in cursor.fetchall()
                if role in {"user", "assistant"}
            ]
        return merge_history(stored, incoming)

    def save_exchange(
        self,
        context: TrustedContext,
        history: list[dict[str, Any]],
        result: SupervisorResult,
    ) -> MemoryReceipt:
        session_id = session_uuid(context)
        language = self._language(context.locale)
        messages = [
            item
            for item in history
            if item.get("role") in {"user", "assistant"} and message_text(item).strip()
        ]
        messages.append({"role": "assistant", "content": result.text})
        first_user = next(
            (message_text(item).strip() for item in messages if item.get("role") == "user"),
            "Nova conversa",
        )
        title = first_user[:160]
        citations = [evidence.__dict__ for evidence in result.tool_evidence]
        session_context = {
            "roles": list(context.roles),
            "demo": context.demo,
            "experience": context.experience,
            "last_route": result.route.primary,
        }

        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO quilla_app.app_sessions AS existing (
                  session_id, tenant_id, external_conversation_id, actor_id,
                  country_code, locale, title, context
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (session_id) DO UPDATE SET
                  country_code = EXCLUDED.country_code,
                  locale = EXCLUDED.locale,
                  title = COALESCE(existing.title, EXCLUDED.title),
                  context = existing.context || EXCLUDED.context,
                  updated_at = now(),
                  last_activity_at = now()
                """,
                (
                    session_id,
                    context.tenant_id,
                    context.conversation_id,
                    context.user_id,
                    context.country_code[:2],
                    context.locale,
                    title,
                    json.dumps(session_context, ensure_ascii=False),
                ),
            )

            for sequence, item in enumerate(messages, start=1):
                role = str(item["role"])
                content = message_text(item)
                message_id = uuid.uuid5(session_id, f"{sequence}:{role}")
                current_citations = citations if sequence == len(messages) else []
                metadata = (
                    {
                        "route": result.route.primary,
                        "models": result.model_sequence,
                        "warnings": result.warnings,
                        "requires_human_review": result.requires_human_review,
                    }
                    if sequence == len(messages)
                    else {}
                )
                cursor.execute(
                    """
                    INSERT INTO quilla_app.app_messages AS existing (
                      message_id, session_id, sequence_number, role, content,
                      language_code, model_endpoint, mlflow_trace_id, citations, metadata
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb)
                    ON CONFLICT (session_id, sequence_number) DO UPDATE SET
                      role = EXCLUDED.role,
                      content = EXCLUDED.content,
                      language_code = EXCLUDED.language_code,
                      model_endpoint = COALESCE(EXCLUDED.model_endpoint, existing.model_endpoint),
                      mlflow_trace_id = COALESCE(EXCLUDED.mlflow_trace_id, existing.mlflow_trace_id),
                      citations = CASE WHEN EXCLUDED.citations = '[]'::jsonb
                        THEN existing.citations ELSE EXCLUDED.citations END,
                      metadata = existing.metadata || EXCLUDED.metadata
                    """,
                    (
                        message_id,
                        session_id,
                        sequence,
                        role,
                        content,
                        language,
                        result.model_sequence[-1] if role == "assistant" and result.model_sequence else None,
                        result.trace_id if sequence == len(messages) else None,
                        json.dumps(current_citations, ensure_ascii=False),
                        json.dumps(metadata, ensure_ascii=False),
                    ),
                )

            run_id = uuid.UUID(result.trace_id)
            cursor.execute(
                """
                INSERT INTO quilla_app.agent_runs (
                  run_id, session_id, agent_name, mlflow_trace_id, status,
                  country_code, language_code, input_payload, output_payload,
                  risk_level, human_review_required, started_at, completed_at
                ) VALUES (
                  %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb,
                  %s, %s, now(), now()
                )
                ON CONFLICT (run_id) DO UPDATE SET
                  status = EXCLUDED.status,
                  output_payload = EXCLUDED.output_payload,
                  completed_at = EXCLUDED.completed_at
                """,
                (
                    run_id,
                    session_id,
                    result.route.primary,
                    result.trace_id,
                    "review_required" if result.requires_human_review else "succeeded",
                    context.country_code[:2],
                    language,
                    json.dumps({"message_count": len(history)}, ensure_ascii=False),
                    json.dumps(
                        {
                            "models": result.model_sequence,
                            "tools": citations,
                            "warnings": result.warnings,
                        },
                        ensure_ascii=False,
                    ),
                    "high" if result.requires_human_review else "low",
                    result.requires_human_review,
                ),
            )
            connection.commit()
        return MemoryReceipt(str(session_id), len(messages))

    def list_sessions(
        self, *, tenant_id: str, actor_id: str, limit: int = 50
    ) -> list[dict[str, Any]]:
        safe_limit = max(1, min(limit, 100))
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT s.session_id, s.external_conversation_id, s.title,
                       s.country_code, s.locale, s.status, s.created_at,
                       s.last_activity_at, count(m.message_id) AS message_count
                FROM quilla_app.app_sessions s
                LEFT JOIN quilla_app.app_messages m ON m.session_id = s.session_id
                WHERE s.tenant_id = %s AND s.actor_id = %s AND s.deleted_at IS NULL
                GROUP BY s.session_id
                ORDER BY s.last_activity_at DESC
                LIMIT %s
                """,
                (tenant_id, actor_id, safe_limit),
            )
            columns = [description.name for description in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_conversation(
        self, *, session_id: str, tenant_id: str, actor_id: str
    ) -> dict[str, Any] | None:
        parsed_session_id = uuid.UUID(session_id)
        with self._connection() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT session_id, external_conversation_id, title, country_code,
                       locale, status, context, created_at, last_activity_at
                FROM quilla_app.app_sessions
                WHERE session_id = %s AND tenant_id = %s AND actor_id = %s
                  AND deleted_at IS NULL
                """,
                (parsed_session_id, tenant_id, actor_id),
            )
            session = cursor.fetchone()
            if session is None:
                return None
            session_columns = [description.name for description in cursor.description]
            payload = dict(zip(session_columns, session))
            cursor.execute(
                """
                SELECT message_id, sequence_number, role, content, language_code,
                       model_endpoint, mlflow_trace_id, citations, metadata, created_at
                FROM quilla_app.app_messages
                WHERE session_id = %s
                ORDER BY sequence_number
                """,
                (parsed_session_id,),
            )
            message_columns = [description.name for description in cursor.description]
            payload["messages"] = [
                dict(zip(message_columns, row)) for row in cursor.fetchall()
            ]
            return payload


def json_ready(value: Any) -> Any:
    """Normaliza UUID/datetime para respostas FastAPI sem acoplar ao framework."""
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    if isinstance(value, Iterable) and not isinstance(value, (str, bytes)):
        return [json_ready(item) for item in value]
    if isinstance(value, (uuid.UUID,)) or hasattr(value, "isoformat"):
        return str(value)
    return value
