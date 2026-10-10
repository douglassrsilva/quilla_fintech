"""Runtime único do App e fallback remoto do MLflow ResponsesAgent."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any

import httpx

from .config import settings


class SupervisorError(RuntimeError):
    pass


@dataclass(frozen=True)
class SupervisorResponse:
    text: str
    trace_id: str | None
    route: str | None
    evidence: list[dict[str, Any]]
    requires_human_review: bool
    raw_custom_outputs: dict[str, Any]


def _answer_text(payload: dict[str, Any]) -> str:
    return "\n".join(
        content.get("text", "")
        for item in payload.get("output", [])
        if item.get("type") == "message"
        for content in item.get("content", [])
        if content.get("type") == "output_text" and content.get("text")
    )


class SupervisorClient:
    def __init__(self, transport: httpx.AsyncBaseTransport | None = None):
        self.transport = transport

    @staticmethod
    def _auth() -> tuple[str, str]:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
        host = client.config.host.rstrip("/")
        authorization = client.config.authenticate().get("Authorization", "")
        if not host or not authorization:
            raise SupervisorError("Databricks endpoint authentication is unavailable")
        return host, authorization

    @staticmethod
    def _body(*, message: str, conversation_id: str, actor_id: str,
              tenant_id: str, roles: tuple[str, ...], demo: bool,
              country_code: str, locale: str, experience: str) -> dict[str, Any]:
        return {
            "input": [{"role": "user", "content": [{"type": "input_text", "text": message}]}],
            "context": {"user_id": actor_id, "conversation_id": conversation_id},
            "custom_inputs": {
                "conversation_id": conversation_id,
                "tenant_id": tenant_id,
                "country_code": country_code,
                "locale": locale,
                "roles": list(roles),
                "demo": demo,
                "experience": experience,
            },
        }

    @staticmethod
    def _parse(payload: dict[str, Any]) -> SupervisorResponse:
        text = _answer_text(payload)
        if not text:
            raise SupervisorError("Supervisor returned no assistant message")
        custom = payload.get("custom_outputs") or {}
        return SupervisorResponse(
            text=text,
            trace_id=custom.get("trace_id"),
            route=custom.get("route"),
            evidence=list(custom.get("tools") or []),
            requires_human_review=bool(custom.get("requires_human_review")),
            raw_custom_outputs=custom,
        )

    @staticmethod
    def _invoke_local(
        body: dict[str, Any],
        *,
        host: str,
        authorization: str,
    ) -> dict[str, Any]:
        """Run the same MLflow contract inside the Databricks App process."""
        from mlflow.types.responses import ResponsesAgentRequest

        from quilla_supervisor.agent import QuillaSupervisor
        from quilla_supervisor.agents_sdk import AgentsSDKGateway
        from quilla_supervisor.auth import DatabricksAuth
        from quilla_supervisor.config import SupervisorSettings
        from quilla_supervisor.memory import LakebaseConversationStore
        from quilla_supervisor.model import QuillaResponsesAgent

        context = body.get("context") or {}
        actor_id = str(context.get("user_id") or "")
        auth = DatabricksAuth(authorization=authorization, workspace_host=host)
        supervisor_settings = SupervisorSettings.from_env()
        supervisor = QuillaSupervisor(
            settings=supervisor_settings,
            auth=auth,
            gateway=AgentsSDKGateway(auth, supervisor_settings.max_output_tokens),
        )
        store = LakebaseConversationStore.for_app(
            user_id=actor_id,
            authorization=authorization,
        )
        response = QuillaResponsesAgent(
            supervisor=supervisor,
            conversation_store=store,
        ).predict(ResponsesAgentRequest(**body))
        return response.model_dump(exclude_none=True)

    async def invoke(self, *, message: str, conversation_id: str, actor_id: str,
                     tenant_id: str, roles: tuple[str, ...], demo: bool,
                     country_code: str, locale: str, request_id: str,
                     experience: str = "general",
                     user_authorization: str | None = None) -> SupervisorResponse:
        host, service_authorization = await asyncio.to_thread(self._auth)
        authorization = user_authorization or service_authorization
        if settings.runtime_mode == "in_process" and settings.obo_required and not user_authorization:
            raise SupervisorError("Databricks Apps OBO token is required for the in-process runtime")
        body = self._body(
            message=message,
            conversation_id=conversation_id,
            actor_id=actor_id,
            tenant_id=tenant_id,
            roles=roles,
            demo=demo,
            country_code=country_code,
            locale=locale,
            experience=experience,
        )
        if settings.runtime_mode == "in_process":
            try:
                payload = await asyncio.to_thread(
                    self._invoke_local,
                    body,
                    host=host,
                    authorization=authorization,
                )
            except TimeoutError:
                raise
            except Exception as exc:
                raise SupervisorError(f"In-process ResponsesAgent failed: {exc}") from exc
            return self._parse(payload)

        timeout = httpx.Timeout(settings.request_timeout_seconds, connect=5.0)
        async with httpx.AsyncClient(timeout=timeout, transport=self.transport) as client:
            try:
                response = await client.post(
                    f"{host}/serving-endpoints/{settings.supervisor_endpoint}/invocations",
                    headers={"Authorization": authorization, "X-Request-ID": request_id},
                    json=body,
                )
                response.raise_for_status()
            except httpx.TimeoutException as exc:
                raise TimeoutError("Supervisor response exceeded the App latency budget") from exc
            except httpx.HTTPError as exc:
                detail = getattr(exc.response, "text", "")[:1000] if getattr(exc, "response", None) else str(exc)
                raise SupervisorError(f"Supervisor invocation failed: {detail}") from exc
        return self._parse(response.json())


supervisor_client = SupervisorClient()
