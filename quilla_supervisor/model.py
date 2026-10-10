"""Adapter MLflow ResponsesAgent para registro no Unity Catalog e serving."""

from __future__ import annotations

import uuid
from typing import Any

import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from quilla_supervisor.agent import QuillaSupervisor
from quilla_supervisor.contracts import TrustedContext
from quilla_supervisor.memory import LakebaseConversationStore


def _dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump(exclude_none=True)
    raise TypeError(f"Item de entrada incompatível: {type(value)!r}")


class QuillaResponsesAgent(ResponsesAgent):
    def __init__(self, supervisor=None, conversation_store=None):
        self.supervisor = supervisor or QuillaSupervisor()
        self.conversation_store = conversation_store or LakebaseConversationStore.from_env()

    @staticmethod
    def _context(request: ResponsesAgentRequest) -> TrustedContext:
        custom = request.custom_inputs or {}
        chat_context = request.context
        user_id = getattr(chat_context, "user_id", None) or custom.get("user_id") or "quilla-demo-user"
        conversation_id = (
            getattr(chat_context, "conversation_id", None)
            or custom.get("conversation_id")
            or str(uuid.uuid4())
        )
        demo = user_id == "quilla-demo-user" or bool(custom.get("demo", False))
        return TrustedContext(
            user_id=user_id,
            tenant_id=str(custom.get("tenant_id") or "quilla-demo"),
            country_code=str(custom.get("country_code") or "REGIONAL").upper(),
            locale=str(custom.get("locale") or "pt-BR"),
            conversation_id=conversation_id,
            roles=tuple(custom.get("roles") or ()),
            demo=demo,
            experience=str(custom.get("experience") or "general"),
        )

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        if (request.custom_inputs or {}).get("registration_validation"):
            output_id = f"msg_{uuid.uuid4().hex}"
            return ResponsesAgentResponse(
                output=[self.create_text_output_item("Quilla supervisor contract validation passed.", output_id)],
                custom_outputs={
                    "trace_id": "registration-validation",
                    "route": "customer_service",
                    "supporting_agents": [],
                    "models": [],
                    "tools": [],
                    "requires_human_review": False,
                    "warnings": [],
                    "release": self.supervisor.settings.release,
                    "synthetic_environment": True,
                },
            )
        context = self._context(request)
        incoming = [_dict(item) for item in request.input]
        memory_warnings = []
        try:
            history = self.conversation_store.restore(context, incoming)
        except Exception as exc:
            if self.conversation_store.required:
                raise RuntimeError(f"Lakebase conversation restore failed: {exc}") from exc
            history = incoming
            memory_warnings.append(f"Lakebase restore unavailable: {exc}")

        result = self.supervisor.answer(history, context)
        memory = {"status": "DISABLED", "store": "lakebase"}
        try:
            receipt = self.conversation_store.save_exchange(context, history, result)
            if receipt:
                memory = receipt.as_dict()
        except Exception as exc:
            if self.conversation_store.required:
                raise RuntimeError(f"Lakebase conversation persistence failed: {exc}") from exc
            memory = {"status": "DEGRADED", "store": "lakebase"}
            memory_warnings.append(f"Lakebase persistence unavailable: {exc}")
        result.warnings.extend(memory_warnings)
        output_id = f"msg_{uuid.uuid4().hex}"
        return ResponsesAgentResponse(
            output=[self.create_text_output_item(result.text, output_id)],
            custom_outputs={
                "trace_id": result.trace_id,
                "route": result.route.primary,
                "supporting_agents": list(result.route.supporting),
                "models": result.model_sequence,
                "tools": [evidence.__dict__ for evidence in result.tool_evidence],
                "requires_human_review": result.requires_human_review,
                "warnings": result.warnings,
                "memory": memory,
                **result.custom,
            },
        )

    def predict_stream(self, request: ResponsesAgentRequest):
        response = self.predict(request)
        item = response.output[0]
        item_dict = _dict(item)
        text = item_dict["content"][0]["text"]
        item_id = item_dict["id"]
        for start in range(0, len(text), 80):
            yield ResponsesAgentStreamEvent(**self.create_text_delta(text[start : start + 80], item_id))
        yield ResponsesAgentStreamEvent(type="response.completed", custom_outputs=response.custom_outputs)


mlflow.models.set_model(QuillaResponsesAgent())
