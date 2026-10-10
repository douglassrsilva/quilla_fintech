"""OpenAI Agents SDK adapter for the in-process Databricks App runtime.

The SDK owns the specialist loop while Databricks remains the model, policy,
identity, MCP and tracing control plane.  The OpenAI client points exclusively
to the Unity AI Gateway OpenAI-compatible endpoint; no OpenAI-hosted model or
credential is used.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from typing import Any

from .gateway import Generation, GatewayError, UnityGateway, clean_model_text
from .contracts import ToolEvidence
from .mcp import McpToolbox


SDK_MCP_LABEL_PREFIXES = ("genie_", "regulatory_search")


def _message_text(message: dict[str, Any]) -> str:
    content = message.get("content", "")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            str(item.get("text", ""))
            for item in content
            if isinstance(item, dict) and item.get("text")
        )
    return str(content)


def _agent_input(messages: list[dict[str, Any]]) -> tuple[str, str]:
    """Split trusted instructions from the conversational transcript."""
    instructions = ""
    turns: list[str] = []
    for message in messages:
        text = _message_text(message).strip()
        if not text:
            continue
        role = str(message.get("role") or "user")
        if role == "system" and not instructions:
            instructions = text
            continue
        label = "USUÁRIO" if role == "user" else "ASSISTENTE"
        turns.append(f"{label}:\n{text}")
    return instructions, "\n\n".join(turns)


def _raw_value(value: Any, key: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(key, default)
    return getattr(value, key, default)


def _tool_evidence(result: Any) -> list[ToolEvidence]:
    """Extract a safe audit summary without exposing full governed results."""
    evidence: list[ToolEvidence] = []
    for item in getattr(result, "new_items", ()) or ():
        item_type = str(getattr(item, "type", ""))
        raw = getattr(item, "raw_item", item)
        raw_type = str(_raw_value(raw, "type", item_type))
        if "tool_call" not in item_type and "tool_call" not in raw_type:
            continue
        name = str(
            _raw_value(raw, "name")
            or _raw_value(_raw_value(raw, "function", {}), "name")
            or "managed_mcp_tool"
        )
        server = str(_raw_value(raw, "server_label") or _raw_value(raw, "server") or "managed_mcp")
        evidence.append(ToolEvidence(server=server, tool=name, status="SUCCEEDED"))
    return evidence


class AgentsSDKGateway(UnityGateway):
    """Use OpenAI Agents SDK for governed Genie and regulatory MCP routes."""

    @staticmethod
    def _sdk_classes():
        try:
            from agents import (
                Agent,
                ModelSettings,
                OpenAIChatCompletionsModel,
                RunConfig,
                Runner,
            )
            from agents.mcp import MCPServerStreamableHttp
            from openai import AsyncOpenAI
        except ImportError as exc:  # pragma: no cover - exercised by deployment readiness
            raise GatewayError(
                "OpenAI Agents SDK is not installed; install the openai-agents runtime dependency"
            ) from exc
        return (
            Agent,
            ModelSettings,
            OpenAIChatCompletionsModel,
            RunConfig,
            Runner,
            MCPServerStreamableHttp,
            AsyncOpenAI,
        )

    @staticmethod
    def _uses_sdk_mcp(toolbox: McpToolbox) -> bool:
        return any(
            label.startswith(SDK_MCP_LABEL_PREFIXES)
            for label in getattr(toolbox, "clients", {})
        )

    def run_agent(
        self,
        endpoint: str,
        messages: list[dict[str, Any]],
        toolbox: McpToolbox,
        *,
        max_rounds: int,
        max_output_tokens: int | None = None,
    ) -> Generation:
        # Keep the controlled Web Search/You.com fallback on the existing
        # deterministic loop. Genie and AI Search use the Agents SDK natively.
        if not self._uses_sdk_mcp(toolbox):
            return super().run_agent(
                endpoint,
                messages,
                toolbox,
                max_rounds=max_rounds,
                max_output_tokens=max_output_tokens,
            )
        try:
            return asyncio.run(
                self._run_agent_sdk(
                    endpoint,
                    messages,
                    toolbox,
                    max_rounds=max_rounds,
                    max_output_tokens=max_output_tokens,
                )
            )
        except GatewayError:
            raise
        except Exception as exc:
            raise GatewayError(f"OpenAI Agents SDK specialist failed: {exc}") from exc

    async def _run_agent_sdk(
        self,
        endpoint: str,
        messages: list[dict[str, Any]],
        toolbox: McpToolbox,
        *,
        max_rounds: int,
        max_output_tokens: int | None,
    ) -> Generation:
        (
            Agent,
            ModelSettings,
            OpenAIChatCompletionsModel,
            RunConfig,
            Runner,
            MCPServerStreamableHttp,
            AsyncOpenAI,
        ) = self._sdk_classes()
        authorization = self.auth.headers().get("Authorization", "")
        token = authorization.split(" ", 1)[1].strip() if authorization.lower().startswith("bearer ") else authorization.strip()
        if not token:
            raise GatewayError("Databricks OBO authorization is unavailable for the agent runtime")

        client = AsyncOpenAI(
            api_key=token,
            base_url=f"{self.auth.host()}/ai-gateway/mlflow/v1",
            max_retries=1,
        )
        model = OpenAIChatCompletionsModel(model=endpoint, openai_client=client)
        instructions, transcript = _agent_input(messages)
        if not transcript:
            raise GatewayError("The specialist received no conversational input")

        sdk_servers = []
        for label, mcp_client in getattr(toolbox, "clients", {}).items():
            server = mcp_client.server
            if server.fallback_for or not label.startswith(SDK_MCP_LABEL_PREFIXES):
                continue

            def read_only_filter(_context, tool, *, server_label=label):
                tool_name = str(_raw_value(tool, "name", ""))
                return McpToolbox._read_only_tool(server_label, tool_name)

            sdk_servers.append(
                MCPServerStreamableHttp(
                    {
                        "url": f"{self.auth.host()}{server.path}",
                        "headers": self.auth.headers(),
                        "timeout": 180,
                        "sse_read_timeout": 180,
                        "terminate_on_close": True,
                        "ignore_initialized_notification_failure": True,
                    },
                    cache_tools_list=True,
                    name=label,
                    client_session_timeout_seconds=180,
                    tool_filter=read_only_filter,
                    require_approval="never",
                    failure_error_function=None,
                    max_retry_attempts=1,
                )
            )

        if not sdk_servers:
            raise GatewayError("No governed MCP server is available for the selected specialist")

        specialist = Agent(
            name="Quilla governed specialist",
            instructions=instructions,
            model=model,
            mcp_servers=sdk_servers,
            model_settings=ModelSettings(
                temperature=0.15,
                max_tokens=max_output_tokens or self.max_output_tokens,
                parallel_tool_calls=False,
            ),
        )
        async with AsyncExitStack() as stack:
            for server in sdk_servers:
                await stack.enter_async_context(server)
            result = await Runner.run(
                specialist,
                transcript,
                max_turns=max(2, max_rounds),
                run_config=RunConfig(
                    tracing_disabled=True,
                    workflow_name="Quilla in-process Databricks App supervisor",
                ),
            )
        text = clean_model_text(getattr(result, "final_output", ""))
        if not text:
            raise GatewayError("OpenAI Agents SDK returned no final specialist output")
        self._raise_if_policy_blocked(text)
        return Generation(
            text=text,
            models=[endpoint],
            evidence=_tool_evidence(result),
            warnings=list(getattr(toolbox, "discovery_warnings", ())),
        )
