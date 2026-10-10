"""Cliente MCP Streamable HTTP com descoberta dinâmica e execução allowlisted."""

from __future__ import annotations

from dataclasses import dataclass
import json
import re
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .auth import DatabricksAuth
from .contracts import RouteDecision, ToolEvidence
from .guardrails import guard_tool_arguments
from .quota import LakebaseToolQuota, RequestCallLimit


@dataclass(frozen=True)
class McpServer:
    label: str
    path: str
    purpose: str
    fallback_for: str | None = None


@dataclass(frozen=True)
class McpTool:
    public_name: str
    server: McpServer
    remote_name: str
    description: str
    input_schema: dict[str, Any]


class McpProtocolError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = False):
        self.retryable = retryable
        super().__init__(message)


def _retryable_tool_result(result: Any) -> str | None:
    """Detect availability errors returned inside a nominal MCP success envelope."""
    if not isinstance(result, dict):
        return None
    if result.get("isError") is True:
        return "MCP tool returned isError=true"
    content = result.get("content")
    if not isinstance(content, list):
        return None
    text = " ".join(
        str(item.get("text", ""))
        for item in content
        if isinstance(item, dict) and item.get("text")
    ).strip()
    # Restrict semantic classification to short provider-status messages so an
    # article that happens to mention a timeout cannot trigger paid fallback.
    if len(text) <= 1000 and re.search(
        r"(?i)\b(web search timed out|search timeout|temporar(?:y|ily) unavailable|rate limit(?:ed)?)\b",
        text,
    ):
        return text
    return None


def _decode_payload(content_type: str, data: bytes) -> dict[str, Any]:
    text = data.decode("utf-8", errors="replace").strip()
    if not text:
        return {}
    if "text/event-stream" in content_type or text.startswith("event:") or text.startswith("data:"):
        events = []
        for line in text.splitlines():
            if line.startswith("data:"):
                candidate = line[5:].strip()
                if candidate and candidate != "[DONE]":
                    events.append(json.loads(candidate))
        return events[-1] if events else {}
    return json.loads(text)


class McpClient:
    def __init__(self, auth: DatabricksAuth, server: McpServer, timeout: int = 120):
        self.auth = auth
        self.server = server
        self.timeout = timeout
        self.session_id: str | None = None
        self._request_id = 0

    def _post(self, method: str, params: dict[str, Any] | None = None, *, notification: bool = False) -> dict[str, Any]:
        self._request_id += 1
        payload: dict[str, Any] = {"jsonrpc": "2.0", "method": method}
        if not notification:
            payload["id"] = self._request_id
        if params is not None:
            payload["params"] = params
        headers = {
            **self.auth.headers(),
            "Content-Type": "application/json",
            "Accept": "application/json, text/event-stream",
        }
        if self.session_id:
            headers["Mcp-Session-Id"] = self.session_id
        request = Request(
            f"{self.auth.host()}{self.server.path}",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=self.timeout) as response:
                if response.headers.get("Mcp-Session-Id"):
                    self.session_id = response.headers["Mcp-Session-Id"]
                raw = response.read()
                # Notifications do not have a JSON-RPC response. Some preview
                # servers return a plain acknowledgement body, which must not
                # be interpreted as a JSON document.
                decoded = {} if notification else _decode_payload(response.headers.get("Content-Type", ""), raw)
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")[:1200]
            raise McpProtocolError(
                f"{self.server.label}:{method} HTTP {exc.code}: {detail}",
                retryable=exc.code == 429 or 500 <= exc.code < 600,
            ) from exc
        except (TimeoutError, URLError) as exc:
            raise McpProtocolError(
                f"{self.server.label}:{method} transport failure: {exc}", retryable=True
            ) from exc
        if decoded.get("error"):
            raise McpProtocolError(f"{self.server.label}:{method}: {decoded['error']}")
        return decoded.get("result", decoded)

    def initialize(self) -> None:
        self._post(
            "initialize",
            {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {"name": "quilla-custom-supervisor", "version": "1.0.0"},
            },
        )
        self._post("notifications/initialized", notification=True)

    def list_tools(self) -> list[dict[str, Any]]:
        if self.session_id is None:
            self.initialize()
        return self._post("tools/list").get("tools", [])

    def call_tool(self, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if self.session_id is None:
            self.initialize()
        safe_arguments = guard_tool_arguments(self.server.label, name, arguments)
        return self._post("tools/call", {"name": name, "arguments": safe_arguments})


class McpToolbox:
    def __init__(
        self,
        auth: DatabricksAuth,
        servers: list[McpServer],
        required_server: str | None = None,
        web_search_timeout_seconds: int = 45,
        you_financial_timeout_seconds: int = 30,
        you_financial_max_input_chars: int = 1200,
        you_financial_daily_call_limit: int = 3,
        you_financial_total_call_limit: int = 10,
        you_financial_conversation_call_limit: int = 2,
        quota_context: str = "unknown-conversation",
        quota_request_id: str = "unknown-request",
        quota=None,
    ):
        self.clients = {
            server.label: McpClient(
                auth,
                server,
                timeout=(
                    web_search_timeout_seconds
                    if server.label == "web_search"
                    else you_financial_timeout_seconds
                    if server.label == "you_financial"
                    else 30
                ),
            )
            for server in servers
        }
        self.tools: dict[str, McpTool] = {}
        self.discovery_warnings: list[str] = []
        self.required_server = required_server
        self.you_financial_max_input_chars = you_financial_max_input_chars
        self.you_financial_daily_call_limit = you_financial_daily_call_limit
        self.you_financial_total_call_limit = you_financial_total_call_limit
        self.you_financial_conversation_call_limit = you_financial_conversation_call_limit
        self.quota_context = quota_context
        self.quota_request_id = quota_request_id
        self.quota = quota
        self.request_call_limit = RequestCallLimit(1)

    @staticmethod
    def _safe_name(label: str, index: int) -> str:
        return f"quilla_{label}_{index}".replace("-", "_")

    @staticmethod
    def _read_only_tool(label: str, name: str) -> bool:
        normalized = name.lower()
        if label == "web_search":
            return normalized == "web_search"
        if label == "you_financial":
            return normalized == "you-finance"
        if label == "governed_skills":
            return (normalized.startswith("skill_") and ".quilla-" in normalized) or normalized in {
                "load_skill", "get_skill_files", "list_skills"
            }
        forbidden = ("create", "update", "delete", "drop", "insert", "write", "execute", "approve", "reject")
        return not any(token in normalized for token in forbidden)

    def _discover_client(self, label: str, client: McpClient) -> list[dict[str, Any]]:
        definitions = []
        try:
            remote_tools = client.list_tools()
        except Exception as exc:
            self.discovery_warnings.append(f"MCP {label} indisponível: {exc}")
            return definitions
        existing_remote = {
            tool.remote_name for tool in self.tools.values() if tool.server.label == label
        }
        for index, raw in enumerate(remote_tools):
            if raw["name"] in existing_remote or not self._read_only_tool(label, raw["name"]):
                continue
            public_name = self._safe_name(label, index)
            tool = McpTool(
                public_name=public_name,
                server=client.server,
                remote_name=raw["name"],
                description=raw.get("description", "Ferramenta governada de leitura")[:900],
                input_schema=raw.get("inputSchema") or raw.get("input_schema") or {"type": "object", "properties": {}},
            )
            self.tools[public_name] = tool
            definitions.append(
                {
                    "type": "function",
                    "function": {
                        "name": public_name,
                        "description": f"[{label}] {tool.description}",
                        "parameters": tool.input_schema,
                    },
                }
            )
        return definitions

    def discover(self) -> list[dict[str, Any]]:
        definitions = []
        for label, client in self.clients.items():
            # Paid tools are never exposed to the model. They are discovered
            # lazily and invoked only after a retryable primary failure.
            if client.server.fallback_for:
                continue
            definitions.extend(self._discover_client(label, client))
        return definitions

    def _execute_you_financial(self, query: str, primary_error: Exception) -> tuple[str, ToolEvidence]:
        client = self.clients.get("you_financial")
        if client is None:
            raise McpProtocolError(f"Primary Web Search failed and no fallback is configured: {primary_error}")
        self._discover_client("you_financial", client)
        fallback_tools = [
            tool for tool in self.tools.values() if tool.server.label == "you_financial"
        ]
        if not fallback_tools:
            raise McpProtocolError(
                f"Primary Web Search failed and You.com discovery failed: {primary_error}",
                retryable=True,
            )
        self.request_call_limit.acquire()
        quota = self.quota or LakebaseToolQuota.from_env(
            daily_limit=self.you_financial_daily_call_limit,
            total_limit=self.you_financial_total_call_limit,
            conversation_limit=self.you_financial_conversation_call_limit,
        )
        reservation = quota.reserve(
            "you_financial", self.quota_context, self.quota_request_id
        )
        started = time.monotonic()
        try:
            result = client.call_tool(
                fallback_tools[0].remote_name,
                {
                    "input": query[: self.you_financial_max_input_chars],
                    # Exhaustive is intentionally disallowed for cost control.
                    "research_effort": "deep",
                },
            )
        except Exception as exc:
            quota.complete(
                reservation,
                status="failed",
                latency_ms=round((time.monotonic() - started) * 1000),
                error=str(exc),
            )
            raise McpProtocolError(
                f"Primary Web Search and controlled You.com fallback failed: {exc}",
                retryable=True,
            ) from exc
        quota.complete(
            reservation,
            status="succeeded",
            latency_ms=round((time.monotonic() - started) * 1000),
        )
        serialized = json.dumps(result, ensure_ascii=False, default=str)
        self.discovery_warnings.append(
            "Primary Web Search failed; controlled you_financial fallback was used."
        )
        return serialized, ToolEvidence(
            server="you_financial",
            tool=fallback_tools[0].remote_name,
            status="SUCCEEDED",
            preview=(
                f"fallback_after={str(primary_error)[:180]}; "
                f"daily_calls={reservation.daily_calls}; total_calls={reservation.total_calls}; "
                f"result={serialized[:240]}"
            ),
        )

    def execute_external_search(self, query: str) -> tuple[str, ToolEvidence]:
        self.discover()
        primary_tools = self.public_tools_for_server("web_search")
        if not primary_tools:
            return self._execute_you_financial(
                query, McpProtocolError("Primary Web Search discovery unavailable", retryable=True)
            )
        return self.execute(primary_tools[0], {"query": query})

    def execute(self, public_name: str, arguments: dict[str, Any]) -> tuple[str, ToolEvidence]:
        if public_name not in self.tools:
            raise McpProtocolError(f"Ferramenta não descoberta: {public_name}")
        tool = self.tools[public_name]
        try:
            result = self.clients[tool.server.label].call_tool(tool.remote_name, arguments)
        except McpProtocolError as exc:
            if tool.server.label == "web_search" and exc.retryable:
                query = str(arguments.get("query") or arguments.get("input") or "")
                return self._execute_you_financial(query, exc)
            raise
        semantic_failure = _retryable_tool_result(result)
        if tool.server.label == "web_search" and semantic_failure:
            query = str(arguments.get("query") or arguments.get("input") or "")
            return self._execute_you_financial(
                query,
                McpProtocolError(
                    f"web_search returned an availability error: {semantic_failure}",
                    retryable=True,
                ),
            )
        serialized = json.dumps(result, ensure_ascii=False, default=str)
        return serialized, ToolEvidence(
            server=tool.server.label,
            tool=tool.remote_name,
            status="SUCCEEDED",
            preview=serialized[:500],
        )

    def public_tools_for_server(self, label: str | None) -> list[str]:
        if not label:
            return []
        return [name for name, tool in self.tools.items() if tool.server.label == label]


def servers_for_route(route: RouteDecision, settings) -> list[McpServer]:
    servers: list[McpServer] = []
    keys = {route.primary, *route.supporting}
    if route.external_data or "external_intelligence" in keys:
        servers.append(McpServer("web_search", settings.web_search_mcp_path, "Pesquisa pública atual"))
        if settings.you_financial_enabled:
            servers.append(
                McpServer(
                    "you_financial",
                    settings.you_financial_mcp_path,
                    "Fallback financeiro You.com com quota persistente",
                    fallback_for="web_search",
                )
            )
    if "regulatory" in keys:
        servers.append(McpServer("regulatory_search", settings.regulatory_search_mcp_path, "Conhecimento regulatório governado"))
    # Supporting agents express reasoning context, not permission to query every
    # underlying data source. External-first questions must not fan out to Genie;
    # an internal-first route may use only its primary governed Space.
    if route.primary != "external_intelligence":
        if space_id := settings.genie_spaces.get(route.primary):
            servers.append(
                McpServer(
                    f"genie_{route.primary}",
                    f"/api/2.0/mcp/genie/{space_id}",
                    f"Genie {route.primary}",
                )
            )
    unique = {server.label: server for server in servers}
    return list(unique.values())
