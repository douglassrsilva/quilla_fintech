#!/usr/bin/env python3
"""Measure the governed Web Search MCP stages directly, without a Job."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from quilla_supervisor.auth import DatabricksAuth
from quilla_supervisor.config import SupervisorSettings
from quilla_supervisor.mcp import McpClient, McpServer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--label", default="web_search")
    parser.add_argument("--path")
    parser.add_argument("--list-only", action="store_true")
    args = parser.parse_args()
    settings = SupervisorSettings()
    path = args.path or settings.web_search_mcp_path
    client = McpClient(
        DatabricksAuth(profile=args.profile),
        McpServer(args.label, path, "Pesquisa pública atual"),
        timeout=args.timeout,
    )
    started = time.monotonic()
    tools = client.list_tools()
    discovered = time.monotonic()
    if args.list_only:
        print(json.dumps({
            "discovery_seconds": round(discovered - started, 2),
            "server": args.label,
            "path": path,
            "tools": tools,
        }, ensure_ascii=False, indent=2))
        return
    web_search = next(tool for tool in tools if tool["name"].lower() == "web_search")
    result = client.call_tool(
        web_search["name"],
        {"query": "economia do Equador agosto setembro 2026 fontes oficiais"},
    )
    completed = time.monotonic()
    print(json.dumps({
        "discovery_seconds": round(discovered - started, 2),
        "search_seconds": round(completed - discovered, 2),
        "total_seconds": round(completed - started, 2),
        "tool_schema": web_search.get("inputSchema") or web_search.get("input_schema"),
        "result_preview": json.dumps(result, ensure_ascii=False)[:500],
    }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
