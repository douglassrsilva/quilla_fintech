"""Configuração imutável e autenticação do runtime Quilla."""

from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Mapping


DEFAULT_GENIE_SPACES = {
    "payments_performance": "QUILLA_GENIE_PAYMENTS_PERFORMANCE",
    "customer_service": "QUILLA_GENIE_CUSTOMER_OPERATIONS",
    "fraud_risk": "QUILLA_GENIE_RISK_FRAUD",
    "responsible_credit": "QUILLA_GENIE_RESPONSIBLE_CREDIT",
    "investment_suitability": "QUILLA_GENIE_GROWTH_WEALTH",
    "next_best_action": "QUILLA_GENIE_GROWTH_WEALTH",
    "executive": "QUILLA_GENIE_EXECUTIVE_REGIONAL",
}


@dataclass(frozen=True)
class SupervisorSettings:
    catalog: str = "example_catalog"
    ai_schema: str = "example_ai"
    primary_model: str = "example_catalog.example_ai.quilla-supervisor-primary"
    verifier_model: str = "example_catalog.example_ai.quilla-supervisor-verifier"
    long_context_model: str = "example_catalog.example_ai.quilla-supervisor-long-context"
    release: str = "2026-10-09-app-runtime-v1-agents-sdk-genie-mcp"
    max_tool_rounds: int = 6
    max_output_tokens: int = 4096
    web_search_timeout_seconds: int = 45
    you_financial_enabled: bool = True
    you_financial_timeout_seconds: int = 90
    you_financial_max_input_chars: int = 1200
    you_financial_daily_call_limit: int = 6
    you_financial_total_call_limit: int = 10
    you_financial_conversation_call_limit: int = 2
    verify_high_impact: bool = True
    allow_demo_context: bool = True
    fallback_endpoint: str = "legacy-supervisor-endpoint"
    genie_spaces: Mapping[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls) -> "SupervisorSettings":
        spaces = {
            key: value
            for key, env_name in DEFAULT_GENIE_SPACES.items()
            if (value := os.getenv(env_name, "").strip())
        }
        return cls(
            catalog=os.getenv("QUILLA_CATALOG", cls.catalog),
            ai_schema=os.getenv("QUILLA_AI_SCHEMA", cls.ai_schema),
            primary_model=os.getenv("QUILLA_PRIMARY_MODEL", cls.primary_model),
            verifier_model=os.getenv("QUILLA_VERIFIER_MODEL", cls.verifier_model),
            long_context_model=os.getenv("QUILLA_LONG_CONTEXT_MODEL", cls.long_context_model),
            release=os.getenv("QUILLA_SUPERVISOR_RELEASE", cls.release),
            max_tool_rounds=int(os.getenv("QUILLA_MAX_TOOL_ROUNDS", "6")),
            max_output_tokens=int(os.getenv("QUILLA_MAX_OUTPUT_TOKENS", "4096")),
            web_search_timeout_seconds=max(
                5,
                min(50, int(os.getenv("QUILLA_WEB_SEARCH_TIMEOUT_SECONDS", "45"))),
            ),
            you_financial_enabled=os.getenv("QUILLA_YOU_FINANCIAL_ENABLED", "true").lower() == "true",
            you_financial_timeout_seconds=max(
                5,
                min(120, int(os.getenv("QUILLA_YOU_FINANCIAL_TIMEOUT_SECONDS", "90"))),
            ),
            you_financial_max_input_chars=max(
                200,
                min(4000, int(os.getenv("QUILLA_YOU_FINANCIAL_MAX_INPUT_CHARS", "1200"))),
            ),
            you_financial_daily_call_limit=max(
                0, int(os.getenv("QUILLA_YOU_FINANCIAL_DAILY_CALL_LIMIT", "6"))
            ),
            you_financial_total_call_limit=max(
                0, int(os.getenv("QUILLA_YOU_FINANCIAL_TOTAL_CALL_LIMIT", "10"))
            ),
            you_financial_conversation_call_limit=max(
                0, int(os.getenv("QUILLA_YOU_FINANCIAL_CONVERSATION_CALL_LIMIT", "2"))
            ),
            verify_high_impact=os.getenv("QUILLA_VERIFY_HIGH_IMPACT", "true").lower() == "true",
            allow_demo_context=os.getenv("QUILLA_ALLOW_DEMO_CONTEXT", "true").lower() == "true",
            fallback_endpoint=os.getenv("QUILLA_FALLBACK_ENDPOINT", cls.fallback_endpoint),
            genie_spaces=spaces,
        )

    @property
    def skill_mcp_path(self) -> str:
        return f"/ai-gateway/skills/?schema={self.catalog}.{self.ai_schema}"

    @property
    def web_search_mcp_path(self) -> str:
        return "/ai-gateway/mcp-services/system.ai.web_search"

    @property
    def you_financial_mcp_path(self) -> str:
        return f"/ai-gateway/mcp-services/{self.catalog}.{self.ai_schema}.you_financial_mcp"

    @property
    def regulatory_search_mcp_path(self) -> str:
        return (
            f"/api/2.0/mcp/ai-search/{self.catalog}/{self.ai_schema}/"
            "regulatory_knowledge_hybrid_idx"
        )
