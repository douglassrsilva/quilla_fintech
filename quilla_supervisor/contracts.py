"""Contratos internos independentes do framework de serving."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


SUPPORTED_COUNTRIES = frozenset(
    {"AR", "BO", "BR", "CL", "CO", "CR", "EC", "MX", "PA", "PE", "PY", "UY", "REGIONAL"}
)
SUPPORTED_LOCALES = frozenset(
    {"pt-BR", "es-AR", "es-BO", "es-CL", "es-CO", "es-CR", "es-EC", "es-MX", "es-PA", "es-PE", "es-PY", "es-UY"}
)


@dataclass(frozen=True)
class TrustedContext:
    user_id: str
    tenant_id: str
    country_code: str
    locale: str
    conversation_id: str | None = None
    roles: tuple[str, ...] = ()
    demo: bool = False
    experience: str = "general"


@dataclass(frozen=True)
class RouteDecision:
    primary: str
    supporting: tuple[str, ...]
    scores: dict[str, int]
    reasons: tuple[str, ...]
    high_impact: bool
    external_data: bool


@dataclass
class ToolEvidence:
    server: str
    tool: str
    status: str
    preview: str = ""


@dataclass
class SupervisorResult:
    text: str
    route: RouteDecision
    trace_id: str
    model_sequence: list[str]
    tool_evidence: list[ToolEvidence] = field(default_factory=list)
    requires_human_review: bool = False
    warnings: list[str] = field(default_factory=list)
    custom: dict[str, Any] = field(default_factory=dict)
