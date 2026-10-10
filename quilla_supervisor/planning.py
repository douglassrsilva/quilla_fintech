"""Semantic intent planning through a governed Unity AI Gateway model service."""

from __future__ import annotations

import json
import re
from typing import Any

from .contracts import RouteDecision
from .mcp import McpServer, McpToolbox
from .prompts import ROUTING_PLANNER_PROMPT
from .routing import IntentRouter


ALLOWED_ROUTES = frozenset(
    {
        "customer_service",
        "fraud_risk",
        "responsible_credit",
        "investment_suitability",
        "regulatory",
        "next_best_action",
        "executive",
        "external_intelligence",
    }
)


class GovernedSkillLoader:
    """Load the finalized supervisor skill from Unity AI Gateway's Skills MCP."""

    def __init__(self, auth, path: str, full_name: str):
        self.auth = auth
        self.path = path
        self.full_name = full_name.lower()
        self._cached: str | None = None

    def __call__(self) -> str:
        if self._cached is not None:
            return self._cached
        toolbox = McpToolbox(
            self.auth,
            [McpServer("governed_skills", self.path, "Governed Skill de roteamento Quilla")],
        )
        toolbox.discover()
        target = next(
            (
                public_name
                for public_name, tool in toolbox.tools.items()
                if tool.remote_name.lower().endswith(self.full_name)
            ),
            None,
        )
        if target is None:
            raise ValueError(f"governed skill {self.full_name!r} was not discovered")
        serialized, _ = toolbox.execute(target, {})
        self._cached = serialized
        return serialized


def _json_object(text: str) -> dict[str, Any]:
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.IGNORECASE)
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("planner did not return a JSON object")
    value = json.loads(cleaned[start : end + 1])
    if not isinstance(value, dict):
        raise ValueError("planner JSON must be an object")
    return value


class SemanticIntentRouter:
    """Use GLM through the governed model-service alias; fail closed to local routing."""

    def __init__(
        self,
        gateway,
        model: str,
        fallback: IntentRouter | None = None,
        skill_loader=None,
    ):
        self.gateway = gateway
        self.model = model
        self.fallback = fallback or IntentRouter()
        self.skill_loader = skill_loader
        self.last_warning: str | None = None
        self.last_model_used: str | None = None

    def route(self, text: str) -> RouteDecision:
        safe_fallback = self.fallback.route(text)
        self.last_warning = None
        self.last_model_used = None
        skill_text = ""
        skill_warning = None
        if self.skill_loader is not None:
            try:
                skill_text = self.skill_loader()
            except Exception as exc:
                skill_warning = f"Governed Skill de roteamento indisponível: {exc}"
        # A privacy-sensitive request does not need its raw field names copied
        # into the semantic planner.  Send a data-minimized routing envelope
        # instead: GLM still makes the governed final routing decision while
        # the planner call cannot accidentally reproduce requested PII.
        planner_input = text
        planner_context = ""
        if "risk:sensitive-data-export" in safe_fallback.reasons:
            planner_context = json.dumps(
                {
                    "task": "route_only",
                    "language": "pt-BR_or_es-LATAM",
                    "request_domain": "customer_service",
                    "privacy_sensitive_export": True,
                    "external_data_required": False,
                },
                ensure_ascii=False,
            )
            planner_input = "Classifique somente os metadados de roteamento fornecidos pelo sistema."
        try:
            system_content = ROUTING_PLANNER_PROMPT
            if skill_text:
                system_content += f"\n\nGoverned Skill quilla-supervisor:\n{skill_text}"
            if planner_context:
                system_content += f"\n\nMetadados minimizados e confiáveis da solicitação:\n{planner_context}"
            raw = self.gateway.chat(
                self.model,
                [
                    {"role": "system", "content": system_content},
                    {"role": "user", "content": planner_input},
                ],
                temperature=0.0,
            )
            plan = _json_object(raw)
            primary = str(plan.get("primary", ""))
            if primary not in ALLOWED_ROUTES:
                raise ValueError(f"unsupported primary route: {primary!r}")
            supporting = tuple(
                key
                for key in dict.fromkeys(plan.get("supporting") or ())
                if key in ALLOWED_ROUTES and key != primary
            )[:2]
            external_data = bool(plan.get("external_data")) or primary == "external_intelligence"
            if external_data and primary not in {"external_intelligence", "regulatory"}:
                supporting = tuple(dict.fromkeys((*supporting, "external_intelligence")))[:2]
            high_impact = bool(plan.get("high_impact")) or safe_fallback.high_impact
            reasons = tuple(str(item)[:160] for item in (plan.get("reasons") or ()) if item)
            self.last_model_used = self.model
            self.last_warning = skill_warning
            return RouteDecision(
                primary=primary,
                supporting=supporting,
                scores=safe_fallback.scores,
                reasons=reasons or ("semantic-planner:glm-5.3",),
                high_impact=high_impact,
                external_data=external_data,
            )
        except Exception as exc:
            messages = [item for item in (skill_warning, f"Planner semântico indisponível; fallback local aplicado: {exc}") if item]
            self.last_warning = " | ".join(messages)
            return safe_fallback
