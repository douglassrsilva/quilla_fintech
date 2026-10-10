"""Supervisor customizado: policy-first, multi-modelo e MCP nativo."""

from __future__ import annotations

from datetime import datetime
import uuid
from typing import Any, Callable
from zoneinfo import ZoneInfo

from .auth import DatabricksAuth
from .config import SupervisorSettings
from .contracts import RouteDecision, SupervisorResult, TrustedContext
from .gateway import GatewayError, GatewayPolicyBlocked, Generation, UnityGateway
from .guardrails import output_warnings, validate_context, validate_input
from .mcp import McpToolbox, servers_for_route
from .planning import GovernedSkillLoader, SemanticIntentRouter
from .prompts import REVISION_PROMPT, VERIFIER_PROMPT, system_prompt
from .routing import IntentRouter
from .telemetry import attributes, span


FOLLOW_UP_MARKERS = (
    "contexto anterior", "anterior", "esses ", "essas ", "desses ", "dessas ",
    "isso", "isto", "agora ", "preserve o contexto", "contexto previo", "previo",
    "esos ", "esas ", "de esos", "de esas", "ahora ", "recomendação", "recomendación",
)


def latest_user_text(messages: list[dict[str, Any]]) -> str:
    for message in reversed(messages):
        if message.get("role") != "user":
            continue
        content = message.get("content", "")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = [
                item.get("text", "")
                for item in content
                if isinstance(item, dict) and item.get("type") in {"input_text", "text"}
            ]
            return "\n".join(parts)
    raise ValueError("A conversa não contém uma mensagem do usuário")


def to_chat_messages(messages: list[dict[str, Any]]) -> list[dict[str, str]]:
    converted: list[dict[str, str]] = []
    for message in messages[-24:]:
        role = message.get("role")
        if role not in {"user", "assistant"}:
            continue
        content = message.get("content", "")
        if isinstance(content, list):
            content = "\n".join(
                item.get("text", "")
                for item in content
                if isinstance(item, dict) and item.get("text")
            )
        if content:
            converted.append({"role": role, "content": str(content)})
    return converted


def routing_text(messages: list[dict[str, Any]], question: str) -> str:
    normalized = question.lower()
    if not is_follow_up(question):
        return question
    previous = []
    skipped_current = False
    for message in reversed(messages):
        if message.get("role") != "user":
            continue
        text = latest_user_text([message])
        if not skipped_current:
            skipped_current = True
            continue
        previous.append(text)
        if len(previous) == 2:
            break
    return "\n".join([*reversed(previous), question]) if previous else question


def is_follow_up(question: str) -> bool:
    normalized = question.lower()
    return any(marker in normalized for marker in FOLLOW_UP_MARKERS)


class QuillaSupervisor:
    def __init__(
        self,
        settings: SupervisorSettings | None = None,
        *,
        auth: DatabricksAuth | None = None,
        gateway: UnityGateway | None = None,
        toolbox_factory: Callable[..., McpToolbox] = McpToolbox,
        router=None,
    ):
        self.settings = settings or SupervisorSettings.from_env()
        self.auth = auth or DatabricksAuth()
        self.gateway = gateway or UnityGateway(self.auth, self.settings.max_output_tokens)
        self.toolbox_factory = toolbox_factory
        self.router = router or SemanticIntentRouter(
            self.gateway,
            self.settings.primary_model,
            fallback=IntentRouter(),
            skill_loader=GovernedSkillLoader(
                self.auth,
                self.settings.skill_mcp_path,
                f"skill_{self.settings.catalog}.{self.settings.ai_schema}.quilla-supervisor",
            ),
        )

    def answer(self, messages: list[dict[str, Any]], context: TrustedContext) -> SupervisorResult:
        validate_context(context)
        question = latest_user_text(messages)
        validate_input(question)
        trace_id = str(uuid.uuid4())

        with span("quilla.route", "CHAIN") as route_span:
            route = (
                RouteDecision(
                    primary="external_intelligence",
                    supporting=(),
                    scores={"external_intelligence": 1},
                    reasons=("experience:market_intelligence",),
                    high_impact=False,
                    external_data=True,
                )
                if context.experience == "market_intelligence"
                else self.router.route(routing_text(messages, question))
            )
            attributes(
                route_span,
                {
                    "quilla.trace_id": trace_id,
                    "quilla.route.primary": route.primary,
                    "quilla.route.supporting": ",".join(route.supporting),
                    "quilla.country": context.country_code,
                    "quilla.high_impact": route.high_impact,
                },
            )

        servers = servers_for_route(route, self.settings)
        required_server = "web_search" if route.external_data else None
        try:
            toolbox = self.toolbox_factory(
                self.auth,
                servers,
                required_server=required_server,
                web_search_timeout_seconds=self.settings.web_search_timeout_seconds,
                you_financial_timeout_seconds=self.settings.you_financial_timeout_seconds,
                you_financial_max_input_chars=self.settings.you_financial_max_input_chars,
                you_financial_daily_call_limit=self.settings.you_financial_daily_call_limit,
                you_financial_total_call_limit=self.settings.you_financial_total_call_limit,
                you_financial_conversation_call_limit=(
                    self.settings.you_financial_conversation_call_limit
                ),
                quota_context=context.conversation_id or trace_id,
                quota_request_id=trace_id,
            )
        except TypeError:
            # Compatibility with test doubles and lightweight custom factories.
            try:
                toolbox = self.toolbox_factory(self.auth, servers, required_server=required_server)
            except TypeError:
                toolbox = self.toolbox_factory(self.auth, servers)
                setattr(toolbox, "required_server", required_server)
        chat_history = (
            to_chat_messages(messages)
            if is_follow_up(question)
            else [{"role": "user", "content": question}]
        )
        today = datetime.now(ZoneInfo("America/Sao_Paulo")).date().isoformat()
        prompt = system_prompt(route.primary, context, today=today)
        prompt += f"\nRoteamento auditável: primário={route.primary}; apoio={','.join(route.supporting) or 'nenhum'}."
        model = (
            self.settings.long_context_model
            if sum(len(item["content"]) for item in chat_history) > 18_000
            else self.settings.primary_model
        )

        with span("quilla.specialist", "AGENT") as specialist_span:
            attributes(specialist_span, {"quilla.agent": route.primary, "quilla.model": model})
            try:
                if context.experience == "market_intelligence":
                    generated = self.gateway.run_grounded_search(
                        model,
                        [{"role": "system", "content": prompt}, *chat_history],
                        toolbox,
                        search_query=routing_text(messages, question),
                        max_output_tokens=4096,
                    )
                else:
                    generated = self.gateway.run_agent(
                        model,
                        [{"role": "system", "content": prompt}, *chat_history],
                        toolbox,
                        max_rounds=min(self.settings.max_tool_rounds, 2) if route.external_data else self.settings.max_tool_rounds,
                        max_output_tokens=700 if route.external_data else None,
                    )
                governed_policy_block = None
            except GatewayPolicyBlocked as exc:
                # The decision is made by the Unity AI Gateway policy.  The
                # application layer only renders that governed decision as a
                # safe response instead of leaking an endpoint exception.
                governed_policy_block = exc.policy
                refusal = (
                    "Não posso atender a esta solicitação porque ela foi bloqueada pelas políticas "
                    "governadas de segurança e privacidade da Quilla. Posso ajudar com uma visão "
                    "agregada, anonimizada e autorizada."
                    if context.locale == "pt-BR"
                    else "No puedo atender esta solicitud porque fue bloqueada por las políticas "
                    "gobernadas de seguridad y privacidad de Quilla. Puedo ayudar con una vista "
                    "agregada, anonimizada y autorizada."
                )
                generated = Generation(
                    refusal,
                    [model],
                    warnings=[f"Bloqueio governado aplicado pelo Unity AI Gateway: {exc.policy}"],
                )

        answer = generated.text
        planner_model = getattr(self.router, "last_model_used", None)
        models = list(dict.fromkeys([*([planner_model] if planner_model else []), *generated.models]))
        runtime_warnings = list(generated.warnings)
        if planner_warning := getattr(self.router, "last_warning", None):
            runtime_warnings.append(planner_warning)
        # External intelligence already crosses governed AI Gateway input/output
        # policies and produces source evidence. Its semantic scoring remains in
        # MLflow monitoring/review instead of adding another LLM to the user's
        # synchronous latency budget.
        should_verify = governed_policy_block is None and self.settings.verify_high_impact and not route.external_data and (
            route.high_impact or route.primary == "regulatory"
        )
        if should_verify:
            critique = None
            try:
                with span("quilla.verify", "LLM") as verifier_span:
                    attributes(verifier_span, {"quilla.model": self.settings.verifier_model})
                    critique = self.gateway.chat(
                        self.settings.verifier_model,
                        [
                            {
                                "role": "system",
                                "content": f"{VERIFIER_PROMPT}\nA data atual confiável é {today}.",
                            },
                            {
                                "role": "user",
                                "content": f"PERGUNTA:\n{question[:4000]}\n\nRESPOSTA CANDIDATA:\n{answer[:10000]}",
                            },
                        ],
                        temperature=0.0,
                    )
                    models.append(self.settings.verifier_model)
            except GatewayError as exc:
                runtime_warnings.append(
                    f"Verificação independente indisponível; revisão humana obrigatória: {exc}"
                )

            if critique is not None and not critique.strip().upper().startswith("APROVADO"):
                try:
                    with span("quilla.revise", "LLM") as revision_span:
                        attributes(revision_span, {"quilla.model": self.settings.primary_model})
                        answer = self.gateway.chat(
                            self.settings.primary_model,
                            [
                                {
                                    "role": "system",
                                    "content": (
                                        f"{REVISION_PROMPT}\n\n"
                                        f"A data atual confiável é {today}.\n"
                                        f"Correções governadas a aplicar:\n{critique[:6000]}"
                                    ),
                                },
                                {
                                    "role": "assistant",
                                    "content": answer[:12000],
                                },
                                {"role": "user", "content": "Entregue a versão final corrigida."},
                            ],
                            temperature=0.0,
                        )
                        models.append(self.settings.primary_model)
                except GatewayError as exc:
                    runtime_warnings.append(
                        f"Revisão governada indisponível; resposta fundamentada original preservada: {exc}"
                    )

        warnings = [*runtime_warnings, *output_warnings(answer, external=route.external_data, high_impact=route.high_impact)]
        if route.high_impact and not any("revisão humana" in item.lower() or "revisión humana" in item.lower() for item in [answer]):
            suffix = (
                "\n\nEsta análise apoia a decisão, mas qualquer ação de alto impacto exige revisão humana."
                if context.locale == "pt-BR"
                else "\n\nEste análisis apoya la decisión, pero toda acción de alto impacto exige revisión humana."
            )
            answer += suffix

        return SupervisorResult(
            text=answer,
            route=route,
            trace_id=trace_id,
            model_sequence=models,
            tool_evidence=generated.evidence,
            requires_human_review=route.high_impact,
            warnings=warnings,
            custom={
                "release": self.settings.release,
                "country_code": context.country_code,
                "locale": context.locale,
                "conversation_id": context.conversation_id,
                "synthetic_environment": context.demo,
            },
        )
