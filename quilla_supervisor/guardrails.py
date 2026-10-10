"""Controles estruturais locais; políticas de conteúdo vivem no Unity AI Gateway."""

from __future__ import annotations

import re
from typing import Any

from .contracts import SUPPORTED_COUNTRIES, SUPPORTED_LOCALES, TrustedContext


class GuardrailViolation(ValueError):
    pass


def validate_context(context: TrustedContext) -> None:
    if context.country_code not in SUPPORTED_COUNTRIES:
        raise GuardrailViolation(f"País não autorizado: {context.country_code}")
    if context.locale not in SUPPORTED_LOCALES:
        raise GuardrailViolation(f"Locale não suportado: {context.locale}")
    if not context.user_id or not context.tenant_id:
        raise GuardrailViolation("Identidade autenticada incompleta")


def validate_input(text: str) -> None:
    if not text.strip():
        raise GuardrailViolation("A pergunta não pode estar vazia")
    if len(text) > 24_000:
        raise GuardrailViolation("Entrada acima do limite seguro de 24.000 caracteres")


def guard_tool_arguments(server_label: str, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    destructive = ("create", "delete", "drop", "update", "insert", "execute_payment", "trade", "block_account")
    normalized = tool_name.lower()
    if any(token in normalized for token in destructive):
        raise GuardrailViolation(f"Ferramenta mutável não autorizada: {tool_name}")
    return arguments


def output_warnings(text: str, *, external: bool, high_impact: bool) -> list[str]:
    warnings = []
    if external and not re.search(r"https?://", text):
        warnings.append("A resposta externa não apresentou URL verificável.")
    if high_impact and not re.search(r"(?i)revis[aã]o humana|revisi[oó]n humana", text):
        warnings.append("Uma decisão de alto impacto exige revisão humana antes de qualquer ação.")
    return warnings
