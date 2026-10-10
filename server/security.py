"""Identidade e contexto confiável derivados no servidor."""

from __future__ import annotations

from dataclasses import dataclass
import re
import uuid

from fastapi import Header, HTTPException, Request

from .config import DEFAULT_LOCALE_BY_COUNTRY, SUPPORTED_COUNTRIES, SUPPORTED_LOCALES, settings


SAFE_ACTOR = re.compile(r"^[^\s]{1,320}$")


@dataclass(frozen=True)
class ActorContext:
    actor_id: str
    tenant_id: str
    roles: tuple[str, ...]
    demo: bool


def trusted_actor(
    x_forwarded_email: str | None = Header(default=None, alias="X-Forwarded-Email"),
) -> ActorContext:
    actor = (x_forwarded_email or "").strip().lower()
    if not actor and settings.allow_demo_context:
        actor = settings.demo_actor
    if not actor:
        raise HTTPException(status_code=401, detail="Authenticated Databricks user required")
    if not SAFE_ACTOR.fullmatch(actor):
        raise HTTPException(status_code=400, detail="Invalid authenticated actor")
    return ActorContext(actor, settings.tenant_id, settings.demo_roles, settings.demo_mode)


def trusted_user_authorization(
    x_forwarded_access_token: str | None = Header(
        default=None, alias="X-Forwarded-Access-Token", include_in_schema=False
    ),
) -> str | None:
    """Token OBO injetado pelo proxy do Databricks Apps; nunca vem do payload."""
    token = (x_forwarded_access_token or "").strip()
    if not token:
        return None
    return token if token.lower().startswith("bearer ") else f"Bearer {token}"


def trusted_geo(country_code: str | None, locale: str | None) -> tuple[str, str]:
    country = (country_code or settings.default_country).upper()
    if country not in SUPPORTED_COUNTRIES:
        raise HTTPException(status_code=422, detail="Unsupported country_code")
    selected_locale = locale or DEFAULT_LOCALE_BY_COUNTRY[country]
    if selected_locale not in SUPPORTED_LOCALES:
        raise HTTPException(status_code=422, detail="Unsupported locale")
    return country, selected_locale


def request_id(request: Request) -> str:
    return getattr(request.state, "request_id", str(uuid.uuid4()))
