"""Genie Agent Mode: pesquisa multi-etapas sobre a inteligência interna governada."""

from __future__ import annotations

import asyncio
from contextlib import suppress
import json
import os
import re
import time
from typing import Any, Literal
import uuid

import httpx
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field

from .db import DatabaseUnavailable, db
from .observability import log_event
from .security import ActorContext, trusted_actor, trusted_geo, trusted_user_authorization


router = APIRouter(prefix="/api/genie", tags=["genie"])

SPACE_ENV = {
    "executive": "QUILLA_GENIE_EXECUTIVE_REGIONAL",
    "payments": "QUILLA_GENIE_PAYMENTS_PERFORMANCE",
    "risk": "QUILLA_GENIE_RISK_FRAUD",
    "credit": "QUILLA_GENIE_RESPONSIBLE_CREDIT",
    "customers": "QUILLA_GENIE_CUSTOMER_OPERATIONS",
    "growth": "QUILLA_GENIE_GROWTH_WEALTH",
}


class GenieRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(min_length=1, max_length=8_000)
    domain: Literal["executive", "payments", "risk", "credit", "customers", "growth"] = "executive"
    conversation_id: str | None = Field(default=None, max_length=160)
    request_id: str = Field(default_factory=lambda: str(uuid.uuid4()), min_length=1, max_length=160)
    country_code: str | None = None
    locale: str | None = None


def _sse(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


def _workspace_auth() -> tuple[str, str]:
    from databricks.sdk import WorkspaceClient

    client = WorkspaceClient()
    return client.config.host.rstrip("/"), client.config.authenticate().get("Authorization", "")


def _text(item: dict[str, Any]) -> str:
    blocks = item.get("content") or item.get("summary") or []
    return "\n".join(str(block.get("text", "")) for block in blocks if isinstance(block, dict) and block.get("text"))


def _research_prompt(question: str) -> str:
    return f"""Pergunta do usuário:
{question}

Contrato de resposta do Quilla Genie Research:
- Responda no mesmo idioma da pergunta.
- Faça uma investigação multi-etapas usando somente os dados governados disponíveis no Genie Space.
- Comece com uma resposta objetiva e informe claramente período, unidade, moeda e escopo utilizados.
- Para perguntas agregadas, apresente também a decomposição mais relevante (por país, produto ou segmento) e compare com o período anterior equivalente, quando esses dados estiverem disponíveis.
- Termine com os principais insights executivos, ressalvas de qualidade ou cobertura e próximos passos úteis.
- Cite as consultas e fontes governadas utilizadas. Não invente valores nem complete lacunas com suposições.
- Se alguma decomposição ou comparação não estiver disponível, declare a lacuna explicitamente.
""".strip()


PARTY_ID_PATTERN = re.compile(r"\bQPTY-\d{12}\b")


def _sanitize_individual_answer(question: str, answer: str) -> str:
    """Minimiza identificadores e tabelas técnicas em uma análise individual sintética."""
    if not PARTY_ID_PATTERN.search(question):
        return answer
    portuguese = any(token in question.lower() for token in ("analise", " para o cliente", " não "))
    placeholder = "cliente selecionado" if portuguese else "cliente seleccionado"
    clean: list[str] = []
    technical_table = False
    for line in answer.splitlines():
        stripped = line.strip()
        lowered = stripped.lower()
        if stripped.startswith("|"):
            if "party_id" in lowered:
                technical_table = True
            if technical_table:
                continue
        elif technical_table:
            technical_table = False
        if "perfil completo customer 360" in lowered or lowered.startswith("*showing "):
            continue
        clean.append(PARTY_ID_PATTERN.sub(placeholder, line))
    return "\n".join(clean).strip()


GENIE_SESSION_NAMESPACE = uuid.UUID("609636d3-0e96-4bc0-92a8-3812568b83c5")


def _genie_session_id(actor: ActorContext, conversation_id: str) -> uuid.UUID:
    return uuid.uuid5(
        GENIE_SESSION_NAMESPACE,
        "\x1f".join((actor.tenant_id, actor.actor_id, "genie_research", conversation_id)),
    )


async def _persist_exchange(
    *,
    actor: ActorContext,
    conversation_id: str,
    request_id: str,
    question: str,
    answer: str,
    domain: str,
    country_code: str,
    locale: str,
    steps: list[str],
    queries: list[dict[str, str]],
) -> str:
    session_id = _genie_session_id(actor, conversation_id)
    language = "pt" if locale.startswith("pt") else "es"
    user_message_id = uuid.uuid5(session_id, f"{request_id}:user")
    assistant_message_id = uuid.uuid5(session_id, f"{request_id}:assistant")
    context = json.dumps(
        {
            "experience": "genie_research",
            "domain": domain,
            "genie_conversation_id": conversation_id,
            "demo": actor.demo,
        },
        ensure_ascii=False,
    )
    assistant_metadata = json.dumps(
        {
            "experience": "genie_research",
            "domain": domain,
            "request_id": request_id,
            "genie_steps": steps,
            "genie_queries": queries,
        },
        ensure_ascii=False,
    )
    user_metadata = json.dumps(
        {"experience": "genie_research", "domain": domain, "request_id": request_id},
        ensure_ascii=False,
    )
    async with db.connection() as connection:
        async with connection.transaction():
            await connection.execute(
                """
                INSERT INTO quilla_app.app_sessions AS existing (
                  session_id, tenant_id, external_conversation_id, actor_id,
                  country_code, locale, title, context
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb)
                ON CONFLICT (session_id) DO UPDATE SET
                  country_code = EXCLUDED.country_code,
                  locale = EXCLUDED.locale,
                  context = existing.context || EXCLUDED.context,
                  updated_at = now(),
                  last_activity_at = now()
                """,
                session_id,
                actor.tenant_id,
                conversation_id,
                actor.actor_id,
                country_code,
                locale,
                question.strip()[:160],
                context,
            )
            already_saved = await connection.fetchval(
                "SELECT EXISTS (SELECT 1 FROM quilla_app.app_messages WHERE message_id = $1)",
                assistant_message_id,
            )
            if not already_saved:
                next_sequence = int(await connection.fetchval(
                    "SELECT COALESCE(MAX(sequence_number), 0) + 1 FROM quilla_app.app_messages WHERE session_id = $1",
                    session_id,
                ))
                await connection.execute(
                    """
                    INSERT INTO quilla_app.app_messages (
                      message_id, session_id, sequence_number, role, content,
                      language_code, metadata
                    ) VALUES ($1, $2, $3, 'user', $4, $5, $6::jsonb)
                    """,
                    user_message_id,
                    session_id,
                    next_sequence,
                    question,
                    language,
                    user_metadata,
                )
                await connection.execute(
                    """
                    INSERT INTO quilla_app.app_messages (
                      message_id, session_id, sequence_number, role, content,
                      language_code, model_endpoint, metadata
                    ) VALUES ($1, $2, $3, 'assistant', $4, $5, 'databricks-genie-agent', $6::jsonb)
                    """,
                    assistant_message_id,
                    session_id,
                    next_sequence + 1,
                    answer,
                    language,
                    assistant_metadata,
                )
    return str(session_id)


@router.get("/mode")
async def mode(_actor: ActorContext = Depends(trusted_actor)):
    return {
        "agent_mode": True,
        "domains": [key for key, env in SPACE_ENV.items() if os.getenv(env)],
        "fallback": None,
    }


@router.post("/research/stream")
async def research_stream(
    body: GenieRequest,
    actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
):
    space_id = os.getenv(SPACE_ENV[body.domain], "").strip()
    if not space_id:
        raise HTTPException(status_code=503, detail="Genie Space not configured")
    host, service_authorization = await asyncio.to_thread(_workspace_auth)
    authorization = user_authorization or service_authorization
    country_code, locale = trusted_geo(body.country_code, body.locale)

    async def events():
        started_at = time.monotonic()
        stage = "connecting"
        yield _sse("accepted", {
            "mode": "agent", "domain": body.domain, "stage": stage, "elapsed_seconds": 0,
        })
        resolved_conversation_id = body.conversation_id
        steps: list[str] = []
        queries: list[dict[str, str]] = []
        payload: dict[str, Any] = {
            "input": [{
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": _research_prompt(body.question)}],
            }],
            "stream": True,
        }
        if body.conversation_id:
            payload["conversation_id"] = body.conversation_id
        try:
            timeout = httpx.Timeout(240.0, connect=5.0)
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream(
                    "POST",
                    f"{host}/api/2.0/genie/agents/{space_id}/responses",
                    headers={"Authorization": authorization, "Accept": "text/event-stream"},
                    json=payload,
                ) as response:
                    response.raise_for_status()
                    stage = "planning"
                    yield _sse("progress", {
                        "stage": stage, "elapsed_seconds": round(time.monotonic() - started_at, 1),
                    })
                    lines = response.aiter_lines().__aiter__()
                    next_line: asyncio.Task[str] | None = asyncio.create_task(lines.__anext__())
                    try:
                        while True:
                            try:
                                line = await asyncio.wait_for(asyncio.shield(next_line), timeout=4.0)
                            except asyncio.TimeoutError:
                                yield _sse("progress", {
                                    "stage": stage,
                                    "elapsed_seconds": round(time.monotonic() - started_at, 1),
                                })
                                continue
                            except StopAsyncIteration:
                                break
                            next_line = asyncio.create_task(lines.__anext__())
                            if not line.startswith("data:"):
                                continue
                            raw = line[5:].strip()
                            if not raw or raw == "[DONE]":
                                continue
                            event = json.loads(raw)
                            item = event.get("item") or {}
                            kind = item.get("type")
                            if event.get("type") == "response.created":
                                conversation_id = (event.get("response") or {}).get("conversation_id")
                                if conversation_id:
                                    resolved_conversation_id = conversation_id
                                    yield _sse("conversation", {"conversation_id": conversation_id})
                            elif event.get("type") == "response.output_item.done" and kind == "reasoning":
                                if text := _text(item):
                                    stage = "planning"
                                    steps.append(text)
                                    yield _sse("step", {"text": text})
                            elif event.get("type") == "response.output_item.done" and kind == "function_call":
                                stage = "querying"
                                arguments = item.get("arguments") or "{}"
                                try:
                                    arguments = json.loads(arguments) if isinstance(arguments, str) else arguments
                                except json.JSONDecodeError:
                                    arguments = {"sql": str(arguments)}
                                query = {
                                    "title": arguments.get("title", "Consulta governada"),
                                    "sql": arguments.get("sql") or arguments.get("query") or "",
                                }
                                queries.append(query)
                                yield _sse("query", query)
                            elif event.get("type") == "response.output_item.done" and kind == "message":
                                if text := _text(item):
                                    stage = "synthesizing"
                                    yield _sse("answer", {"text": _sanitize_individual_answer(body.question, text)})
                            elif event.get("type") == "response.completed":
                                completed = event.get("response") or {}
                                answer = next((_text(item) for item in reversed(completed.get("output") or []) if item.get("type") == "message" and _text(item)), "")
                                answer = _sanitize_individual_answer(body.question, answer)
                                resolved_conversation_id = completed.get("conversation_id") or resolved_conversation_id
                                if not resolved_conversation_id or not answer:
                                    raise RuntimeError("Genie completed without a recoverable conversation or answer")
                                stage = "persisting"
                                yield _sse("progress", {
                                    "stage": stage,
                                    "elapsed_seconds": round(time.monotonic() - started_at, 1),
                                })
                                session_id = await _persist_exchange(
                                    actor=actor,
                                    conversation_id=resolved_conversation_id,
                                    request_id=body.request_id,
                                    question=body.question,
                                    answer=answer,
                                    domain=body.domain,
                                    country_code=country_code,
                                    locale=locale,
                                    steps=steps,
                                    queries=queries,
                                )
                                yield _sse("completed", {
                                    "mode": "agent",
                                    "conversation_id": resolved_conversation_id,
                                    "session_id": session_id,
                                    "persisted": True,
                                    "answer": answer,
                                    "elapsed_seconds": round(time.monotonic() - started_at, 1),
                                })
                                return
                    finally:
                        if next_line is not None and not next_line.done():
                            next_line.cancel()
                            with suppress(asyncio.CancelledError, StopAsyncIteration):
                                await next_line
        except httpx.HTTPStatusError as exc:
            yield _sse("error", {"message": f"Genie Agent indisponível: HTTP {exc.response.status_code}"})
        except DatabaseUnavailable:
            yield _sse("error", {"message": "A resposta foi gerada, mas não pôde ser armazenada no Lakebase."})
        except Exception as exc:
            log_event("genie_research_failed", error=type(exc).__name__, domain=body.domain)
            yield _sse("error", {"message": f"Genie Agent falhou: {type(exc).__name__}"})

    return StreamingResponse(events(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
