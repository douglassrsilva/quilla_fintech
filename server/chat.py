"""BFF de chat: contexto confiável, budget de latência e SSE."""

from __future__ import annotations

import asyncio
import json
import time
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from typing import Literal

from .config import settings
from .db import DatabaseUnavailable
from .idempotency import IdempotencyInProgress, claim, complete, fail, payload_hash
from .observability import log_event
from .security import (
    ActorContext,
    request_id,
    trusted_actor,
    trusted_geo,
    trusted_user_authorization,
)
from .supervisor import SupervisorError, supervisor_client


router = APIRouter(prefix="/api/chat", tags=["chat"])


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message: str = Field(min_length=1, max_length=20_000)
    conversation_id: str | None = Field(default=None, min_length=1, max_length=160)
    country_code: str | None = None
    locale: str | None = None
    experience: Literal["market_intelligence", "general"] = "market_intelligence"


def _payload(result, conversation_id: str, rid: str) -> dict:
    return {
        "conversation_id": conversation_id,
        "message": result.text,
        "trace_id": result.trace_id,
        "route": result.route,
        "evidence": result.evidence,
        "requires_human_review": result.requires_human_review,
        "request_id": rid,
    }


async def _invoke(body: ChatRequest, actor: ActorContext, rid: str, key: str,
                  user_authorization: str | None = None) -> tuple[dict, bool]:
    country, locale = trusted_geo(body.country_code, body.locale)
    conversation_id = body.conversation_id or str(uuid.uuid4())
    fingerprint = {
        "message": body.message,
        "conversation_id": conversation_id,
        "country_code": country,
        "locale": locale,
    }
    digest = payload_hash(fingerprint)
    try:
        cached = await claim(key, actor.tenant_id, actor.actor_id, digest)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except IdempotencyInProgress as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except DatabaseUnavailable as exc:
        if settings.database_required:
            raise HTTPException(status_code=503, detail="Idempotency store unavailable") from exc
        cached = None
    if cached:
        return cached, True
    try:
        result = await asyncio.wait_for(
            supervisor_client.invoke(
                message=body.message,
                conversation_id=conversation_id,
                actor_id=actor.actor_id,
                tenant_id=actor.tenant_id,
                roles=actor.roles,
                demo=actor.demo,
                country_code=country,
                locale=locale,
                request_id=rid,
                experience=body.experience,
                user_authorization=user_authorization,
            ),
            timeout=settings.request_timeout_seconds,
        )
        response = _payload(result, conversation_id, rid)
        try:
            await complete(key, actor.tenant_id, actor.actor_id, response)
        except DatabaseUnavailable:
            if settings.database_required:
                raise
        return response, False
    except Exception as exc:
        try:
            await fail(key, actor.tenant_id, actor.actor_id, str(exc))
        except Exception:
            pass
        raise


@router.post("")
async def chat(
    body: ChatRequest,
    request: Request,
    actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
    idempotency_key: str | None = Header(default=None, alias="X-Idempotency-Key"),
):
    rid = request_id(request)
    key = (idempotency_key or rid)[:160]
    try:
        response, cached = await _invoke(body, actor, rid, key, user_authorization)
    except asyncio.TimeoutError as exc:
        raise HTTPException(status_code=504, detail="Supervisor response timed out") from exc
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail=str(exc)) from exc
    except SupervisorError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except DatabaseUnavailable as exc:
        raise HTTPException(status_code=503, detail="Conversation persistence unavailable") from exc
    response["idempotent_replay"] = cached
    return response


def _sse(event: str, data: dict) -> str:
    return f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


@router.post("/stream")
async def chat_stream(
    body: ChatRequest,
    request: Request,
    actor: ActorContext = Depends(trusted_actor),
    user_authorization: str | None = Depends(trusted_user_authorization),
    idempotency_key: str | None = Header(default=None, alias="X-Idempotency-Key"),
):
    rid = request_id(request)
    key = (idempotency_key or rid)[:160]
    conversation_id = body.conversation_id or str(uuid.uuid4())
    normalized = body.model_copy(update={"conversation_id": conversation_id})

    async def events():
        started_at = time.monotonic()
        yield _sse("accepted", {
            "request_id": rid,
            "conversation_id": conversation_id,
            "stage": "request_accepted",
            "elapsed_seconds": 0,
        })
        task = asyncio.create_task(_invoke(normalized, actor, rid, key, user_authorization))
        try:
            while not task.done():
                if await request.is_disconnected():
                    task.cancel()
                    log_event("chat_cancelled", request_id=rid, actor_id=actor.actor_id)
                    return
                try:
                    await asyncio.wait_for(asyncio.shield(task), timeout=settings.heartbeat_seconds)
                except asyncio.TimeoutError:
                    yield _sse("heartbeat", {
                        "request_id": rid,
                        "stage": "awaiting_supervisor",
                        "elapsed_seconds": round(time.monotonic() - started_at, 1),
                    })
            response, cached = await task
            for start in range(0, len(response["message"]), 160):
                yield _sse("delta", {"text": response["message"][start:start + 160]})
            if response["evidence"]:
                yield _sse("evidence", {"items": response["evidence"]})
            yield _sse("completed", {**response, "idempotent_replay": cached})
        except asyncio.CancelledError:
            task.cancel()
            raise
        except (asyncio.TimeoutError, TimeoutError):
            message = "Tempo limite excedido" if normalized.locale == "pt-BR" else "Se agotó el tiempo de respuesta"
            yield _sse("error", {"request_id": rid, "code": "timeout", "message": message})
        except ValueError as exc:
            yield _sse("error", {"request_id": rid, "code": "idempotency_conflict", "message": str(exc)})
        except Exception as exc:
            log_event("chat_stream_failed", request_id=rid, error=type(exc).__name__)
            message = "Não foi possível concluir a resposta" if normalized.locale == "pt-BR" else "No fue posible completar la respuesta"
            yield _sse("error", {"request_id": rid, "code": "upstream_error", "message": message})

    return StreamingResponse(
        events(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
