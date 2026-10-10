"""Histórico conversacional paginado e isolado por ator/tenant."""

from __future__ import annotations

import base64
from datetime import datetime
import json
import uuid
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from .db import DatabaseUnavailable, db
from .security import ActorContext, trusted_actor


router = APIRouter(prefix="/api/conversations", tags=["conversations"])


class ConversationPatch(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=160)
    archived: bool | None = None


def _encode_cursor(at: datetime, item_id: uuid.UUID | str) -> str:
    raw = json.dumps([at.isoformat(), str(item_id)], separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _decode_cursor(value: str | None) -> tuple[datetime, uuid.UUID] | None:
    if not value:
        return None
    try:
        padded = value + "=" * (-len(value) % 4)
        at, item_id = json.loads(base64.urlsafe_b64decode(padded).decode())
        return datetime.fromisoformat(at), uuid.UUID(item_id)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="Invalid cursor") from exc


@router.get("")
async def list_conversations(
    limit: int = Query(default=30, ge=1, le=100),
    cursor: str | None = Query(default=None),
    include_archived: bool = Query(default=False),
    experience: Literal["market_intelligence", "genie_research"] | None = Query(default=None),
    actor: ActorContext = Depends(trusted_actor),
):
    page = _decode_cursor(cursor)
    conditions = ["s.tenant_id = $1", "s.actor_id = $2", "s.deleted_at IS NULL"]
    arguments: list = [actor.tenant_id, actor.actor_id]
    if not include_archived:
        conditions.append("s.status = 'active'")
    if experience == "market_intelligence":
        conditions.append("(s.context->>'experience' = 'market_intelligence' OR NOT (s.context ? 'experience'))")
    elif experience:
        arguments.append(experience)
        conditions.append(f"s.context->>'experience' = ${len(arguments)}")
    if page:
        arguments.extend(page)
        conditions.append(f"(s.last_activity_at, s.session_id) < (${len(arguments)-1}, ${len(arguments)})")
    arguments.append(limit + 1)
    query = f"""
        SELECT s.session_id, s.external_conversation_id, s.title, s.country_code,
               s.locale, s.status, s.created_at, s.last_activity_at,
               count(m.message_id)::int AS message_count
        FROM quilla_app.app_sessions s
        LEFT JOIN quilla_app.app_messages m ON m.session_id = s.session_id
        WHERE {' AND '.join(conditions)}
        GROUP BY s.session_id
        ORDER BY s.last_activity_at DESC, s.session_id DESC
        LIMIT ${len(arguments)}
    """
    try:
        async with db.connection() as connection:
            rows = await connection.fetch(query, *arguments)
    except DatabaseUnavailable as exc:
        raise HTTPException(status_code=503, detail="Conversation store unavailable") from exc
    values = [dict(row) for row in rows[:limit]]
    next_cursor = None
    if len(rows) > limit and values:
        last = values[-1]
        next_cursor = _encode_cursor(last["last_activity_at"], last["session_id"])
    return {"conversations": values, "next_cursor": next_cursor}


@router.get("/{session_id}")
async def get_conversation(
    session_id: uuid.UUID,
    message_limit: int = Query(default=100, ge=1, le=200),
    message_cursor: str | None = Query(default=None),
    actor: ActorContext = Depends(trusted_actor),
):
    page = _decode_cursor(message_cursor)
    try:
        async with db.connection() as connection:
            session = await connection.fetchrow(
                """
                SELECT session_id, external_conversation_id, title, country_code,
                       locale, status, context, created_at, last_activity_at
                FROM quilla_app.app_sessions
                WHERE session_id = $1 AND tenant_id = $2 AND actor_id = $3
                  AND deleted_at IS NULL
                """,
                session_id, actor.tenant_id, actor.actor_id,
            )
            if session is None:
                raise HTTPException(status_code=404, detail="Conversation not found")
            arguments: list = [session_id]
            condition = "session_id = $1"
            if page:
                arguments.extend(page)
                condition += " AND (created_at, message_id) > ($2, $3)"
            arguments.append(message_limit + 1)
            messages = await connection.fetch(
                f"""
                SELECT message_id, sequence_number, role, content, language_code,
                       model_endpoint, mlflow_trace_id, citations, metadata, created_at
                FROM quilla_app.app_messages
                WHERE {condition}
                ORDER BY created_at, message_id
                LIMIT ${len(arguments)}
                """,
                *arguments,
            )
    except DatabaseUnavailable as exc:
        raise HTTPException(status_code=503, detail="Conversation store unavailable") from exc
    selected = [dict(row) for row in messages[:message_limit]]
    next_cursor = None
    if len(messages) > message_limit and selected:
        last = selected[-1]
        next_cursor = _encode_cursor(last["created_at"], last["message_id"])
    return {**dict(session), "messages": selected, "next_message_cursor": next_cursor}


@router.patch("/{session_id}")
async def update_conversation(
    session_id: uuid.UUID,
    changes: ConversationPatch,
    actor: ActorContext = Depends(trusted_actor),
):
    if changes.title is None and changes.archived is None:
        raise HTTPException(status_code=422, detail="No conversation change supplied")
    status = None if changes.archived is None else ("closed" if changes.archived else "active")
    try:
        async with db.connection() as connection:
            row = await connection.fetchrow(
                """
                UPDATE quilla_app.app_sessions
                SET title = COALESCE($4, title), status = COALESCE($5, status),
                    updated_at = now()
                WHERE session_id = $1 AND tenant_id = $2 AND actor_id = $3
                  AND deleted_at IS NULL
                RETURNING session_id, title, status, updated_at
                """,
                session_id, actor.tenant_id, actor.actor_id, changes.title, status,
            )
    except DatabaseUnavailable as exc:
        raise HTTPException(status_code=503, detail="Conversation store unavailable") from exc
    if row is None:
        raise HTTPException(status_code=404, detail="Conversation not found")
    return dict(row)
