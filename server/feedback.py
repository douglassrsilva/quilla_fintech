"""Feedback idempotente vinculado ao trace e à fila de revisão."""

from __future__ import annotations

import json
import uuid

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field

from .config import settings
from .db import DatabaseUnavailable, db
from .security import ActorContext, trusted_actor


router = APIRouter(prefix="/api/feedback", tags=["feedback"])


class FeedbackRequest(BaseModel):
    conversation_id: uuid.UUID
    message_id: uuid.UUID | None = None
    trace_id: str = Field(min_length=1, max_length=256)
    rating: int = Field(ge=-1, le=1)
    category: str | None = Field(default=None, max_length=80)
    comment: str | None = Field(default=None, max_length=2000)


@router.post("")
async def create_feedback(
    body: FeedbackRequest,
    actor: ActorContext = Depends(trusted_actor),
    idempotency_key: str | None = Header(default=None, alias="X-Idempotency-Key"),
):
    key = (idempotency_key or str(uuid.uuid4()))[:160]
    feedback_id = uuid.uuid5(uuid.NAMESPACE_URL, f"{actor.tenant_id}:{actor.actor_id}:{key}")
    try:
        async with db.connection() as connection:
            async with connection.transaction():
                owns_session = await connection.fetchval(
                    """
                    SELECT EXISTS (
                      SELECT 1 FROM quilla_app.app_sessions
                      WHERE session_id = $1 AND tenant_id = $2 AND actor_id = $3
                        AND deleted_at IS NULL
                    )
                    """,
                    body.conversation_id, actor.tenant_id, actor.actor_id,
                )
                if not owns_session:
                    raise HTTPException(status_code=404, detail="Conversation not found")
                trace_owned = await connection.fetchval(
                    """
                    SELECT EXISTS (
                      SELECT 1 FROM quilla_app.app_messages
                      WHERE session_id = $1 AND mlflow_trace_id = $2
                        AND ($3::uuid IS NULL OR message_id = $3)
                    )
                    """,
                    body.conversation_id, body.trace_id, body.message_id,
                )
                if not trace_owned:
                    raise HTTPException(status_code=422, detail="Trace does not belong to this conversation")
                row = await connection.fetchrow(
                    """
                    INSERT INTO quilla_app.app_feedback (
                      feedback_id, tenant_id, actor_id, session_id, message_id,
                      mlflow_trace_id, rating, category, comment, idempotency_key,
                      review_queue_name, review_status
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'pending')
                    ON CONFLICT (tenant_id, actor_id, idempotency_key) DO UPDATE
                      SET idempotency_key = EXCLUDED.idempotency_key
                    RETURNING feedback_id, mlflow_trace_id, rating, review_queue_name,
                              review_status, created_at
                    """,
                    feedback_id, actor.tenant_id, actor.actor_id, body.conversation_id,
                    body.message_id, body.trace_id, body.rating, body.category,
                    body.comment, key, settings.review_queue_name,
                )
                await connection.execute(
                    """
                    INSERT INTO quilla_app.outbox_events (
                      event_id, aggregate_type, aggregate_id, event_type, payload
                    ) VALUES ($1, 'feedback', $2, 'mlflow.review.requested', $3::jsonb)
                    ON CONFLICT (event_id) DO NOTHING
                    """,
                    uuid.uuid5(feedback_id, "review-queue"), str(feedback_id),
                    json.dumps({"trace_id": body.trace_id, "review_queue": settings.review_queue_name}),
                )
    except DatabaseUnavailable as exc:
        raise HTTPException(status_code=503, detail="Feedback store unavailable") from exc
    return dict(row)
