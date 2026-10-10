"""Ledger de idempotência para impedir turnos duplicados em retries."""

from __future__ import annotations

import hashlib
import json

from .db import db


class IdempotencyInProgress(RuntimeError):
    pass


def decode_response_payload(payload):
    return json.loads(payload) if isinstance(payload, str) else payload


def payload_hash(payload: dict) -> str:
    canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def claim(key: str, tenant_id: str, actor_id: str, digest: str) -> dict | None:
    async with db.connection() as connection:
        row = await connection.fetchrow(
            """
            INSERT INTO quilla_app.app_idempotency (
              idempotency_key, tenant_id, actor_id, request_hash, status
            ) VALUES ($1, $2, $3, $4, 'processing')
            ON CONFLICT (tenant_id, actor_id, idempotency_key) DO UPDATE
              SET status = 'processing', error_message = NULL, updated_at = now()
              WHERE app_idempotency.status = 'failed'
                AND app_idempotency.request_hash = EXCLUDED.request_hash
            RETURNING request_hash, status, response_payload, (xmax = 0) AS inserted
            """,
            key, tenant_id, actor_id, digest,
        )
        if row is None:
            row = await connection.fetchrow(
                """
                SELECT request_hash, status, response_payload, false AS inserted
                FROM quilla_app.app_idempotency
                WHERE idempotency_key = $1 AND tenant_id = $2 AND actor_id = $3
                """,
                key, tenant_id, actor_id,
            )
    value = dict(row)
    if value["request_hash"] != digest:
        raise ValueError("Idempotency key was already used for a different payload")
    if value["status"] == "completed":
        return decode_response_payload(value["response_payload"])
    if value["status"] == "processing" and not value["inserted"]:
        raise IdempotencyInProgress("An identical request is already processing")
    return None


async def complete(key: str, tenant_id: str, actor_id: str, response: dict) -> None:
    async with db.connection() as connection:
        await connection.execute(
            """
            UPDATE quilla_app.app_idempotency
            SET status = 'completed', response_payload = $4::jsonb, updated_at = now()
            WHERE idempotency_key = $1 AND tenant_id = $2 AND actor_id = $3
            """,
            key, tenant_id, actor_id, json.dumps(response, ensure_ascii=False),
        )


async def fail(key: str, tenant_id: str, actor_id: str, error: str) -> None:
    async with db.connection() as connection:
        await connection.execute(
            """
            UPDATE quilla_app.app_idempotency
            SET status = 'failed', error_message = $4, updated_at = now()
            WHERE idempotency_key = $1 AND tenant_id = $2 AND actor_id = $3
            """,
            key, tenant_id, actor_id, error[:1000],
        )
