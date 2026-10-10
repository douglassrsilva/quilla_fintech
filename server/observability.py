"""Logs estruturados, correlação e probes operacionais."""

from __future__ import annotations

import json
import logging
import time
import uuid

from fastapi import APIRouter, Request


logger = logging.getLogger("quilla.app")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO, format="%(message)s")


def log_event(event: str, **fields) -> None:
    logger.info(json.dumps({"event": event, **fields}, ensure_ascii=False, default=str))


async def correlation_middleware(request: Request, call_next):
    supplied = request.headers.get("X-Request-ID", "").strip()
    request.state.request_id = supplied[:128] if supplied else str(uuid.uuid4())
    started = time.perf_counter()
    try:
        response = await call_next(request)
    except Exception:
        log_event(
            "http_request_failed",
            request_id=request.state.request_id,
            method=request.method,
            path=request.url.path,
            duration_ms=round((time.perf_counter() - started) * 1000),
        )
        raise
    response.headers["X-Request-ID"] = request.state.request_id
    log_event(
        "http_request_completed",
        request_id=request.state.request_id,
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=round((time.perf_counter() - started) * 1000),
    )
    return response


router = APIRouter(tags=["operations"])


@router.get("/api/health/live")
async def liveness():
    return {"ok": True, "status": "live"}
