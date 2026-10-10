"""Quilla — backend BFF e frontend para Databricks Apps."""

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from server.chat import router as chat_router
from server.analytics import router as analytics_router
from server.conversations import router as conversations_router
from server.db import db
from server.feedback import router as feedback_router
from server.genie import router as genie_router
from server.observability import correlation_middleware, router as operations_router
from server.platform import router as platform_router


@asynccontextmanager
async def lifespan(_app: FastAPI):
    yield
    await db.close()


app = FastAPI(title="Quilla · Financial Intelligence", lifespan=lifespan)
app.middleware("http")(correlation_middleware)
app.include_router(chat_router)
app.include_router(analytics_router)
app.include_router(conversations_router)
app.include_router(feedback_router)
app.include_router(genie_router)
app.include_router(operations_router)
app.include_router(platform_router)
dist = Path(__file__).parent / "frontend" / "dist"


@app.get("/api/health")
async def health():
    return {"ok": True, "mode": "databricks-app", "data": "synthetic"}


@app.get("/api/health/ready")
async def readiness():
    ready = await db.ping()
    payload = {"ok": ready, "status": "ready" if ready else "not_ready", "lakebase": ready}
    return payload if ready else JSONResponse(payload, status_code=503)


if dist.is_dir():
    assets = dist / "assets"
    if assets.is_dir():
        app.mount("/assets", StaticFiles(directory=assets), name="assets")

    @app.get("/{full_path:path}")
    def spa(full_path: str):
        requested = dist / full_path
        if full_path and requested.is_file():
            return FileResponse(requested)
        return FileResponse(dist / "index.html")
