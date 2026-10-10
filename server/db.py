"""Pool assíncrono Lakebase com renovação de OAuth e retry controlado."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import time
from typing import Any, AsyncIterator

from .config import settings
from .observability import log_event


class DatabaseUnavailable(RuntimeError):
    pass


class LakebasePool:
    def __init__(self):
        self._pool = None
        self._created_at = 0.0
        self._lock = asyncio.Lock()

    @property
    def configured(self) -> bool:
        return bool(settings.lakebase_host)

    @staticmethod
    def _credentials() -> tuple[str, str]:
        from databricks.sdk import WorkspaceClient

        client = WorkspaceClient()
        principal = client.current_user.me()
        user = settings.lakebase_user or principal.user_name or principal.display_name
        authorization = client.config.authenticate().get("Authorization", "")
        token = authorization.removeprefix("Bearer ").strip()
        if not user or not token:
            raise DatabaseUnavailable("Lakebase OAuth identity is incomplete")
        return user, token

    async def _close_unlocked(self) -> None:
        if self._pool is not None:
            await self._pool.close()
        self._pool = None
        self._created_at = 0.0

    async def pool(self):
        if not self.configured:
            raise DatabaseUnavailable("Lakebase is not configured")
        expired = time.monotonic() - self._created_at >= settings.token_refresh_seconds
        if self._pool is not None and not expired:
            return self._pool
        async with self._lock:
            expired = time.monotonic() - self._created_at >= settings.token_refresh_seconds
            if self._pool is not None and not expired:
                return self._pool
            await self._close_unlocked()
            import asyncpg

            user, token = await asyncio.to_thread(self._credentials)
            self._pool = await asyncpg.create_pool(
                host=settings.lakebase_host,
                port=settings.lakebase_port,
                database=settings.lakebase_database,
                user=user,
                password=token,
                ssl="require",
                min_size=settings.db_pool_min_size,
                max_size=max(settings.db_pool_min_size, settings.db_pool_max_size),
                command_timeout=15,
            )
            self._created_at = time.monotonic()
            log_event("lakebase_pool_created", max_size=settings.db_pool_max_size)
            return self._pool

    async def refresh(self) -> None:
        async with self._lock:
            await self._close_unlocked()

    @asynccontextmanager
    async def connection(self) -> AsyncIterator[Any]:
        pool = None
        connection = None
        for attempt in range(2):
            try:
                pool = await self.pool()
                connection = await pool.acquire()
                break
            except Exception as exc:
                auth_failure = any(term in str(exc).lower() for term in ("password", "authentication", "token"))
                if attempt == 0 and auth_failure:
                    await self.refresh()
                    continue
                raise DatabaseUnavailable(str(exc)) from exc
        if pool is None or connection is None:
            raise DatabaseUnavailable("Lakebase connection could not be acquired")
        try:
            yield connection
        except Exception as exc:
            if any(term in str(exc).lower() for term in ("password", "authentication", "token")):
                await self.refresh()
            if exc.__class__.__module__.startswith("asyncpg"):
                raise DatabaseUnavailable(str(exc)) from exc
            raise
        finally:
            try:
                await pool.release(connection)
            except Exception:
                pass

    async def ping(self) -> bool:
        try:
            async with self.connection() as connection:
                return bool(
                    await connection.fetchval(
                        "SELECT to_regclass('quilla_app.app_sessions') IS NOT NULL"
                    )
                )
        except DatabaseUnavailable:
            return False

    async def close(self) -> None:
        async with self._lock:
            await self._close_unlocked()


db = LakebasePool()
