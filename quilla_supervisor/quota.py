"""Fail-closed persistent quotas for billable external MCP tools."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import threading
from typing import Callable
import uuid


class QuotaExceeded(RuntimeError):
    pass


@dataclass(frozen=True)
class QuotaReservation:
    usage_id: str
    daily_calls: int
    total_calls: int
    conversation_calls: int


class LakebaseToolQuota:
    """Atomically reserves billable calls in Lakebase before execution."""

    def __init__(
        self,
        connection_factory: Callable,
        *,
        daily_limit: int,
        total_limit: int,
        conversation_limit: int,
    ):
        self.connection_factory = connection_factory
        self.daily_limit = daily_limit
        self.total_limit = total_limit
        self.conversation_limit = conversation_limit

    @classmethod
    def from_env(cls, *, daily_limit: int, total_limit: int, conversation_limit: int):
        from .memory import LakebaseConversationStore

        store = LakebaseConversationStore.from_env()
        if not getattr(store, "enabled", False):
            raise QuotaExceeded("Lakebase quota ledger is unavailable; paid fallback denied")
        return cls(
            store._connection,
            daily_limit=daily_limit,
            total_limit=total_limit,
            conversation_limit=conversation_limit,
        )

    def reserve(self, provider: str, conversation_id: str, request_id: str) -> QuotaReservation:
        if min(self.daily_limit, self.total_limit, self.conversation_limit) <= 0:
            raise QuotaExceeded(f"Paid fallback {provider} is disabled by quota")
        usage_id = str(uuid.uuid4())
        today = date.today()
        try:
            with self.connection_factory() as connection, connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO quilla_app.external_tool_budget (
                      provider, daily_window, daily_calls, total_calls
                    ) VALUES (%s, %s, 0, 0)
                    ON CONFLICT (provider) DO NOTHING
                    """,
                    (provider, today),
                )
                cursor.execute(
                    """
                    SELECT daily_window, daily_calls, total_calls
                    FROM quilla_app.external_tool_budget
                    WHERE provider = %s
                    FOR UPDATE
                    """,
                    (provider,),
                )
                daily_window, daily_calls, total_calls = cursor.fetchone()
                effective_daily = daily_calls if daily_window == today else 0
                cursor.execute(
                    """
                    SELECT count(*)
                    FROM quilla_app.external_tool_usage
                    WHERE provider = %s AND conversation_id = %s
                    """,
                    (provider, conversation_id),
                )
                conversation_calls = int(cursor.fetchone()[0])
                if effective_daily >= self.daily_limit:
                    raise QuotaExceeded(f"Daily limit reached for {provider}: {self.daily_limit}")
                if total_calls >= self.total_limit:
                    raise QuotaExceeded(f"Demo total limit reached for {provider}: {self.total_limit}")
                if conversation_calls >= self.conversation_limit:
                    raise QuotaExceeded(
                        f"Conversation limit reached for {provider}: {self.conversation_limit}"
                    )
                effective_daily += 1
                total_calls += 1
                conversation_calls += 1
                cursor.execute(
                    """
                    UPDATE quilla_app.external_tool_budget
                    SET daily_window = %s, daily_calls = %s, total_calls = %s,
                        updated_at = now()
                    WHERE provider = %s
                    """,
                    (today, effective_daily, total_calls, provider),
                )
                cursor.execute(
                    """
                    INSERT INTO quilla_app.external_tool_usage (
                      usage_id, provider, conversation_id, request_id, status
                    ) VALUES (%s, %s, %s, %s, 'reserved')
                    """,
                    (usage_id, provider, conversation_id, request_id),
                )
        except QuotaExceeded:
            raise
        except Exception as exc:
            raise QuotaExceeded(
                f"Persistent quota check failed for {provider}; paid fallback denied: {exc}"
            ) from exc
        return QuotaReservation(usage_id, effective_daily, total_calls, conversation_calls)

    def complete(self, reservation: QuotaReservation, *, status: str, latency_ms: int, error: str = "") -> None:
        try:
            with self.connection_factory() as connection, connection.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE quilla_app.external_tool_usage
                    SET status = %s, latency_ms = %s, error_message = NULLIF(%s, ''),
                        completed_at = now()
                    WHERE usage_id = %s
                    """,
                    (status, latency_ms, error[:1000], reservation.usage_id),
                )
        except Exception:
            # The call was already reserved and counted. Never compensate the
            # budget on an uncertain write, which could permit double spend.
            return


class RequestCallLimit:
    """Prevents more than one paid fallback call from a single model request."""

    def __init__(self, maximum: int = 1):
        self.maximum = maximum
        self.calls = 0
        self._lock = threading.Lock()

    def acquire(self) -> None:
        with self._lock:
            if self.calls >= self.maximum:
                raise QuotaExceeded("Paid fallback request limit reached")
            self.calls += 1
