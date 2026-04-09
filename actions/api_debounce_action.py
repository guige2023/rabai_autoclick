"""
API Debounce Action Module.

Provides debouncing functionality for API requests, coalescing
multiple rapid requests into single operations.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone
import asyncio
import logging
import time

logger = logging.getLogger(__name__)


@dataclass
class DebounceCall:
    """Represents a debounced function call."""
    call_id: str
    key: str
    args: tuple
    kwargs: dict
    created_at: float
    scheduled_at: Optional[float] = None
    executed_at: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "call_id": self.call_id,
            "key": self.key,
            "created_at": self.created_at,
            "scheduled_at": self.scheduled_at,
            "executed_at": self.executed_at,
        }


@dataclass
class DebounceStats:
    """Statistics for debounce operations."""
    total_calls: int = 0
    coalesced_calls: int = 0
    executed_calls: int = 0
    dropped_calls: int = 0
    active_keys: int = 0

    def coalesce_rate(self) -> float:
        """Calculate coalesce rate."""
        if self.total_calls == 0:
            return 0.0
        return (self.coalesced_calls / self.total_calls) * 100

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_calls": self.total_calls,
            "coalesced_calls": self.coalesced_calls,
            "executed_calls": self.executed_calls,
            "dropped_calls": self.dropped_calls,
            "active_keys": self.active_keys,
            "coalesce_rate_percent": self.coalesce_rate(),
        }


class ApiDebounceAction:
    """
    Provides debouncing for API requests.

    This action implements debounce functionality that delays function
    execution until after a specified wait time has elapsed since the
    last call. Useful for rate limiting and coalescing rapid requests.

    Example:
        >>> debouncer = ApiDebounceAction(wait_ms=500)
        >>> debouncer.debounce("fetch_user", fetch_user_data, user_id=123)
        >>> # Multiple calls within 500ms are coalesced into one
    """

    def __init__(
        self,
        wait_ms: float = 300,
        max_wait_ms: Optional[float] = None,
        leading: bool = True,
        trailing: bool = True,
        max_coalesce: Optional[int] = None,
    ):
        """
        Initialize the API Debounce Action.

        Args:
            wait_ms: Wait time in milliseconds before executing.
            max_wait_ms: Maximum wait time.
            leading: Execute on leading edge (immediately on first call).
            trailing: Execute on trailing edge (after wait time).
            max_coalesce: Maximum calls to coalesce before forcing execution.
        """
        self.wait_seconds = wait_ms / 1000
        self.max_wait_seconds = max_wait_ms / 1000 if max_wait_ms else None
        self.leading = leading
        self.trailing = trailing
        self.max_coalesce = max_coalesce

        self._pending: Dict[str, DebounceCall] = {}
        self._timers: Dict[str, asyncio.TimerHandle] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._lock = asyncio.Lock()

        self._stats = DebounceStats()

    async def debounce(
        self,
        key: str,
        func: Callable,
        *args,
        **kwargs,
    ) -> Optional[Any]:
        """
        Debounce a function call.

        Args:
            key: Unique key for this debounce group.
            func: Function to call.
            *args: Positional arguments for function.
            **kwargs: Keyword arguments for function.

        Returns:
            Function result if executed, None otherwise.
        """
        import uuid

        self._stats.total_calls += 1

        call = DebounceCall(
            call_id=str(uuid.uuid4()),
            key=key,
            args=args,
            kwargs=kwargs,
            created_at=time.time(),
        )

        existing = self._pending.get(key)

        if existing:
            self._stats.coalesced_calls += 1
            call.scheduled_at = existing.scheduled_at

            if self.max_coalesce:
                coalesce_count = getattr(existing, "_coalesce_count", 0) + 1
                call._coalesce_count = coalesce_count  # type: ignore

                if coalesce_count >= self.max_coalesce:
                    await self._execute(key, func)
                    return None

        else:
            if self.leading:
                call.scheduled_at = time.time()
                call.executed_at = time.time()
                self._stats.executed_calls += 1

                try:
                    result = await self._safe_execute(func, *args, **kwargs)
                    return result
                except Exception as e:
                    logger.error(f"Debounce leading edge error: {e}")
                    return None

        self._pending[key] = call

        await self._schedule_timer(key, func)

        return None

    async def _schedule_timer(self, key: str, func: Callable) -> None:
        """Schedule a timer for debounced execution."""
        if key in self._timers:
            self._timers[key].cancel()

        loop = asyncio.get_running_loop()

        wait_time = self.wait_seconds
        if self.max_wait_seconds and self._pending.get(key):
            elapsed = time.time() - self._pending[key].created_at
            wait_time = min(wait_time, self.max_wait_seconds - elapsed)

        timer = loop.call_later(
            wait_time,
            lambda: asyncio.create_task(self._execute(key, func)),
        )

        self._timers[key] = timer

    async def _execute(self, key: str, func: Callable) -> None:
        """Execute the debounced function."""
        async with self._lock:
            call = self._pending.pop(key, None)

        if call is None:
            return

        if key in self._timers:
            try:
                del self._timers[key]
            except KeyError:
                pass

        if not self.trailing:
            return

        call.executed_at = time.time()
        self._stats.executed_calls += 1

        await self._safe_execute(func, *call.args, **call.kwargs)

    async def _safe_execute(self, func: Callable, *args, **kwargs) -> Any:
        """Safely execute a function."""
        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Debounce execution error: {e}")
            raise

    async def cancel(self, key: str) -> bool:
        """
        Cancel a pending debounced call.

        Args:
            key: Key of the call to cancel.

        Returns:
            True if cancelled.
        """
        async with self._lock:
            if key in self._pending:
                del self._pending[key]

            if key in self._timers:
                self._timers[key].cancel()
                del self._timers[key]
                return True

        return False

    async def cancel_all(self) -> int:
        """Cancel all pending debounced calls."""
        count = 0

        async with self._lock:
            count = len(self._pending)

            for timer in self._timers.values():
                timer.cancel()

            self._pending.clear()
            self._timers.clear()

        return count

    def flush(self, key: Optional[str] = None) -> None:
        """
        Immediately execute pending calls.

        Args:
            key: Optional specific key to flush.
        """
        keys = [key] if key else list(self._pending.keys())

        for k in keys:
            if k in self._timers:
                self._timers[k].cancel()

    def get_pending(self, key: str) -> Optional[DebounceCall]:
        """Get pending call for a key."""
        return self._pending.get(key)

    def get_pending_count(self, key: str) -> int:
        """Get number of coalesced calls pending for a key."""
        call = self._pending.get(key)
        if call is None:
            return 0
        return getattr(call, "_coalesce_count", 1)

    def get_stats(self) -> DebounceStats:
        """Get debounce statistics."""
        stats = DebounceStats(
            total_calls=self._stats.total_calls,
            coalesced_calls=self._stats.coalesced_calls,
            executed_calls=self._stats.executed_calls,
            dropped_calls=self._stats.dropped_calls,
            active_keys=len(self._pending),
        )
        return stats

    def reset_stats(self) -> None:
        """Reset statistics."""
        self._stats = DebounceStats()


class ThrottleDebounce(ApiDebounceAction):
    """Combined throttle and debounce implementation."""

    def __init__(
        self,
        rate_limit: float,
        period_ms: float = 1000,
        **kwargs,
    ):
        """
        Initialize throttle-debounce.

        Args:
            rate_limit: Maximum calls per period.
            period_ms: Period in milliseconds.
            **kwargs: Additional debounce arguments.
        """
        super().__init__(**kwargs)
        self.rate_limit = rate_limit
        self.period_seconds = period_ms / 1000
        self._call_times: Dict[str, List[float]] = {}

    async def _check_rate_limit(self, key: str) -> bool:
        """Check if rate limit allows execution."""
        now = time.time()
        cutoff = now - self.period_seconds

        if key not in self._call_times:
            self._call_times[key] = []

        self._call_times[key] = [t for t in self._call_times[key] if t > cutoff]

        if len(self._call_times[key]) >= self.rate_limit:
            return False

        self._call_times[key].append(now)
        return True


def create_debounce_action(
    wait_ms: float = 300,
    **kwargs,
) -> ApiDebounceAction:
    """Factory function to create an ApiDebounceAction."""
    return ApiDebounceAction(wait_ms=wait_ms, **kwargs)
