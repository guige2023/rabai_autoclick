"""
API Concurrency Limiter Action Module.

Limits concurrent API calls per client/endpoint.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, Optional


@dataclass
class ConcurrencyLimit:
    """A concurrency limit configuration."""
    max_concurrent: int
    window_seconds: float = 1.0


@dataclass
class ActiveCall:
    """An active concurrent call."""
    call_id: str
    started_at: float
    task: asyncio.Task


class ApiConcurrencyLimiterAction:
    """
    Limit concurrent API calls.

    Supports per-client, per-endpoint, and global limits.
    """

    def __init__(self) -> None:
        self._limits: Dict[str, ConcurrencyLimit] = {}
        self._active: Dict[str, Dict[str, ActiveCall]] = defaultdict(dict)
        self._stats: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

    def set_limit(
        self,
        key: str,
        max_concurrent: int,
        window_seconds: float = 1.0,
    ) -> None:
        """
        Set a concurrency limit.

        Args:
            key: Limit key (client_id, endpoint, "global", etc.)
            max_concurrent: Maximum concurrent calls
            window_seconds: Sliding window
        """
        self._limits[key] = ConcurrencyLimit(
            max_concurrent=max_concurrent,
            window_seconds=window_seconds,
        )

    def get_limit(self, key: str) -> Optional[ConcurrencyLimit]:
        """Get limit configuration."""
        return self._limits.get(key)

    async def acquire(
        self,
        key: str,
        call_id: str,
        coro: Coroutine[Any, Any, Any],
    ) -> Any:
        """
        Acquire concurrency slot and execute call.

        Args:
            key: Limit key
            call_id: Unique call ID
            coro: Coroutine to execute

        Returns:
            Result from coroutine

        Raises:
            ConcurrencyLimitExceeded: If limit reached
        """
        limit = self._limits.get(key)

        if limit is None:
            return await coro

        async with self._locks[key]:
            self._cleanup_expired(key)

            active = self._active[key]

            if len(active) >= limit.max_concurrent:
                self._stats[key]["rejected"] += 1
                raise ConcurrencyLimitExceeded(
                    f"Concurrency limit reached for {key}: {limit.max_concurrent}"
                )

            task = asyncio.create_task(coro)
            active[call_id] = ActiveCall(
                call_id=call_id,
                started_at=time.time(),
                task=task,
            )

            self._stats[key]["acquired"] += 1

        try:
            result = await task
            self._stats[key]["completed"] += 1
            return result
        except Exception:
            self._stats[key]["failed"] += 1
            raise
        finally:
            async with self._locks[key]:
                self._active[key].pop(call_id, None)

    def _cleanup_expired(self, key: str) -> None:
        """Remove expired calls from active set."""
        limit = self._limits.get(key)
        if limit is None:
            return

        now = time.time()
        cutoff = now - limit.window_seconds

        active = self._active[key]
        expired = [
            call_id
            for call_id, call in active.items()
            if call.started_at < cutoff
        ]

        for call_id in expired:
            call = active.pop(call_id, None)
            if call:
                call.task.cancel()

    def get_active_count(self, key: str) -> int:
        """Get number of active calls for key."""
        self._cleanup_expired(key)
        return len(self._active[key])

    def is_available(self, key: str) -> bool:
        """Check if a slot is available."""
        limit = self._limits.get(key)
        if limit is None:
            return True

        return self.get_active_count(key) < limit.max_concurrent

    def get_stats(self, key: str) -> Dict[str, Any]:
        """Get statistics for a key."""
        limit = self._limits.get(key)

        return {
            "key": key,
            "limit": limit.max_concurrent if limit else None,
            "active": self.get_active_count(key),
            "acquired": self._stats[key]["acquired"],
            "completed": self._stats[key]["completed"],
            "failed": self._stats[key]["failed"],
            "rejected": self._stats[key]["rejected"],
        }

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get stats for all keys."""
        all_keys = set(self._limits.keys()) | set(self._active.keys())
        return {key: self.get_stats(key) for key in all_keys}

    def reset(self, key: Optional[str] = None) -> None:
        """Reset stats and active calls."""
        if key:
            self._active[key].clear()
            self._stats[key].clear()
        else:
            self._active.clear()
            self._stats.clear()


class ConcurrencyLimitExceeded(Exception):
    """Raised when concurrency limit is exceeded."""
    pass
