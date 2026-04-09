"""API Request Coalescing Action.

Coalesces concurrent duplicate API requests into a single request,
reducing redundant calls and improving efficiency.
"""
from typing import Any, Callable, Dict, List, Optional, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict
import asyncio
import threading
import time


T = TypeVar("T")


@dataclass
class PendingRequest:
    key: str
    future: "asyncio.Future[T]"
    created_at: datetime
    callback: Optional[Callable[[], T]] = None
    waiters: int = 1


@dataclass
class CoalescingStats:
    total_requests: int = 0
    coalesced_requests: int = 0
    active_requests: int = 0
    cache_hits: int = 0

    def hit_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return self.coalesced_requests / self.total_requests


class APIRequestCoalescingAction:
    """Coalesces duplicate concurrent API requests."""

    def __init__(self, ttl_seconds: float = 5.0) -> None:
        self.ttl_seconds = ttl_seconds
        self._pending: Dict[str, PendingRequest] = {}
        self._lock = threading.RLock()
        self._stats = CoalescingStats()
        self._cache: Dict[str, Any] = {}
        self._cache_timestamps: Dict[str, datetime] = {}

    def _make_key(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> str:
        import json
        param_str = json.dumps(params, sort_keys=True) if params else ""
        return f"{endpoint}:{param_str}"

    def _evict_expired(self) -> None:
        now = datetime.now()
        expired = [
            k for k, ts in self._cache_timestamps.items()
            if (now - ts).total_seconds() > self.ttl_seconds
        ]
        for k in expired:
            self._cache.pop(k, None)
            self._cache_timestamps.pop(k, None)

    async def request_async(
        self,
        endpoint: str,
        fetch_fn: Callable[[], T],
        params: Optional[Dict[str, Any]] = None,
        cache: bool = True,
    ) -> T:
        key = self._make_key(endpoint, params)
        self._evict_expired()
        if cache and key in self._cache:
            self._stats.cache_hits += 1
            return self._cache[key]
        with self._lock:
            if key in self._pending:
                self._stats.total_requests += 1
                self._stats.coalesced_requests += 1
                self._pending[key].waiters += 1
                try:
                    return await asyncio.shield(self._pending[key].future)
                finally:
                    self._pending[key].waiters -= 1
                    if self._pending[key].waiters == 0:
                        self._pending.pop(key, None)
        self._stats.total_requests += 1
        self._stats.active_requests += 1
        loop = asyncio.get_event_loop()
        future: "asyncio.Future[T]" = loop.create_future()
        with self._lock:
            self._pending[key] = PendingRequest(
                key=key,
                future=future,
                created_at=datetime.now(),
                callback=None,
                waiters=1,
            )
        try:
            result = await fetch_fn()
            if cache:
                self._cache[key] = result
                self._cache_timestamps[key] = datetime.now()
            future.set_result(result)
        except Exception as e:
            future.set_exception(e)
            raise
        finally:
            self._stats.active_requests -= 1
            with self._lock:
                self._pending.pop(key, None)
        return result

    def request_sync(
        self,
        endpoint: str,
        fetch_fn: Callable[[], T],
        params: Optional[Dict[str, Any]] = None,
        cache: bool = True,
    ) -> T:
        """Synchronous wrapper."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.run_until_complete(
            self.request_async(endpoint, fetch_fn, params, cache)
        )

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total_requests": self._stats.total_requests,
            "coalesced_requests": self._stats.coalesced_requests,
            "active_requests": self._stats.active_requests,
            "cache_hits": self._stats.cache_hits,
            "coalescing_hit_rate": round(self._stats.hit_rate(), 4),
            "pending_count": len(self._pending),
        }
