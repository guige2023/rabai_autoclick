"""
API Request Deduplicator Action Module.

Prevents duplicate API requests using in-flight request tracking and
content-based request matching.
"""

import asyncio
import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional


@dataclass
class InFlightRequest:
    """Represents an in-flight API request."""

    future: asyncio.Future
    created_at: float
    request_key: str
    ref_count: int = 1


@dataclass
class DeduplicationStats:
    """Statistics for deduplication performance."""

    total_requests: int = 0
    deduplicated_requests: int = 0
    active_in_flight: int = 0
    expired_cleanups: int = 0

    @property
    def deduplication_rate(self) -> float:
        """Calculate deduplication percentage."""
        if self.total_requests == 0:
            return 0.0
        return self.deduplicated_requests / self.total_requests

    def to_dict(self) -> dict[str, Any]:
        """Export stats as dictionary."""
        return {
            "total_requests": self.total_requests,
            "deduplicated_requests": self.deduplicated_requests,
            "active_in_flight": self.active_in_flight,
            "expired_cleanups": self.expired_cleanups,
            "deduplication_rate": round(self.deduplication_rate, 4),
        }


class APIDeduplicator:
    """
    Deduplicates concurrent API requests.

    When multiple callers request the same resource simultaneously,
    only one actual request is made. Other callers receive the same result.
    """

    def __init__(
        self,
        in_flight_ttl: float = 300.0,
        max_concurrent_keys: int = 10000,
        cleanup_interval: float = 60.0,
    ) -> None:
        """
        Initialize the deduplicator.

        Args:
            in_flight_ttl: Seconds before an in-flight request is considered stale.
            max_concurrent_keys: Maximum concurrent in-flight request keys.
            cleanup_interval: Seconds between cleanup runs.
        """
        self._in_flight: dict[str, InFlightRequest] = {}
        self._in_flight_ttl = in_flight_ttl
        self._max_concurrent_keys = max_concurrent_keys
        self._cleanup_interval = cleanup_interval
        self._last_cleanup = time.time()
        self._stats = DeduplicationStats()
        self._lock = asyncio.Lock()

    def _make_request_key(
        self,
        method: str,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        body: Optional[bytes] = None,
    ) -> str:
        """
        Generate a content-based key for the request.

        Args:
            method: HTTP method.
            url: Request URL.
            params: Query parameters.
            headers: Request headers.
            body: Request body bytes.

        Returns:
            Deterministic hash key.
        """
        parts = [method.upper(), url]
        if params:
            sorted_params = sorted(params.items())
            parts.append(str(sorted_params))
        if headers:
            filtered = {
                k.lower(): v
                for k, v in sorted(headers.items())
                if k.lower() not in ("authorization", "cookie", "x-api-key")
            }
            parts.append(str(filtered))
        if body:
            body_hash = hashlib.sha256(body).hexdigest()[:16]
            parts.append(body_hash)
        combined = "|".join(parts)
        return hashlib.sha256(combined.encode()).hexdigest()[:32]

    def _cleanup_stale(self) -> int:
        """Remove stale in-flight entries. Returns count of removed entries."""
        now = time.time()
        stale_keys = [
            k
            for k, v in self._in_flight.items()
            if now - v.created_at > self._in_flight_ttl
        ]
        for key in stale_keys:
            if key in self._in_flight:
                del self._in_flight[key]
                self._stats.expired_cleanups += 1
        return len(stale_keys)

    async def deduplicated_request(
        self,
        request_func: Callable[..., Awaitable[bytes]],
        method: str,
        url: str,
        params: Optional[dict[str, Any]] = None,
        headers: Optional[dict[str, str]] = None,
        body: Optional[bytes] = None,
        **kwargs: Any,
    ) -> bytes:
        """
        Execute a request with deduplication.

        Args:
            request_func: Async function that executes the actual request.
            method: HTTP method.
            url: Request URL.
            params: Query parameters.
            headers: Request headers.
            body: Request body.
            **kwargs: Additional args passed to request_func.

        Returns:
            Response bytes.
        """
        async with self._lock:
            if time.time() - self._last_cleanup > self._cleanup_interval:
                self._cleanup_stale()
                self._last_cleanup = time.time()

            key = self._make_request_key(method, url, params, headers, body)
            self._stats.total_requests += 1

            if key in self._in_flight:
                entry = self._in_flight[key]
                entry.ref_count += 1
                self._stats.deduplicated_requests += 1
                self._stats.active_in_flight = len(self._in_flight)
                try:
                    result = await entry.future
                    return result
                except Exception:
                    del self._in_flight[key]
                    raise

            loop = asyncio.get_running_loop()
            future = loop.create_future()

            self._in_flight[key] = InFlightRequest(
                future=future,
                created_at=time.time(),
                request_key=key,
            )
            self._stats.active_in_flight = len(self._in_flight)

        try:
            result = await request_func(
                method=method,
                url=url,
                params=params,
                headers=headers,
                body=body,
                **kwargs,
            )
            if not future.done():
                future.set_result(result)
            return result
        except Exception as e:
            if not future.done():
                future.set_exception(e)
            raise
        finally:
            async with self._lock:
                if key in self._in_flight:
                    entry = self._in_flight[key]
                    entry.ref_count -= 1
                    if entry.ref_count <= 0:
                        del self._in_flight[key]
                    self._stats.active_in_flight = len(self._in_flight)

    def stats(self) -> DeduplicationStats:
        """Return current deduplication statistics."""
        return self._stats

    def clear(self) -> None:
        """Clear all in-flight requests."""
        self._in_flight.clear()
        self._stats.active_in_flight = 0


def create_deduplicator(
    in_flight_ttl: float = 300.0,
    max_concurrent_keys: int = 10000,
) -> APIDeduplicator:
    """
    Factory function to create a configured deduplicator.

    Args:
        in_flight_ttl: TTL for in-flight requests.
        max_concurrent_keys: Max concurrent keys tracked.

    Returns:
        Configured APIDeduplicator instance.
    """
    return APIDeduplicator(
        in_flight_ttl=in_flight_ttl,
        max_concurrent_keys=max_concurrent_keys,
    )
