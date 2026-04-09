"""Sliding Window Rate Limiter Action Module.

Sliding window rate limiter with precise request tracking.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class SlidingWindowConfig:
    """Sliding window configuration."""
    window_size_seconds: float = 60.0
    max_requests: int = 100
    precision: int = 3


@dataclass
class SlidingWindowResult:
    """Result of sliding window check."""
    allowed: bool
    current_count: int
    max_requests: int
    window_size: float
    reset_at: float


class SlidingWindowRateLimiter(Generic[T]):
    """Sliding window rate limiter."""

    def __init__(self, config: SlidingWindowConfig | None = None) -> None:
        self.config = config or SlidingWindowConfig()
        self.window_size = self.config.window_size_seconds
        self.max_requests = self.config.max_requests
        self._requests: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> SlidingWindowResult:
        """Check if request is allowed."""
        async with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_size
            while self._requests and self._requests[0] < cutoff:
                self._requests.popleft()
            current = len(self._requests)
            if current < self.max_requests:
                self._requests.append(now)
                return SlidingWindowResult(
                    allowed=True,
                    current_count=current + 1,
                    max_requests=self.max_requests,
                    window_size=self.window_size,
                    reset_at=self._requests[0] + self.window_size if self._requests else now + self.window_size
                )
            return SlidingWindowResult(
                allowed=False,
                current_count=current,
                max_requests=self.max_requests,
                window_size=self.window_size,
                reset_at=self._requests[0] + self.window_size
            )

    async def wait_for_slot(self, timeout: float | None = None) -> SlidingWindowResult:
        """Wait until a request slot is available."""
        start = time.monotonic()
        while True:
            result = await self.acquire()
            if result.allowed:
                return result
            if timeout and (time.monotonic() - start) >= timeout:
                return SlidingWindowResult(
                    allowed=False,
                    current_count=result.current_count,
                    max_requests=self.max_requests,
                    window_size=self.window_size,
                    reset_at=result.reset_at
                )
            wait_time = result.reset_at - time.monotonic()
            await asyncio.sleep(min(max(wait_time, 0.01), 0.1))

    def get_current_count(self) -> int:
        """Get current request count in window."""
        now = time.monotonic()
        cutoff = now - self.window_size
        while self._requests and self._requests[0] < cutoff:
            self._requests.popleft()
        return len(self._requests)

    def reset(self) -> None:
        """Reset the rate limiter."""
        self._requests.clear()

    def get_stats(self) -> dict:
        """Get rate limiter statistics."""
        return {
            "current_count": self.get_current_count(),
            "max_requests": self.max_requests,
            "window_size": self.window_size,
            "utilization": self.get_current_count() / self.max_requests
        }


class DistributedSlidingWindow:
    """Sliding window rate limiter with distributed support."""

    def __init__(
        self,
        config: SlidingWindowConfig | None = None,
        storage: Generic[T] | None = None
    ) -> None:
        self.config = config or SlidingWindowConfig()
        self.window_size = self.config.window_size_seconds
        self.max_requests = self.config.max_requests
        self._storage = storage

    async def acquire(self, key: str) -> SlidingWindowResult:
        """Acquire from distributed window."""
        now = time.time()
        window_key = f"rate_limit:{key}"
        cutoff = now - self.window_size
        if self._storage:
            window_data = await self._storage.get(window_key, [])
            window_data = [t for t in window_data if t > cutoff]
            current = len(window_data)
            if current < self.max_requests:
                window_data.append(now)
                await self._storage.set(window_key, window_data)
                return SlidingWindowResult(
                    allowed=True,
                    current_count=current + 1,
                    max_requests=self.max_requests,
                    window_size=self.window_size,
                    reset_at=now + self.window_size
                )
            return SlidingWindowResult(
                allowed=False,
                current_count=current,
                max_requests=self.max_requests,
                window_size=self.window_size,
                reset_at=window_data[0] + self.window_size if window_data else now + self.window_size
            )
        return SlidingWindowResult(
            allowed=False,
            current_count=0,
            max_requests=self.max_requests,
            window_size=self.window_size,
            reset_at=now
        )
