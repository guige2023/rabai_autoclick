"""Rate Limit Window Action Module.

Fixed and sliding window rate limiting implementation.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")


@dataclass
class WindowConfig:
    """Window rate limit configuration."""
    window_size: float = 60.0
    max_requests: int = 100
    window_type: str = "sliding"


class WindowRateLimiter(Generic[T]):
    """Window-based rate limiter."""

    def __init__(self, config: WindowConfig | None = None) -> None:
        self.config = config or WindowConfig()
        self.window_size = self.config.window_size
        self.max_requests = self.config.max_requests
        self._requests: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> tuple[bool, float]:
        """Acquire a slot. Returns (allowed, wait_time)."""
        async with self._lock:
            now = time.time()
            cutoff = now - self.window_size
            while self._requests and self._requests[0] <= cutoff:
                self._requests.popleft()
            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True, 0.0
            oldest = self._requests[0]
            wait_time = oldest + self.window_size - now
            return False, max(0, wait_time)

    async def wait_for_slot(self, timeout: float | None = None) -> bool:
        """Wait until slot is available."""
        start = time.time()
        while True:
            allowed, wait_time = await self.acquire()
            if allowed:
                return True
            if timeout and (time.time() - start) >= timeout:
                return False
            await asyncio.sleep(min(wait_time, 0.1))

    def get_remaining(self) -> int:
        """Get remaining requests in window."""
        now = time.time()
        cutoff = now - self.window_size
        while self._requests and self._requests[0] <= cutoff:
            self._requests.popleft()
        return max(0, self.max_requests - len(self._requests))
