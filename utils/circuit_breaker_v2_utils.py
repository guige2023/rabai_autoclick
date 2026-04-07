"""Rate limiting utilities: token bucket, sliding window, fixed window, and leaky bucket."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any

__all__ = [
    "TokenBucket",
    "SlidingWindowRateLimiter",
    "FixedWindowRateLimiter",
    "LeakyBucket",
    "MultiLimiter",
]


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int) -> None:
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def allow(self, tokens: int = 1) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            self.capacity,
            self._tokens + elapsed * self.rate,
        )
        self._last_refill = now

    def wait_and_allow(self, tokens: int = 1) -> None:
        while not self.allow(tokens):
            time.sleep(0.01)


class SlidingWindowRateLimiter:
    """Sliding window rate limiter with precise hit counting."""

    def __init__(self, max_hits: int, window_seconds: float) -> None:
        self.max_hits = max_hits
        self.window_seconds = window_seconds
        self._hits: list[float] = []
        self._lock = threading.Lock()

    def allow(self) -> bool:
        with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_seconds
            self._hits = [h for h in self._hits if h > cutoff]
            if len(self._hits) < self.max_hits:
                self._hits.append(now)
                return True
            return False

    def get_count(self) -> int:
        with self._lock:
            now = time.monotonic()
            cutoff = now - self.window_seconds
            return sum(1 for h in self._hits if h > cutoff)


class FixedWindowRateLimiter:
    """Fixed window rate limiter (simpler but has boundary surge issue)."""

    def __init__(self, max_hits: int, window_seconds: float) -> None:
        self.max_hits = max_hits
        self.window_seconds = window_seconds
        self._count = 0
        self._window_start = time.monotonic()
        self._lock = threading.Lock()

    def allow(self) -> bool:
        with self._lock:
            now = time.monotonic()
            if now - self._window_start >= self.window_seconds:
                self._count = 0
                self._window_start = now
            if self._count < self.max_hits:
                self._count += 1
                return True
            return False

    def reset(self) -> None:
        with self._lock:
            self._count = 0
            self._window_start = time.monotonic()


class LeakyBucket:
    """Leaky bucket algorithm for smooth rate limiting."""

    def __init__(self, leak_rate: float, capacity: int) -> None:
        self.leak_rate = leak_rate
        self.capacity = capacity
        self._level = 0.0
        self._last_leak = time.monotonic()
        self._lock = threading.Lock()

    def accept(self) -> bool:
        with self._lock:
            self._leak()
            if self._level < self.capacity:
                self._level += 1
                return True
            return False

    def _leak(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_leak
        leaked = elapsed * self.leak_rate
        self._level = max(0.0, self._level - leaked)
        self._last_leak = now

    def wait_and_accept(self) -> None:
        while not self.accept():
            time.sleep(0.01)


class MultiLimiter:
    """Combine multiple rate limiters (AND logic: all must allow)."""

    def __init__(self, *limiters: Any) -> None:
        self.limiters = list(limiters)

    def allow(self) -> bool:
        return all(limiter.allow() for limiter in self.limiters)

    def wait_and_allow(self) -> None:
        while not self.allow():
            time.sleep(0.01)
