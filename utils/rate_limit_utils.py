"""Rate limiting utilities: token bucket, sliding window, and leaky bucket algorithms."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

__all__ = [
    "RateLimitAlgorithm",
    "TokenBucketRateLimiter",
    "SlidingWindowRateLimiter",
    "LeakyBucketRateLimiter",
    "RateLimitMiddleware",
]


class RateLimitAlgorithm(Enum):
    """Rate limiting algorithm types."""

    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    LEAKY_BUCKET = "leaky_bucket"


@dataclass
class TokenBucketRateLimiter:
    """Token bucket rate limiter."""

    capacity: float
    refill_rate: float
    _tokens: float | None = None
    _last_refill: float | None = None
    _lock: threading.Lock | None = None

    def __post_init__(self) -> None:
        self._tokens = float(self.capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def allow_request(self, cost: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if self._tokens >= cost:
                self._tokens -= cost
                return True
            return False

    def get_wait_time(self, cost: float = 1.0) -> float:
        with self._lock:
            self._refill()
            if self._tokens >= cost:
                return 0.0
            return (cost - self._tokens) / self.refill_rate


@dataclass
class SlidingWindowRateLimiter:
    """Sliding window rate limiter for precise rate limiting."""

    max_requests: int
    window_seconds: float

    def __post_init__(self) -> None:
        self._requests: list[float] = []
        self._lock = threading.Lock()

    def allow_request(self) -> bool:
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            self._requests = [t for t in self._requests if t > cutoff]

            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    def get_remaining(self) -> int:
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds
            self._requests = [t for t in self._requests if t > cutoff]
            return max(0, self.max_requests - len(self._requests))

    def reset(self) -> None:
        with self._lock:
            self._requests.clear()


@dataclass
class LeakyBucketRateLimiter:
    """Leaky bucket rate limiter for smooth outflow."""

    capacity: int
    leak_rate: float

    def __post_init__(self) -> None:
        self._level = 0
        self._last_leak = time.time()
        self._lock = threading.Lock()

    def _leak(self) -> None:
        now = time.time()
        elapsed = now - self._last_leak
        leaked = elapsed * self.leak_rate
        self._level = max(0, self._level - leaked)
        self._last_leak = now

    def allow_request(self) -> bool:
        with self._lock:
            self._leak()
            if self._level < self.capacity:
                self._level += 1
                return True
            return False

    def get_wait_time(self) -> float:
        with self._lock:
            self._leak()
            if self._level < self.capacity:
                return 0.0
            return (self._level - self.capacity) / self.leak_rate


class RateLimitMiddleware:
    """WSGI/ASGI middleware for rate limiting."""

    def __init__(self, app: Any, limiter: SlidingWindowRateLimiter) -> None:
        self.app = app
        self.limiter = limiter

    def __call__(self, environ: dict[str, Any], start_response: Any) -> Any:
        client_ip = environ.get("REMOTE_ADDR", "")
        if not self.limiter.allow_request():
            return start_response("429 Too Many Requests", [("Content-Type", "text/plain")])
        return self.app(environ, start_response)
