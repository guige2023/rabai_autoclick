"""Rate limiting utilities.

Provides token bucket, sliding window, and fixed window
rate limiters for controlling request rates.
"""

import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class RateLimitResult:
    """Result of a rate limit check."""
    allowed: bool
    remaining: int
    reset_in: float


class TokenBucket:
    """Token bucket rate limiter.

    Example:
        limiter = TokenBucket(capacity=100, refill_rate=10)
        if limiter.allow():
            process_request()
    """

    def __init__(self, capacity: int, refill_rate: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = float(capacity)
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def allow(self, tokens: int = 1) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def get_result(self) -> RateLimitResult:
        """Get detailed rate limit status."""
        with self._lock:
            self._refill()
            return RateLimitResult(
                allowed=self._tokens >= 1,
                remaining=int(self._tokens),
                reset_in=0.0 if self._tokens >= 1 else (1 - self._tokens) / self.refill_rate,
            )


class SlidingWindow:
    """Sliding window rate limiter.

    Example:
        limiter = SlidingWindow(max_requests=100, window_seconds=60)
        if limiter.allow():
            process_request()
    """

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: deque = deque()
        self._lock = threading.Lock()

    def allow(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds

            while self._requests and self._requests[0] < cutoff:
                self._requests.popleft()

            if len(self._requests) < self.max_requests:
                self._requests.append(now)
                return True
            return False

    def get_result(self) -> RateLimitResult:
        """Get detailed rate limit status."""
        with self._lock:
            now = time.time()
            cutoff = now - self.window_seconds

            while self._requests and self._requests[0] < cutoff:
                self._requests.popleft()

            remaining = max(0, self.max_requests - len(self._requests))
            reset_in = 0.0
            if remaining == 0 and self._requests:
                oldest = self._requests[0]
                reset_in = max(0.0, (oldest + self.window_seconds) - now)

            return RateLimitResult(
                allowed=remaining > 0,
                remaining=remaining,
                reset_in=reset_in,
            )

    def reset(self) -> None:
        """Clear all requests."""
        with self._lock:
            self._requests.clear()


class FixedWindow:
    """Fixed window rate limiter.

    Example:
        limiter = FixedWindow(max_requests=100, window_seconds=60)
        if limiter.allow():
            process_request()
    """

    def __init__(self, max_requests: int, window_seconds: float) -> None:
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._count = 0
        self._window_start = time.time()
        self._lock = threading.Lock()

    def allow(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            now = time.time()
            if now - self._window_start >= self.window_seconds:
                self._count = 0
                self._window_start = now

            if self._count < self.max_requests:
                self._count += 1
                return True
            return False

    def get_result(self) -> RateLimitResult:
        """Get detailed rate limit status."""
        with self._lock:
            now = time.time()
            elapsed = now - self._window_start
            remaining = max(0, self.max_requests - self._count)
            reset_in = max(0.0, self.window_seconds - elapsed)

            return RateLimitResult(
                allowed=remaining > 0,
                remaining=remaining,
                reset_in=reset_in,
            )

    def reset(self) -> None:
        """Reset counter."""
        with self._lock:
            self._count = 0
            self._window_start = time.time()


class MultiRateLimiter:
    """Rate limiter for multiple clients/keys.

    Example:
        limiter = MultiRateLimiter(max_requests=100, window_seconds=60)
        if limiter.allow("user_123"):
            process_request()
    """

    def __init__(
        self,
        limiter_factory,
        max_keys: int = 1000,
    ) -> None:
        self._factory = limiter_factory
        self._limiters: Dict[str, TokenBucket] = {}
        self._lock = threading.Lock()
        self._max_keys = max_keys

    def allow(self, key: str, tokens: int = 1) -> bool:
        """Check if request is allowed for key."""
        with self._lock:
            if key not in self._limiters:
                if len(self._limiters) >= self._max_keys:
                    oldest_key = next(iter(self._limiters))
                    del self._limiters[oldest_key]
                self._limiters[key] = self._factory()

            return self._limiters[key].allow(tokens)

    def get_result(self, key: str) -> Optional[RateLimitResult]:
        """Get rate limit status for key."""
        with self._lock:
            if key in self._limiters:
                return self._limiters[key].get_result()
            return None

    def reset(self, key: str) -> None:
        """Reset limit for key."""
        with self._lock:
            if key in self._limiters:
                del self._limiters[key]

    def cleanup(self) -> int:
        """Remove stale limiters."""
        with self._lock:
            count = len(self._limiters)
            self._limiters.clear()
            return count


class LeakyBucket:
    """Leaky bucket rate limiter.

    Example:
        limiter = LeakyBucket(capacity=100, leak_rate=10)
        if limiter.allow():
            process_request()
    """

    def __init__(self, capacity: int, leak_rate: float) -> None:
        self.capacity = capacity
        self.leak_rate = leak_rate
        self._level = 0.0
        self._last_leak = time.time()
        self._lock = threading.Lock()

    def _leak(self) -> None:
        """Leak water from bucket."""
        now = time.time()
        elapsed = now - self._last_leak
        leaked = elapsed * self.leak_rate
        self._level = max(0.0, self._level - leaked)
        self._last_leak = now

    def allow(self) -> bool:
        """Check if request is allowed."""
        with self._lock:
            self._leak()
            if self._level < self.capacity:
                self._level += 1
                return True
            return False

    def get_result(self) -> RateLimitResult:
        """Get detailed rate limit status."""
        with self._lock:
            self._leak()
            remaining = int(self.capacity - self._level)
            reset_in = 1.0 / self.leak_rate if remaining == 0 else 0.0

            return RateLimitResult(
                allowed=self._level < self.capacity,
                remaining=max(0, remaining),
                reset_in=reset_in,
            )


def create_limiter(
    limiter_type: str,
    **kwargs,
) -> TokenBucket:
    """Create rate limiter by type."""
    if limiter_type == "token_bucket":
        return TokenBucket(capacity=kwargs["capacity"], refill_rate=kwargs["refill_rate"])
    elif limiter_type == "sliding_window":
        return SlidingWindow(max_requests=kwargs["max_requests"], window_seconds=kwargs["window_seconds"])
    elif limiter_type == "fixed_window":
        return FixedWindow(max_requests=kwargs["max_requests"], window_seconds=kwargs["window_seconds"])
    elif limiter_type == "leaky_bucket":
        return LeakyBucket(capacity=kwargs["capacity"], leak_rate=kwargs["leak_rate"])
    else:
        raise ValueError(f"Unknown limiter type: {limiter_type}")
