"""Rate limiting utilities for RabAI AutoClick.

Provides:
- Token bucket rate limiter
- Sliding window rate limiter
"""

import threading
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class RateLimitResult:
    """Result of a rate-limited operation."""
    allowed: bool
    remaining: int
    retry_after: Optional[float] = None


class TokenBucket:
    """Token bucket rate limiter.

    Allows bursty traffic while enforcing average rate.
    """

    def __init__(
        self,
        rate: float,
        capacity: Optional[float] = None,
    ) -> None:
        """Initialize token bucket.

        Args:
            rate: Tokens per second.
            capacity: Maximum bucket capacity.
        """
        self.rate = rate
        self.capacity = capacity or rate
        self._tokens = float(self.capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()

    def _add_tokens(self) -> None:
        """Add tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_update = now

    def consume(self, tokens: float = 1) -> RateLimitResult:
        """Try to consume tokens.

        Args:
            tokens: Number of tokens to consume.

        Returns:
            RateLimitResult indicating if allowed.
        """
        with self._lock:
            self._add_tokens()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return RateLimitResult(
                    allowed=True,
                    remaining=int(self._tokens),
                )

            retry_after = (tokens - self._tokens) / self.rate
            return RateLimitResult(
                allowed=False,
                remaining=int(self._tokens),
                retry_after=retry_after,
            )

    @property
    def available(self) -> float:
        """Get available tokens."""
        with self._lock:
            self._add_tokens()
            return self._tokens


class SlidingWindow:
    """Sliding window rate limiter.

    Tracks requests in a sliding time window.
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
    ) -> None:
        """Initialize sliding window.

        Args:
            max_requests: Max requests per window.
            window_seconds: Window size in seconds.
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: list = []
        self._lock = threading.Lock()

    def _cleanup(self) -> None:
        """Remove expired requests."""
        cutoff = time.time() - self.window_seconds
        self._requests = [t for t in self._requests if t > cutoff]

    def is_allowed(self) -> RateLimitResult:
        """Check if request is allowed.

        Returns:
            RateLimitResult indicating if allowed.
        """
        with self._lock:
            self._cleanup()

            if len(self._requests) < self.max_requests:
                self._requests.append(time.time())
                return RateLimitResult(
                    allowed=True,
                    remaining=self.max_requests - len(self._requests),
                )

            oldest = self._requests[0]
            retry_after = (oldest + self.window_seconds) - time.time()

            return RateLimitResult(
                allowed=False,
                remaining=0,
                retry_after=retry_after,
            )

    @property
    def current_count(self) -> int:
        """Get current request count in window."""
        with self._lock:
            self._cleanup()
            return len(self._requests)


class LeakyBucket:
    """Leaky bucket rate limiter.

    Smooths out bursty traffic.
    """

    def __init__(
        self,
        rate: float,
        capacity: float,
    ) -> None:
        """Initialize leaky bucket.

        Args:
            rate: Leaks per second.
            capacity: Maximum bucket capacity.
        """
        self.rate = rate
        self.capacity = capacity
        self._level = 0.0
        self._last_update = time.time()
        self._lock = threading.Lock()

    def _update(self) -> None:
        """Update bucket level based on leak rate."""
        now = time.time()
        elapsed = now - self._last_update
        self._level = max(0, self._level - elapsed * self.rate)
        self._last_update = now

    def add(self, amount: float = 1) -> bool:
        """Add to bucket.

        Args:
            amount: Amount to add.

        Returns:
            True if added (bucket not full).
        """
        with self._lock:
            self._update()

            if self._level + amount <= self.capacity:
                self._level += amount
                return True

            return False

    @property
    def level(self) -> float:
        """Get current bucket level."""
        with self._lock:
            self._update()
            return self._level


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts based on success/failure.

    Useful for API clients that need to back off on errors.
    """

    def __init__(
        self,
        initial_rate: float,
        min_rate: float = 0.1,
        max_rate: float = 100,
        increase_factor: float = 1.1,
        decrease_factor: float = 0.5,
    ) -> None:
        """Initialize adaptive rate limiter.

        Args:
            initial_rate: Starting rate.
            min_rate: Minimum rate.
            max_rate: Maximum rate.
            increase_factor: Multiply rate by this on success.
            decrease_factor: Multiply rate by this on failure.
        """
        self.rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self._lock = threading.Lock()

    def record_success(self) -> None:
        """Record successful request, increase rate."""
        with self._lock:
            self.rate = min(self.max_rate, self.rate * self.increase_factor)

    def record_failure(self) -> None:
        """Record failed request, decrease rate."""
        with self._lock:
            self.rate = max(self.min_rate, self.rate * self.decrease_factor)

    def get_token_bucket(self) -> TokenBucket:
        """Get token bucket with current rate.

        Returns:
            TokenBucket configured with current rate.
        """
        with self._lock:
            return TokenBucket(self.rate, self.rate)


def rate_limited(
    calls: int,
    period: float,
) -> Callable:
    """Decorator to rate limit a function.

    Args:
        calls: Number of calls allowed.
        period: Time period in seconds.

    Returns:
        Decorated function.
    """
    limiter = SlidingWindow(calls, period)

    def decorator(func):
        def wrapper(*args, **kwargs):
            result = limiter.is_allowed()
            if not result.allowed:
                raise RuntimeError(f"Rate limit exceeded. Retry after {result.retry_after}s")
            return func(*args, **kwargs)
        return wrapper
    return decorator