"""
Rate Limiter Utility

Implements token bucket and sliding window rate limiting.
Controls the rate of actions to avoid overwhelming target systems.

Example:
    >>> limiter = RateLimiter(rate=10, capacity=20)
    >>> if limiter.try_acquire():
    ...     perform_action()
    ... else:
    ...     print("Rate limit exceeded")
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class RateLimitStrategy(Enum):
    """Rate limiting strategies."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW = "sliding_window"
    FIXED_WINDOW = "fixed_window"


@dataclass
class RateLimitConfig:
    """Configuration for rate limiting."""
    rate: float = 10.0  # Tokens/operations per second
    capacity: int = 20  # Maximum burst size
    strategy: RateLimitStrategy = RateLimitStrategy.TOKEN_BUCKET


class RateLimiter:
    """
    Thread-safe rate limiter using token bucket algorithm.

    Args:
        rate: Tokens added per second.
        capacity: Maximum token capacity.
    """

    def __init__(
        self,
        rate: float = 10.0,
        capacity: Optional[int] = None,
    ) -> None:
        self.rate = rate
        self.capacity = capacity or int(rate * 2)
        self._tokens = float(self.capacity)
        self._last_update = time.time()
        self._lock = threading.Lock()

    def try_acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire.

        Returns:
            True if tokens were acquired, False otherwise.
        """
        with self._lock:
            self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

            return False

    def acquire(self, tokens: int = 1, timeout: Optional[float] = None) -> bool:
        """
        Block until tokens are available.

        Args:
            tokens: Number of tokens to acquire.
            timeout: Maximum seconds to wait (None = wait forever).

        Returns:
            True if acquired, False on timeout.
        """
        start = time.time()

        while True:
            if self.try_acquire(tokens):
                return True

            if timeout is not None and (time.time() - start) >= timeout:
                return False

            # Calculate wait time
            with self._lock:
                self._refill()
                needed = tokens - self._tokens
                wait_time = needed / self.rate if self.rate > 0 else 0.1

            time.sleep(min(wait_time, 0.1))

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_update = now

    def reset(self) -> None:
        """Reset the bucket to full capacity."""
        with self._lock:
            self._tokens = float(self.capacity)
            self._last_update = time.time()

    @property
    def available_tokens(self) -> float:
        """Get current available tokens."""
        with self._lock:
            self._refill()
            return self._tokens

    @property
    def utilization(self) -> float:
        """Get bucket utilization (0.0 to 1.0)."""
        with self._lock:
            self._refill()
            return 1.0 - (self._tokens / self.capacity)


class SlidingWindowRateLimiter:
    """
    Rate limiter using sliding window algorithm.

    More accurate than token bucket but uses more memory.
    """

    def __init__(
        self,
        rate: float = 10.0,
        window_size: float = 1.0,
    ) -> None:
        self.rate = rate
        self.window_size = window_size
        self._timestamps: list[float] = []
        self._lock = threading.Lock()

    def try_acquire(self, count: int = 1) -> bool:
        """Try to acquire within rate limit."""
        with self._lock:
            self._prune()

            if len(self._timestamps) + count <= self.rate * self.window_size:
                now = time.time()
                for _ in range(count):
                    self._timestamps.append(now)
                return True

            return False

    def acquire(self, count: int = 1, timeout: Optional[float] = None) -> bool:
        """Block until acquisition succeeds or timeout."""
        start = time.time()

        while True:
            if self.try_acquire(count):
                return True

            if timeout is not None and (time.time() - start) >= timeout:
                return False

            time.sleep(0.05)

    def _prune(self) -> None:
        """Remove timestamps outside the window."""
        cutoff = time.time() - self.window_size
        self._timestamps = [t for t in self._timestamps if t >= cutoff]

    def get_current_rate(self) -> float:
        """Get current request rate."""
        with self._lock:
            self._prune()
            return len(self._timestamps) / self.window_size


class MultiLimiter:
    """
    Combines multiple rate limiters for different dimensions.

    Useful for limiting by both per-second and per-minute rates.
    """

    def __init__(
        self,
        limiters: dict[str, RateLimiter],
    ) -> None:
        self.limiters = limiters
        self._lock = threading.Lock()

    def try_acquire(self, dimensions: list[str]) -> bool:
        """
        Try to acquire from multiple limiters.

        Args:
            dimensions: List of limiter names to acquire from.

        Returns:
            True if all requested limiters allow acquisition.
        """
        with self._lock:
            for dim in dimensions:
                limiter = self.limiters.get(dim)
                if limiter and not limiter.try_acquire():
                    return False
            return True

    def acquire(
        self,
        dimensions: list[str],
        timeout: Optional[float] = None,
    ) -> bool:
        """Block until all dimensions allow acquisition."""
        start = time.time()

        while True:
            with self._lock:
                can_acquire = True
                for dim in dimensions:
                    limiter = self.limiters.get(dim)
                    if limiter and not limiter.try_acquire():
                        can_acquire = False
                        break

                if can_acquire:
                    return True

            if timeout is not None and (time.time() - start) >= timeout:
                return False

            time.sleep(0.05)

    def add_limiter(self, name: str, limiter: RateLimiter) -> None:
        """Add a new limiter dimension."""
        with self._lock:
            self.limiters[name] = limiter

    def remove_limiter(self, name: str) -> None:
        """Remove a limiter dimension."""
        with self._lock:
            if name in self.limiters:
                del self.limiters[name]
