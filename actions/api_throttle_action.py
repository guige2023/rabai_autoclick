"""
API Throttle Action Module.

Provides request throttling and rate limiting for API endpoints,
managing request queues and enforcing rate limits.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from enum import Enum, auto
import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class ThrottleStrategy(Enum):
    """Throttling strategies."""
    TOKEN_BUCKET = auto()
    LEAKY_BUCKET = auto()
    SLIDING_WINDOW = auto()
    FIXED_WINDOW = auto()


@dataclass
class ThrottleLimit:
    """Defines a throttling limit."""
    requests_per_second: Optional[float] = None
    requests_per_minute: Optional[float] = None
    requests_per_hour: Optional[float] = None
    burst_size: Optional[int] = None
    concurrent_limit: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "requests_per_second": self.requests_per_second,
            "requests_per_minute": self.requests_per_minute,
            "requests_per_hour": self.requests_per_hour,
            "burst_size": self.burst_size,
            "concurrent_limit": self.concurrent_limit,
        }


@dataclass
class ThrottleResult:
    """Result of a throttle check."""
    allowed: bool
    wait_time_ms: float = 0.0
    remaining: int = 0
    reset_at: Optional[datetime] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "allowed": self.allowed,
            "wait_time_ms": self.wait_time_ms,
            "remaining": self.remaining,
            "reset_at": self.reset_at.isoformat() if self.reset_at else None,
            "error": self.error,
        }


@dataclass
class ThrottleStats:
    """Statistics for throttling."""
    total_requests: int = 0
    allowed_requests: int = 0
    rejected_requests: int = 0
    throttled_requests: int = 0
    wait_time_ms: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_requests": self.total_requests,
            "allowed_requests": self.allowed_requests,
            "rejected_requests": self.rejected_requests,
            "throttled_requests": self.throttled_requests,
            "wait_time_ms": self.wait_time_ms,
        }


class TokenBucket:
    """Token bucket implementation for rate limiting."""

    def __init__(self, rate: float, capacity: int):
        """
        Initialize token bucket.

        Args:
            rate: Tokens added per second.
            capacity: Maximum tokens.
        """
        self.rate = rate
        self.capacity = capacity
        self._tokens = float(capacity)
        self._last_update = time.time()

    def consume(self, tokens: int = 1) -> Tuple[bool, float]:
        """
        Try to consume tokens.

        Returns:
            Tuple of (success, wait_time_ms).
        """
        self._refill()

        if self._tokens >= tokens:
            self._tokens -= tokens
            return True, 0.0

        wait_time = (tokens - self._tokens) / self.rate * 1000
        return False, wait_time

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_update
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_update = now


class SlidingWindowCounter:
    """Sliding window counter implementation."""

    def __init__(self, max_requests: int, window_seconds: float):
        """
        Initialize sliding window.

        Args:
            max_requests: Maximum requests in window.
            window_seconds: Window size in seconds.
        """
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._requests: List[float] = []

    def allow(self) -> Tuple[bool, float]:
        """
        Check if request is allowed.

        Returns:
            Tuple of (allowed, wait_time_ms).
        """
        now = time.time()
        cutoff = now - self.window_seconds

        self._requests = [t for t in self._requests if t > cutoff]

        if len(self._requests) < self.max_requests:
            self._requests.append(now)
            return True, 0.0

        oldest = min(self._requests)
        wait_time = (oldest + self.window_seconds - now) * 1000
        return False, wait_time


class ApiThrottleAction:
    """
    Provides request throttling and rate limiting.

    This action implements various throttling strategies including
    token bucket, leaky bucket, and sliding window for controlling
    API request rates.

    Example:
        >>> throttle = ApiThrottleAction()
        >>> throttle.set_limit("api", ThrottleLimit(requests_per_second=10))
        >>> result = await throttle.check("api")
        >>> if result.allowed:
        ...     await process_request()
    """

    def __init__(
        self,
        strategy: ThrottleStrategy = ThrottleStrategy.TOKEN_BUCKET,
    ):
        """
        Initialize the API Throttle Action.

        Args:
            strategy: Default throttling strategy.
        """
        self.strategy = strategy
        self._limits: Dict[str, ThrottleLimit] = {}
        self._buckets: Dict[str, TokenBucket] = {}
        self._windows: Dict[str, SlidingWindowCounter] = {}
        self._concurrent_counts: Dict[str, int] = {}
        self._stats: Dict[str, ThrottleStats] = {}
        self._lock = asyncio.Lock()

    def set_limit(self, name: str, limit: ThrottleLimit) -> None:
        """
        Set a throttling limit.

        Args:
            name: Limit name/identifier.
            limit: Throttle limit configuration.
        """
        self._limits[name] = limit

        if limit.requests_per_second:
            bucket = TokenBucket(
                rate=limit.requests_per_second,
                capacity=limit.burst_size or int(limit.requests_per_second * 2),
            )
            self._buckets[name] = bucket

        if limit.requests_per_minute:
            window = SlidingWindowCounter(
                max_requests=int(limit.requests_per_minute),
                window_seconds=60.0,
            )
            self._windows[f"{name}:minute"] = window

        self._stats[name] = ThrottleStats()

    async def check(self, name: str) -> ThrottleResult:
        """
        Check if a request is allowed.

        Args:
            name: Limit name to check.

        Returns:
            ThrottleResult with decision and metadata.
        """
        async with self._lock:
            limit = self._limits.get(name)
            if not limit:
                return ThrottleResult(allowed=True)

            stats = self._stats.get(name, ThrottleStats())
            stats.total_requests += 1

            if limit.concurrent_limit:
                current = self._concurrent_counts.get(name, 0)
                if current >= limit.concurrent_limit:
                    stats.rejected_requests += 1
                    return ThrottleResult(
                        allowed=False,
                        error="Concurrent limit exceeded",
                    )

            if name in self._buckets:
                bucket = self._buckets[name]
                allowed, wait_ms = bucket.consume()

                if not allowed:
                    stats.throttled_requests += 1
                    stats.wait_time_ms += wait_ms
                    return ThrottleResult(
                        allowed=False,
                        wait_time_ms=wait_ms,
                        remaining=int(bucket._tokens),
                    )

            if f"{name}:minute" in self._windows:
                window = self._windows[f"{name}:minute"]
                allowed, wait_ms = window.allow()

                if not allowed:
                    stats.throttled_requests += 1
                    stats.wait_time_ms += wait_ms
                    return ThrottleResult(
                        allowed=False,
                        wait_time_ms=wait_ms,
                    )

            self._concurrent_counts[name] = self._concurrent_counts.get(name, 0) + 1
            stats.allowed_requests += 1

            return ThrottleResult(
                allowed=True,
                remaining=self._get_remaining(name),
            )

    async def release(self, name: str) -> None:
        """
        Release a request slot.

        Args:
            name: Limit name.
        """
        async with self._lock:
            if name in self._concurrent_counts:
                self._concurrent_counts[name] = max(0, self._concurrent_counts[name] - 1)

    def _get_remaining(self, name: str) -> int:
        """Get remaining requests for a limit."""
        if name in self._buckets:
            return int(self._buckets[name]._tokens)
        return 0

    async def with_throttle(
        self,
        name: str,
        operation: Callable,
    ) -> Any:
        """
        Execute operation with throttle checking.

        Args:
            name: Limit name.
            operation: Async operation.

        Returns:
            Operation result.

        Raises:
            RuntimeError: If throttled.
        """
        result = await self.check(name)

        if not result.allowed:
            raise RuntimeError(f"Throttled: {result.error}, wait {result.wait_time_ms}ms")

        try:
            return await operation()
        finally:
            await self.release(name)

    def get_stats(self, name: str) -> Optional[Dict[str, Any]]:
        """Get stats for a limit."""
        stats = self._stats.get(name)
        if not stats:
            return None

        return stats.to_dict()

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get all stats."""
        return {
            name: stats.to_dict()
            for name, stats in self._stats.items()
        }

    async def reset(self, name: Optional[str] = None) -> None:
        """
        Reset throttle state.

        Args:
            name: Optional specific limit to reset.
        """
        async with self._lock:
            if name:
                self._buckets.pop(name, None)
                self._windows.pop(f"{name}:minute", None)
                self._stats[name] = ThrottleStats()
                self._concurrent_counts.pop(name, None)
            else:
                self._buckets.clear()
                self._windows.clear()
                self._stats.clear()
                self._concurrent_counts.clear()


def create_throttle_action(
    strategy: ThrottleStrategy = ThrottleStrategy.TOKEN_BUCKET,
) -> ApiThrottleAction:
    """Factory function to create an ApiThrottleAction."""
    return ApiThrottleAction(strategy=strategy)
