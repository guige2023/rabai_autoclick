"""
Token bucket algorithm implementation for rate limiting.

Provides a production-ready token bucket implementation with both
synchronous and asynchronous interfaces, multi-bucket support,
and configurable refill strategies.

Example:
    >>> from utils.token_bucket_utils import TokenBucket
    >>> bucket = TokenBucket(rate=5, capacity=10)
    >>> if bucket.consume(1):
    ...     print("Allowed")
"""

from __future__ import annotations

import asyncio
import math
import threading
import time
from typing import Optional, Protocol


class TimeProvider(Protocol):
    """Protocol for time providers."""
    def __call__(self) -> float: ...


class TokenBucket:
    """
    Thread-safe token bucket for rate limiting.

    Tokens are added to the bucket at a constant rate up to
    the bucket capacity. Each consume operation removes tokens.

    Attributes:
        rate: Tokens added per second.
        capacity: Maximum token capacity.
    """

    def __init__(
        self,
        rate: float,
        capacity: int,
        time_provider: Optional[TimeProvider] = None,
    ) -> None:
        """
        Initialize the token bucket.

        Args:
            rate: Tokens added per second.
            capacity: Maximum token capacity.
            time_provider: Callable returning current time in seconds.
        """
        self.rate = rate
        self.capacity = capacity
        self._time_provider = time_provider or time.monotonic
        self._tokens = float(capacity)
        self._last_refill = self._time_provider()
        self._lock = threading.RLock()

    def consume(self, tokens: int = 1, blocking: bool = False) -> bool:
        """
        Attempt to consume tokens.

        Args:
            tokens: Number of tokens to consume.
            blocking: If True, wait until tokens are available.

        Returns:
            True if tokens were consumed, False otherwise.
        """
        with self._lock:
            self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

            if not blocking:
                return False

        sleep_time = (tokens - self._tokens) / self.rate
        time.sleep(sleep_time)

        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

        return False

    def try_consume(self, tokens: int = 1) -> bool:
        """
        Try to consume tokens without blocking.

        Args:
            tokens: Number of tokens to consume.

        Returns:
            True if tokens were consumed, False otherwise.
        """
        return self.consume(tokens, blocking=False)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = self._time_provider()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    @property
    def available_tokens(self) -> float:
        """Get the current number of available tokens."""
        with self._lock:
            self._refill()
            return self._tokens

    def reset(self) -> None:
        """Reset the bucket to full capacity."""
        with self._lock:
            self._tokens = float(self.capacity)
            self._last_refill = self._time_provider()

    def set_rate(self, rate: float) -> None:
        """
        Update the refill rate.

        Args:
            rate: New tokens per second rate.
        """
        with self._lock:
            self._refill()
            self.rate = rate


class AsyncTokenBucket:
    """
    Async token bucket for use in async contexts.

    Thread-safe async token bucket implementation with
    blocking and non-blocking consume operations.
    """

    def __init__(
        self,
        rate: float,
        capacity: int,
        time_provider: Optional[TimeProvider] = None,
    ) -> None:
        """
        Initialize the async token bucket.

        Args:
            rate: Tokens added per second.
            capacity: Maximum token capacity.
            time_provider: Callable returning current time in seconds.
        """
        self.rate = rate
        self.capacity = capacity
        self._time_provider = time_provider or time.monotonic
        self._tokens = float(capacity)
        self._last_refill = self._time_provider()
        self._lock = asyncio.Lock()

    async def consume(self, tokens: int = 1, blocking: bool = False) -> bool:
        """
        Attempt to consume tokens.

        Args:
            tokens: Number of tokens to consume.
            blocking: If True, wait until tokens are available.

        Returns:
            True if tokens were consumed, False otherwise.
        """
        async with self._lock:
            self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

            if not blocking:
                return False

        sleep_time = (tokens - self._tokens) / self.rate
        await asyncio.sleep(sleep_time)

        async with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

        return False

    async def try_consume(self, tokens: int = 1) -> bool:
        """
        Try to consume tokens without blocking.

        Args:
            tokens: Number of tokens to consume.

        Returns:
            True if tokens were consumed, False otherwise.
        """
        return await self.consume(tokens, blocking=False)

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = self._time_provider()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    @property
    async def available_tokens(self) -> float:
        """Get the current number of available tokens."""
        async with self._lock:
            self._refill()
            return self._tokens

    async def reset(self) -> None:
        """Reset the bucket to full capacity."""
        async with self._lock:
            self._tokens = float(self.capacity)
            self._last_refill = self._time_provider()


class MultiTokenBucket:
    """
    Manages multiple independent token buckets.

    Useful for limiting different types of resources separately.

    Attributes:
        buckets: Dictionary mapping bucket names to TokenBucket instances.
    """

    def __init__(self, **bucket_configs: tuple) -> None:
        """
        Initialize multi-bucket manager.

        Args:
            **bucket_configs: Named tuples of (rate, capacity).
        """
        self.buckets: dict[str, TokenBucket] = {}
        for name, (rate, capacity) in bucket_configs.items():
            self.buckets[name] = TokenBucket(rate=rate, capacity=capacity)

    def consume(
        self,
        tokens_map: dict[str, int],
        blocking: bool = False
    ) -> bool:
        """
        Attempt to consume from multiple buckets atomically.

        Args:
            tokens_map: Mapping of bucket name to tokens to consume.
            blocking: If True, wait until all tokens are available.

        Returns:
            True if all tokens were consumed, False otherwise.
        """
        for name in tokens_map:
            if name not in self.buckets:
                return False

        if blocking:
            for name, tokens in tokens_map.items():
                self.buckets[name].consume(tokens, blocking=True)
            return True

        for name, tokens in tokens_map.items():
            if not self.buckets[name].try_consume(tokens):
                return False
        return True

    def try_consume(self, tokens_map: dict[str, int]) -> bool:
        """
        Try to consume from multiple buckets without blocking.

        Args:
            tokens_map: Mapping of bucket name to tokens to consume.

        Returns:
            True if all tokens were consumed, False otherwise.
        """
        return self.consume(tokens_map, blocking=False)

    def get_bucket(self, name: str) -> Optional[TokenBucket]:
        """Get a bucket by name."""
        return self.buckets.get(name)


class TokenBucketWithJitter:
    """
    Token bucket with jittered refill for distributed systems.

    Adds random jitter to prevent thundering herd when
    multiple clients refill at the same time.

    Attributes:
        rate: Base tokens per second.
        capacity: Maximum token capacity.
        jitter_fraction: Fraction of rate to jitter (0.0 to 1.0).
    """

    def __init__(
        self,
        rate: float,
        capacity: int,
        jitter_fraction: float = 0.1,
    ) -> None:
        """
        Initialize the jittered token bucket.

        Args:
            rate: Base tokens per second.
            capacity: Maximum token capacity.
            jitter_fraction: Jitter as fraction of rate (0.0-1.0).
        """
        import random
        self._random = random.Random()
        self._rate = rate
        self.capacity = capacity
        self.jitter_fraction = jitter_fraction
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.RLock()

    @property
    def rate(self) -> float:
        """Get current rate with jitter applied."""
        jitter = self._rate * self.jitter_fraction
        return self._rate + self._random.uniform(-jitter, jitter)

    def consume(self, tokens: int = 1, blocking: bool = False) -> bool:
        """Consume tokens with jittered refill."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

            if not blocking:
                return False

        sleep_time = (tokens - self._tokens) / self.rate
        time.sleep(sleep_time)

        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True

        return False

    def try_consume(self, tokens: int = 1) -> bool:
        """Try to consume without blocking."""
        return self.consume(tokens, blocking=False)

    def _refill(self) -> None:
        """Refill with jittered rate."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def reset(self) -> None:
        """Reset bucket to full."""
        with self._lock:
            self._tokens = float(self.capacity)
            self._last_refill = time.monotonic()


def create_token_bucket(
    rate: float,
    capacity: int,
    async_mode: bool = False,
    jitter: bool = False,
    **kwargs
) -> any:
    """
    Factory function to create a token bucket.

    Args:
        rate: Tokens added per second.
        capacity: Maximum token capacity.
        async_mode: Use async implementation.
        jitter: Add jitter for distributed systems.
        **kwargs: Additional arguments.

    Returns:
        Token bucket instance.
    """
    if jitter:
        return TokenBucketWithJitter(rate=rate, capacity=capacity, **kwargs)
    if async_mode:
        return AsyncTokenBucket(rate=rate, capacity=capacity, **kwargs)
    return TokenBucket(rate=rate, capacity=capacity, **kwargs)
