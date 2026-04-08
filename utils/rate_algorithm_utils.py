"""
Rate limiting algorithm implementations.

Provides multiple rate limiting algorithms including Token Bucket,
Leaky Bucket, Sliding Window, Sliding Window Log, and Fixed Window.
Each algorithm is implemented as a reusable class with async support.

Example:
    >>> from utils.rate_algorithm_utils import TokenBucketLimiter
    >>> limiter = TokenBucketLimiter(rate=10, capacity=20)
    >>> await limiter.acquire()
"""

from __future__ import annotations

import asyncio
import math
import time
from abc import ABC, abstractmethod
from typing import Optional


class RateLimiter(ABC):
    """
    Abstract base class for rate limiting algorithms.

    All implementations provide an async-compatible interface for
    acquiring permits and checking rate limits.
    """

    @abstractmethod
    async def acquire(self, tokens: int = 1) -> bool:
        """
        Acquire tokens from the rate limiter.

        Args:
            tokens: Number of tokens to acquire.

        Returns:
            True if tokens were acquired, False otherwise.
        """
        pass

    @abstractmethod
    async def try_acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens without blocking.

        Args:
            tokens: Number of tokens to acquire.

        Returns:
            True if tokens were acquired, False if rate limited.
        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """Reset the rate limiter state."""
        pass


class TokenBucketLimiter(RateLimiter):
    """
    Token bucket rate limiter.

    Tokens are added to the bucket at a constant rate up to
    the bucket capacity. Each acquire consumes tokens.

    Attributes:
        rate: Tokens added per second.
        capacity: Maximum tokens in the bucket.
    """

    def __init__(
        self,
        rate: float,
        capacity: int,
        initial_tokens: Optional[float] = None
    ) -> None:
        """
        Initialize the token bucket limiter.

        Args:
            rate: Tokens added per second.
            capacity: Maximum token capacity.
            initial_tokens: Starting token count (defaults to capacity).
        """
        self.rate = rate
        self.capacity = capacity
        self._tokens = initial_tokens if initial_tokens is not None else capacity
        self._last_update = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """
        Acquire tokens, waiting if necessary.

        Args:
            tokens: Number of tokens to acquire.

        Returns:
            True when tokens are acquired.
        """
        async with self._lock:
            await self._refill()

            while self._tokens < tokens:
                sleep_time = (tokens - self._tokens) / self.rate
                await asyncio.sleep(sleep_time)
                await self._refill()

            self._tokens -= tokens
            return True

    async def try_acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens without blocking.

        Args:
            tokens: Number of tokens to acquire.

        Returns:
            True if tokens were acquired, False if rate limited.
        """
        async with self._lock:
            await self._refill()

            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    async def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_update
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
        self._last_update = now

    def reset(self) -> None:
        """Reset bucket to full capacity."""
        self._tokens = self.capacity
        self._last_update = time.monotonic()


class LeakyBucketLimiter(RateLimiter):
    """
    Leaky bucket rate limiter.

    The bucket leaks at a constant rate. If the bucket is full,
    new requests are rejected.

    Attributes:
        rate: Leaks per second.
        capacity: Maximum bucket capacity.
    """

    def __init__(self, rate: float, capacity: int) -> None:
        """
        Initialize the leaky bucket limiter.

        Args:
            rate: Leak rate (requests per second).
            capacity: Maximum bucket capacity.
        """
        self.rate = rate
        self.capacity = capacity
        self._level = 0.0
        self._last_leak = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """
        Acquire space in the bucket.

        Args:
            tokens: Number of spaces to acquire.

        Returns:
            True when space is acquired.
        """
        async with self._lock:
            await self._leak()

            while self._level + tokens > self.capacity:
                sleep_time = (self._level + tokens - self.capacity) / self.rate
                await asyncio.sleep(sleep_time)
                await self._leak()

            self._level += tokens
            return True

    async def try_acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire space without blocking.

        Args:
            tokens: Number of spaces to acquire.

        Returns:
            True if space was acquired, False if bucket is full.
        """
        async with self._lock:
            await self._leak()

            if self._level + tokens <= self.capacity:
                self._level += tokens
                return True
            return False

    async def _leak(self) -> None:
        """Leak based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_leak
        leaked = elapsed * self.rate
        self._level = max(0.0, self._level - leaked)
        self._last_leak = now

    def reset(self) -> None:
        """Reset bucket to empty."""
        self._level = 0.0
        self._last_leak = time.monotonic()


class SlidingWindowLimiter(RateLimiter):
    """
    Sliding window rate limiter.

    Tracks requests within a sliding time window.

    Attributes:
        rate: Maximum requests per window.
        window_size: Window size in seconds.
    """

    def __init__(self, rate: int, window_size: float) -> None:
        """
        Initialize the sliding window limiter.

        Args:
            rate: Maximum requests per window.
            window_size: Window size in seconds.
        """
        self.rate = rate
        self.window_size = window_size
        self._window_start: float = time.monotonic()
        self._request_count = 0
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """
        Acquire within the rate limit.

        Args:
            tokens: Number of requests to count.

        Returns:
            True when acquired.
        """
        async with self._lock:
            await self._slide_window()

            while self._request_count + tokens > self.rate:
                sleep_time = self.window_size - (time.monotonic() - self._window_start)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                await self._slide_window()

            self._request_count += tokens
            return True

    async def try_acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire without blocking.

        Args:
            tokens: Number of requests to count.

        Returns:
            True if within rate limit, False otherwise.
        """
        async with self._lock:
            await self._slide_window()

            if self._request_count + tokens <= self.rate:
                self._request_count += tokens
                return True
            return False

    async def _slide_window(self) -> None:
        """Slide the window forward, discarding old requests."""
        now = time.monotonic()
        elapsed = now - self._window_start

        if elapsed >= self.window_size:
            self._window_start = now
            self._request_count = 0
        else:
            self._request_count = int(
                self._request_count * (1 - elapsed / self.window_size)
            )
            self._window_start = now - elapsed

    def reset(self) -> None:
        """Reset the sliding window."""
        self._window_start = time.monotonic()
        self._request_count = 0


class FixedWindowLimiter(RateLimiter):
    """
    Fixed window rate limiter.

    Tracks requests in fixed time windows.

    Attributes:
        rate: Maximum requests per window.
        window_size: Window size in seconds.
    """

    def __init__(self, rate: int, window_size: float) -> None:
        """
        Initialize the fixed window limiter.

        Args:
            rate: Maximum requests per window.
            window_size: Window size in seconds.
        """
        self.rate = rate
        self.window_size = window_size
        self._window_start = math.floor(time.time() / window_size) * window_size
        self._request_count = 0
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """
        Acquire within the rate limit.

        Args:
            tokens: Number of requests to count.

        Returns:
            True when acquired.
        """
        async with self._lock:
            await self._check_window()

            while self._request_count + tokens > self.rate:
                sleep_time = self.window_size - (time.time() - self._window_start)
                if sleep_time > 0:
                    await asyncio.sleep(sleep_time)
                await self._check_window()

            self._request_count += tokens
            return True

    async def try_acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire without blocking.

        Args:
            tokens: Number of requests to count.

        Returns:
            True if within rate limit, False otherwise.
        """
        async with self._lock:
            await self._check_window()

            if self._request_count + tokens <= self.rate:
                self._request_count += tokens
                return True
            return False

    async def _check_window(self) -> None:
        """Check and advance window if needed."""
        current_window = math.floor(time.time() / self.window_size) * self.window_size
        if current_window > self._window_start:
            self._window_start = current_window
            self._request_count = 0

    def reset(self) -> None:
        """Reset the window."""
        self._window_start = math.floor(time.time() / self.window_size) * self.window_size
        self._request_count = 0


class AdaptiveRateLimiter(RateLimiter):
    """
    Adaptive rate limiter that adjusts based on success/failure rates.

    Increases rate when requests succeed, decreases when they fail.

    Attributes:
        initial_rate: Starting rate.
        min_rate: Minimum allowed rate.
        max_rate: Maximum allowed rate.
        increase_factor: Multiplier when rate increases.
        decrease_factor: Multiplier when rate decreases.
    """

    def __init__(
        self,
        initial_rate: float,
        min_rate: float = 1.0,
        max_rate: float = 1000.0,
        increase_factor: float = 1.1,
        decrease_factor: float = 0.5,
    ) -> None:
        """
        Initialize the adaptive rate limiter.

        Args:
            initial_rate: Starting rate.
            min_rate: Minimum allowed rate.
            max_rate: Maximum allowed rate.
            increase_factor: Multiplier for rate increases.
            decrease_factor: Multiplier for rate decreases.
        """
        self.initial_rate = initial_rate
        self.min_rate = min_rate
        self.max_rate = max_rate
        self.increase_factor = increase_factor
        self.decrease_factor = decrease_factor
        self._current_rate = initial_rate
        self._bucket: Optional[TokenBucketLimiter] = None

    def _get_bucket(self) -> TokenBucketLimiter:
        """Get or create the underlying token bucket."""
        if self._bucket is None or self._bucket.rate != self._current_rate:
            self._bucket = TokenBucketLimiter(
                rate=self._current_rate,
                capacity=int(self._current_rate * 2),
            )
        return self._bucket

    async def acquire(self, tokens: int = 1) -> bool:
        """Acquire with current rate."""
        return await self._get_bucket().acquire(tokens)

    async def try_acquire(self, tokens: int = 1) -> bool:
        """Try to acquire with current rate."""
        return await self._get_bucket().try_acquire(tokens)

    def report_success(self) -> None:
        """Report a successful request to increase rate."""
        self._current_rate = min(
            self.max_rate,
            self._current_rate * self.increase_factor
        )
        self._bucket = None

    def report_failure(self) -> None:
        """Report a failed request to decrease rate."""
        self._current_rate = max(
            self.min_rate,
            self._current_rate * self.decrease_factor
        )
        self._bucket = None

    def reset(self) -> None:
        """Reset to initial rate."""
        self._current_rate = self.initial_rate
        self._bucket = None


def create_limiter(
    algorithm: str,
    **kwargs
) -> RateLimiter:
    """
    Factory function to create a rate limiter.

    Args:
        algorithm: One of 'token_bucket', 'leaky_bucket',
                   'sliding_window', 'fixed_window', 'adaptive'.
        **kwargs: Arguments passed to the limiter constructor.

    Returns:
        Configured rate limiter instance.

    Raises:
        ValueError: If algorithm is unknown.
    """
    algorithms = {
        "token_bucket": TokenBucketLimiter,
        "leaky_bucket": LeakyBucketLimiter,
        "sliding_window": SlidingWindowLimiter,
        "fixed_window": FixedWindowLimiter,
        "adaptive": AdaptiveRateLimiter,
    }

    if algorithm not in algorithms:
        raise ValueError(
            f"Unknown algorithm: {algorithm}. "
            f"Available: {list(algorithms.keys())}"
        )

    return algorithms[algorithm](**kwargs)
