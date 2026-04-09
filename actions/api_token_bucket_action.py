"""
Token bucket rate limiter for API request throttling.

This module provides a token bucket algorithm implementation for controlling
the rate of API requests with support for burst handling and configurable limits.

Author: RabAiBot
License: MIT
"""

from __future__ import annotations

import asyncio
import logging
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, Callable, Any
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    """Configuration for a rate limit bucket."""
    rate: float  # Tokens per second
    capacity: float  # Maximum bucket capacity
    initial_tokens: Optional[float] = None
    refill_callback: Optional[Callable[[], None]] = None
    blocked_callback: Optional[Callable[[str, float], None]] = None

    def __post_init__(self):
        if self.initial_tokens is None:
            self.initial_tokens = self.capacity


class TokenBucket:
    """
    Token bucket implementation for rate limiting.

    The token bucket algorithm allows bursts up to the bucket capacity
    while maintaining an average rate limit over time.

    Example:
        >>> bucket = TokenBucket(rate=10.0, capacity=20)
        >>> if bucket.try_acquire():
        ...     make_api_request()
        ... else:
        ...     wait_and_retry()
    """

    def __init__(
        self,
        rate: float,
        capacity: float,
        initial_tokens: Optional[float] = None,
    ):
        """
        Initialize a token bucket.

        Args:
            rate: Tokens added per second
            capacity: Maximum token capacity
            initial_tokens: Starting tokens (defaults to capacity)
        """
        if rate <= 0:
            raise ValueError(f"Rate must be positive, got {rate}")
        if capacity <= 0:
            raise ValueError(f"Capacity must be positive, got {capacity}")

        self.rate = rate
        self.capacity = capacity
        self._tokens = initial_tokens if initial_tokens is not None else capacity
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        tokens_to_add = elapsed * self.rate
        self._tokens = min(self.capacity, self._tokens + tokens_to_add)
        self._last_refill = now

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """
        Try to acquire tokens from the bucket.

        Args:
            tokens: Number of tokens to acquire

        Returns:
            True if tokens were acquired, False otherwise
        """
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def acquire(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """
        Acquire tokens, waiting if necessary.

        Args:
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait in seconds

        Returns:
            True if acquired, False if timeout
        """
        start = time.monotonic()
        while True:
            if self.try_acquire(tokens):
                return True
            if timeout is not None and (time.monotonic() - start) >= timeout:
                return False
            time.sleep(0.01)

    async def acquire_async(self, tokens: float = 1.0, timeout: Optional[float] = None) -> bool:
        """
        Async version of acquire.

        Args:
            tokens: Number of tokens to acquire
            timeout: Maximum time to wait in seconds

        Returns:
            True if acquired, False if timeout
        """
        start = time.monotonic()
        while True:
            if self.try_acquire(tokens):
                return True
            if timeout is not None and (time.monotonic() - start) >= timeout:
                return False
            await asyncio.sleep(0.01)

    @property
    def available_tokens(self) -> float:
        """Get current available tokens."""
        with self._lock:
            self._refill()
            return self._tokens

    @property
    def fill_percentage(self) -> float:
        """Get bucket fill percentage."""
        return (self.available_tokens / self.capacity) * 100


class RateLimiter:
    """
    Multi-client rate limiter using token bucket algorithm.

    Supports:
    - Per-client rate limiting
    - Shared buckets across clients
    - Callback hooks for monitoring
    - Thread-safe operations

    Example:
        >>> limiter = RateLimiter()
        >>> limiter.add_client("user1", rate=10, capacity=20)
        >>> if limiter.try_acquire("user1"):
        ...     make_api_request()
    """

    def __init__(self):
        """Initialize the rate limiter."""
        self._buckets: Dict[str, TokenBucket] = {}
        self._configs: Dict[str, RateLimitConfig] = {}
        self._lock = threading.RLock()
        self._total_requests: int = 0
        self._total_blocked: int = 0
        self._blocked_by_client: Dict[str, int] = defaultdict(int)
        logger.info("RateLimiter initialized")

    def add_client(
        self,
        client_id: str,
        rate: float,
        capacity: float,
        config: Optional[RateLimitConfig] = None,
    ) -> TokenBucket:
        """
        Add a client with rate limit configuration.

        Args:
            client_id: Unique client identifier
            rate: Requests per second
            capacity: Burst capacity
            config: Optional RateLimitConfig with callbacks

        Returns:
            The created TokenBucket
        """
        with self._lock:
            if client_id in self._buckets:
                raise ValueError(f"Client '{client_id}' already exists")

            bucket = TokenBucket(rate=rate, capacity=capacity)
            self._buckets[client_id] = bucket
            self._configs[client_id] = config or RateLimitConfig(rate=rate, capacity=capacity)
            logger.info(f"Client '{client_id}' added: rate={rate}, capacity={capacity}")
            return bucket

    def remove_client(self, client_id: str) -> bool:
        """Remove a client from rate limiting."""
        with self._lock:
            if client_id in self._buckets:
                del self._buckets[client_id]
                del self._configs[client_id]
                logger.info(f"Client '{client_id}' removed")
                return True
            return False

    def try_acquire(self, client_id: str, tokens: float = 1.0) -> bool:
        """
        Try to acquire rate limit tokens for a client.

        Args:
            client_id: Client identifier
            tokens: Number of tokens to acquire

        Returns:
            True if acquired, False if rate limited
        """
        with self._lock:
            if client_id not in self._buckets:
                logger.warning(f"Unknown client '{client_id}', using default bucket")
                return True

            bucket = self._buckets[client_id]
            config = self._configs[client_id]
            acquired = bucket.try_acquire(tokens)

            self._total_requests += 1
            if not acquired:
                self._total_blocked += 1
                self._blocked_by_client[client_id] += 1
                if config.blocked_callback:
                    try:
                        config.blocked_callback(client_id, bucket.available_tokens)
                    except Exception as e:
                        logger.warning(f"blocked_callback failed: {e}")

            return acquired

    def acquire(
        self,
        client_id: str,
        tokens: float = 1.0,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Acquire rate limit tokens, waiting if necessary.

        Args:
            client_id: Client identifier
            tokens: Number of tokens to acquire
            timeout: Maximum wait time in seconds

        Returns:
            True if acquired, False if timeout
        """
        with self._lock:
            if client_id not in self._buckets:
                return True

        bucket = self._buckets[client_id]
        return bucket.acquire(tokens=tokens, timeout=timeout)

    async def acquire_async(
        self,
        client_id: str,
        tokens: float = 1.0,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Async acquire rate limit tokens.

        Args:
            client_id: Client identifier
            tokens: Number of tokens to acquire
            timeout: Maximum wait time in seconds

        Returns:
            True if acquired, False if timeout
        """
        with self._lock:
            if client_id not in self._buckets:
                return True

        bucket = self._buckets[client_id]
        return await bucket.acquire_async(tokens=tokens, timeout=timeout)

    def get_bucket(self, client_id: str) -> Optional[TokenBucket]:
        """Get the bucket for a client."""
        return self._buckets.get(client_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get rate limiter statistics."""
        with self._lock:
            return {
                "total_requests": self._total_requests,
                "total_blocked": self._total_blocked,
                "block_rate": (
                    self._total_blocked / self._total_requests
                    if self._total_requests > 0
                    else 0.0
                ),
                "clients": {
                    cid: {
                        "available_tokens": bucket.available_tokens,
                        "fill_percentage": bucket.fill_percentage,
                        "blocked": self._blocked_by_client.get(cid, 0),
                    }
                    for cid, bucket in self._buckets.items()
                },
            }

    def reset(self) -> None:
        """Reset all buckets and statistics."""
        with self._lock:
            for bucket in self._buckets.values():
                bucket._tokens = bucket.capacity
            self._total_requests = 0
            self._total_blocked = 0
            self._blocked_by_client.clear()
            logger.info("RateLimiter reset")
