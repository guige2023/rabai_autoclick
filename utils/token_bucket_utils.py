"""Token bucket rate limiter utilities.

Provides token bucket algorithm for rate limiting
in automation workflows and API clients.
"""

import threading
import time
from typing import Optional


class TokenBucket:
    """Token bucket rate limiter with refill.

    Example:
        bucket = TokenBucket(capacity=10, refill_rate=5.0)
        # capacity = max tokens, refill_rate = tokens per second
        if bucket.consume(1):
            # do work
        else:
            # rate limited
    """

    def __init__(
        self,
        capacity: int,
        refill_rate: float,
        initial_tokens: Optional[float] = None,
    ) -> None:
        """Initialize token bucket.

        Args:
            capacity: Maximum tokens in bucket.
            refill_rate: Tokens added per second.
            initial_tokens: Initial token count. Defaults to capacity.
        """
        self._capacity = float(capacity)
        self._refill_rate = float(refill_rate)
        self._tokens = float(initial_tokens if initial_tokens is not None else capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_refill
        tokens_to_add = elapsed * self._refill_rate
        self._tokens = min(self._capacity, self._tokens + tokens_to_add)
        self._last_refill = now

    def consume(self, tokens: int = 1, blocking: bool = False) -> bool:
        """Try to consume tokens from bucket.

        Args:
            tokens: Number of tokens to consume.
            blocking: If True, wait until tokens are available.

        Returns:
            True if tokens were consumed, False otherwise.
        """
        if blocking:
            wait_time = self.wait_time(tokens)
            if wait_time > 0:
                time.sleep(wait_time)

        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def wait_time(self, tokens: int = 1) -> float:
        """Calculate time to wait for tokens.

        Args:
            tokens: Number of tokens needed.

        Returns:
            Seconds to wait, 0 if tokens available.
        """
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                return 0.0
            deficit = tokens - self._tokens
            return deficit / self._refill_rate

    @property
    def tokens(self) -> float:
        """Get current token count."""
        with self._lock:
            self._refill()
            return self._tokens

    @property
    def available(self) -> bool:
        """Check if at least one token is available."""
        return self.tokens >= 1

    def reset(self) -> None:
        """Reset bucket to full capacity."""
        with self._lock:
            self._tokens = self._capacity
            self._last_refill = time.monotonic()


class MultiTierBucket:
    """Multi-tier token bucket for different rate limits.

    Example:
        bucket = MultiTierBucket()
        bucket.add_tier("fast", capacity=10, rate=5.0)
        bucket.add_tier("slow", capacity=100, rate=1.0)
        if bucket.consume("fast"):
            # do fast operation
    """

    def __init__(self) -> None:
        self._buckets: dict = {}
        self._lock = threading.Lock()

    def add_tier(
        self,
        name: str,
        capacity: int,
        refill_rate: float,
    ) -> None:
        """Add a rate limit tier.

        Args:
            name: Tier name.
            capacity: Max tokens.
            refill_rate: Tokens per second.
        """
        with self._lock:
            self._buckets[name] = TokenBucket(capacity, refill_rate)

    def consume(self, tier: str, tokens: int = 1, blocking: bool = False) -> bool:
        """Consume tokens from a tier.

        Args:
            tier: Tier name.
            tokens: Tokens to consume.
            blocking: Wait for tokens.

        Returns:
            True if consumed.
        """
        if tier not in self._buckets:
            return True
        return self._buckets[tier].consume(tokens, blocking)

    def wait_time(self, tier: str, tokens: int = 1) -> float:
        """Get wait time for tier."""
        if tier not in self._buckets:
            return 0.0
        return self._buckets[tier].wait_time(tokens)

    def tier_tokens(self, tier: str) -> float:
        """Get current tokens for tier."""
        if tier not in self._buckets:
            return 0.0
        return self._buckets[tier].tokens

    def reset(self, tier: Optional[str] = None) -> None:
        """Reset bucket(s)."""
        with self._lock:
            if tier:
                if tier in self._buckets:
                    self._buckets[tier].reset()
            else:
                for bucket in self._buckets.values():
                    bucket.reset()
