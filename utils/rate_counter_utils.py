"""
Rate Counter Utilities

Provides utilities for counting and rate limiting
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

import time
from dataclasses import dataclass


@dataclass
class RateBucket:
    """Token bucket for rate limiting."""
    tokens: float
    last_update: float
    capacity: float
    refill_rate: float


class RateCounter:
    """
    Counts events and enforces rate limits.
    
    Uses token bucket algorithm for
    rate limiting.
    """

    def __init__(
        self,
        max_rate: float,
        time_window: float = 60.0,
    ) -> None:
        self._max_rate = max_rate
        self._time_window = time_window
        self._events: list[float] = []
        self._bucket = RateBucket(
            tokens=max_rate,
            last_update=time.time(),
            capacity=max_rate,
            refill_rate=max_rate / time_window,
        )

    def count(self) -> int:
        """Get current count within time window."""
        now = time.time()
        cutoff = now - self._time_window
        self._events = [e for e in self._events if e > cutoff]
        return len(self._events)

    def try_acquire(self, tokens: float = 1.0) -> bool:
        """Try to acquire tokens from bucket."""
        self._refill_bucket()
        if self._bucket.tokens >= tokens:
            self._bucket.tokens -= tokens
            return True
        return False

    def _refill_bucket(self) -> None:
        """Refill bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self._bucket.last_update
        self._bucket.tokens = min(
            self._bucket.capacity,
            self._bucket.tokens + elapsed * self._bucket.refill_rate,
        )
        self._bucket.last_update = now
