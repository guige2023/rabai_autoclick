"""
Rate counter and sliding window rate limiter utilities.

Provides thread-safe rate counting with sliding window algorithms.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass


@dataclass
class RateStats:
    """Rate statistics snapshot."""
    total_count: int
    current_rate: float
    avg_rate: float
    peak_rate: float


class SlidingWindowRateCounter:
    """
    Thread-safe sliding window rate counter.

    Tracks events in a sliding time window for accurate rate calculation.
    """

    def __init__(self, window_seconds: float = 60.0):
        self.window_seconds = window_seconds
        self._lock = threading.Lock()
        self._events: deque[float] = deque()
        self._total_count: int = 0
        self._peak_rate: float = 0.0

    def add(self, count: int = 1) -> None:
        """
        Record an event.

        Args:
            count: Number of events to record
        """
        now = time.time()
        with self._lock:
            self._prune(now)
            for _ in range(count):
                self._events.append(now)
            self._total_count += count
            current_rate = self.current_rate
            if current_rate > self._peak_rate:
                self._peak_rate = current_rate

    def _prune(self, now: float) -> None:
        cutoff = now - self.window_seconds
        while self._events and self._events[0] < cutoff:
            self._events.popleft()

    @property
    def count(self) -> int:
        """Current count in window."""
        with self._lock:
            self._prune(time.time())
            return len(self._events)

    @property
    def current_rate(self) -> float:
        """Current rate (events per second)."""
        with self._lock:
            now = time.time()
            self._prune(now)
            if len(self._events) < 2:
                return 0.0
            span = self._events[-1] - self._events[0]
            if span <= 0:
                return 0.0
            return len(self._events) / span

    @property
    def total_count(self) -> int:
        """Total count across all time."""
        with self._lock:
            return self._total_count

    @property
    def peak_rate(self) -> float:
        """Peak rate observed."""
        with self._lock:
            return self._peak_rate

    def get_stats(self) -> RateStats:
        """Get full statistics snapshot."""
        with self._lock:
            now = time.time()
            self._prune(now)
            current = len(self._events)
            span = self._events[-1] - self._events[0] if len(self._events) >= 2 else 0.0
            current_rate = current / span if span > 0 else 0.0
            avg_rate = self._total_count / (now - self._events[0]) if self._events else 0.0
            return RateStats(
                total_count=self._total_count,
                current_rate=current_rate,
                avg_rate=avg_rate,
                peak_rate=self._peak_rate,
            )

    def reset(self) -> None:
        """Reset all counters."""
        with self._lock:
            self._events.clear()
            self._total_count = 0
            self._peak_rate = 0.0


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        tokens: float | None = None,
    ):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._tokens = tokens if tokens is not None else capacity
        self._last_refill = time.time()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.refill_rate)
        self._last_refill = now

    def consume(self, tokens: float = 1.0) -> bool:
        """
        Try to consume tokens.

        Args:
            tokens: Number of tokens to consume

        Returns:
            True if tokens were consumed, False if insufficient
        """
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return True
            return False

    def wait_and_consume(self, tokens: float = 1.0, timeout: float | None = None) -> bool:
        """
        Wait until tokens are available and consume.

        Args:
            tokens: Number of tokens
            timeout: Max wait time (None = wait forever)

        Returns:
            True if consumed, False on timeout
        """
        start = time.time()
        while True:
            if self.consume(tokens):
                return True
            if timeout is not None and time.time() - start >= timeout:
                return False
            time.sleep(0.01)

    @property
    def available_tokens(self) -> float:
        """Current available tokens."""
        with self._lock:
            self._refill()
            return self._tokens


class LeakyBucket:
    """Leaky bucket rate limiter."""

    def __init__(self, capacity: int, leak_rate: float):
        self.capacity = capacity
        self.leak_rate = leak_rate
        self._level: float = 0.0
        self._last_leak = time.time()
        self._lock = threading.Lock()

    def _leak(self) -> None:
        now = time.time()
        elapsed = now - self._last_leak
        self._level = max(0.0, self._level - elapsed * self.leak_rate)
        self._last_leak = now

    def add(self, amount: int = 1) -> bool:
        """
        Try to add to the bucket.

        Args:
            amount: Amount to add

        Returns:
            True if accepted, False if would overflow
        """
        with self._lock:
            self._leak()
            if self._level + amount <= self.capacity:
                self._level += amount
                return True
            return False

    def wait_and_add(self, amount: int = 1, timeout: float | None = None) -> bool:
        """
        Wait for space and add.

        Args:
            amount: Amount to add
            timeout: Max wait time

        Returns:
            True if added, False on timeout
        """
        start = time.time()
        while True:
            if self.add(amount):
                return True
            if timeout is not None and time.time() - start >= timeout:
                return False
            time.sleep(0.01)

    @property
    def current_level(self) -> int:
        """Current bucket level (rounded)."""
        with self._lock:
            self._leak()
            return int(self._level)
