"""
Leaky bucket algorithm implementation for rate limiting and traffic shaping.

Provides production-ready leaky bucket implementations with both
synchronous and asynchronous interfaces, suitable for network
traffic shaping and request rate limiting.

Example:
    >>> from utils.leaky_bucket_utils import LeakyBucket
    >>> bucket = LeakyBucket(rate=10, capacity=100)
    >>> if bucket.add(1):
    ...     print("Request queued")
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Callable, List, Optional


class LeakyBucket:
    """
    Thread-safe leaky bucket for rate limiting.

    The bucket leaks at a constant rate. If the bucket is full,
    new items are rejected. This provides uniform output rate.

    Attributes:
        rate: Items leaked per second.
        capacity: Maximum bucket capacity.
    """

    def __init__(
        self,
        rate: float,
        capacity: int,
        time_provider: Optional[Callable[[], float]] = None,
    ) -> None:
        """
        Initialize the leaky bucket.

        Args:
            rate: Items leaked (processed) per second.
            capacity: Maximum bucket capacity.
            time_provider: Callable returning current time in seconds.
        """
        self.rate = rate
        self.capacity = capacity
        self._time_provider = time_provider or time.monotonic
        self._level = 0.0
        self._last_leak = self._time_provider()
        self._lock = threading.RLock()

    def add(self, items: int = 1, blocking: bool = False) -> bool:
        """
        Add items to the bucket.

        Args:
            items: Number of items to add.
            blocking: If True, wait for space to become available.

        Returns:
            True if items were added, False if bucket is full.
        """
        with self._lock:
            self._leak()

            while self._level + items > self.capacity:
                if not blocking:
                    return False
                sleep_time = (self._level + items - self.capacity) / self.rate
                time.sleep(sleep_time)
                self._leak()

            self._level += items
            return True

    def try_add(self, items: int = 1) -> bool:
        """
        Try to add items without blocking.

        Args:
            items: Number of items to add.

        Returns:
            True if items were added, False if bucket is full.
        """
        return self.add(items, blocking=False)

    def _leak(self) -> None:
        """Leak items based on elapsed time."""
        now = self._time_provider()
        elapsed = now - self._last_leak
        leaked = elapsed * self.rate
        self._level = max(0.0, self._level - leaked)
        self._last_leak = now

    @property
    def current_level(self) -> float:
        """Get current bucket level."""
        with self._lock:
            self._leak()
            return self._level

    @property
    def available_space(self) -> float:
        """Get available space in the bucket."""
        with self._lock:
            self._leak()
            return self.capacity - self._level

    def reset(self) -> None:
        """Reset bucket to empty."""
        with self._lock:
            self._level = 0.0
            self._last_leak = self._time_provider()


class AsyncLeakyBucket:
    """
    Async leaky bucket for use in async contexts.

    Thread-safe async implementation with blocking and non-blocking
    add operations.
    """

    def __init__(
        self,
        rate: float,
        capacity: int,
        time_provider: Optional[Callable[[], float]] = None,
    ) -> None:
        """
        Initialize the async leaky bucket.

        Args:
            rate: Items leaked per second.
            capacity: Maximum bucket capacity.
            time_provider: Callable returning current time in seconds.
        """
        self.rate = rate
        self.capacity = capacity
        self._time_provider = time_provider or time.monotonic
        self._level = 0.0
        self._last_leak = self._time_provider()
        self._lock = asyncio.Lock()

    async def add(self, items: int = 1, blocking: bool = False) -> bool:
        """
        Add items to the bucket.

        Args:
            items: Number of items to add.
            blocking: If True, wait for space to become available.

        Returns:
            True if items were added, False if bucket is full.
        """
        async with self._lock:
            self._leak()

            while self._level + items > self.capacity:
                if not blocking:
                    return False
                sleep_time = (self._level + items - self.capacity) / self.rate
                await asyncio.sleep(sleep_time)
                self._leak()

            self._level += items
            return True

    async def try_add(self, items: int = 1) -> bool:
        """
        Try to add items without blocking.

        Args:
            items: Number of items to add.

        Returns:
            True if items were added, False if bucket is full.
        """
        return await self.add(items, blocking=False)

    def _leak(self) -> None:
        """Leak items based on elapsed time."""
        now = self._time_provider()
        elapsed = now - self._last_leak
        leaked = elapsed * self.rate
        self._level = max(0.0, self._level - leaked)
        self._last_leak = now

    @property
    async def current_level(self) -> float:
        """Get current bucket level."""
        async with self._lock:
            self._leak()
            return self._level

    async def reset(self) -> None:
        """Reset bucket to empty."""
        async with self._lock:
            self._level = 0.0
            self._last_leak = self._time_provider()


class LeakyBucketQueue:
    """
    Leaky bucket with internal queue for async processing.

    Items added to the bucket are queued and leaked at a constant
    rate for processing by a worker.

    Attributes:
        rate: Items processed per second.
        capacity: Maximum queue capacity.
    """

    def __init__(
        self,
        rate: float,
        capacity: int,
        processor: Optional[Callable[[any], None]] = None,
    ) -> None:
        """
        Initialize the leaky bucket queue.

        Args:
            rate: Items processed per second.
            capacity: Maximum queue capacity.
            processor: Optional function to process leaked items.
        """
        self.rate = rate
        self.capacity = capacity
        self.processor = processor
        self._queue: List[any] = []
        self._level = 0.0
        self._last_leak = time.monotonic()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)

    def put(
        self,
        item: any,
        blocking: bool = False,
        timeout: Optional[float] = None
    ) -> bool:
        """
        Put an item into the bucket.

        Args:
            item: Item to add.
            blocking: If True, wait for space.
            timeout: Maximum wait time in seconds.

        Returns:
            True if item was added, False if timeout or full.
        """
        with self._not_empty:
            while self._level >= self.capacity:
                if not blocking:
                    return False
                if not self._not_empty.wait(timeout):
                    return False

            self._queue.append(item)
            self._level += 1
            self._not_empty.notify()
            return True

    def start(self) -> None:
        """Start the leaky bucket worker in a background thread."""
        def worker() -> None:
            while True:
                with self._not_empty:
                    while self._level == 0:
                        self._not_empty.wait()

                    self._leak()
                    if self._level > 0 and self._queue:
                        item = self._queue.pop(0)

                if self.processor:
                    self.processor(item)

        thread = threading.Thread(target=worker, daemon=True)
        thread.start()

    def _leak(self) -> None:
        """Leak items based on elapsed time."""
        now = time.monotonic()
        elapsed = now - self._last_leak
        leaked = elapsed * self.rate

        while leaked >= 1.0 and self._queue:
            self._queue.pop(0)
            leaked -= 1.0
            self._level = max(0.0, self._level - 1.0)

        self._last_leak = now


class TokenReplenishmentStrategy:
    """
    Configurable token replenishment strategies.

    Supports different refill patterns including:
    - Constant rate
    - Burst (refill at beginning of period)
    - Scheduled (refill at specific times)
    """

    def __init__(self, strategy: str = "constant", **kwargs: any) -> None:
        """
        Initialize the replenishment strategy.

        Args:
            strategy: One of 'constant', 'burst', 'scheduled'.
            **kwargs: Strategy-specific parameters.
        """
        self.strategy = strategy
        self.kwargs = kwargs

    def get_refill_amount(self, elapsed: float, rate: float) -> float:
        """
        Calculate refill amount based on strategy.

        Args:
            elapsed: Time elapsed since last refill.
            rate: Base rate for constant strategy.

        Returns:
            Number of tokens to add.
        """
        if self.strategy == "constant":
            return elapsed * rate
        elif self.strategy == "burst":
            return rate
        elif self.strategy == "scheduled":
            period = self.kwargs.get("period", 1.0)
            return rate * min(elapsed, period)
        return elapsed * rate


def create_leaky_bucket(
    rate: float,
    capacity: int,
    async_mode: bool = False,
    **kwargs
) -> any:
    """
    Factory function to create a leaky bucket.

    Args:
        rate: Items leaked per second.
        capacity: Maximum bucket capacity.
        async_mode: Use async implementation.
        **kwargs: Additional arguments.

    Returns:
        Leaky bucket instance.
    """
    if async_mode:
        return AsyncLeakyBucket(rate=rate, capacity=capacity, **kwargs)
    return LeakyBucket(rate=rate, capacity=capacity, **kwargs)
