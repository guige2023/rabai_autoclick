"""
Object Pool Pattern Implementation

Manages a pool of reusable objects to avoid expensive allocation/deallocation.
"""

from __future__ import annotations

import copy
import threading
import time
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class Poolable(ABC):
    """
    Interface for objects that can be pooled.
    """

    @abstractmethod
    def reset(self) -> None:
        """Reset the object to its initial state."""
        pass

    @abstractmethod
    def validate(self) -> bool:
        """Check if the object is still valid for use."""
        pass


@dataclass
class PoolMetrics:
    """Metrics for pool operations."""
    total_acquires: int = 0
    total_releases: int = 0
    pool_hits: int = 0
    pool_misses: int = 0
    invalidations: int = 0
    peak_size: int = 0
    current_size: int = 0


class ObjectPool(Generic[T]):
    """
    Generic object pool with thread-safe operations.
    """

    def __init__(
        self,
        factory: Callable[[], T],
        initial_size: int = 0,
        max_size: int | None = None,
        validation_func: Callable[[T], bool] | None = None,
        reset_func: Callable[[T], None] | None = None,
    ):
        self._factory = factory
        self._max_size = max_size
        self._validation_func = validation_func
        self._reset_func = reset_func

        self._available: list[T] = []
        self._in_use: set[T] = set()
        self._lock = threading.RLock()
        self._metrics = PoolMetrics()

        # Pre-populate pool
        for _ in range(initial_size):
            self._available.append(factory())

        self._metrics.peak_size = initial_size
        self._metrics.current_size = initial_size

    def acquire(self, timeout: float | None = None) -> T | None:
        """
        Acquire an object from the pool.

        Args:
            timeout: Maximum time to wait for an object.

        Returns:
            An object from the pool, or None if unavailable.
        """
        start_time = time.time()

        with self._lock:
            self._metrics.total_acquires += 1

            # Try to get from available pool
            while self._available:
                obj = self._available.pop()

                # Validate object
                if self._validation_func and not self._validation_func(obj):
                    self._metrics.invalidations += 1
                    self._metrics.current_size -= 1
                    continue

                self._in_use.add(obj)
                self._metrics.pool_hits += 1
                return obj

            # Pool is empty, try to create new if under max
            if self._max_size is None or len(self._in_use) < self._max_size:
                new_obj = self._factory()
                self._in_use.add(new_obj)
                self._metrics.current_size += 1
                self._metrics.peak_size = max(self._metrics.peak_size, self._metrics.current_size)
                self._metrics.pool_misses += 1
                return new_obj

            # At max capacity, wait for release
            if timeout is None:
                return None

            # Wait loop
            while time.time() - start_time < timeout:
                time.sleep(0.01)
                if self._available:
                    obj = self._available.pop()
                    if self._validation_func and not self._validation_func(obj):
                        self._metrics.invalidations += 1
                        self._metrics.current_size -= 1
                        continue
                    self._in_use.add(obj)
                    return obj

            return None

    def release(self, obj: T) -> bool:
        """
        Return an object to the pool.

        Args:
            obj: The object to return.

        Returns:
            True if the object was successfully released.
        """
        with self._lock:
            if obj not in self._in_use:
                return False

            self._in_use.remove(obj)

            # Reset the object before returning to pool
            if self._reset_func:
                self._reset_func(obj)
            elif hasattr(obj, "reset"):
                obj.reset()

            self._available.append(obj)
            self._metrics.total_releases += 1
            return True

    def prewarm(self, count: int) -> int:
        """
        Pre-create objects in the pool.

        Args:
            count: Number of objects to create.

        Returns:
            Number of objects actually created.
        """
        with self._lock:
            created = 0
            for _ in range(count):
                if self._max_size is not None and self._metrics.current_size >= self._max_size:
                    break
                self._available.append(self._factory())
                self._metrics.current_size += 1
                created += 1

            self._metrics.peak_size = max(self._metrics.peak_size, self._metrics.current_size)
            return created

    def clear(self) -> int:
        """
        Clear all objects from the pool.

        Returns:
            Number of objects cleared.
        """
        with self._lock:
            count = self._metrics.current_size
            self._available.clear()
            self._in_use.clear()
            self._metrics.current_size = 0
            return count

    @property
    def available_count(self) -> int:
        """Get number of available objects."""
        with self._lock:
            return len(self._available)

    @property
    def in_use_count(self) -> int:
        """Get number of objects currently in use."""
        with self._lock:
            return len(self._in_use)

    @property
    def total_count(self) -> int:
        """Get total objects in pool."""
        with self._lock:
            return self._metrics.current_size

    @property
    def metrics(self) -> PoolMetrics:
        """Get pool metrics."""
        return copy.copy(self._metrics)


class PoolStats:
    """Statistics for pool usage."""

    def __init__(self, pool: ObjectPool):
        self._pool = pool

    @property
    def utilization(self) -> float:
        """Get pool utilization as a percentage."""
        metrics = self._pool.metrics
        if metrics.peak_size == 0:
            return 0.0
        return (metrics.peak_size - self._pool.available_count) / metrics.peak_size

    @property
    def hit_rate(self) -> float:
        """Get pool hit rate."""
        metrics = self._pool.metrics
        total = metrics.pool_hits + metrics.pool_misses
        return metrics.pool_hits / total if total > 0 else 0.0


def create_pool(
    factory: Callable[[], T],
    *,
    min_size: int = 0,
    max_size: int | None = None,
) -> ObjectPool[T]:
    """
    Create a configured object pool.

    Args:
        factory: Function to create new objects.
        min_size: Initial pool size.
        max_size: Maximum pool size.

    Returns:
        Configured ObjectPool instance.
    """
    return ObjectPool(factory=factory, initial_size=min_size, max_size=max_size)
