"""
Object pool pattern implementation.

Provides reusable object pools for expensive resources
like database connections or thread pools.
"""

from __future__ import annotations

import threading
import time
from typing import Callable, Generic, TypeVar


T = TypeVar("T")


class ObjectPool(Generic[T]):
    """
    Generic object pool for managing reusable resources.

    Thread-safe implementation with min/max pool size limits.
    """

    def __init__(
        self,
        factory: Callable[[], T],
        min_size: int = 0,
        max_size: int = 10,
        idle_timeout: float = 300.0,
        validator: Callable[[T], bool] | None = None,
    ):
        self.factory = factory
        self.min_size = min_size
        self.max_size = max_size
        self.idle_timeout = idle_timeout
        self.validator = validator

        self._available: list[T] = []
        self._in_use: set[T] = set()
        self._lock = threading.Lock()
        self._total_created = 0

        self._initialize()

    def _initialize(self) -> None:
        """Pre-create minimum number of objects."""
        for _ in range(self.min_size):
            obj = self.factory()
            self._available.append(obj)
            self._total_created += 1

    def acquire(self, timeout: float | None = None) -> T:
        """
        Acquire an object from the pool.

        Args:
            timeout: Max wait time (None = wait forever)

        Returns:
            Pooled object

        Raises:
            RuntimeError: If timeout exceeded
        """
        deadline = time.time() + timeout if timeout else None

        while True:
            obj = self._try_acquire()
            if obj is not None:
                return obj
            if deadline and time.time() >= deadline:
                raise RuntimeError("Pool acquisition timeout")
            time.sleep(0.01)

    def _try_acquire(self) -> T | None:
        with self._lock:
            while self._available:
                obj = self._available.pop()
                if self.validator and not self.validator(obj):
                    self._destroy(obj)
                    continue
                self._in_use.add(obj)
                return obj
            if self._total_created < self.max_size:
                obj = self.factory()
                self._total_created += 1
                self._in_use.add(obj)
                return obj
        return None

    def release(self, obj: T) -> None:
        """
        Return an object to the pool.

        Args:
            obj: Object to return
        """
        with self._lock:
            if obj in self._in_use:
                self._in_use.discard(obj)
                if self.validator and not self.validator(obj):
                    self._destroy(obj)
                    return
                self._available.append(obj)

    def _destroy(self, obj: T) -> None:
        """Destroy an object (call cleanup)."""
        self._total_created -= 1

    def shrink(self, target_size: int | None = None) -> int:
        """
        Shrink pool to target size.

        Args:
            target_size: Desired pool size (defaults to min_size)

        Returns:
            Number of objects removed
        """
        if target_size is None:
            target_size = self.min_size
        removed = 0
        with self._lock:
            while len(self._available) > target_size:
                self._available.pop()
                self._total_created -= 1
                removed += 1
        return removed

    def grow(self, target_size: int) -> int:
        """
        Grow pool to target size.

        Args:
            target_size: Desired pool size

        Returns:
            Number of objects added
        """
        added = 0
        while self._total_created < target_size and self._total_created < self.max_size:
            self.acquire()
            added += 1
        return added

    def stats(self) -> dict:
        """Get pool statistics."""
        with self._lock:
            return {
                "total": self._total_created,
                "available": len(self._available),
                "in_use": len(self._in_use),
                "max_size": self.max_size,
            }

    @property
    def available_count(self) -> int:
        return len(self._available)

    @property
    def in_use_count(self) -> int:
        with self._lock:
            return len(self._in_use)


class PooledObject:
    """Context manager wrapper for pooled objects."""

    def __init__(self, pool: ObjectPool, obj: T):
        self._pool = pool
        self._obj = obj

    def __enter__(self) -> T:
        return self._obj

    def __exit__(self, *args: object) -> None:
        self._pool.release(self._obj)


def create_pool(
    factory: Callable[[], T],
    **kwargs: Any,
) -> ObjectPool[T]:
    """Factory to create an ObjectPool."""
    return ObjectPool(factory=factory, **kwargs)
