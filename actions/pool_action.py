"""pool action module for rabai_autoclick.

Provides object pooling, connection pooling, and resource pooling
with configurable sizing, eviction, and health checking.
"""

from __future__ import annotations

import threading
import time
import weakref
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Generic, Optional, TypeVar, Protocol
from concurrent.futures import Future

__all__ = [
    "Pool",
    "ObjectPool",
    "ConnectionPool",
    "PooledObject",
    "EvictionPolicy",
    "PoolExhaustedError",
    "PooledConnection",
    "create_pool",
    "pool_context",
]


T = TypeVar("T")


class EvictionPolicy(Enum):
    """Object eviction strategies."""
    LRU = auto()
    LFU = auto()
    FIFO = auto()
    LIFO = auto()
    RANDOM = auto()


class PoolExhaustedError(Exception):
    """Raised when pool has no available objects."""
    pass


@dataclass
class PooledObject(Generic[T]):
    """Wrapper around a pooled object with metadata."""
    obj: T
    created_at: float = field(default_factory=time.perf_counter)
    last_used: float = field(default_factory=time.perf_counter)
    use_count: int = 0
    in_use: bool = False

    def touch(self) -> None:
        """Update last_used timestamp and increment counter."""
        self.last_used = time.perf_counter()
        self.use_count += 1

    def age(self) -> float:
        """Return seconds since creation."""
        return time.perf_counter() - self.created_at

    def idle_time(self) -> float:
        """Return seconds since last use."""
        return time.perf_counter() - self.last_used


class ObjectPool(Generic[T]):
    """Generic object pool with configurable eviction and sizing."""

    def __init__(
        self,
        factory: Callable[[], T],
        min_size: int = 0,
        max_size: int = 10,
        max_idle_seconds: float = 300.0,
        eviction_policy: EvictionPolicy = EvictionPolicy.LRU,
        validator: Optional[Callable[[T], bool]] = None,
        destroyer: Optional[Callable[[T], None]] = None,
    ) -> None:
        self.factory = factory
        self.min_size = min_size
        self.max_size = max_size
        self.max_idle_seconds = max_idle_seconds
        self.eviction_policy = eviction_policy
        self.validator = validator
        self.destroyer = destroyer or (lambda x: None)

        self._pool: deque[PooledObject[T]] = deque()
        self._lock = threading.RLock()
        self._cond = threading.Condition(self._lock)
        self._total_created = 0
        self._total_destroyed = 0
        self._eviction_running = False
        self._closed = False

        for _ in range(min_size):
            self._create_object()

    def _create_object(self) -> Optional[PooledObject[T]]:
        with self._lock:
            if len(self._pool) >= self.max_size:
                return None
            try:
                obj = self.factory()
                pooled = PooledObject(obj=obj)
                self._pool.append(pooled)
                self._total_created += 1
                return pooled
            except Exception:
                return None

    def acquire(self, timeout: Optional[float] = None) -> T:
        """Acquire an object from the pool.

        Args:
            timeout: Max seconds to wait (None = infinite).

        Returns:
            Pooled object instance.

        Raises:
            PoolExhaustedError: If timeout expires.
        """
        if self._closed:
            raise RuntimeError("Pool is closed")

        deadline = None if timeout is None else time.monotonic() + timeout

        with self._cond:
            while True:
                obj = self._try_get_available()
                if obj is not None:
                    return obj

                if self._total_created < self.max_size:
                    created = self._create_object()
                    if created is not None:
                        created.in_use = True
                        return created.obj

                if timeout is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolExhaustedError(f"Pool exhausted after {timeout}s")
                    self._cond.wait(timeout=remaining)
                else:
                    self._cond.wait()

    def release(self, obj: T) -> None:
        """Return an object to the pool."""
        with self._lock:
            for pooled in self._pool:
                if pooled.obj is obj:
                    pooled.in_use = False
                    pooled.touch()
                    self._cond.notify()
                    return

    @contextmanager
    def connection(self, timeout: Optional[float] = None):
        """Context manager for acquiring and releasing objects.

        Usage:
            with pool.connection() as obj:
                obj.do_something()
        """
        obj = self.acquire(timeout=timeout)
        try:
            yield obj
        finally:
            self.release(obj)

    def _try_get_available(self) -> Optional[T]:
        """Find an available, valid object in the pool."""
        candidates = []
        for pooled in self._pool:
            if pooled.in_use:
                continue
            if pooled.obj is None:
                continue
            if self.validator is not None and not self.validator(pooled.obj):
                candidates.append(pooled)
                continue
            pooled.in_use = True
            return pooled.obj

        for pooled in candidates:
            self._evict_object(pooled)

        for pooled in self._pool:
            if not pooled.in_use and pooled.obj is not None:
                if self.validator is None or self.validator(pooled.obj):
                    pooled.in_use = True
                    return pooled.obj

        return None

    def _evict_object(self, pooled: PooledObject[T]) -> None:
        """Remove a specific object from the pool."""
        try:
            self._pool.remove(pooled)
            self.destroyer(pooled.obj)
            self._total_destroyed += 1
        except ValueError:
            pass

    def evict_expired(self) -> int:
        """Evict objects idle longer than max_idle_seconds.

        Returns:
            Number of objects evicted.
        """
        with self._lock:
            evicted = 0
            for pooled in list(self._pool):
                if not pooled.in_use and pooled.idle_time() > self.max_idle_seconds:
                    self._evict_object(pooled)
                    evicted += 1
        return evicted

    def evict_by_policy(self, count: int = 1) -> int:
        """Evict objects according to eviction policy.

        Args:
            count: Maximum number to evict.

        Returns:
            Number of objects evicted.
        """
        with self._lock:
            available = [p for p in self._pool if not p.in_use]
            if len(available) <= self.min_size:
                return 0

            if self.eviction_policy == EvictionPolicy.LRU:
                sorted_pool = sorted(available, key=lambda p: p.last_used)
            elif self.eviction_policy == EvictionPolicy.LFU:
                sorted_pool = sorted(available, key=lambda p: p.use_count)
            elif self.eviction_policy == EvictionPolicy.FIFO:
                sorted_pool = sorted(available, key=lambda p: p.created_at)
            elif self.eviction_policy == EvictionPolicy.LIFO:
                sorted_pool = sorted(available, key=lambda p: p.created_at, reverse=True)
            else:
                import random
                sorted_pool = list(available)
                random.shuffle(sorted_pool)

            to_evict = sorted_pool[:min(count, len(available) - self.min_size)]
            for pooled in to_evict:
                self._evict_object(pooled)
            return len(to_evict)

    def close(self) -> None:
        """Close pool and destroy all objects."""
        with self._lock:
            self._closed = True
            while self._pool:
                pooled = self._pool.popleft()
                try:
                    self.destroyer(pooled.obj)
                    self._total_destroyed += 1
                except Exception:
                    pass
            self._cond.notify_all()

    def stats(self) -> dict[str, Any]:
        """Return pool statistics."""
        with self._lock:
            in_use = sum(1 for p in self._pool if p.in_use)
            return {
                "total": len(self._pool),
                "in_use": in_use,
                "available": len(self._pool) - in_use,
                "total_created": self._total_created,
                "total_destroyed": self._total_destroyed,
                "max_size": self.max_size,
                "closed": self._closed,
            }

    def __enter__(self) -> "ObjectPool[T]":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()


class PooledConnection(Generic[T]):
    """Wrapper that provides context-manager semantics for pooled objects."""

    def __init__(self, pool: ObjectPool[T], timeout: Optional[float] = None) -> None:
        self._pool = pool
        self._timeout = timeout
        self._obj: Optional[T] = None

    def __enter__(self) -> T:
        self._obj = self._pool.acquire(timeout=self._timeout)
        return self._obj

    def __exit__(self, *args: Any) -> None:
        if self._obj is not None:
            self._pool.release(self._obj)


class ConnectionPool(ObjectPool[T]):
    """Specialized pool for database/network connections with ping/health check."""

    def __init__(
        self,
        factory: Callable[[], T],
        min_size: int = 0,
        max_size: int = 10,
        max_idle_seconds: float = 300.0,
        health_check: Optional[Callable[[T], bool]] = None,
        reconnect: Optional[Callable[[T], T]] = None,
    ) -> None:
        self._health_check = health_check
        self._reconnect = reconnect
        super().__init__(
            factory=factory,
            min_size=min_size,
            max_size=max_size,
            max_idle_seconds=max_idle_seconds,
            validator=health_check,
        )

    def health_check(self, obj: T) -> bool:
        """Check if connection is still healthy."""
        if self._health_check is not None:
            return self._health_check(obj)
        return True

    def ensure_healthy(self, obj: T) -> T:
        """Ensure object is healthy, reconnect if needed."""
        if not self.health_check(obj):
            if self._reconnect:
                return self._reconnect(obj)
            raise RuntimeError("Connection unhealthy and no reconnect available")
        return obj


def create_pool(
    factory: Callable,
    min_size: int = 0,
    max_size: int = 10,
    pool_type: str = "object",
    **kwargs: Any,
) -> ObjectPool:
    """Factory function to create a pool.

    Args:
        factory: Object/connection factory.
        min_size: Minimum pool size.
        max_size: Maximum pool size.
        pool_type: Type of pool ("object" or "connection").
        **kwargs: Additional pool options.

    Returns:
        Configured pool instance.
    """
    if pool_type == "connection":
        return ConnectionPool(factory, min_size=min_size, max_size=max_size, **kwargs)
    return ObjectPool(factory, min_size=min_size, max_size=max_size, **kwargs)


@contextmanager
def pool_context(pool: ObjectPool[T], timeout: Optional[float] = None):
    """Context manager for working with any pool."""
    obj = pool.acquire(timeout=timeout)
    try:
        yield obj
    finally:
        pool.release(obj)
