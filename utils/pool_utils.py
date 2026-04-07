"""Pool utilities for RabAI AutoClick.

Provides:
- Object pool
- Connection pool
- Worker pool
- Pool statistics
"""

from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Generic,
    Iterator,
    Optional,
    TypeVar,
)


T = TypeVar("T")


@dataclass
class PoolStats:
    """Statistics for a pool."""

    total_acquired: int = 0
    total_released: int = 0
    current_in_use: int = 0
    total_created: int = 0
    total_destroyed: int = 0
    wait_time_total: float = 0.0
    wait_count: int = 0

    @property
    def avg_wait_time(self) -> float:
        if self.wait_count == 0:
            return 0.0
        return self.wait_time_total / self.wait_count


class PooledObject(Generic[T]):
    """Wrapper for pooled object with lifecycle tracking."""

    def __init__(
        self,
        obj: T,
        pool: ObjectPool[T],
    ) -> None:
        self._obj = obj
        self._pool = pool
        self._in_use = False
        self._created_at = time.time()
        self._last_used_at = self._created_at
        self._use_count = 0

    @property
    def value(self) -> T:
        return self._obj

    def release(self) -> None:
        self._pool.release(self)

    def __enter__(self) -> T:
        self._in_use = True
        self._use_count += 1
        self._last_used_at = time.time()
        return self._obj

    def __exit__(self, *args: Any) -> None:
        self._in_use = False
        self.release()


class ObjectPool(Generic[T]):
    """Generic object pool with min/max limits.

    Example:
        pool = ObjectPool(factory=lambda: MyConnection(), min_size=2, max_size=10)

        with pool.acquire() as conn:
            conn.query("SELECT 1")

        pool.close()
    """

    def __init__(
        self,
        factory: Callable[[], T],
        min_size: int = 0,
        max_size: int = 10,
        idle_timeout: Optional[float] = 60.0,
        validator: Optional[Callable[[T], bool]] = None,
    ) -> None:
        self._factory = factory
        self._min_size = min_size
        self._max_size = max_size
        self._idle_timeout = idle_timeout
        self._validator = validator

        self._pool: deque[PooledObject[T]] = deque()
        self._in_use: set[PooledObject[T]] = set()
        self._lock = threading.RLock()
        self._stats = PoolStats()
        self._closed = False

        self._initialize_pool()

    def _initialize_pool(self) -> None:
        for _ in range(self._min_size):
            obj = self._factory()
            pooled = PooledObject(obj, self)
            self._pool.append(pooled)
            self._stats.total_created += 1

    def acquire(self) -> PooledObject[T]:
        """Acquire an object from the pool.

        Returns:
            PooledObject wrapper.

        Raises:
            RuntimeError: If pool is closed.
        """
        if self._closed:
            raise RuntimeError("Pool is closed")

        wait_start = time.time()

        while True:
            with self._lock:
                while self._pool:
                    pooled = self._pool.popleft()

                    if self._validator and not self._validator(pooled.value):
                        self._stats.total_destroyed += 1
                        continue

                    self._in_use.add(pooled)
                    self._stats.total_acquired += 1
                    wait_time = time.time() - wait_start
                    self._stats.wait_time_total += wait_time
                    self._stats.wait_count += 1
                    return pooled

                if len(self._in_use) < self._max_size:
                    obj = self._factory()
                    pooled = PooledObject(obj, self)
                    self._in_use.add(pooled)
                    self._stats.total_acquired += 1
                    self._stats.total_created += 1
                    wait_time = time.time() - wait_start
                    self._stats.wait_time_total += wait_time
                    self._stats.wait_count += 1
                    return pooled

            time.sleep(0.01)

    def release(self, pooled: PooledObject[T]) -> None:
        """Release an object back to the pool."""
        with self._lock:
            if pooled in self._in_use:
                self._in_use.remove(pooled)
                self._stats.total_released += 1

                if pooled._in_use:
                    return

                self._pool.append(pooled)

    def close(self) -> None:
        """Close the pool and destroy all objects."""
        with self._lock:
            self._closed = True
            while self._pool:
                self._pool.pop()
            self._in_use.clear()

    def resize(self, new_min: int, new_max: int) -> None:
        """Resize the pool."""
        with self._lock:
            self._min_size = new_min
            self._max_size = new_max

    @property
    def stats(self) -> PoolStats:
        return self._stats

    def __len__(self) -> int:
        return len(self._pool) + len(self._in_use)


class AsyncObjectPool(Generic[T]):
    """Async object pool."""

    def __init__(
        self,
        factory: Callable[[], T],
        min_size: int = 0,
        max_size: int = 10,
        idle_timeout: Optional[float] = 60.0,
    ) -> None:
        self._factory = factory
        self._min_size = min_size
        self._max_size = max_size
        self._idle_timeout = idle_timeout
        self._pool: asyncio.Queue[T] = asyncio.Queue()
        self._semaphore = asyncio.Semaphore(max_size)
        self._in_use_count = 0
        self._closed = False

        asyncio.create_task(self._initialize())

    async def _initialize(self) -> None:
        for _ in range(self._min_size):
            obj = self._factory()
            await self._pool.put(obj)

    async def acquire(self) -> T:
        if self._closed:
            raise RuntimeError("Pool is closed")

        await self._semaphore.acquire()
        try:
            obj = await asyncio.wait_for(self._pool.get(), timeout=5.0)
            self._in_use_count += 1
            return obj
        except asyncio.TimeoutError:
            self._semaphore.release()
            raise RuntimeError("Timeout acquiring from pool")

    async def release(self, obj: T) -> None:
        self._in_use_count -= 1
        if not self._closed:
            await self._pool.put(obj)
        self._semaphore.release()

    async def close(self) -> None:
        self._closed = True
        while not self._pool.empty():
            try:
                self._pool.get_nowait()
            except asyncio.QueueEmpty:
                break
