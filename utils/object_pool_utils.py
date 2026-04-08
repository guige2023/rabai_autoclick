"""Object pool utilities for resource reuse.

Provides object pooling to reduce allocation overhead
for frequently created/destroyed objects.
"""

import threading
from typing import Any, Callable, Generic, List, Optional, TypeVar


T = TypeVar("T")


class ObjectPool(Generic[T]):
    """Pool of reusable objects.

    Example:
        pool = ObjectPool(factory=lambda: ExpensiveObject())
        obj = pool.acquire()
        # use obj
        pool.release(obj)
    """

    def __init__(
        self,
        factory: Callable[[], T],
        max_size: int = 100,
        validator: Optional[Callable[[T], bool]] = None,
    ) -> None:
        self._factory = factory
        self._max_size = max_size
        self._validator = validator
        self._pool: List[T] = []
        self._lock = threading.Lock()
        self._total_created = 0
        self._total_reused = 0

    def acquire(self) -> T:
        """Acquire an object from the pool.

        Returns:
            Object from pool or newly created.
        """
        with self._lock:
            while self._pool:
                obj = self._pool.pop()
                if self._validator is None or self._validator(obj):
                    self._total_reused += 1
                    return obj
            self._total_created += 1
            return self._factory()

    def release(self, obj: T) -> None:
        """Return an object to the pool.

        Args:
            obj: Object to return.
        """
        if obj is None:
            return
        with self._lock:
            if len(self._pool) < self._max_size:
                self._pool.append(obj)

    def clear(self) -> None:
        """Clear all objects from pool."""
        with self._lock:
            self._pool.clear()

    @property
    def size(self) -> int:
        """Get current pool size."""
        with self._lock:
            return len(self._pool)

    @property
    def stats(self) -> dict:
        """Get pool statistics."""
        with self._lock:
            return {
                "pool_size": len(self._pool),
                "max_size": self._max_size,
                "total_created": self._total_created,
                "total_reused": self._total_reused,
                "reuse_rate": self._total_reused / max(1, self._total_created + self._total_reused),
            }


class PooledObject(Generic[T]):
    """Wrapper for pooled objects with context manager.

    Example:
        pool = ObjectPool(factory=lambda: MyObject())
        with PooledObject(pool) as obj:
            obj.do_work()
    """

    def __init__(self, pool: ObjectPool[T]) -> None:
        self._pool = pool
        self._obj: Optional[T] = None

    def __enter__(self) -> T:
        self._obj = self._pool.acquire()
        return self._obj

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._obj is not None:
            self._pool.release(self._obj)
            self._obj = None


class ByteStringPool:
    """Pool for byte string buffers.

    Example:
        pool = ByteStringPool(min_size=64, max_size=4096)
        buf = pool.acquire(1024)
        # use buffer
        pool.release(buf)
    """

    def __init__(self, min_size: int = 64, max_size: int = 65536) -> None:
        self._min_size = min_size
        self._max_size = max_size
        self._pools: dict = {}
        self._lock = threading.Lock()

    def _get_pool(self, size: int) -> List[bytearray]:
        if size not in self._pools:
            self._pools[size] = []
        return self._pools[size]

    def acquire(self, size: int) -> bytearray:
        """Acquire buffer of given size.

        Args:
            size: Required buffer size.

        Returns:
            Bytearray buffer.
        """
        size = self._round_size(size)
        if size > self._max_size:
            return bytearray(size)

        with self._lock:
            pool = self._get_pool(size)
            if pool:
                return pool.pop()
        return bytearray(size)

    def release(self, buf: bytearray) -> None:
        """Release buffer back to pool.

        Args:
            buf: Buffer to release.
        """
        size = len(buf)
        if size > self._max_size:
            return

        size = self._round_size(size)
        with self._lock:
            pool = self._get_pool(size)
            if len(pool) < 100:  # Limit per size
                buf[:] = b""
                pool.append(buf)

    def _round_size(self, size: int) -> int:
        """Round size to power of 2 or nearest bucket."""
        if size <= self._min_size:
            return self._min_size
        if size >= self._max_size:
            return self._max_size
        v = 1
        while v < size:
            v *= 2
        return v

    def clear(self) -> None:
        """Clear all pools."""
        with self._lock:
            self._pools.clear()


class ConnectionPool(Generic[T]):
    """Pool for managing reusable connections.

    Example:
        pool = ConnectionPool(
            factory=lambda: create_connection(),
            validator=lambda c: c.is_open(),
            max_size=10,
        )
        conn = pool.get_connection()
        # use connection
        pool.release_connection(conn)
    """

    def __init__(
        self,
        factory: Callable[[], T],
        validator: Optional[Callable[[T], bool]] = None,
        max_size: int = 10,
    ) -> None:
        self._factory = factory
        self._validator = validator
        self._max_size = max_size
        self._available: List[T] = []
        self._in_use: List[T] = []
        self._lock = threading.Lock()

    def get_connection(self, timeout: float = None) -> Optional[T]:
        """Get a connection from pool.

        Args:
            timeout: Maximum wait time.

        Returns:
            Connection or None if timeout.
        """
        import time
        start = time.time()
        while True:
            conn = None
            with self._lock:
                while self._available:
                    c = self._available.pop()
                    if self._validator is None or self._validator(c):
                        conn = c
                        self._in_use.append(c)
                        break
                if not conn and len(self._in_use) + len(self._available) < self._max_size:
                    conn = self._factory()
                    self._in_use.append(conn)

            if conn:
                return conn

            if timeout and (time.time() - start) >= timeout:
                return None

            time.sleep(0.01)

    def release_connection(self, conn: T) -> None:
        """Release connection back to pool.

        Args:
            conn: Connection to release.
        """
        with self._lock:
            if conn in self._in_use:
                self._in_use.remove(conn)
                if self._validator is None or self._validator(conn):
                    self._available.append(conn)

    def close_all(self) -> None:
        """Close all connections."""
        with self._lock:
            for conn in self._available + self._in_use:
                try:
                    if hasattr(conn, "close"):
                        conn.close()
                except Exception:
                    pass
            self._available.clear()
            self._in_use.clear()

    @property
    def stats(self) -> dict:
        """Get pool statistics."""
        with self._lock:
            return {
                "available": len(self._available),
                "in_use": len(self._in_use),
                "max_size": self._max_size,
            }
