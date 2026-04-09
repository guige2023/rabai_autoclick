"""
Resource pool utilities for managing reusable object pools.

Provides generic object pools, connection pools, and pool lifecycle
management for efficient resource reuse in automation workflows.

Example:
    >>> from resource_pool_utils import ObjectPool, ConnectionPool, PoolConfig
    >>> pool = ObjectPool(factory=lambda: create_resource(), cleanup=lambda r: r.close())
    >>> resource = pool.acquire()
"""

from __future__ import annotations

import queue
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar


# =============================================================================
# Types
# =============================================================================


T = TypeVar("T")


@dataclass
class PoolConfig:
    """Configuration for a resource pool."""
    min_size: int = 0
    max_size: int = 10
    max_idle_time: float = 300.0
    max_total_time: float = 3600.0
    acquire_timeout: float = 30.0
    validation_interval: float = 60.0
    grow_on_demand: bool = True
    shrink_on_idle: bool = True


class PooledResource(Generic[T]):
    """Wrapper for a pooled resource with metadata."""

    def __init__(
        self,
        resource: T,
        created_at: float,
        last_used: float,
        use_count: int = 0,
    ):
        self.resource = resource
        self.created_at = created_at
        self.last_used = last_used
        self.use_count = use_count

    @property
    def idle_time(self) -> float:
        """Time since last use."""
        return time.monotonic() - self.last_used

    @property
    def age(self) -> float:
        """Total age of the resource."""
        return time.monotonic() - self.created_at


# =============================================================================
# Object Pool
# =============================================================================


class ObjectPool(Generic[T]):
    """
    Generic object pool for reusing expensive objects.

    Attributes:
        factory: Function to create new resources.
        cleanup: Function to destroy resources.
        config: Pool configuration.

    Example:
        >>> pool = ObjectPool(factory=lambda: DatabaseConnection(), cleanup=lambda c: c.close())
        >>> conn = pool.acquire()
        >>> # use conn...
        >>> pool.release(conn)
    """

    def __init__(
        self,
        factory: Callable[[], T],
        cleanup: Optional[Callable[[T], None]] = None,
        config: Optional[PoolConfig] = None,
    ):
        self._factory = factory
        self._cleanup = cleanup or (lambda x: None)
        self._config = config or PoolConfig()
        self._lock = threading.RLock()
        self._available: queue.Queue = queue.Queue()
        self._in_use: Dict[int, PooledResource[T]] = {}
        self._all_resources: List[PooledResource[T]] = []
        self._total_acquired: int = 0
        self._total_released: int = 0
        self._total_created: int = 0

    def acquire(self, timeout: Optional[float] = None) -> T:
        """
        Acquire a resource from the pool.

        Args:
            timeout: Maximum seconds to wait. None uses config default.

        Returns:
            A pooled resource.

        Raises:
            TimeoutError: If no resource available within timeout.
        """
        timeout = timeout or self._config.acquire_timeout
        deadline = time.monotonic() + timeout

        with self._lock:
            # Try to get from available
            while True:
                try:
                    pooled = self._available.get_nowait()
                    if self._is_valid(pooled):
                        self._mark_used(pooled)
                        return pooled.resource
                    else:
                        self._destroy(pooled)
                except queue.Empty:
                    break

                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Pool acquire timed out after {timeout}s")

            # Try to grow pool
            if self._config.grow_on_demand and len(self._all_resources) < self._config.max_size:
                pooled = self._create()
                self._mark_used(pooled)
                return pooled.resource

            # Wait for available
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Pool acquire timed out after {timeout}s")

                try:
                    pooled = self._available.get(timeout=remaining)
                    if self._is_valid(pooled):
                        self._mark_used(pooled)
                        return pooled.resource
                    else:
                        self._destroy(pooled)
                except queue.Empty:
                    raise TimeoutError(f"Pool acquire timed out after {timeout}s")

    def release(self, resource: T) -> None:
        """
        Release a resource back to the pool.

        Args:
            resource: The resource to release.
        """
        with self._lock:
            rid = id(resource)
            if rid not in self._in_use:
                return

            pooled = self._in_use.pop(rid)
            pooled.last_used = time.monotonic()

            if self._is_valid(pooled) and len(self._all_resources) < self._config.max_size:
                self._available.put(pooled)
            else:
                self._destroy(pooled)

            self._total_released += 1

    def _create(self) -> PooledResource[T]:
        """Create a new resource."""
        resource = self._factory()
        now = time.monotonic()
        pooled = PooledResource(
            resource=resource,
            created_at=now,
            last_used=now,
        )
        self._all_resources.append(pooled)
        self._total_created += 1
        return pooled

    def _mark_used(self, pooled: PooledResource[T]) -> None:
        """Mark a resource as in use."""
        pooled.use_count += 1
        pooled.last_used = time.monotonic()
        self._in_use[id(pooled.resource)] = pooled
        self._total_acquired += 1

    def _is_valid(self, pooled: PooledResource[T]) -> bool:
        """Check if a pooled resource is still valid."""
        if pooled.age > self._config.max_total_time:
            return False
        if pooled.idle_time > self._config.max_idle_time:
            return False
        return True

    def _destroy(self, pooled: PooledResource[T]) -> None:
        """Destroy a pooled resource."""
        try:
            self._cleanup(pooled.resource)
        except Exception:
            pass
        if pooled in self._all_resources:
            self._all_resources.remove(pooled)

    def prune(self) -> int:
        """
        Remove idle and expired resources from the pool.

        Returns:
            Number of resources removed.
        """
        removed = 0
        with self._lock:
            remaining: List[PooledResource[T]] = []
            while True:
                try:
                    pooled = self._available.get_nowait()
                    if not self._is_valid(pooled) or (
                        self._config.shrink_on_idle
                        and pooled.idle_time > self._config.max_idle_time * 0.5
                    ):
                        self._destroy(pooled)
                        removed += 1
                    else:
                        remaining.append(pooled)
                except queue.Empty:
                    break

            for pooled in remaining:
                self._available.put(pooled)

        return removed

    @property
    def stats(self) -> Dict[str, Any]:
        """Return pool statistics."""
        with self._lock:
            return {
                "total_resources": len(self._all_resources),
                "available": self._available.qsize(),
                "in_use": len(self._in_use),
                "total_acquired": self._total_acquired,
                "total_released": self._total_released,
                "total_created": self._total_created,
            }

    def close(self) -> None:
        """Close the pool and destroy all resources."""
        with self._lock:
            while True:
                try:
                    pooled = self._available.get_nowait()
                    self._destroy(pooled)
                except queue.Empty:
                    break

            for pooled in list(self._all_resources):
                self._destroy(pooled)

            self._all_resources.clear()
            self._in_use.clear()


# =============================================================================
# Connection Pool
# =============================================================================


@dataclass
class ConnectionStats:
    """Statistics for a pooled connection."""
    host: str
    port: int
    created_at: float
    last_used: float
    use_count: int
    is_connected: bool


class ConnectionPool:
    """
    A pool for network connections.

    Manages connection lifecycle, validation, and reconnection.

    Example:
        >>> def connect(host, port):
        ...     return socket.create_connection((host, port))
        >>> pool = ConnectionPool(connect, host="localhost", port=8080, max_size=5)
        >>> conn = pool.get_connection()
    """

    def __init__(
        self,
        connect_factory: Callable[[], Any],
        host: str,
        port: int,
        config: Optional[PoolConfig] = None,
    ):
        self._host = host
        self._port = port
        self._config = config or PoolConfig()
        self._pool: ObjectPool = ObjectPool(
            factory=connect_factory,
            config=self._config,
        )

    def get_connection(self, timeout: Optional[float] = None) -> Any:
        """Get a connection from the pool."""
        return self._pool.acquire(timeout=timeout)

    def release_connection(self, conn: Any) -> None:
        """Release a connection back to the pool."""
        self._pool.release(conn)

    def get_stats(self) -> List[ConnectionStats]:
        """Get statistics for all connections."""
        stats = []
        return stats

    def prune_connections(self) -> int:
        """Remove invalid connections from the pool."""
        return self._pool.prune()

    def close(self) -> None:
        """Close all connections and the pool."""
        self._pool.close()

    @property
    def stats(self) -> Dict[str, Any]:
        """Return pool statistics."""
        return self._pool.stats


# =============================================================================
# Pool Decorator
# =============================================================================


def pooled(
    factory: Callable[[], T],
    cleanup: Optional[Callable[[T], None]] = None,
    config: Optional[PoolConfig] = None,
) -> Callable[[Callable[[], T]], ObjectPool[T]]:
    """
    Decorator to create a pooled version of a factory function.

    Example:
        >>> @pooled(factory=lambda: create ExpensiveObject())
        >>> def get_object():
        ...     pass
        >>> obj = get_object()
    """
    _pool: Optional[ObjectPool[T]] = None

    def get_pool() -> ObjectPool[T]:
        nonlocal _pool
        if _pool is None:
            _pool = ObjectPool(factory=factory, cleanup=cleanup, config=config)
        return _pool

    return get_pool
