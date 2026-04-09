"""API request pooling and connection management action."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable, Coroutine, Optional


@dataclass
class PoolConfig:
    """Configuration for request pool."""

    max_size: int = 10
    min_size: int = 2
    max_idle_seconds: float = 60.0
    acquire_timeout_seconds: float = 30.0
    validation_interval_seconds: float = 30.0


@dataclass
class PooledConnection:
    """A pooled connection wrapper."""

    connection_id: str
    created_at: datetime
    last_used: datetime
    is_valid: bool = True
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PoolStats:
    """Pool statistics."""

    total_connections: int
    active_connections: int
    idle_connections: int
    waiting_requests: int
    total_acquires: int
    total_releases: int
    total_errors: int


class APIRequestPoolAction:
    """Manages a pool of reusable connections for API requests."""

    def __init__(
        self,
        config: Optional[PoolConfig] = None,
        factory: Optional[Callable[[], Coroutine[Any, Any, Any]]] = None,
        validator: Optional[Callable[[Any], Coroutine[Any, Any, bool]]] = None,
        cleanup: Optional[Callable[[Any], Coroutine[Any, Any, None]]] = None,
    ):
        """Initialize request pool.

        Args:
            config: Pool configuration.
            factory: Async factory to create new connections.
            validator: Async callable to validate connections.
            cleanup: Async callable to cleanup connections.
        """
        self._config = config or PoolConfig()
        self._factory = factory
        self._validator = validator
        self._cleanup = cleanup
        self._pool: list[PooledConnection] = []
        self._available: asyncio.Queue = asyncio.Queue()
        self._active: dict[str, PooledConnection] = {}
        self._wait_queue: asyncio.Queue = asyncio.Queue()
        self._lock = asyncio.Lock()
        self._total_acquires: int = 0
        self._total_releases: int = 0
        self._total_errors: int = 0
        self._connection_counter: int = 0
        self._closed: bool = False

    async def _create_connection(self) -> tuple[str, Any]:
        """Create a new connection."""
        self._connection_counter += 1
        conn_id = f"conn_{self._connection_counter}"
        conn = None

        if self._factory:
            conn = await self._factory()

        pooled = PooledConnection(
            connection_id=conn_id,
            created_at=datetime.now(),
            last_used=datetime.now(),
            metadata={"connection": conn} if conn else {},
        )

        return conn_id, pooled

    async def acquire(self) -> tuple[str, Any]:
        """Acquire a connection from the pool.

        Returns:
            Tuple of (connection_id, connection).
        """
        if self._closed:
            raise RuntimeError("Pool is closed")

        self._total_acquires += 1
        acquired = False

        try:
            conn_id, pooled = await asyncio.wait_for(
                self._available.get(),
                timeout=self._config.acquire_timeout_seconds,
            )
            acquired = True

            if self._validator and conn_id in self._active:
                is_valid = await self._validator(pooled.metadata.get("connection"))
                if not is_valid:
                    await self._remove_connection(conn_id)
                    return await self.acquire()

            pooled.last_used = datetime.now()
            self._active[conn_id] = pooled

            return conn_id, pooled.metadata.get("connection")

        except asyncio.TimeoutError:
            if len(self._pool) < self._config.max_size:
                async with self._lock:
                    conn_id, pooled = await self._create_connection()
                    self._pool.append(pooled)
                    self._active[conn_id] = pooled
                    return conn_id, pooled.metadata.get("connection")

            raise TimeoutError(
                f"Could not acquire connection within {self._config.acquire_timeout_seconds}s"
            )

    async def release(self, conn_id: str) -> None:
        """Release a connection back to the pool."""
        if self._closed or conn_id not in self._active:
            return

        self._total_releases += 1
        pooled = self._active.pop(conn_id)

        if pooled.is_valid and len(self._pool) < self._config.max_size:
            await self._available.put((conn_id, pooled))
        else:
            await self._remove_connection(conn_id)

    async def _remove_connection(self, conn_id: str) -> None:
        """Remove a connection from the pool."""
        pooled = self._pool.find(lambda p: p.connection_id == conn_id)
        if pooled:
            self._pool.remove(pooled)

        if self._cleanup and pooled:
            try:
                await self._cleanup(pooled.metadata.get("connection"))
            except Exception:
                pass

    async def prewarm(self, count: Optional[int] = None) -> None:
        """Prewarm the pool with connections.

        Args:
            count: Number of connections to create.
        """
        count = count or self._config.min_size
        async with self._lock:
            for _ in range(count):
                conn_id, pooled = await self._create_connection()
                self._pool.append(pooled)
                await self._available.put((conn_id, pooled))

    async def close(self) -> None:
        """Close the pool and all connections."""
        self._closed = True
        async with self._lock:
            for pooled in self._pool[:]:
                if self._cleanup:
                    try:
                        await self._cleanup(pooled.metadata.get("connection"))
                    except Exception:
                        pass
            self._pool.clear()
            self._active.clear()

        while not self._available.empty():
            try:
                self._available.get_nowait()
            except asyncio.QueueEmpty:
                break

    def get_stats(self) -> PoolStats:
        """Get pool statistics."""
        return PoolStats(
            total_connections=len(self._pool),
            active_connections=len(self._active),
            idle_connections=len(self._pool) - len(self._active),
            waiting_requests=self._wait_queue.qsize(),
            total_acquires=self._total_acquires,
            total_releases=self._total_releases,
            total_errors=self._total_errors,
        )

    async def __aenter__(self) -> "APIRequestPoolAction":
        """Async context manager entry."""
        await self.prewarm()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()
