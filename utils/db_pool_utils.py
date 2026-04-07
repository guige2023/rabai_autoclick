"""Database connection pool utilities with context manager support."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from queue import Queue, Empty
from typing import Any, Callable, Iterator

__all__ = ["ConnectionPool", "PoolConfig", "pool_context"]


@dataclass
class PoolConfig:
    """Configuration for a connection pool."""
    min_size: int = 2
    max_size: int = 10
    max_idle_time: float = 300.0
    checkout_timeout: float = 10.0
    validate_on_checkout: bool = True


class ConnectionPool:
    """Generic database connection pool."""

    def __init__(
        self,
        connect_fn: Callable[[], Any],
        config: PoolConfig | None = None,
    ) -> None:
        self._connect_fn = connect_fn
        self._config = config or PoolConfig()
        self._pool: Queue[Any] = Queue(maxsize=self._config.max_size)
        self._all_conns: list[Any] = []
        self._size = 0
        self._lock = threading.Lock()
        self._created_at = time.monotonic()

        for _ in range(self._config.min_size):
            conn = self._new_conn()
            self._pool.put(conn)

    def _new_conn(self) -> Any:
        conn = self._connect_fn()
        with self._lock:
            self._all_conns.append(conn)
            self._size += 1
        return conn

    def _get(self) -> Any:
        deadline = time.monotonic() + self._config.checkout_timeout
        while time.monotonic() < deadline:
            try:
                conn = self._pool.get(timeout=0.1)
                if self._config.validate_on_checkout and self._is_valid(conn):
                    return conn
                self._close(conn)
            except Empty:
                with self._lock:
                    if self._size < self._config.max_size:
                        return self._new_conn()
        raise TimeoutError("Connection pool checkout timed out")

    def _return(self, conn: Any) -> None:
        if self._is_valid(conn):
            try:
                self._pool.put_nowait(conn)
            except Exception:
                self._close(conn)
        else:
            self._close(conn)

    def _is_valid(self, conn: Any) -> bool:
        try:
            if hasattr(conn, "ping"):
                conn.ping()
            return True
        except Exception:
            return False

    def _close(self, conn: Any) -> None:
        try:
            if hasattr(conn, "close"):
                conn.close()
        except Exception:
            pass
        with self._lock:
            if conn in self._all_conns:
                self._all_conns.remove(conn)
            self._size -= 1

    @contextmanager
    def connection(self) -> Iterator[Any]:
        conn = self._get()
        try:
            yield conn
        finally:
            self._return(conn)

    @contextmanager
    def transaction(self) -> Iterator[Any]:
        with self.connection() as conn:
            try:
                if hasattr(conn, "begin"):
                    conn.begin()
                yield conn
                if hasattr(conn, "commit"):
                    conn.commit()
            except Exception:
                if hasattr(conn, "rollback"):
                    conn.rollback()
                raise

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "total": self._size,
                "available": self._pool.qsize(),
                "in_use": self._size - self._pool.qsize(),
                "max_size": self._config.max_size,
                "uptime_seconds": time.monotonic() - self._created_at,
            }

    def close_all(self) -> None:
        while True:
            try:
                conn = self._pool.get_nowait()
                self._close(conn)
            except Empty:
                break
        with self._lock:
            for conn in list(self._all_conns):
                self._close(conn)
            self._all_conns.clear()
            self._size = 0


@contextmanager
def pool_context(pool: ConnectionPool) -> Iterator[Any]:
    conn = pool._get()
    try:
        yield conn
    finally:
        pool._return(conn)
