"""
Database Connection Pool Management Utilities.

Provides utilities for managing database connection pools, query building,
transaction handling, and read replica routing.

Author: rabai_autoclick team
Version: 1.0.0
"""

from __future__ import annotations

import sqlite3
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Generator, Optional


class DatabaseType(Enum):
    """Supported database types."""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    MONGODB = "mongodb"
    REDIS = "redis"


class PoolStatus(Enum):
    """Connection pool status."""
    ACTIVE = "active"
    EXHAUSTED = "exhausted"
    CLOSED = "closed"


@dataclass
class ConnectionConfig:
    """Database connection configuration."""
    host: str = "localhost"
    port: int = 5432
    database: str = "default"
    username: str = ""
    password: str = ""
    ssl_mode: str = "prefer"
    connect_timeout: int = 10
    application_name: str = "rabai_autoclick"


@dataclass
class PoolStats:
    """Connection pool statistics."""
    total_connections: int
    active_connections: int
    idle_connections: int
    waiting_threads: int
    max_connections: int
    min_connections: int
    checked_out_duration_ms: float
    acquire_duration_ms: float
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class QueryResult:
    """Result of a database query."""
    rows: list[dict[str, Any]]
    row_count: int
    last_insert_id: Optional[int] = None
    affected_rows: int = 0
    execution_time_ms: float = 0.0


class ConnectionWrapper:
    """Wrapper around a database connection."""

    def __init__(
        self,
        connection: sqlite3.Connection,
        pool: "ConnectionPool",
    ) -> None:
        self._conn = connection
        self._pool = pool
        self._checkout_time: Optional[float] = None
        self._closed = False

    def execute(
        self,
        query: str,
        params: Optional[tuple[Any, ...]] = None,
    ) -> sqlite3.Cursor:
        """Execute a query and return cursor."""
        if params:
            return self._conn.execute(query, params)
        return self._conn.execute(query)

    def executemany(
        self,
        query: str,
        params: Optional[list[tuple[Any, ...]]] = None,
    ) -> sqlite3.Cursor:
        """Execute a query with multiple parameter sets."""
        if params:
            return self._conn.executemany(query, params)
        return self._conn.executemany(query)

    def commit(self) -> None:
        """Commit the current transaction."""
        self._conn.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self._conn.rollback()

    def close(self) -> None:
        """Return connection to the pool."""
        if self._closed:
            return
        self._closed = True
        self._pool._return_connection(self)


class ConnectionPool:
    """Database connection pool manager."""

    def __init__(
        self,
        config: ConnectionConfig,
        db_type: DatabaseType = DatabaseType.SQLITE,
        min_connections: int = 2,
        max_connections: int = 10,
        max_idle_time_seconds: int = 300,
        connection_timeout: float = 30.0,
    ) -> None:
        self.config = config
        self.db_type = db_type
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.max_idle_time_seconds = max_idle_time_seconds
        self.connection_timeout = connection_timeout

        self._connections: list[ConnectionWrapper] = []
        self._active_connections: list[ConnectionWrapper] = []
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._closed = False
        self._stats = PoolStats(
            total_connections=0,
            active_connections=0,
            idle_connections=0,
            waiting_threads=0,
            max_connections=max_connections,
            min_connections=min_connections,
            checked_out_duration_ms=0.0,
            acquire_duration_ms=0.0,
        )

        self._initialize_pool()

    def _initialize_pool(self) -> None:
        """Initialize the connection pool with minimum connections."""
        for _ in range(self.min_connections):
            conn = self._create_connection()
            if conn:
                wrapper = ConnectionWrapper(conn, self)
                self._connections.append(wrapper)

    def _create_connection(self) -> Optional[sqlite3.Connection]:
        """Create a new database connection."""
        try:
            if self.db_type == DatabaseType.SQLITE:
                db_path = Path(self.config.database)
                if db_path.exists():
                    db_path.unlink()

                conn = sqlite3.connect(
                    self.config.database,
                    timeout=self.config.connect_timeout,
                    isolation_level=None,
                )
                conn.row_factory = sqlite3.Row
                return conn

        except Exception:
            return None

        return None

    @contextmanager
    def acquire(self) -> Generator[ConnectionWrapper, None, None]:
        """Acquire a connection from the pool."""
        start_time = time.time()

        with self._condition:
            while True:
                if self._closed:
                    raise RuntimeError("Connection pool is closed")

                idle_conns = [c for c in self._connections if c not in self._active_connections]
                if idle_conns:
                    conn = idle_conns[0]
                    self._active_connections.append(conn)
                    conn._checkout_time = start_time
                    break

                if len(self._active_connections) < self.max_connections:
                    conn = self._create_connection()
                    if conn:
                        wrapper = ConnectionWrapper(conn, self)
                        self._connections.append(wrapper)
                        self._active_connections.append(wrapper)
                        wrapper._checkout_time = start_time
                        break

                self._stats.waiting_threads += 1
                try:
                    if not self._condition.wait(timeout=self.connection_timeout):
                        raise TimeoutError("Timed out waiting for connection")
                finally:
                    self._stats.waiting_threads -= 1

        acquire_duration = (time.time() - start_time) * 1000
        self._stats.acquire_duration_ms += acquire_duration

        try:
            yield self._active_connections[-1]
        finally:
            self._return_connection(self._active_connections[-1])

    def _return_connection(self, conn: ConnectionWrapper) -> None:
        """Return a connection to the pool."""
        with self._lock:
            if conn in self._active_connections:
                self._active_connections.remove(conn)

                if conn._checkout_time:
                    duration = (time.time() - conn._checkout_time) * 1000
                    self._stats.checked_out_duration_ms += duration

                conn._checkout_time = None
                self._condition.notify()

    def get_stats(self) -> PoolStats:
        """Get current pool statistics."""
        return PoolStats(
            total_connections=len(self._connections),
            active_connections=len(self._active_connections),
            idle_connections=len(self._connections) - len(self._active_connections),
            waiting_threads=self._stats.waiting_threads,
            max_connections=self.max_connections,
            min_connections=self.min_connections,
            checked_out_duration_ms=self._stats.checked_out_duration_ms,
            acquire_duration_ms=self._stats.acquire_duration_ms,
        )

    def close(self) -> None:
        """Close all connections in the pool."""
        with self._lock:
            self._closed = True
            for conn in self._connections:
                try:
                    conn._conn.close()
                except Exception:
                    pass
            self._connections.clear()
            self._active_connections.clear()


class QueryBuilder:
    """SQL query builder for type-safe query construction."""

    def __init__(self, table: str) -> None:
        self._table = table
        self._columns: list[str] = []
        self._where_clauses: list[str] = []
        self._where_params: list[Any] = []
        self._order_by_clauses: list[str] = []
        self._group_by_clauses: list[str] = []
        self._having_clauses: list[str] = []
        self._limit_value: Optional[int] = None
        self._offset_value: Optional[int] = None
        self._join_clauses: list[str] = []
        self._join_params: list[Any] = []
        self._update_values: dict[str, Any] = {}
        self._insert_values: dict[str, Any] = {}

    def select(self, *columns: str) -> "QueryBuilder":
        """Set columns to select."""
        self._columns = list(columns) if columns else ["*"]
        return self

    def where(
        self,
        condition: str,
        *params: Any,
    ) -> "QueryBuilder":
        """Add WHERE clause."""
        self._where_clauses.append(condition)
        self._where_params.extend(params)
        return self

    def where_in(
        self,
        column: str,
        values: list[Any],
    ) -> "QueryBuilder":
        """Add WHERE IN clause."""
        placeholders = ", ".join(["?" for _ in values])
        self._where_clauses.append(f"{column} IN ({placeholders})")
        self._where_params.extend(values)
        return self

    def where_null(self, column: str) -> "QueryBuilder":
        """Add WHERE column IS NULL clause."""
        self._where_clauses.append(f"{column} IS NULL")
        return self

    def where_not_null(self, column: str) -> "QueryBuilder":
        """Add WHERE column IS NOT NULL clause."""
        self._where_clauses.append(f"{column} IS NOT NULL")
        return self

    def order_by(self, column: str, direction: str = "ASC") -> "QueryBuilder":
        """Add ORDER BY clause."""
        self._order_by_clauses.append(f"{column} {direction}")
        return self

    def group_by(self, *columns: str) -> "QueryBuilder":
        """Add GROUP BY clause."""
        self._group_by_clauses.extend(columns)
        return self

    def having(
        self,
        condition: str,
        *params: Any,
    ) -> "QueryBuilder":
        """Add HAVING clause."""
        self._having_clauses.append(condition)
        self._where_params.extend(params)
        return self

    def limit(self, value: int) -> "QueryBuilder":
        """Set LIMIT."""
        self._limit_value = value
        return self

    def offset(self, value: int) -> "QueryBuilder":
        """Set OFFSET."""
        self._offset_value = value
        return self

    def join(
        self,
        table: str,
        condition: str,
        join_type: str = "INNER",
    ) -> "QueryBuilder":
        """Add JOIN clause."""
        self._join_clauses.append(f"{join_type} JOIN {table} ON {condition}")
        return self

    def left_join(
        self,
        table: str,
        condition: str,
    ) -> "QueryBuilder":
        """Add LEFT JOIN clause."""
        return self.join(table, condition, "LEFT")

    def right_join(
        self,
        table: str,
        condition: str,
    ) -> "QueryBuilder":
        """Add RIGHT JOIN clause."""
        return self.join(table, condition, "RIGHT")

    def update(self, **values: Any) -> "QueryBuilder":
        """Set UPDATE values."""
        self._update_values.update(values)
        return self

    def insert(self, **values: Any) -> "QueryBuilder":
        """Set INSERT values."""
        self._insert_values.update(values)
        return self

    def build_select(self) -> tuple[str, list[Any]]:
        """Build SELECT query."""
        columns = ", ".join(self._columns)
        query = f"SELECT {columns} FROM {self._table}"

        if self._join_clauses:
            query += " " + " ".join(self._join_clauses)

        if self._where_clauses:
            query += " WHERE " + " AND ".join(self._where_clauses)

        if self._group_by_clauses:
            query += " GROUP BY " + ", ".join(self._group_by_clauses)

        if self._having_clauses:
            query += " HAVING " + " AND ".join(self._having_clauses)

        if self._order_by_clauses:
            query += " ORDER BY " + ", ".join(self._order_by_clauses)

        if self._limit_value is not None:
            query += f" LIMIT {self._limit_value}"

        if self._offset_value is not None:
            query += f" OFFSET {self._offset_value}"

        params = self._where_params + self._join_params
        return query, params

    def build_insert(self) -> tuple[str, list[Any]]:
        """Build INSERT query."""
        columns = ", ".join(self._insert_values.keys())
        placeholders = ", ".join(["?" for _ in self._insert_values])
        query = f"INSERT INTO {self._table} ({columns}) VALUES ({placeholders})"
        params = list(self._insert_values.values())
        return query, params

    def build_update(self) -> tuple[str, list[Any]]:
        """Build UPDATE query."""
        set_clauses = [f"{k} = ?" for k in self._update_values.keys()]
        query = f"UPDATE {self._table} SET " + ", ".join(set_clauses)

        if self._where_clauses:
            query += " WHERE " + " AND ".join(self._where_clauses)

        params = list(self._update_values.values()) + self._where_params
        return query, params

    def build_delete(self) -> tuple[str, list[Any]]:
        """Build DELETE query."""
        query = f"DELETE FROM {self._table}"

        if self._where_clauses:
            query += " WHERE " + " AND ".join(self._where_clauses)

        params = self._where_params
        return query, params


class DatabaseSession:
    """Database session with transaction support."""

    def __init__(
        self,
        pool: ConnectionPool,
        read_replica: bool = False,
    ) -> None:
        self.pool = pool
        self.read_replica = read_replica
        self._connection: Optional[ConnectionWrapper] = None
        self._in_transaction = False

    @contextmanager
    def transaction(self) -> Generator["DatabaseSession", None, None]:
        """Start a transaction context."""
        try:
            with self.pool.acquire() as conn:
                conn._conn.execute("BEGIN")
                self._connection = conn
                self._in_transaction = True
                try:
                    yield self
                    conn.commit()
                except Exception:
                    conn.rollback()
                    raise
        finally:
            self._connection = None
            self._in_transaction = False

    def execute(
        self,
        query: str,
        params: Optional[tuple[Any, ...]] = None,
    ) -> list[dict[str, Any]]:
        """Execute a query and return results."""
        with self.pool.acquire() as conn:
            start_time = time.time()

            if params:
                cursor = conn.execute(query, params)
            else:
                cursor = conn.execute(query)

            if query.strip().upper().startswith("SELECT"):
                rows = [dict(row) for row in cursor.fetchall()]
            else:
                rows = []

            execution_time = (time.time() - start_time) * 1000

            return QueryResult(
                rows=rows,
                row_count=len(rows),
                execution_time_ms=execution_time,
            ).rows

    def execute_many(
        self,
        query: str,
        params_list: list[tuple[Any, ...]],
    ) -> int:
        """Execute a query with multiple parameter sets."""
        with self.pool.acquire() as conn:
            cursor = conn.executemany(query, params_list)
            return cursor.rowcount or 0

    def close(self) -> None:
        """Close the session."""
        if self._connection:
            self._connection.close()
