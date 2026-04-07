"""Database transaction helper utilities.

Provides transaction management with automatic commit/rollback,
savepoint support, and context managers for safe database operations.

Example:
    with Transaction(conn) as tx:
        tx.execute("INSERT INTO users (name) VALUES (%s)", ("Alice",))
        tx.execute("INSERT INTO logs (user_id) VALUES (%s)", (tx.last_id(),))
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Generator, ParamSpec, TypeVar

try:
    import psycopg2
    from psycopg2 import sql
    from psycopg2.extensions import AsIs
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False

try:
    import mysql.connector
    MYSQL_AVAILABLE = True
except ImportError:
    MYSQL_AVAILABLE = False

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")


@dataclass
class TransactionStats:
    """Statistics for a transaction."""
    queries_executed: int = 0
    rows_affected: int = 0
    savepoints_created: int = 0
    started_at: float | None = None
    committed_at: float | None = None
    rolled_back_at: float | None = None


class Transaction:
    """Context manager for database transactions with automatic rollback on error.

    Supports savepoints, nested transactions, and automatic resource cleanup.
    """

    def __init__(
        self,
        connection: Any,
        *,
        autocommit: bool = False,
        isolation_level: str | None = None,
    ) -> None:
        """Initialize transaction.

        Args:
            connection: Database connection object (psycopg2/mysqldb/sqlite3).
            autocommit: If True, each statement commits immediately.
            isolation_level: Optional isolation level (READ COMMITTED, SERIALIZABLE, etc).
        """
        self.connection = connection
        self.autocommit = autocommit
        self.isolation_level = isolation_level
        self._savepoints: list[str] = []
        self._closed = False
        self.stats = TransactionStats()
        self._saved_isolation: str | None = None

    def __enter__(self) -> "Transaction":
        self.begin()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        if self._closed:
            return False
        if exc_type is not None:
            self.rollback()
            logger.debug("Transaction rolled back due to: %s", exc_val)
        else:
            self.commit()
        return False

    def begin(self) -> None:
        """Begin the transaction."""
        import time
        if self.stats.started_at is None:
            self.stats.started_at = time.time()

        if not self.autocommit and hasattr(self.connection, "autocommit"):
            self.connection.autocommit = False

        if self.isolation_level:
            try:
                self._saved_isolation = self.connection.isolation_level
                self.connection.set_isolation_level(
                    getattr(__import__("psycopg2.extensions", fromlist=["extensions"]),
                            "extensions")
                )
            except Exception:  # noqa: BLE001
                pass

    def commit(self) -> None:
        """Commit the transaction."""
        import time
        if self._closed:
            return
        try:
            self.connection.commit()
            self.stats.committed_at = time.time()
            logger.debug(
                "Transaction committed: %d queries, %d rows affected",
                self.stats.queries_executed,
                self.stats.rows_affected,
            )
        except Exception as e:
            logger.error("Commit failed: %s", e)
            raise
        finally:
            self._closed = True

    def rollback(self, savepoint: str | None = None) -> None:
        """Rollback the transaction.

        Args:
            savepoint: If provided, rollback to this savepoint only.
        """
        import time
        if self._closed:
            return
        try:
            if savepoint:
                self.connection.cursor().execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                self._savepoints.remove(savepoint)
            else:
                self.connection.rollback()
            self.stats.rolled_back_at = time.time()
            logger.debug("Transaction rolled back")
        except Exception as e:
            logger.error("Rollback failed: %s", e)
            raise
        finally:
            self._closed = True

    def execute(self, query: str, params: tuple | dict | None = None) -> Any:
        """Execute a query within the transaction.

        Args:
            query: SQL query string with %s placeholders.
            params: Query parameters.

        Returns:
            Cursor object with results.
        """
        if self._closed:
            raise RuntimeError("Transaction is closed")
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params or ())
            self.stats.queries_executed += 1
            self.stats.rows_affected += cursor.rowcount
            return cursor
        except Exception as e:
            logger.error("Query failed: %s", e)
            raise

    def execute_many(self, query: str, params_seq: Sequence[tuple]) -> None:
        """Execute a query with multiple parameter sets.

        Args:
            query: SQL query string.
            params_seq: Sequence of parameter tuples.
        """
        cursor = self.connection.cursor()
        cursor.executemany(query, params_seq)
        self.stats.queries_executed += 1
        self.stats.rows_affected += cursor.rowcount

    def last_id(self) -> int | None:
        """Get the ID of the last inserted row."""
        cursor = self.connection.cursor()
        cursor.execute("SELECT lastval()")
        result = cursor.fetchone()
        return result[0] if result else None

    def savepoint(self, name: str | None = None) -> str:
        """Create a named savepoint.

        Args:
            name: Optional savepoint name. Auto-generated if not provided.

        Returns:
            The savepoint name.
        """
        import uuid
        sp_name = name or f"sp_{uuid.uuid4().hex[:8]}"
        self.connection.cursor().execute(f"SAVEPOINT {sp_name}")
        self._savepoints.append(sp_name)
        self.stats.savepoints_created += 1
        return sp_name

    def release_savepoint(self, name: str) -> None:
        """Release a savepoint (commits work since savepoint)."""
        self.connection.cursor().execute(f"RELEASE SAVEPOINT {name}")
        self._savepoints.remove(name)

    @property
    def is_active(self) -> bool:
        """Check if transaction is still active."""
        return not self._closed


class TransactionPool:
    """Pool of reusable transaction connections with automatic lifecycle management."""

    def __init__(
        self,
        factory: Callable[[], Any],
        max_size: int = 10,
    ) -> None:
        """Initialize transaction pool.

        Args:
            factory: Callable that produces new database connections.
            max_size: Maximum number of pooled connections.
        """
        self.factory = factory
        self.max_size = max_size
        self._pool: list[Any] = []
        self._in_use: set[int] = set()

    @contextmanager
    def connection(self) -> Generator[Any, None, None]:
        """Get a connection from the pool."""
        conn = self._acquire()
        tx = Transaction(conn)
        try:
            yield tx
        finally:
            self._release(conn)

    def _acquire(self) -> Any:
        """Acquire a connection from the pool."""
        import threading
        thread_id = threading.current_thread().ident
        if self._pool:
            conn = self._pool.pop()
            self._in_use.add(id(conn))
            return conn
        conn = self.factory()
        self._in_use.add(id(conn))
        return conn

    def _release(self, conn: Any) -> None:
        """Return a connection to the pool."""
        import threading
        thread_id = threading.current_thread().ident
        conn_id = id(conn)
        if conn_id in self._in_use:
            self._in_use.remove(conn_id)
        if len(self._pool) < self.max_size:
            try:
                conn.rollback()
            except Exception:  # noqa: BLE001
                pass
            self._pool.append(conn)
        else:
            try:
                conn.close()
            except Exception:  # noqa: BLE001
                pass


@contextmanager
def atomic(connection: Any) -> Generator[Transaction, None, None]:
    """Shorthand context manager for transactions.

    Example:
        with atomic(conn) as tx:
            tx.execute("DELETE FROM old_records WHERE created_at < %s", (cutoff,))
    """
    with Transaction(connection) as tx:
        yield tx


def with_transaction(
    conn: Any,
    *,
    retries: int = 3,
    isolation_level: str | None = None,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    """Decorator that wraps a function in a transaction.

    Example:
        @with_transaction(conn)
        def transfer_funds(from_id: int, to_id: int, amount: float) -> bool:
            tx.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", (amount, from_id))
            tx.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", (amount, to_id))
            return True
    """
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            last_error: Exception | None = None
            for attempt in range(retries):
                try:
                    with Transaction(conn, isolation_level=isolation_level) as tx:
                        return func(*args, _tx=tx, **kwargs)
                except Exception as e:
                    last_error = e
                    if attempt < retries - 1:
                        logger.warning("Transaction retry %d/%d: %s", attempt + 1, retries, e)
            raise last_error from None
        return wrapper
    return decorator
