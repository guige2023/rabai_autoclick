"""MySQL integration for relational database operations.

Handles MySQL operations including query execution,
transactions, stored procedures, and connection pooling.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

try:
    import mysql.connector
    from mysql.connector import pooling, Error as MySQLError
except ImportError:
    mysql = None
    pooling = None
    MySQLError = Exception

logger = logging.getLogger(__name__)


class IsolationLevel(Enum):
    """Transaction isolation levels for MySQL."""
    READ_COMMITTED = "READ COMMITTED"
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"


@dataclass
class MySQLConfig:
    """Configuration for MySQL connection/pool."""
    host: str = "localhost"
    port: int = 3306
    database: str = "mysql"
    user: str = "root"
    password: str = ""
    pool_name: str = "default_pool"
    pool_size: int = 5
    pool_reset_session: bool = True
    connect_timeout: int = 10
    autocommit: bool = False


@dataclass
class MySQLQuery:
    """Query specification for MySQL operations."""
    query: str
    params: Optional[tuple] = None
    dictionary: bool = True


@dataclass
class MySQLResult:
    """Result from a query execution."""
    rows: list[dict]
    row_count: int
    last_insert_id: int
    affected_rows: int


class MySQLAPIError(Exception):
    """Raised when MySQL operations fail."""
    def __init__(self, message: str, errno: Optional[int] = None):
        super().__init__(message)
        self.errno = errno


class MySQLAction:
    """MySQL client for relational database operations."""

    def __init__(self, config: MySQLConfig):
        """Initialize MySQL client with configuration.

        Args:
            config: MySQLConfig with connection parameters

        Raises:
            ImportError: If mysql-connector-python is not installed
        """
        if mysql is None:
            raise ImportError("mysql-connector-python required: pip install mysql-connector-python")

        self.config = config
        self._pool = None
        self._conn = None

    def connect(self) -> None:
        """Establish connection to MySQL.

        Raises:
            MySQLAPIError: On connection failure
        """
        try:
            self._conn = mysql.connector.connect(
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                connection_timeout=self.config.connect_timeout,
                autocommit=self.config.autocommit
            )
            logger.info(f"Connected to MySQL: {self.config.database}")

        except MySQLError as e:
            raise MySQLAPIError(f"Connection failed: {e}", errno=e.errno)

    def connect_pool(self) -> None:
        """Create a connection pool.

        Raises:
            MySQLAPIError: On pool creation failure
        """
        try:
            self._pool = pooling.MySQLConnectionPool(
                pool_name=self.config.pool_name,
                pool_size=self.config.pool_size,
                pool_reset_session=self.config.pool_reset_session,
                host=self.config.host,
                port=self.config.port,
                database=self.config.database,
                user=self.config.user,
                password=self.config.password,
                connection_timeout=self.config.connect_timeout
            )
            logger.info(f"Created MySQL pool: {self.config.pool_name}")

        except MySQLError as e:
            raise MySQLAPIError(f"Pool creation failed: {e}", errno=e.errno)

    def get_connection(self):
        """Get a connection from pool or direct connection.

        Returns:
            MySQL connection object
        """
        if self._pool:
            return self._pool.get_connection()
        elif self._conn is None or not self._conn.is_connected():
            self.connect()
            return self._conn
        return self._conn

    def disconnect(self) -> None:
        """Close MySQL connection/pool."""
        if self._conn and self._conn.is_connected():
            self._conn.close()
            self._conn = None
        logger.info("Disconnected from MySQL")

    def execute(self, query: MySQLQuery) -> MySQLResult:
        """Execute a query and return results.

        Args:
            query: MySQLQuery with SQL and optional parameters

        Returns:
            MySQLResult with rows and metadata

        Raises:
            MySQLAPIError: On query execution failure
        """
        conn = self.get_connection()
        cursor = None

        try:
            cursor = conn.cursor(dictionary=query.dictionary)
            cursor.execute(query.query, query.params or ())

            rows = []
            if cursor.description:
                rows = cursor.fetchall()

            last_id = cursor.lastrowid if cursor.lastrowid else 0
            affected = cursor.rowcount

            return MySQLResult(
                rows=rows,
                row_count=len(rows),
                last_insert_id=last_id,
                affected_rows=affected
            )

        except MySQLError as e:
            raise MySQLAPIError(f"Query failed: {e}", errno=e.errno)

        finally:
            if cursor:
                cursor.close()

    def execute_many(self, query: str, params_list: list[tuple]) -> int:
        """Execute a query multiple times with different parameters.

        Args:
            query: SQL query with placeholders
            params_list: List of parameter tuples

        Returns:
            Number of affected rows
        """
        conn = self.get_connection()
        cursor = None

        try:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount

        except MySQLError as e:
            conn.rollback()
            raise MySQLAPIError(f"Execute many failed: {e}", errno=e.errno)

        finally:
            if cursor:
                cursor.close()

    def begin(self) -> None:
        """Begin a transaction."""
        conn = self.get_connection()
        if conn.autocommit:
            conn.autocommit = False

    def commit(self) -> None:
        """Commit the current transaction."""
        if self._conn:
            self._conn.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        if self._conn:
            self._conn.rollback()

    def transaction(self):
        """Context manager for transactions."""
        return TransactionContext(self)

    def insert(self, table: str, data: dict, returning: bool = True) -> Any:
        """Insert a row into a table.

        Args:
            table: Table name
            data: Column-value dict
            returning: Whether to return inserted row (MySQL uses LAST_INSERT_ID)

        Returns:
            Last insert ID if returning=True
        """
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ["%s"] * len(columns)

        query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        result = self.execute(MySQLQuery(query, tuple(values)))

        if returning:
            return result.last_insert_id

        return None

    def update(self, table: str, data: dict, where: dict) -> int:
        """Update rows in a table.

        Args:
            table: Table name
            data: Column-value dict of new values
            where: WHERE condition dict

        Returns:
            Number of affected rows
        """
        set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
        where_clause, where_values = self._build_where(where)

        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        params = list(data.values()) + list(where_values.values())

        result = self.execute(MySQLQuery(query, tuple(params)))
        return result.affected_rows

    def delete(self, table: str, where: dict) -> int:
        """Delete rows from a table.

        Args:
            table: Table name
            where: WHERE condition dict

        Returns:
            Number of affected rows
        """
        where_clause, where_values = self._build_where(where)

        query = f"DELETE FROM {table} WHERE {where_clause}"

        result = self.execute(MySQLQuery(query, tuple(where_values.values())))
        return result.affected_rows

    def upsert(self, table: str, data: dict,
               conflict_columns: list[str]) -> int:
        """Insert or update a row on conflict (REPLACE INTO).

        Args:
            table: Table name
            data: Column-value dict
            conflict_columns: Columns to check for conflict

        Returns:
            Last insert ID
        """
        columns = list(data.keys())
        values = list(data.values())
        placeholders = ["%s"] * len(columns)

        query = f"REPLACE INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

        result = self.execute(MySQLQuery(query, tuple(values)))
        return result.last_insert_id

    def select(self, table: str, columns: Optional[list[str]] = None,
               where: Optional[dict] = None, order_by: Optional[dict] = None,
               limit: Optional[int] = None, offset: Optional[int] = None) -> list[dict]:
        """Select rows from a table.

        Args:
            table: Table name
            columns: Columns to select (* if None)
            where: WHERE condition dict
            order_by: Dict of column -> ASC/DESC
            limit: Maximum rows
            offset: Skip rows

        Returns:
            List of row dicts
        """
        col_clause = ", ".join(columns) if columns else "*"
        query = f"SELECT {col_clause} FROM {table}"

        params = []

        if where:
            where_clause, where_params = self._build_where(where)
            query += f" WHERE {where_clause}"
            params.extend(where_params.values())

        if order_by:
            order_parts = []
            for col, direction in order_by.items():
                direction = direction.upper() if isinstance(direction, str) else "ASC"
                order_parts.append(f"{col} {direction}")
            query += f" ORDER BY {', '.join(order_parts)}"

        if limit:
            query += f" LIMIT %s"
            params.append(limit)

        if offset:
            query += f" OFFSET %s"
            params.append(offset)

        result = self.execute(MySQLQuery(query, tuple(params) if params else None))
        return result.rows

    def call_proc(self, proc_name: str, args: Optional[tuple] = None) -> Any:
        """Call a stored procedure.

        Args:
            proc_name: Procedure name
            args: Procedure arguments

        Returns:
            Procedure result
        """
        conn = self.get_connection()
        cursor = None

        try:
            cursor = conn.cursor(dictionary=True)
            cursor.callproc(proc_name, args or ())

            results = []
            for result in cursor.stored_results():
                results.extend(result.fetchall())

            return results

        except MySQLError as e:
            raise MySQLAPIError(f"Call proc failed: {e}", errno=e.errno)

        finally:
            if cursor:
                cursor.close()

    def get_tables(self) -> list[str]:
        """List all tables in current database.

        Returns:
            List of table names
        """
        query = "SHOW TABLES"
        result = self.execute(MySQLQuery(query))
        if result.rows:
            key = list(result.rows[0].keys())[0]
            return [row[key] for row in result.rows]
        return []

    def get_columns(self, table: str) -> list[dict]:
        """Get column information for a table.

        Args:
            table: Table name

        Returns:
            List of column info dicts
        """
        query = f"SHOW FULL COLUMNS FROM {table}"
        return self.execute(MySQLQuery(query)).rows

    def _build_where(self, conditions: dict) -> tuple[str, dict]:
        """Build WHERE clause from dict."""
        clauses = []
        params = {}

        for key, value in conditions.items():
            if isinstance(value, (list, tuple)):
                placeholders = ", ".join(["%s"] * len(value))
                clauses.append(f"{key} IN ({placeholders})")
                params[key] = value
            elif value is None:
                clauses.append(f"{key} IS NULL")
            else:
                clauses.append(f"{key} = %s")
                params[key] = value

        where_clause = " AND ".join(clauses) if clauses else "1=1"
        return where_clause, params


class TransactionContext:
    """Context manager for MySQL transactions."""

    def __init__(self, mysql_action: MySQLAction):
        self.mysql = mysql_action

    def __enter__(self):
        self.mysql.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.mysql.rollback()
        else:
            self.mysql.commit()
        return False
