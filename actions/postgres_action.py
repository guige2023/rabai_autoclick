"""PostgreSQL integration for relational database operations.

Handles PostgreSQL operations including query execution,
transactions, stored procedures, and advanced data types.
"""

from typing import Any, Optional
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import json

try:
    import psycopg2
    from psycopg2 import sql, errors
    from psycopg2.extras import RealDictCursor, Json, execute_values
except ImportError:
    psycopg2 = None
    RealDictCursor = None

logger = logging.getLogger(__name__)


class IsolationLevel(Enum):
    """Transaction isolation levels."""
    READ_COMMITTED = "READ COMMITTED"
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"


@dataclass
class PostgresConfig:
    """Configuration for PostgreSQL connection."""
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = ""
    ssl_mode: str = "prefer"
    connect_timeout: int = 10


@dataclass
class PostgresQuery:
    """Query specification for PostgreSQL operations."""
    query: str
    params: Optional[tuple] = None
    fetch_size: int = 1000


@dataclass
class PostgresResult:
    """Result from a query execution."""
    rows: list[dict]
    row_count: int
    last_insert_id: Optional[int] = None


class PostgresAPIError(Exception):
    """Raised when PostgreSQL operations fail."""
    def __init__(self, message: str, sqlstate: Optional[str] = None):
        super().__init__(message)
        self.sqlstate = sqlstate


class PostgresAction:
    """PostgreSQL client for relational database operations."""

    def __init__(self, config: PostgresConfig):
        """Initialize PostgreSQL client with configuration.

        Args:
            config: PostgresConfig with connection parameters

        Raises:
            ImportError: If psycopg2 is not installed
        """
        if psycopg2 is None:
            raise ImportError("psycopg2 required: pip install psycopg2-binary")

        self.config = config
        self._conn = None

    def connect(self) -> None:
        """Establish connection to PostgreSQL.

        Raises:
            PostgresAPIError: On connection failure
        """
        try:
            self._conn = psycopg2.connect(
                host=self.config.host,
                port=self.config.port,
                dbname=self.config.database,
                user=self.config.user,
                password=self.config.password,
                sslmode=self.config.ssl_mode,
                connect_timeout=self.config.connect_timeout
            )
            logger.info(f"Connected to PostgreSQL: {self.config.database}")

        except psycopg2.Error as e:
            raise PostgresAPIError(f"Connection failed: {e}")

    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
            logger.info("Disconnected from PostgreSQL")

    @property
    def conn(self):
        """Get connection, connect if needed."""
        if self._conn is None or self._conn.closed:
            self.connect()
        return self._conn

    def execute(self, query: PostgresQuery) -> PostgresResult:
        """Execute a query and return results.

        Args:
            query: PostgresQuery with SQL and optional parameters

        Returns:
            PostgresResult with rows and metadata

        Raises:
            PostgresAPIError: On query execution failure
        """
        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query.query, query.params)

                rows = []
                if cur.description:
                    rows = [dict(row) for row in cur.fetchmany(query.fetch_size)]

                    while True:
                        batch = cur.fetchmany(query.fetch_size)
                        if not batch:
                            break
                        rows.extend(dict(row) for row in batch)

                return PostgresResult(
                    rows=rows,
                    row_count=len(rows),
                    last_insert_id=self._get_lastInsert_id(cur)
                )

        except psycopg2.Error as e:
            raise PostgresAPIError(f"Query failed: {e}", sqlstate=e.pgcode)

    def execute_many(self, query: str, params_list: list[tuple]) -> int:
        """Execute a query multiple times with different parameters.

        Args:
            query: SQL query with placeholders
            params_list: List of parameter tuples

        Returns:
            Number of affected rows
        """
        try:
            with self.conn.cursor() as cur:
                cur.executemany(query, params_list)
                return cur.rowcount

        except psycopg2.Error as e:
            raise PostgresAPIError(f"Execute many failed: {e}", sqlstate=e.pgcode)

    def execute_values(self, query: str, values: list[tuple],
                      fetch: bool = True) -> Optional[list]:
        """Execute query with multiple value sets efficiently.

        Uses PostgreSQL COPY protocol for high performance inserts/updates.

        Args:
            query: SQL query with placeholders
            values: List of value tuples
            fetch: Whether to return results

        Returns:
            List of results if fetch=True, else None
        """
        try:
            with self.conn.cursor() as cur:
                result = execute_values(
                    cur,
                    query,
                    values,
                    fetch=fetch
                )
                return result

        except psycopg2.Error as e:
            raise PostgresAPIError(f"Execute values failed: {e}", sqlstate=e.pgcode)

    def begin(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED) -> None:
        """Begin a transaction.

        Args:
            isolation_level: Transaction isolation level
        """
        self.conn.isolation_level = isolation_level.value

    def commit(self) -> None:
        """Commit the current transaction."""
        self.conn.commit()

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.conn.rollback()

    def transaction(self, isolation_level: IsolationLevel = IsolationLevel.READ_COMMITTED):
        """Context manager for transactions.

        Usage:
            with pg.transaction():
                pg.execute(query1)
                pg.execute(query2)
        """
        return TransactionContext(self, isolation_level)

    def insert(self, table: str, data: dict, returning: bool = True) -> Any:
        """Insert a row into a table.

        Args:
            table: Table name
            data: Column-value dict
            returning: Whether to return inserted row

        Returns:
            Inserted row dict if returning=True, else None
        """
        columns = list(data.keys())
        values = list(data.values())
        placeholders = [f"%({col})s" for col in columns]

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
        """

        if returning:
            query += " RETURNING *"
            result = self.execute(PostgresQuery(query, data))
            return result.rows[0] if result.rows else None
        else:
            query += " RETURNING 1"
            self.execute(PostgresQuery(query, data))
            return None

    def update(self, table: str, data: dict, where: dict,
               returning: bool = True) -> Optional[list[dict]]:
        """Update rows in a table.

        Args:
            table: Table name
            data: Column-value dict of new values
            where: WHERE condition dict
            returning: Whether to return updated rows

        Returns:
            List of updated rows if returning=True
        """
        set_clause = ", ".join([f"{k} = %({k})s" for k in data.keys()])
        where_clause, where_values = self._build_where(where)

        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"

        if returning:
            query += " RETURNING *"
            params = {**data, **where_values}
            result = self.execute(PostgresQuery(query, params))
            return result.rows
        else:
            query += " RETURNING 1"
            params = {**data, **where_values}
            self.execute(PostgresQuery(query, params))
            return None

    def delete(self, table: str, where: dict,
               returning: bool = True) -> Optional[list[dict]]:
        """Delete rows from a table.

        Args:
            table: Table name
            where: WHERE condition dict
            returning: Whether to return deleted rows

        Returns:
            List of deleted rows if returning=True
        """
        where_clause, where_values = self._build_where(where)

        query = f"DELETE FROM {table} WHERE {where_clause}"

        if returning:
            query += " RETURNING *"
            result = self.execute(PostgresQuery(query, where_values))
            return result.rows
        else:
            self.execute(PostgresQuery(query, where_values))
            return None

    def upsert(self, table: str, data: dict, conflict_keys: list[str],
               update_columns: Optional[list[str]] = None) -> dict:
        """Insert or update a row on conflict.

        Args:
            table: Table name
            data: Column-value dict
            conflict_keys: Keys to check for conflict
            update_columns: Columns to update on conflict (all if None)

        Returns:
            Upserted row dict
        """
        columns = list(data.keys())
        values = list(data.values())
        placeholders = [f"%({col})s" for col in columns]

        query = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({', '.join(placeholders)})
            ON CONFLICT ({', '.join(conflict_keys)})
        """

        if update_columns:
            set_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in update_columns])
        else:
            set_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in columns if k not in conflict_keys])

        query += f" DO UPDATE SET {set_clause}"
        query += " RETURNING *"

        result = self.execute(PostgresQuery(query, data))
        return result.rows[0] if result.rows else {}

    def select(self, table: str, columns: Optional[list[str]] = None,
               where: Optional[dict] = None, order_by: Optional[dict] = None,
               limit: Optional[int] = None, offset: Optional[int] = None) -> list[dict]:
        """Select rows from a table.

        Args:
            table: Table name
            columns: Columns to select (* if None)
            where: WHERE condition dict
            order_by: Dict of column -> asc/desc
            limit: Maximum rows
            offset: Skip rows

        Returns:
            List of row dicts
        """
        col_clause = ", ".join(columns) if columns else "*"
        query = f"SELECT {col_clause} FROM {table}"

        params = {}
        if where:
            where_clause, where_params = self._build_where(where)
            query += f" WHERE {where_clause}"
            params.update(where_params)

        if order_by:
            order_parts = [f"{col} {direction}" for col, direction in order_by.items()]
            query += f" ORDER BY {', '.join(order_parts)}"

        if limit:
            query += f" LIMIT %(limit)s"
            params["limit"] = limit

        if offset:
            query += f" OFFSET %(offset)s"
            params["offset"] = offset

        return self.execute(PostgresQuery(query, params if params else None)).rows

    def call_proc(self, proc_name: str, args: Optional[tuple] = None) -> Any:
        """Call a stored procedure.

        Args:
            proc_name: Procedure name (can include schema.proc)
            args: Procedure arguments

        Returns:
            Procedure result
        """
        try:
            with self.conn.cursor() as cur:
                cur.callproc(proc_name, args or ())

                if cur.description:
                    return [dict(row) for row in cur.fetchall()]

                return cur.rowcount

        except psycopg2.Error as e:
            raise PostgresAPIError(f"Call proc failed: {e}", sqlstate=e.pgcode)

    def get_tables(self, schema: str = "public") -> list[str]:
        """List all tables in a schema.

        Args:
            schema: Schema name

        Returns:
            List of table names
        """
        query = """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_name
        """
        result = self.execute(PostgresQuery(query, (schema,)))
        return [row["table_name"] for row in result.rows]

    def get_columns(self, table: str, schema: str = "public") -> list[dict]:
        """Get column information for a table.

        Args:
            table: Table name
            schema: Schema name

        Returns:
            List of column info dicts
        """
        query = """
            SELECT
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        result = self.execute(PostgresQuery(query, (schema, table)))
        return result.rows

    def _build_where(self, conditions: dict) -> tuple[str, dict]:
        """Build WHERE clause from dict."""
        clauses = []
        params = {}

        for i, (key, value) in enumerate(conditions.items()):
            if isinstance(value, (list, tuple)):
                clauses.append(f"{key} = %({key})s")
                params[key] = value[0]
            elif value is None:
                clauses.append(f"{key} IS NULL")
            else:
                clauses.append(f"{key} = %({key})s")
                params[key] = value

        where_clause = " AND ".join(clauses) if clauses else "1=1"
        return where_clause, params

    def _get_lastInsert_id(self, cur) -> Optional[int]:
        """Get last insert ID from cursor."""
        try:
            if cur.description:
                row = cur.fetchone()
                if row:
                    return row[0] if isinstance(row, tuple) else row.get(cur.description[0].name)
        except:
            pass
        return None
