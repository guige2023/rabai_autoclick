"""Database action module for RabAI AutoClick.

Provides unified database operations for multiple database types
including SQLite, MySQL, PostgreSQL, and MongoDB.
"""

import os
import sys
import time
import json
import sqlite3
from typing import Any, Dict, List, Optional, Union, Tuple, Type
from contextlib import contextmanager
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DatabaseError(Exception):
    """Custom exception for database errors."""
    pass


class ConnectionPool:
    """Generic database connection pool.
    
    Provides connection management and pooling for database operations.
    """
    
    def __init__(
        self,
        db_type: str,
        **kwargs: Any
    ) -> None:
        """Initialize connection pool.
        
        Args:
            db_type: Type of database ('sqlite', 'mysql', 'postgres', 'mongodb').
            **kwargs: Database connection parameters.
        """
        self.db_type = db_type
        self.kwargs = kwargs
        self._connections: List[Any] = []
        self._max_connections = kwargs.get("max_connections", 5)
        self._in_use: List[Any] = []
    
    def _create_sqlite_connection(self) -> sqlite3.Connection:
        """Create a SQLite connection."""
        db_path = self.kwargs.get("database", ":memory:")
        timeout = self.kwargs.get("timeout", 5.0)
        
        conn = sqlite3.connect(db_path, timeout=timeout)
        conn.row_factory = sqlite3.Row
        
        if self.kwargs.get("isolation_level") is None:
            conn.isolation_level = None
        
        return conn
    
    def _create_mysql_connection(self) -> Any:
        """Create a MySQL connection."""
        try:
            import pymysql
        except ImportError:
            raise ImportError(
                "pymysql is required for MySQL support. Install with: pip install pymysql"
            )
        
        return pymysql.connect(
            host=self.kwargs.get("host", "localhost"),
            port=self.kwargs.get("port", 3306),
            user=self.kwargs.get("user", "root"),
            password=self.kwargs.get("password", ""),
            database=self.kwargs.get("database", ""),
            charset=self.kwargs.get("charset", "utf8mb4"),
            cursorclass=pymysql.cursors.DictCursor
        )
    
    def _create_postgres_connection(self) -> Any:
        """Create a PostgreSQL connection."""
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "psycopg2 is required for PostgreSQL support. Install with: pip install psycopg2"
            )
        
        return psycopg2.connect(
            host=self.kwargs.get("host", "localhost"),
            port=self.kwargs.get("port", 5432),
            user=self.kwargs.get("user", "postgres"),
            password=self.kwargs.get("password", ""),
            database=self.kwargs.get("database", ""),
            cursor_factory=psycopg2.extras.RealDictCursor
        )
    
    def _create_connection(self) -> Any:
        """Create a new database connection based on db_type."""
        if self.db_type == "sqlite":
            return self._create_sqlite_connection()
        elif self.db_type == "mysql":
            return self._create_mysql_connection()
        elif self.db_type == "postgres":
            return self._create_postgres_connection()
        else:
            raise DatabaseError(f"Unsupported database type: {self.db_type}")
    
    @contextmanager
    def get_connection(self) -> Any:
        """Get a connection from the pool (context manager).
        
        Yields:
            A database connection.
        """
        conn = None
        
        try:
            if self._connections:
                conn = self._connections.pop()
            elif len(self._in_use) < self._max_connections:
                conn = self._create_connection()
            
            if conn is None:
                raise DatabaseError("Connection pool exhausted")
            
            self._in_use.append(conn)
            
            try:
                yield conn
            finally:
                self._in_use.remove(conn)
                self._connections.append(conn)
        
        except Exception:
            if conn and conn in self._in_use:
                self._in_use.remove(conn)
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            raise
    
    def close_all(self) -> None:
        """Close all connections in the pool."""
        for conn in self._connections + self._in_use:
            try:
                conn.close()
            except Exception:
                pass
        self._connections.clear()
        self._in_use.clear()


class SQLiteManager:
    """SQLite database manager with helper methods.
    
    Provides convenient methods for common SQLite operations.
    """
    
    def __init__(self, database: str = ":memory:", timeout: float = 5.0) -> None:
        """Initialize SQLite manager.
        
        Args:
            database: Path to the SQLite database or ':memory:'.
            timeout: Connection timeout in seconds.
        """
        self.database = database
        self.timeout = timeout
    
    @contextmanager
    def connect(self) -> sqlite3.Connection:
        """Context manager for database connections."""
        conn = sqlite3.connect(self.database, timeout=self.timeout)
        conn.row_factory = sqlite3.Row
        
        try:
            yield conn
        finally:
            conn.close()
    
    def execute(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results.
        
        Args:
            query: SQL query to execute.
            params: Optional query parameters.
            
        Returns:
            List of result rows as dictionaries.
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            if query.strip().upper().startswith(("SELECT", "PRAGMA")):
                rows = cursor.fetchall()
                return [dict(row) for row in rows]
            
            conn.commit()
            return [{"last_row_id": cursor.lastrowid, "rows_affected": cursor.rowcount}]
    
    def execute_many(
        self,
        query: str,
        params_list: List[Tuple[Any, ...]]
    ) -> int:
        """Execute a query with multiple parameter sets.
        
        Args:
            query: SQL query to execute.
            params_list: List of parameter tuples.
            
        Returns:
            Number of rows affected.
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.executemany(query, params_list)
            conn.commit()
            return cursor.rowcount
    
    def create_table(
        self,
        table_name: str,
        columns: Dict[str, str],
        if_not_exists: bool = True
    ) -> bool:
        """Create a table.
        
        Args:
            table_name: Name of the table to create.
            columns: Dictionary mapping column names to types/constraints.
            if_not_exists: Add IF NOT EXISTS clause.
            
        Returns:
            True if successful.
        """
        column_defs = [f"{name} {dtype}" for name, dtype in columns.items()]
        query = f"CREATE TABLE {'IF NOT EXISTS ' if if_not_exists else ''}{table_name} ({', '.join(column_defs)})"
        
        self.execute(query)
        return True
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.
        
        Args:
            table_name: Name of the table to check.
            
        Returns:
            True if the table exists.
        """
        query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
        results = self.execute(query, (table_name,))
        return len(results) > 0
    
    def get_tables(self) -> List[str]:
        """Get list of all table names.
        
        Returns:
            List of table names.
        """
        query = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        results = self.execute(query)
        return [row["name"] for row in results]
    
    def describe_table(self, table_name: str) -> List[Dict[str, Any]]:
        """Get table schema information.
        
        Args:
            table_name: Name of the table.
            
        Returns:
            List of column definitions.
        """
        query = f"PRAGMA table_info({table_name})"
        return self.execute(query)


class DatabaseAction(BaseAction):
    """Database action for executing queries and managing databases.
    
    Supports SQLite (built-in), MySQL, and PostgreSQL.
    """
    action_type: str = "database"
    display_name: str = "数据库动作"
    description: str = "执行SQL查询和数据库操作，支持SQLite、MySQL、PostgreSQL"
    
    def __init__(self) -> None:
        super().__init__()
        self._pool: Optional[ConnectionPool] = None
        self._sqlite: Optional[SQLiteManager] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute database operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "query")
            
            if operation == "connect":
                return self._connect(params, start_time)
            elif operation == "disconnect":
                return self._disconnect(start_time)
            elif operation == "query":
                return self._query(params, start_time)
            elif operation == "execute":
                return self._execute(params, start_time)
            elif operation == "create_table":
                return self._create_table(params, start_time)
            elif operation == "list_tables":
                return self._list_tables(params, start_time)
            elif operation == "table_exists":
                return self._table_exists(params, start_time)
            elif operation == "describe_table":
                return self._describe_table(params, start_time)
            elif operation == "insert":
                return self._insert(params, start_time)
            elif operation == "insert_many":
                return self._insert_many(params, start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except ImportError as e:
            return ActionResult(
                success=False,
                message=f"Import error: {str(e)}",
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Database operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _connect(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Connect to a database."""
        db_type = params.get("db_type", "sqlite")
        database = params.get("database", ":memory:")
        
        try:
            if db_type == "sqlite":
                self._sqlite = SQLiteManager(database=database)
                
                return ActionResult(
                    success=True,
                    message=f"Connected to SQLite: {database}",
                    data={"db_type": "sqlite", "database": database},
                    duration=time.time() - start_time
                )
            
            else:
                self._pool = ConnectionPool(
                    db_type=db_type,
                    host=params.get("host", "localhost"),
                    port=params.get("port"),
                    user=params.get("user"),
                    password=params.get("password"),
                    database=params.get("database"),
                    max_connections=params.get("max_connections", 5)
                )
                
                return ActionResult(
                    success=True,
                    message=f"Connected to {db_type}",
                    data={"db_type": db_type},
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to connect: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _disconnect(self, start_time: float) -> ActionResult:
        """Disconnect from database."""
        if self._pool:
            self._pool.close_all()
            self._pool = None
        
        self._sqlite = None
        
        return ActionResult(
            success=True,
            message="Disconnected from database",
            duration=time.time() - start_time
        )
    
    def _query(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a SELECT query."""
        query = params.get("query", "")
        
        if not query:
            return ActionResult(
                success=False,
                message="query is required",
                duration=time.time() - start_time
            )
        
        if not query.strip().upper().startswith("SELECT"):
            return ActionResult(
                success=False,
                message="query must be a SELECT statement. Use 'execute' for other operations.",
                duration=time.time() - start_time
            )
        
        params_list = params.get("params", [])
        
        try:
            if self._sqlite:
                results = self._sqlite.execute(query, tuple(params_list) if params_list else None)
            elif self._pool:
                with self._pool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, tuple(params_list) if params_list else None)
                    rows = cursor.fetchall()
                    results = [dict(row) for row in rows]
            else:
                return ActionResult(
                    success=False,
                    message="Not connected to any database",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"Query returned {len(results)} rows",
                data={"rows": results, "count": len(results)},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Query failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _execute(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Execute a non-SELECT statement."""
        query = params.get("query", "")
        
        if not query:
            return ActionResult(
                success=False,
                message="query is required",
                duration=time.time() - start_time
            )
        
        if query.strip().upper().startswith("SELECT"):
            return ActionResult(
                success=False,
                message="Use 'query' for SELECT statements.",
                duration=time.time() - start_time
            )
        
        params_list = params.get("params", [])
        
        try:
            if self._sqlite:
                results = self._sqlite.execute(query, tuple(params_list) if params_list else None)
            elif self._pool:
                with self._pool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, tuple(params_list) if params_list else None)
                    conn.commit()
                    results = {"rows_affected": cursor.rowcount}
            else:
                return ActionResult(
                    success=False,
                    message="Not connected to any database",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message="Statement executed successfully",
                data=results,
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Execute failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _create_table(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Create a table."""
        table_name = params.get("table_name", "")
        columns = params.get("columns", {})
        
        if not table_name or not columns:
            return ActionResult(
                success=False,
                message="table_name and columns are required",
                duration=time.time() - start_time
            )
        
        if not self._sqlite:
            return ActionResult(
                success=False,
                message="create_table only supported for SQLite",
                duration=time.time() - start_time
            )
        
        try:
            self._sqlite.create_table(
                table_name=table_name,
                columns=columns,
                if_not_exists=params.get("if_not_exists", True)
            )
            
            return ActionResult(
                success=True,
                message=f"Table '{table_name}' created",
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to create table: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _list_tables(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """List all tables in the database."""
        if not self._sqlite:
            return ActionResult(
                success=False,
                message="list_tables only supported for SQLite",
                duration=time.time() - start_time
            )
        
        try:
            tables = self._sqlite.get_tables()
            
            return ActionResult(
                success=True,
                message=f"Found {len(tables)} tables",
                data={"tables": tables, "count": len(tables)},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to list tables: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _table_exists(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Check if a table exists."""
        table_name = params.get("table_name", "")
        
        if not table_name:
            return ActionResult(
                success=False,
                message="table_name is required",
                duration=time.time() - start_time
            )
        
        if not self._sqlite:
            return ActionResult(
                success=False,
                message="table_exists only supported for SQLite",
                duration=time.time() - start_time
            )
        
        try:
            exists = self._sqlite.table_exists(table_name)
            
            return ActionResult(
                success=True,
                message=f"Table '{table_name}' exists: {exists}",
                data={"exists": exists},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to check table: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _describe_table(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get table schema."""
        table_name = params.get("table_name", "")
        
        if not table_name:
            return ActionResult(
                success=False,
                message="table_name is required",
                duration=time.time() - start_time
            )
        
        if not self._sqlite:
            return ActionResult(
                success=False,
                message="describe_table only supported for SQLite",
                duration=time.time() - start_time
            )
        
        try:
            columns = self._sqlite.describe_table(table_name)
            
            return ActionResult(
                success=True,
                message=f"Retrieved schema for '{table_name}'",
                data={"columns": columns, "count": len(columns)},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to describe table: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _insert(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Insert a single row."""
        table_name = params.get("table_name", "")
        data = params.get("data", {})
        
        if not table_name or not data:
            return ActionResult(
                success=False,
                message="table_name and data are required",
                duration=time.time() - start_time
            )
        
        columns = list(data.keys())
        placeholders = ["?"] * len(columns)
        values = list(data.values())
        
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        try:
            if self._sqlite:
                results = self._sqlite.execute(query, tuple(values))
            elif self._pool:
                with self._pool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, tuple(values))
                    conn.commit()
                    results = {"last_row_id": cursor.lastrowid, "rows_affected": cursor.rowcount}
            else:
                return ActionResult(
                    success=False,
                    message="Not connected to any database",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"Inserted row into '{table_name}'",
                data=results,
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Insert failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _insert_many(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Insert multiple rows."""
        table_name = params.get("table_name", "")
        data = params.get("data", [])
        
        if not table_name or not data:
            return ActionResult(
                success=False,
                message="table_name and data are required",
                duration=time.time() - start_time
            )
        
        if not data:
            return ActionResult(
                success=True,
                message="No rows to insert",
                data={"rows_affected": 0},
                duration=time.time() - start_time
            )
        
        columns = list(data[0].keys())
        placeholders = ["?"] * len(columns)
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        params_list = [tuple(row[c] for c in columns) for row in data]
        
        try:
            if self._sqlite:
                count = self._sqlite.execute_many(query, params_list)
            elif self._pool:
                with self._pool.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.executemany(query, params_list)
                    conn.commit()
                    count = cursor.rowcount
            else:
                return ActionResult(
                    success=False,
                    message="Not connected to any database",
                    duration=time.time() - start_time
                )
            
            return ActionResult(
                success=True,
                message=f"Inserted {count} rows into '{table_name}'",
                data={"rows_affected": count},
                duration=time.time() - start_time
            )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Insert many failed: {str(e)}",
                duration=time.time() - start_time
            )
