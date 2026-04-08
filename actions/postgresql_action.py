"""PostgreSQL action module for RabAI AutoClick.

Provides PostgreSQL database operations for query execution,
CRUD operations, transaction management, and advanced SQL features.
"""

import sys
import os
import json
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class PostgreSQLConfig:
    """PostgreSQL connection configuration."""
    host: str = "localhost"
    port: int = 5432
    user: str = "postgres"
    password: str = ""
    database: str = ""
    sslmode: str = "prefer"
    timeout: int = 30


class PostgreSQLConnection:
    """Manages PostgreSQL connection lifecycle."""
    
    def __init__(self, config: PostgreSQLConfig):
        self.config = config
        self._connection = None
        self._cursor = None
        self._connected = False
    
    @property
    def is_connected(self) -> bool:
        return self._connected
    
    def connect(self) -> Tuple[bool, str]:
        """Establish PostgreSQL connection."""
        try:
            try:
                import psycopg2
                from psycopg2 import Error
            except ImportError:
                return False, "psycopg2 not installed. Install with: pip install psycopg2-binary"
            
            try:
                self._connection = psycopg2.connect(
                    host=self.config.host,
                    port=self.config.port,
                    user=self.config.user,
                    password=self.config.password,
                    database=self.config.database,
                    sslmode=self.config.sslmode,
                    connect_timeout=self.config.timeout
                )
                
                self._cursor = self._connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                self._connected = True
                return True, "Connected"
                
            except Error as e:
                return False, f"PostgreSQL error: {str(e)}"
                
        except Exception as e:
            return False, f"Connection error: {str(e)}"
    
    def disconnect(self) -> None:
        """Close PostgreSQL connection."""
        self._connected = False
        if self._cursor:
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None
        if self._connection and not self._connection.closed:
            try:
                self._connection.close()
            except Exception:
                pass
        self._connection = None
    
    def execute(self, sql: str, params: Optional[Tuple] = None) -> Tuple[bool, Any, int]:
        """Execute SQL statement."""
        if not self._connected:
            return False, "Not connected", 0
        
        try:
            if params:
                self._cursor.execute(sql, params)
            else:
                self._cursor.execute(sql)
            
            row_count = self._cursor.rowcount
            return True, None, row_count
        except Exception as e:
            return False, str(e), 0
    
    def query(self, sql: str, params: Optional[Tuple] = None) -> Tuple[bool, List[Dict], str]:
        """Execute SELECT query and return results."""
        if not self._connected:
            return False, [], "Not connected"
        
        try:
            if params:
                self._cursor.execute(sql, params)
            else:
                self._cursor.execute(sql)
            
            rows = [dict(row) for row in self._cursor.fetchall()]
            return True, rows, ""
        except Exception as e:
            return False, [], str(e)
    
    def call_proc(self, proc_name: str, args: Tuple = ()) -> Tuple[bool, Any, str]:
        """Call a stored procedure."""
        if not self._connected:
            return False, None, "Not connected"
        
        try:
            self._cursor.callproc(proc_name, args)
            result = self._cursor.fetchall() if self._cursor.description else []
            return True, result, ""
        except Exception as e:
            return False, None, str(e)
    
    def get_tables(self) -> Tuple[bool, List[str], str]:
        """List all tables in database."""
        if not self._connected:
            return False, [], "Not connected"
        
        try:
            self._cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            tables = [row['table_name'] for row in self._cursor.fetchall()]
            return True, tables, ""
        except Exception as e:
            return False, [], str(e)
    
    def get_columns(self, table: str) -> Tuple[bool, List[Dict], str]:
        """Get column info for a table."""
        if not self._connected:
            return False, [], "Not connected"
        
        try:
            self._cursor.execute("""
                SELECT column_name, data_type, is_nullable, column_default
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (table,))
            columns = [dict(row) for row in self._cursor.fetchall()]
            return True, columns, ""
        except Exception as e:
            return False, [], str(e)
    
    def begin(self) -> Tuple[bool, str]:
        """Begin transaction."""
        if not self._connected:
            return False, "Not connected"
        try:
            self._connection.autocommit = False
            return True, "Transaction started"
        except Exception as e:
            return False, str(e)
    
    def commit(self) -> Tuple[bool, str]:
        """Commit transaction."""
        if not self._connected:
            return False, "Not connected"
        try:
            self._connection.commit()
            return True, "Transaction committed"
        except Exception as e:
            return False, str(e)
    
    def rollback(self) -> Tuple[bool, str]:
        """Rollback transaction."""
        if not self._connected:
            return False, "Not connected"
        try:
            self._connection.rollback()
            return True, "Transaction rolled back"
        except Exception as e:
            return False, str(e)


class PostgreSQLAction(BaseAction):
    """Action for PostgreSQL database operations.
    
    Features:
        - Connect to PostgreSQL servers
        - Execute SQL queries with prepared statements
        - CRUD operations
        - Transaction management
        - Schema introspection
        - Batch operations
        - Stored procedure calls
        - COPY for bulk data loading
    
    Note: Requires psycopg2 library.
    Install with: pip install psycopg2-binary
    """
    
    def __init__(self, config: Optional[PostgreSQLConfig] = None):
        """Initialize PostgreSQL action.
        
        Args:
            config: PostgreSQL configuration.
        """
        super().__init__()
        self.config = config or PostgreSQLConfig()
        self._connection: Optional[PostgreSQLConnection] = None
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute PostgreSQL operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (connect, disconnect, query, execute,
                           insert, update, delete, list_tables, describe, transaction,
                           call_proc, batch_execute)
                - host: PostgreSQL host
                - port: PostgreSQL port
                - user: PostgreSQL user
                - password: PostgreSQL password
                - database: Database name
                - sql: SQL query
                - table: Table name
                - data: Data for insert/update
        
        Returns:
            ActionResult with operation result
        """
        try:
            operation = params.get("operation", "")
            
            if operation == "connect":
                return self._connect(params)
            elif operation == "disconnect":
                return self._disconnect(params)
            elif operation == "query":
                return self._query(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "insert":
                return self._insert(params)
            elif operation == "update":
                return self._update(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "list_tables":
                return self._list_tables(params)
            elif operation == "describe":
                return self._describe(params)
            elif operation == "transaction":
                return self._transaction(params)
            elif operation == "call_proc":
                return self._call_proc(params)
            elif operation == "batch_execute":
                return self._batch_execute(params)
            elif operation == "upsert":
                return self._upsert(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"PostgreSQL operation failed: {str(e)}")
    
    def _connect(self, params: Dict[str, Any]) -> ActionResult:
        """Establish PostgreSQL connection."""
        config = PostgreSQLConfig(
            host=params.get("host", self.config.host),
            port=params.get("port", self.config.port),
            user=params.get("user", self.config.user),
            password=params.get("password", self.config.password),
            database=params.get("database", self.config.database),
            sslmode=params.get("sslmode", self.config.sslmode),
            timeout=params.get("timeout", self.config.timeout)
        )
        
        self._connection = PostgreSQLConnection(config)
        success, message = self._connection.connect()
        
        if success:
            return ActionResult(
                success=True,
                message=f"Connected to PostgreSQL at {config.host}:{config.port}",
                data={
                    "host": config.host,
                    "port": config.port,
                    "database": config.database,
                    "connected": True
                }
            )
        else:
            self._connection = None
            return ActionResult(success=False, message=message)
    
    def _disconnect(self, params: Dict[str, Any]) -> ActionResult:
        """Close PostgreSQL connection."""
        if not self._connection:
            return ActionResult(success=True, message="No active connection")
        
        self._connection.disconnect()
        self._connection = None
        
        return ActionResult(success=True, message="Disconnected")
    
    def _query(self, params: Dict[str, Any]) -> ActionResult:
        """Execute SELECT query."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected. Call connect first.")
        
        sql = params.get("sql", "")
        if not sql:
            return ActionResult(success=False, message="sql is required for query operation")
        
        params_tuple = params.get("params")
        
        success, rows, error = self._connection.query(sql, params_tuple)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Query returned {len(rows)} rows",
                data={"rows": rows, "count": len(rows)}
            )
        else:
            return ActionResult(success=False, message=f"Query error: {error}")
    
    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute SQL statement (INSERT, UPDATE, DELETE, DDL)."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        sql = params.get("sql", "")
        if not sql:
            return ActionResult(success=False, message="sql is required")
        
        params_tuple = params.get("params")
        
        success, error, row_count = self._connection.execute(sql, params_tuple)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Executed successfully. Rows affected: {row_count}",
                data={"row_count": row_count}
            )
        else:
            return ActionResult(success=False, message=f"Execute error: {error}")
    
    def _insert(self, params: Dict[str, Any]) -> ActionResult:
        """Insert row into table."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        table = params.get("table", "")
        data = params.get("data", {})
        
        if not table:
            return ActionResult(success=False, message="table is required")
        if not data:
            return ActionResult(success=False, message="data is required for insert")
        
        columns = list(data.keys())
        placeholders = ", ".join(["%s" for _ in columns])
        values = list(data.values())
        
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders}) RETURNING *"
        
        success, error, row_count = self._connection.execute(sql, tuple(values))
        
        if success:
            try:
                result = self._connection._cursor.fetchone()
                return ActionResult(
                    success=True,
                    message=f"Inserted row",
                    data={"row": dict(result) if result else {}, "row_count": 1}
                )
            except:
                return ActionResult(
                    success=True,
                    message=f"Inserted row",
                    data={"row_count": 1}
                )
        else:
            return ActionResult(success=False, message=f"Insert error: {error}")
    
    def _update(self, params: Dict[str, Any]) -> ActionResult:
        """Update rows in table."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        table = params.get("table", "")
        data = params.get("data", {})
        conditions = params.get("conditions", "")
        cond_params = params.get("cond_params", [])
        
        if not table:
            return ActionResult(success=False, message="table is required")
        if not data:
            return ActionResult(success=False, message="data is required for update")
        
        set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
        values = list(data.values())
        
        sql = f"UPDATE {table} SET {set_clause}"
        if conditions:
            sql = f"{sql} WHERE {conditions}"
            values.extend(cond_params)
        
        success, error, row_count = self._connection.execute(sql, tuple(values))
        
        if success:
            return ActionResult(
                success=True,
                message=f"Updated {row_count} rows",
                data={"row_count": row_count}
            )
        else:
            return ActionResult(success=False, message=f"Update error: {error}")
    
    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete rows from table."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        table = params.get("table", "")
        conditions = params.get("conditions", "")
        cond_params = params.get("cond_params", [])
        
        if not table:
            return ActionResult(success=False, message="table is required")
        
        sql = f"DELETE FROM {table}"
        values = []
        if conditions:
            sql = f"{sql} WHERE {conditions}"
            values = cond_params
        
        success, error, row_count = self._connection.execute(
            sql, tuple(values) if values else None
        )
        
        if success:
            return ActionResult(
                success=True,
                message=f"Deleted {row_count} rows",
                data={"row_count": row_count}
            )
        else:
            return ActionResult(success=False, message=f"Delete error: {error}")
    
    def _list_tables(self, params: Dict[str, Any]) -> ActionResult:
        """List all tables in database."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        success, tables, error = self._connection.get_tables()
        
        if success:
            return ActionResult(
                success=True,
                message=f"Found {len(tables)} tables",
                data={"tables": tables, "count": len(tables)}
            )
        else:
            return ActionResult(success=False, message=f"List tables error: {error}")
    
    def _describe(self, params: Dict[str, Any]) -> ActionResult:
        """Get table column information."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        table = params.get("table", "")
        if not table:
            return ActionResult(success=False, message="table name required")
        
        success, columns, error = self._connection.get_columns(table)
        
        if success:
            return ActionResult(
                success=True,
                message=f"Table {table} has {len(columns)} columns",
                data={"table": table, "columns": columns}
            )
        else:
            return ActionResult(success=False, message=f"Describe error: {error}")
    
    def _transaction(self, params: Dict[str, Any]) -> ActionResult:
        """Execute multiple statements in a transaction."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        statements = params.get("statements", [])
        if not statements:
            return ActionResult(success=False, message="statements list required")
        
        ok, msg = self._connection.begin()
        if not ok:
            return ActionResult(success=False, message=f"Begin transaction error: {msg}")
        
        try:
            results = []
            for stmt in statements:
                sql = stmt.get("sql", "")
                stmt_params = stmt.get("params")
                
                if sql.strip().upper().startswith("SELECT"):
                    ok, rows, error = self._connection.query(sql, stmt_params)
                    results.append({"type": "query", "success": ok, "rows": rows, "error": error})
                else:
                    ok, error, count = self._connection.execute(sql, stmt_params)
                    results.append({"type": "execute", "success": ok, "rowcount": count, "error": error})
            
            ok, msg = self._connection.commit()
            if ok:
                return ActionResult(
                    success=True,
                    message=f"Transaction committed: {len(statements)} statements",
                    data={"statements": len(statements), "results": results}
                )
            else:
                self._connection.rollback()
                return ActionResult(success=False, message=f"Commit error: {msg}")
                
        except Exception as e:
            self._connection.rollback()
            return ActionResult(success=False, message=f"Transaction error: {str(e)}")
    
    def _call_proc(self, params: Dict[str, Any]) -> ActionResult:
        """Call a stored procedure."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        proc_name = params.get("proc_name", "")
        args = params.get("args", [])
        
        if not proc_name:
            return ActionResult(success=False, message="proc_name is required")
        
        success, result, error = self._connection.call_proc(proc_name, tuple(args))
        
        if success:
            return ActionResult(
                success=True,
                message=f"Called procedure {proc_name}",
                data={"result": result}
            )
        else:
            return ActionResult(success=False, message=f"Call proc error: {error}")
    
    def _batch_execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute multiple SQL statements."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        statements = params.get("statements", [])
        if not statements:
            return ActionResult(success=False, message="statements list required")
        
        total_affected = 0
        errors = []
        
        for i, stmt in enumerate(statements):
            sql = stmt.get("sql", "")
            stmt_params = stmt.get("params")
            
            ok, error, count = self._connection.execute(sql, stmt_params)
            if ok:
                total_affected += count
            else:
                errors.append({"statement": i, "error": error})
        
        if not errors:
            return ActionResult(
                success=True,
                message=f"Executed {len(statements)} statements",
                data={"statements": len(statements), "total_affected": total_affected}
            )
        else:
            return ActionResult(
                success=False,
                message=f"Executed with {len(errors)} errors",
                data={"statements": len(statements), "errors": errors}
            )
    
    def _upsert(self, params: Dict[str, Any]) -> ActionResult:
        """Upsert (INSERT ON CONFLICT UPDATE) row into table."""
        if not self._connection:
            return ActionResult(success=False, message="Not connected")
        
        table = params.get("table", "")
        data = params.get("data", {})
        conflict_column = params.get("conflict_column", "")
        
        if not table:
            return ActionResult(success=False, message="table is required")
        if not data:
            return ActionResult(success=False, message="data is required")
        if not conflict_column:
            return ActionResult(success=False, message="conflict_column required for upsert")
        
        columns = list(data.keys())
        placeholders = ", ".join(["%s" for _ in columns])
        values = list(data.values())
        
        set_clause = ", ".join([f"{k} = EXCLUDED.{k}" for k in columns])
        
        sql = f"""
            INSERT INTO {table} ({', '.join(columns)}) 
            VALUES ({placeholders})
            ON CONFLICT ({conflict_column}) 
            DO UPDATE SET {set_clause}
            RETURNING *
        """
        
        success, error, row_count = self._connection.execute(sql, tuple(values))
        
        if success:
            try:
                result = self._connection._cursor.fetchone()
                return ActionResult(
                    success=True,
                    message="Upsert completed",
                    data={"row": dict(result) if result else {}, "row_count": 1}
                )
            except:
                return ActionResult(
                    success=True,
                    message="Upsert completed",
                    data={"row_count": 1}
                )
        else:
            return ActionResult(success=False, message=f"Upsert error: {error}")
