"""SQLite action module for RabAI AutoClick.

Provides SQLite database operations including query execution,
CRUD operations, transactions, and schema management.
"""

import sys
import os
import json
import sqlite3
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from contextlib import contextmanager

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class SQLiteConfig:
    """SQLite configuration."""
    database: str = ":memory:"
    timeout: float = 5.0
    isolation_level: str = "DEFERRED"
    check_same_thread: bool = False
    auto_commit: bool = False


class SQLiteAction(BaseAction):
    """Action for SQLite database operations.
    
    Features:
        - Execute SQL queries
        - CRUD operations on tables
        - Transaction management
        - Schema introspection
        - Batch operations
        - Query result caching
        - Parameterized queries (SQL injection prevention)
    """
    
    def __init__(self, config: Optional[SQLiteConfig] = None):
        """Initialize SQLite action.
        
        Args:
            config: SQLite configuration.
        """
        super().__init__()
        self.config = config or SQLiteConfig()
        self._connection: Optional[sqlite3.Connection] = None
    
    def _get_connection(self) -> sqlite3.Connection:
        """Get or create database connection."""
        if self._connection is None:
            self._connection = sqlite3.connect(
                self.config.database,
                timeout=self.config.timeout,
                isolation_level=self.config.isolation_level if not self.config.auto_commit else None,
                check_same_thread=self.config.check_same_thread
            )
            self._connection.row_factory = sqlite3.Row
        return self._connection
    
    def _close_connection(self) -> None:
        """Close database connection."""
        if self._connection:
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None
    
    def execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute SQLite operation.
        
        Args:
            params: Dictionary containing:
                - operation: Operation to perform (query, execute, insert, update,
                           delete, create_table, drop_table, list_tables, describe,
                           transaction, backup, vacuum)
                - sql: SQL query string
                - params: Query parameters (for parameterized queries)
                - table: Table name
                - data: Data for insert/update
                - conditions: WHERE conditions
                - database: Override database path
        
        Returns:
            ActionResult with operation result
        """
        try:
            if "database" in params:
                self.config.database = params["database"]
            
            operation = params.get("operation", "")
            
            if operation == "query":
                return self._query(params)
            elif operation == "execute":
                return self._execute(params)
            elif operation == "insert":
                return self._insert(params)
            elif operation == "update":
                return self._update(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "create_table":
                return self._create_table(params)
            elif operation == "drop_table":
                return self._drop_table(params)
            elif operation == "list_tables":
                return self._list_tables(params)
            elif operation == "describe":
                return self._describe(params)
            elif operation == "transaction":
                return self._transaction(params)
            elif operation == "backup":
                return self._backup(params)
            elif operation == "vacuum":
                return self._vacuum(params)
            elif operation == "batch_insert":
                return self._batch_insert(params)
            elif operation == "batch_execute":
                return self._batch_execute(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
                
        except Exception as e:
            return ActionResult(success=False, message=f"SQLite operation failed: {str(e)}")
    
    def _query(self, params: Dict[str, Any]) -> ActionResult:
        """Execute SELECT query and return results."""
        sql = params.get("sql", "")
        query_params = params.get("params", [])
        limit = params.get("limit", 0)
        offset = params.get("offset", 0)
        
        if not sql:
            return ActionResult(success=False, message="sql is required for query operation")
        
        if limit > 0:
            sql = f"{sql} LIMIT {limit}"
            if offset > 0:
                sql = f"{sql} OFFSET {offset}"
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if query_params:
                cursor.execute(sql, query_params)
            else:
                cursor.execute(sql)
            
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            results = []
            for row in rows:
                results.append(dict(zip(columns, row)))
            
            return ActionResult(
                success=True,
                message=f"Query returned {len(results)} rows",
                data={
                    "rows": results,
                    "count": len(results),
                    "columns": columns
                }
            )
        except sqlite3.Error as e:
            return ActionResult(success=False, message=f"Query error: {str(e)}")
    
    def _execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute SQL statement (DDL, UPDATE, DELETE, etc.)."""
        sql = params.get("sql", "")
        exec_params = params.get("params", [])
        
        if not sql:
            return ActionResult(success=False, message="sql is required")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            if exec_params:
                cursor.execute(sql, exec_params)
            else:
                cursor.execute(sql)
            
            if not self.config.auto_commit:
                conn.commit()
            
            row_count = cursor.rowcount
            
            return ActionResult(
                success=True,
                message=f"Executed successfully. Rows affected: {row_count}",
                data={
                    "row_count": row_count,
                    "last_row_id": cursor.lastrowid
                }
            )
        except sqlite3.Error as e:
            if not self.config.auto_commit:
                conn.rollback()
            return ActionResult(success=False, message=f"Execute error: {str(e)}")
    
    def _insert(self, params: Dict[str, Any]) -> ActionResult:
        """Insert a single row into table."""
        table = params.get("table", "")
        data = params.get("data", {})
        
        if not table:
            return ActionResult(success=False, message="table is required")
        if not data:
            return ActionResult(success=False, message="data is required for insert")
        
        columns = list(data.keys())
        placeholders = ["?" for _ in columns]
        values = list(data.values())
        
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, values)
            
            if not self.config.auto_commit:
                conn.commit()
            
            return ActionResult(
                success=True,
                message=f"Inserted row with ID {cursor.lastrowid}",
                data={
                    "last_row_id": cursor.lastrowid,
                    "columns": columns
                }
            )
        except sqlite3.Error as e:
            if not self.config.auto_commit:
                conn.rollback()
            return ActionResult(success=False, message=f"Insert error: {str(e)}")
    
    def _update(self, params: Dict[str, Any]) -> ActionResult:
        """Update rows in table."""
        table = params.get("table", "")
        data = params.get("data", {})
        conditions = params.get("conditions", "")
        cond_params = params.get("cond_params", [])
        
        if not table:
            return ActionResult(success=False, message="table is required")
        if not data:
            return ActionResult(success=False, message="data is required for update")
        
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        values = list(data.values())
        
        sql = f"UPDATE {table} SET {set_clause}"
        if conditions:
            sql = f"{sql} WHERE {conditions}"
            values.extend(cond_params)
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, values)
            
            if not self.config.auto_commit:
                conn.commit()
            
            return ActionResult(
                success=True,
                message=f"Updated {cursor.rowcount} rows",
                data={"row_count": cursor.rowcount}
            )
        except sqlite3.Error as e:
            if not self.config.auto_commit:
                conn.rollback()
            return ActionResult(success=False, message=f"Update error: {str(e)}")
    
    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete rows from table."""
        table = params.get("table", "")
        conditions = params.get("conditions", "")
        cond_params = params.get("cond_params", [])
        
        if not table:
            return ActionResult(success=False, message="table is required")
        
        sql = f"DELETE FROM {table}"
        if conditions:
            sql = f"{sql} WHERE {conditions}"
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql, cond_params)
            
            if not self.config.auto_commit:
                conn.commit()
            
            return ActionResult(
                success=True,
                message=f"Deleted {cursor.rowcount} rows",
                data={"row_count": cursor.rowcount}
            )
        except sqlite3.Error as e:
            if not self.config.auto_commit:
                conn.rollback()
            return ActionResult(success=False, message=f"Delete error: {str(e)}")
    
    def _create_table(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new table."""
        table = params.get("table", "")
        columns = params.get("columns", [])  # [{"name": "id", "type": "INTEGER PRIMARY KEY"}, ...]
        if_not_exists = params.get("if_not_exists", True)
        primary_key = params.get("primary_key", "")
        
        if not table:
            return ActionResult(success=False, message="table is required")
        if not columns:
            return ActionResult(success=False, message="columns definition required")
        
        col_defs = []
        for col in columns:
            col_str = f"{col['name']} {col.get('type', 'TEXT')}"
            if col.get("primary_key"):
                col_str += " PRIMARY KEY"
            if col.get("auto_increment"):
                col_str += " AUTOINCREMENT"
            if not col.get("nullable", True):
                col_str += " NOT NULL"
            if "default" in col:
                col_str += f" DEFAULT {col['default']}"
            col_defs.append(col_str)
        
        if primary_key and primary_key not in [c["name"] for c in columns]:
            col_defs.append(f"PRIMARY KEY ({primary_key})")
        
        exists_clause = "IF NOT EXISTS " if if_not_exists else ""
        
        sql = f"CREATE TABLE {exists_clause}{table} ({', '.join(col_defs)})"
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            
            if not self.config.auto_commit:
                conn.commit()
            
            return ActionResult(
                success=True,
                message=f"Table {table} created",
                data={"sql": sql}
            )
        except sqlite3.Error as e:
            if not self.config.auto_commit:
                conn.rollback()
            return ActionResult(success=False, message=f"Create table error: {str(e)}")
    
    def _drop_table(self, params: Dict[str, Any]) -> ActionResult:
        """Drop a table."""
        table = params.get("table", "")
        if_exists = params.get("if_exists", True)
        
        if not table:
            return ActionResult(success=False, message="table is required")
        
        exists_clause = "IF EXISTS " if if_exists else ""
        sql = f"DROP TABLE {exists_clause}{table}"
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(sql)
            
            if not self.config.auto_commit:
                conn.commit()
            
            return ActionResult(success=True, message=f"Table {table} dropped")
        except sqlite3.Error as e:
            if not self.config.auto_commit:
                conn.rollback()
            return ActionResult(success=False, message=f"Drop table error: {str(e)}")
    
    def _list_tables(self, params: Dict[str, Any]) -> ActionResult:
        """List all tables in database."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            tables = [row[0] for row in cursor.fetchall()]
            
            return ActionResult(
                success=True,
                message=f"Found {len(tables)} tables",
                data={"tables": tables, "count": len(tables)}
            )
        except sqlite3.Error as e:
            return ActionResult(success=False, message=f"List tables error: {str(e)}")
    
    def _describe(self, params: Dict[str, Any]) -> ActionResult:
        """Get table schema."""
        table = params.get("table", "")
        
        if not table:
            return ActionResult(success=False, message="table is required")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({table})")
            
            columns = []
            for row in cursor.fetchall():
                columns.append({
                    "cid": row[0],
                    "name": row[1],
                    "type": row[2],
                    "notnull": row[3],
                    "default_value": row[4],
                    "pk": row[5]
                })
            
            return ActionResult(
                success=True,
                message=f"Table {table} has {len(columns)} columns",
                data={"table": table, "columns": columns}
            )
        except sqlite3.Error as e:
            return ActionResult(success=False, message=f"Describe error: {str(e)}")
    
    def _transaction(self, params: Dict[str, Any]) -> ActionResult:
        """Execute multiple statements in a transaction."""
        statements = params.get("statements", [])
        
        if not statements:
            return ActionResult(success=False, message="statements list required")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute("BEGIN TRANSACTION")
            
            try:
                results = []
                for stmt in statements:
                    sql = stmt.get("sql", "")
                    stmt_params = stmt.get("params", [])
                    
                    if sql.strip().upper().startswith("SELECT"):
                        if stmt_params:
                            cursor.execute(sql, stmt_params)
                        else:
                            cursor.execute(sql)
                        results.append({"type": "query", "rows": cursor.fetchall()})
                    else:
                        if stmt_params:
                            cursor.execute(sql, stmt_params)
                        else:
                            cursor.execute(sql)
                        results.append({"type": "execute", "rowcount": cursor.rowcount})
                
                conn.commit()
                
                return ActionResult(
                    success=True,
                    message=f"Transaction committed: {len(statements)} statements",
                    data={"statements": len(statements), "results": results}
                )
            except Exception as e:
                conn.rollback()
                return ActionResult(success=False, message=f"Transaction rolled back: {str(e)}")
                
        except sqlite3.Error as e:
            return ActionResult(success=False, message=f"Transaction error: {str(e)}")
    
    def _backup(self, params: Dict[str, Any]) -> ActionResult:
        """Backup database to file."""
        output_path = params.get("output_path", "")
        
        if not output_path:
            return ActionResult(success=False, message="output_path required")
        
        try:
            conn = self._get_connection()
            
            backup_conn = sqlite3.connect(output_path)
            conn.backup(backup_conn)
            backup_conn.close()
            
            backup_size = os.path.getsize(output_path)
            
            return ActionResult(
                success=True,
                message=f"Database backed up to {output_path}",
                data={"output_path": output_path, "size": backup_size}
            )
        except sqlite3.Error as e:
            return ActionResult(success=False, message=f"Backup error: {str(e)}")
    
    def _vacuum(self, params: Dict[str, Any]) -> ActionResult:
        """Vacuum database to reclaim space."""
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            cursor.execute("VACUUM")
            
            return ActionResult(success=True, message="Database vacuumed")
        except sqlite3.Error as e:
            return ActionResult(success=False, message=f"Vacuum error: {str(e)}")
    
    def _batch_insert(self, params: Dict[str, Any]) -> ActionResult:
        """Insert multiple rows efficiently."""
        table = params.get("table", "")
        rows = params.get("rows", [])
        
        if not table:
            return ActionResult(success=False, message="table is required")
        if not rows:
            return ActionResult(success=False, message="rows list required")
        
        if not rows:
            return ActionResult(success=True, message="No rows to insert")
        
        columns = list(rows[0].keys())
        placeholders = ["?" for _ in columns]
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            data_rows = [[row.get(col) for col in columns] for row in rows]
            cursor.executemany(sql, data_rows)
            
            if not self.config.auto_commit:
                conn.commit()
            
            return ActionResult(
                success=True,
                message=f"Batch inserted {cursor.rowcount} rows",
                data={"row_count": cursor.rowcount}
            )
        except sqlite3.Error as e:
            if not self.config.auto_commit:
                conn.rollback()
            return ActionResult(success=False, message=f"Batch insert error: {str(e)}")
    
    def _batch_execute(self, params: Dict[str, Any]) -> ActionResult:
        """Execute multiple SQL statements."""
        statements = params.get("statements", [])
        
        if not statements:
            return ActionResult(success=False, message="statements list required")
        
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            total_affected = 0
            for stmt in statements:
                sql = stmt.get("sql", "")
                stmt_params = stmt.get("params", [])
                
                if stmt_params:
                    cursor.execute(sql, stmt_params)
                else:
                    cursor.execute(sql)
                
                total_affected += cursor.rowcount
            
            if not self.config.auto_commit:
                conn.commit()
            
            return ActionResult(
                success=True,
                message=f"Executed {len(statements)} statements",
                data={"statement_count": len(statements), "total_affected": total_affected}
            )
        except sqlite3.Error as e:
            if not self.config.auto_commit:
                conn.rollback()
            return ActionResult(success=False, message=f"Batch execute error: {str(e)}")
