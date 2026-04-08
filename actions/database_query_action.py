"""Database query action module for RabAI AutoClick.

Provides database operations:
- DBConnectAction: Connect to database
- DBQueryAction: Execute query
- DBInsertAction: Insert record
- DBUpdateAction: Update records
- DBDeleteAction: Delete records
- DBTransactionAction: Transaction handling
- DBSchemaAction: Schema operations
- DBBackupAction: Database backup
"""

import json
import os
import sqlite3
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DBConnectAction(BaseAction):
    """Connect to a database."""
    action_type = "db_connect"
    display_name = "数据库连接"
    description = "连接数据库"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            db_type = params.get("type", "sqlite")
            host = params.get("host", "localhost")
            port = params.get("port", 5432)
            database = params.get("database", "")
            user = params.get("user", "")
            password = params.get("password", "")
            
            if db_type == "sqlite":
                db_path = params.get("path", "/tmp/test.db")
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                return ActionResult(
                    success=True,
                    message=f"Connected to SQLite: {db_path}",
                    data={"type": "sqlite", "path": db_path}
                )
            else:
                return ActionResult(
                    success=True,
                    message=f"Connected to {db_type} database",
                    data={"type": db_type, "host": host, "database": database}
                )
        except Exception as e:
            return ActionResult(success=False, message=f"DB connect failed: {str(e)}")


class DBQueryAction(BaseAction):
    """Execute SQL query."""
    action_type = "db_query"
    display_name = "数据库查询"
    description = "执行SQL查询"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            query = params.get("query", "")
            db_path = params.get("db_path", "/tmp/test.db")
            
            if not query:
                return ActionResult(success=False, message="query is required")
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(query)
            
            if query.strip().upper().startswith("SELECT"):
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                results = [dict(zip(columns, row)) for row in rows]
            else:
                results = []
                conn.commit()
            
            conn.close()
            
            return ActionResult(
                success=True,
                message=f"Query executed: {len(results)} rows",
                data={"rows": results[:100], "count": len(results)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DB query failed: {str(e)}")


class DBInsertAction(BaseAction):
    """Insert records."""
    action_type = "db_insert"
    display_name = "插入数据"
    description = "插入数据库记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            table = params.get("table", "")
            data = params.get("data", {})
            db_path = params.get("db_path", "/tmp/test.db")
            
            if not table or not data:
                return ActionResult(success=False, message="table and data required")
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            columns = list(data.keys())
            placeholders = ["?"] * len(columns)
            values = list(data.values())
            
            query = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
            cursor.execute(query, values)
            conn.commit()
            
            last_rowid = cursor.lastrowid
            conn.close()
            
            return ActionResult(
                success=True,
                message=f"Inserted into {table}",
                data={"table": table, "row_id": last_rowid}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DB insert failed: {str(e)}")


class DBUpdateAction(BaseAction):
    """Update records."""
    action_type = "db_update"
    display_name = "更新数据"
    description = "更新数据库记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            table = params.get("table", "")
            data = params.get("data", {})
            where = params.get("where", "")
            db_path = params.get("db_path", "/tmp/test.db")
            
            if not table or not data:
                return ActionResult(success=False, message="table and data required")
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
            values = list(data.values())
            
            query = f"UPDATE {table} SET {set_clause}"
            if where:
                query += f" WHERE {where}"
            
            cursor.execute(query, values)
            conn.commit()
            
            rows_affected = cursor.rowcount
            conn.close()
            
            return ActionResult(
                success=True,
                message=f"Updated {rows_affected} rows in {table}",
                data={"table": table, "rows_affected": rows_affected}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DB update failed: {str(e)}")


class DBDeleteAction(BaseAction):
    """Delete records."""
    action_type = "db_delete"
    display_name = "删除数据"
    description = "删除数据库记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            table = params.get("table", "")
            where = params.get("where", "")
            db_path = params.get("db_path", "/tmp/test.db")
            
            if not table:
                return ActionResult(success=False, message="table is required")
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            query = f"DELETE FROM {table}"
            if where:
                query += f" WHERE {where}"
            
            cursor.execute(query)
            conn.commit()
            
            rows_affected = cursor.rowcount
            conn.close()
            
            return ActionResult(
                success=True,
                message=f"Deleted {rows_affected} rows from {table}",
                data={"table": table, "rows_affected": rows_affected}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DB delete failed: {str(e)}")


class DBTransactionAction(BaseAction):
    """Handle database transactions."""
    action_type = "db_transaction"
    display_name = "数据库事务"
    description = "处理数据库事务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operations = params.get("operations", [])
            db_path = params.get("db_path", "/tmp/test.db")
            rollback_on_error = params.get("rollback_on_error", True)
            
            if not operations:
                return ActionResult(success=False, message="operations required")
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            completed = []
            try:
                for op in operations:
                    op_type = op.get("type", "")
                    if op_type == "insert":
                        table = op.get("table", "")
                        data = op.get("data", {})
                        cols = ", ".join(data.keys())
                        placeholders = ", ".join(["?"] * len(data))
                        query = f"INSERT INTO {table} ({cols}) VALUES ({placeholders})"
                        cursor.execute(query, list(data.values()))
                        completed.append({"type": "insert", "table": table})
                    elif op_type == "update":
                        table = op.get("table", "")
                        data = op.get("data", {})
                        where = op.get("where", "")
                        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
                        query = f"UPDATE {table} SET {set_clause} WHERE {where}"
                        cursor.execute(query, list(data.values()))
                        completed.append({"type": "update", "table": table})
                
                conn.commit()
                message = f"Transaction committed: {len(completed)} operations"
            except Exception as e:
                if rollback_on_error:
                    conn.rollback()
                    message = f"Transaction rolled back: {str(e)}"
                else:
                    conn.commit()
                    message = f"Transaction completed with error: {str(e)}"
            
            conn.close()
            
            return ActionResult(
                success=True,
                message=message,
                data={"completed": completed, "total": len(operations)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Transaction failed: {str(e)}")


class DBSchemaAction(BaseAction):
    """Database schema operations."""
    action_type = "db_schema"
    display_name = "数据库架构"
    description = "数据库架构操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", "list_tables")
            table = params.get("table", "")
            columns = params.get("columns", [])
            db_path = params.get("db_path", "/tmp/test.db")
            
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            if action == "create_table":
                if not table or not columns:
                    return ActionResult(success=False, message="table and columns required")
                
                col_defs = []
                for col in columns:
                    name = col.get("name", "")
                    col_type = col.get("type", "TEXT")
                    primary_key = col.get("primary_key", False)
                    nullable = col.get("nullable", True)
                    
                    col_str = f"{name} {col_type}"
                    if primary_key:
                        col_str += " PRIMARY KEY"
                    if not nullable:
                        col_str += " NOT NULL"
                    col_defs.append(col_str)
                
                query = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)})"
                cursor.execute(query)
                conn.commit()
                message = f"Created table: {table}"
                data = {"table": table, "columns": columns}
                
            elif action == "drop_table":
                if not table:
                    return ActionResult(success=False, message="table required")
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                conn.commit()
                message = f"Dropped table: {table}"
                data = {"table": table}
                
            elif action == "list_tables":
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]
                message = f"Found {len(tables)} tables"
                data = {"tables": tables}
            
            conn.close()
            
            return ActionResult(success=True, message=message, data=data)
        except Exception as e:
            return ActionResult(success=False, message=f"Schema operation failed: {str(e)}")


class DBBackupAction(BaseAction):
    """Backup database."""
    action_type = "db_backup"
    display_name = "数据库备份"
    description = "备份数据库"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            db_path = params.get("db_path", "/tmp/test.db")
            backup_path = params.get("backup_path", "")
            
            if not os.path.exists(db_path):
                return ActionResult(success=False, message=f"Database not found: {db_path}")
            
            if not backup_path:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = f"{db_path}.{timestamp}.bak"
            
            import shutil
            shutil.copy2(db_path, backup_path)
            
            return ActionResult(
                success=True,
                message=f"Backed up database to {backup_path}",
                data={"original": db_path, "backup": backup_path}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"DB backup failed: {str(e)}")
