"""Database action module for RabAI AutoClick.

Provides database operations including SQLite queries and data manipulation.
"""

import sqlite3
import json
import sys
import os
from typing import Any, Dict, List, Optional, Union, Tuple

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DbQueryAction(BaseAction):
    """Execute SQL query on SQLite database.
    
    Supports SELECT queries with parameter binding.
    """
    action_type = "db_query"
    display_name = "数据库查询"
    description = "执行SQL查询"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SQL query.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: db_path, query, params, 
                   fetch_one, fetch_all.
        
        Returns:
            ActionResult with query results.
        """
        db_path = params.get('db_path', '')
        query = params.get('query', '')
        query_params = params.get('params', ())
        fetch_one = params.get('fetch_one', False)
        fetch_all = params.get('fetch_all', True)
        
        if not db_path:
            return ActionResult(success=False, message="db_path is required")
        
        if not os.path.exists(db_path):
            return ActionResult(success=False, message=f"Database not found: {db_path}")
        
        if not query:
            return ActionResult(success=False, message="query is required")
        
        try:
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute(query, query_params)
            
            rows = None
            if query.strip().upper().startswith('SELECT'):
                if fetch_one:
                    row = cursor.fetchone()
                    rows = dict(row) if row else None
                elif fetch_all:
                    rows = [dict(row) for row in cursor.fetchall()]
                else:
                    rows = []
            else:
                conn.commit()
                rows = {'affected_rows': cursor.rowcount}
            
            cursor.close()
            conn.close()
            
            return ActionResult(
                success=True,
                message=f"Query executed successfully",
                data={'rows': rows, 'query': query}
            )
            
        except sqlite3.Error as e:
            return ActionResult(
                success=False,
                message=f"SQL error: {e}",
                data={'error': str(e), 'query': query}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Query error: {e}",
                data={'error': str(e)}
            )


class DbExecuteAction(BaseAction):
    """Execute SQL statements (INSERT, UPDATE, DELETE).
    
    Executes non-query SQL with parameter binding and commit.
    """
    action_type = "db_execute"
    display_name = "数据库执行"
    description = "执行INSERT/UPDATE/DELETE"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SQL statement.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: db_path, statement, params,
                   get_last_rowid.
        
        Returns:
            ActionResult with execution status.
        """
        db_path = params.get('db_path', '')
        statement = params.get('statement', '')
        params_list = params.get('params', ())
        get_last_rowid = params.get('get_last_rowid', False)
        
        if not db_path:
            return ActionResult(success=False, message="db_path is required")
        
        if not statement:
            return ActionResult(success=False, message="statement is required")
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute(statement, params_list)
            conn.commit()
            
            last_rowid = cursor.lastrowid if get_last_rowid else None
            affected = cursor.rowcount
            
            cursor.close()
            conn.close()
            
            return ActionResult(
                success=True,
                message=f"Executed: {affected} row(s) affected",
                data={
                    'affected_rows': affected,
                    'last_rowid': last_rowid,
                    'statement': statement
                }
            )
            
        except sqlite3.Error as e:
            return ActionResult(
                success=False,
                message=f"SQL error: {e}",
                data={'error': str(e), 'statement': statement}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Execute error: {e}",
                data={'error': str(e)}
            )


class DbCreateTableAction(BaseAction):
    """Create database table.
    
    Creates a new table with specified columns and schema.
    """
    action_type = "db_create_table"
    display_name = "创建数据表"
    description = "创建数据库表"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Create table.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: db_path, table_name, columns,
                   if_not_exists.
        
        Returns:
            ActionResult with creation status.
        """
        db_path = params.get('db_path', '')
        table_name = params.get('table_name', '')
        columns = params.get('columns', [])
        if_not_exists = params.get('if_not_exists', True)
        
        if not db_path:
            return ActionResult(success=False, message="db_path is required")
        
        if not table_name:
            return ActionResult(success=False, message="table_name is required")
        
        if not columns:
            return ActionResult(success=False, message="columns required")
        
        # Build CREATE TABLE statement
        col_defs = []
        for col in columns:
            if isinstance(col, dict):
                name = col.get('name', '')
                col_type = col.get('type', 'TEXT')
                constraints = col.get('constraints', '')
                col_defs.append(f"{name} {col_type} {constraints}".strip())
            else:
                col_defs.append(str(col))
        
        if_exists = 'IF NOT EXISTS' if if_not_exists else ''
        
        statement = f"CREATE TABLE {if_exists} {table_name} ({', '.join(col_defs)})"
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(statement)
            conn.commit()
            cursor.close()
            conn.close()
            
            return ActionResult(
                success=True,
                message=f"Table '{table_name}' created",
                data={'table_name': table_name, 'statement': statement}
            )
            
        except sqlite3.Error as e:
            return ActionResult(
                success=False,
                message=f"Create table error: {e}",
                data={'error': str(e), 'statement': statement}
            )


class DbBackupAction(BaseAction):
    """Backup SQLite database to file.
    
    Creates a backup copy of the database.
    """
    action_type = "db_backup"
    display_name = "数据库备份"
    description = "备份SQLite数据库"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Backup database.
        
        Args:
            context: Execution context (ContextManager instance).
            params: Dict with keys: db_path, backup_path.
        
        Returns:
            ActionResult with backup status.
        """
        db_path = params.get('db_path', '')
        backup_path = params.get('backup_path', '')
        
        if not db_path:
            return ActionResult(success=False, message="db_path is required")
        
        if not os.path.exists(db_path):
            return ActionResult(success=False, message=f"Database not found: {db_path}")
        
        if not backup_path:
            backup_path = f"{db_path}.backup"
        
        try:
            conn = sqlite3.connect(db_path)
            backup_conn = sqlite3.connect(backup_path)
            conn.backup(backup_conn)
            backup_conn.close()
            conn.close()
            
            backup_size = os.path.getsize(backup_path)
            
            return ActionResult(
                success=True,
                message=f"Backup created: {backup_path}",
                data={'backup_path': backup_path, 'size': backup_size}
            )
            
        except sqlite3.Error as e:
            return ActionResult(
                success=False,
                message=f"Backup error: {e}",
                data={'error': str(e)}
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Backup error: {e}",
                data={'error': str(e)}
            )
