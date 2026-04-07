"""SQLite database action module for RabAI AutoClick.

Provides SQLite operations:
- SqliteConnectAction: Connect to SQLite database
- SqliteCreateTableAction: Create table
- SqliteInsertAction: Insert rows
- SqliteSelectAction: Select rows
- SqliteUpdateAction: Update rows
- SqliteDeleteAction: Delete rows
- SqliteDropTableAction: Drop table
- SqliteExecuteAction: Execute raw SQL
- SqliteDescribeAction: Describe table schema
- SqliteListTablesAction: List all tables
"""

from __future__ import annotations

import sqlite3
import json
import sys
from typing import Any, Dict, List, Optional

import os as _os
_parent_dir = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SqliteConnectAction(BaseAction):
    """Connect to SQLite database."""
    action_type = "sqlite_connect"
    display_name = "SQLite连接"
    description = "连接到SQLite数据库"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute SQLite connect."""
        database = params.get('database', ':memory:')
        output_var = params.get('output_var', 'sqlite_conn')

        try:
            resolved_db = context.resolve_value(database) if context else database
            conn = sqlite3.connect(resolved_db)
            conn.row_factory = sqlite3.Row

            result = {'connected': True, 'database': resolved_db}
            if context:
                context.set(output_var, conn)
            return ActionResult(success=True, message=f"Connected to {resolved_db}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"SQLite connect error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': ':memory:', 'output_var': 'sqlite_conn'}


class SqliteCreateTableAction(BaseAction):
    """Create SQLite table."""
    action_type = "sqlite_create_table"
    display_name = "SQLite创建表"
    description = "创建SQLite表"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute create table."""
        database = params.get('database', ':memory:')
        table_name = params.get('table_name', '')
        columns = params.get('columns', [])  # [{name, type, primary_key, not_null, default}]
        if_not_exists = params.get('if_not_exists', True)

        if not table_name or not columns:
            return ActionResult(success=False, message="table_name and columns are required")

        try:
            resolved_db = context.resolve_value(database) if context else database
            resolved_table = context.resolve_value(table_name) if context else table_name
            resolved_columns = context.resolve_value(columns) if context else columns

            conn = sqlite3.connect(resolved_db)
            cursor = conn.cursor()

            col_defs = []
            for col in resolved_columns:
                col_name = col.get('name', '')
                col_type = col.get('type', 'TEXT')
                primary = col.get('primary_key', False)
                not_null = col.get('not_null', False)
                default = col.get('default', None)

                col_str = f"{col_name} {col_type}"
                if primary:
                    col_str += " PRIMARY KEY"
                if not_null:
                    col_str += " NOT NULL"
                if default is not None:
                    col_str += f" DEFAULT {repr(default)}"
                col_defs.append(col_str)

            sql = f"CREATE TABLE {'IF NOT EXISTS' if if_not_exists else ''} {resolved_table} ({', '.join(col_defs)})"
            cursor.execute(sql)
            conn.commit()
            conn.close()

            return ActionResult(success=True, message=f"Table {resolved_table} created", data={'table': resolved_table})
        except Exception as e:
            return ActionResult(success=False, message=f"Create table error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['table_name', 'columns']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': ':memory:', 'if_not_exists': True}


class SqliteInsertAction(BaseAction):
    """Insert rows into SQLite table."""
    action_type = "sqlite_insert"
    display_name = "SQLite插入"
    description = "向SQLite表插入数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute insert."""
        database = params.get('database', ':memory:')
        table_name = params.get('table_name', '')
        rows = params.get('rows', [])  # list of dicts or list of lists
        output_var = params.get('output_var', 'insert_result')

        if not table_name or not rows:
            return ActionResult(success=False, message="table_name and rows are required")

        try:
            resolved_db = context.resolve_value(database) if context else database
            resolved_table = context.resolve_value(table_name) if context else table_name
            resolved_rows = context.resolve_value(rows) if context else rows

            conn = sqlite3.connect(resolved_db)
            cursor = conn.cursor()

            first_row = resolved_rows[0]
            if isinstance(first_row, dict):
                columns = list(first_row.keys())
                placeholders = ','.join(['?' for _ in columns])
                sql = f"INSERT INTO {resolved_table} ({','.join(columns)}) VALUES ({placeholders})"
                for row in resolved_rows:
                    cursor.execute(sql, [row.get(c) for c in columns])
            else:
                placeholders = ','.join(['?' for _ in first_row])
                sql = f"INSERT INTO {resolved_table} VALUES ({placeholders})"
                for row in resolved_rows:
                    cursor.execute(sql, row)

            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()

            result = {'rows_inserted': len(resolved_rows), 'table': resolved_table}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Inserted {len(resolved_rows)} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Insert error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['table_name', 'rows']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': ':memory:', 'output_var': 'insert_result'}


class SqliteSelectAction(BaseAction):
    """Select rows from SQLite table."""
    action_type = "sqlite_select"
    display_name = "SQLite查询"
    description = "查询SQLite表"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute select."""
        database = params.get('database', ':memory:')
        table_name = params.get('table_name', '')
        columns = params.get('columns', '*')
        where = params.get('where', '')
        where_params = params.get('where_params', [])
        order_by = params.get('order_by', '')
        limit = params.get('limit', None)
        output_var = params.get('output_var', 'select_result')

        if not table_name:
            return ActionResult(success=False, message="table_name is required")

        try:
            resolved_db = context.resolve_value(database) if context else database
            resolved_table = context.resolve_value(table_name) if context else table_name
            resolved_columns = context.resolve_value(columns) if context else columns
            resolved_limit = context.resolve_value(limit) if context else limit

            conn = sqlite3.connect(resolved_db)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            col_str = ','.join(resolved_columns) if isinstance(resolved_columns, list) else resolved_columns
            sql = f"SELECT {col_str} FROM {resolved_table}"
            params_list = []

            if where:
                resolved_where = context.resolve_value(where) if context else where
                sql += f" WHERE {resolved_where}"
                if where_params:
                    resolved_params = context.resolve_value(where_params) if context else where_params
                    params_list = resolved_params

            if order_by:
                resolved_order = context.resolve_value(order_by) if context else order_by
                sql += f" ORDER BY {resolved_order}"

            if resolved_limit:
                sql += f" LIMIT {resolved_limit}"

            cursor.execute(sql, params_list)
            rows = [dict(row) for row in cursor.fetchall()]
            conn.close()

            result = {'rows': rows, 'count': len(rows), 'columns': col_str}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Selected {len(rows)} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Select error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['table_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            'database': ':memory:', 'columns': '*', 'where': '', 'where_params': [],
            'order_by': '', 'limit': None, 'output_var': 'select_result'
        }


class SqliteUpdateAction(BaseAction):
    """Update rows in SQLite table."""
    action_type = "sqlite_update"
    display_name = "SQLite更新"
    description = "更新SQLite表数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute update."""
        database = params.get('database', ':memory:')
        table_name = params.get('table_name', '')
        set_values = params.get('set_values', {})
        where = params.get('where', '')
        output_var = params.get('output_var', 'update_result')

        if not table_name or not set_values:
            return ActionResult(success=False, message="table_name and set_values are required")

        try:
            resolved_db = context.resolve_value(database) if context else database
            resolved_table = context.resolve_value(table_name) if context else table_name
            resolved_set = context.resolve_value(set_values) if context else set_values
            resolved_where = context.resolve_value(where) if context else where

            conn = sqlite3.connect(resolved_db)
            cursor = conn.cursor()

            set_clause = ', '.join([f"{k}=?" for k in resolved_set.keys()])
            sql = f"UPDATE {resolved_table} SET {set_clause}"
            params_list = list(resolved_set.values())

            if resolved_where:
                sql += f" WHERE {resolved_where}"

            cursor.execute(sql, params_list)
            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()

            result = {'rows_affected': rows_affected}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Updated {rows_affected} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Update error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['table_name', 'set_values']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': ':memory:', 'where': '', 'output_var': 'update_result'}


class SqliteDeleteAction(BaseAction):
    """Delete rows from SQLite table."""
    action_type = "sqlite_delete"
    display_name = "SQLite删除"
    description = "删除SQLite表数据"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute delete."""
        database = params.get('database', ':memory:')
        table_name = params.get('table_name', '')
        where = params.get('where', '')
        output_var = params.get('output_var', 'delete_result')

        if not table_name:
            return ActionResult(success=False, message="table_name is required")

        try:
            resolved_db = context.resolve_value(database) if context else database
            resolved_table = context.resolve_value(table_name) if context else table_name
            resolved_where = context.resolve_value(where) if context else where

            conn = sqlite3.connect(resolved_db)
            cursor = conn.cursor()

            sql = f"DELETE FROM {resolved_table}"
            if resolved_where:
                sql += f" WHERE {resolved_where}"

            cursor.execute(sql)
            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()

            result = {'rows_deleted': rows_affected}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Deleted {rows_affected} rows", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Delete error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['table_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': ':memory:', 'where': '', 'output_var': 'delete_result'}


class SqliteExecuteAction(BaseAction):
    """Execute raw SQL in SQLite."""
    action_type = "sqlite_execute"
    display_name = "SQLite执行SQL"
    description = "执行原始SQL语句"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute raw SQL."""
        database = params.get('database', ':memory:')
        sql = params.get('sql', '')
        params_list = params.get('params', [])
        output_var = params.get('output_var', 'execute_result')

        if not sql:
            return ActionResult(success=False, message="sql is required")

        try:
            resolved_db = context.resolve_value(database) if context else database
            resolved_sql = context.resolve_value(sql) if context else sql
            resolved_params = context.resolve_value(params_list) if context else params_list

            conn = sqlite3.connect(resolved_db)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(resolved_sql, resolved_params)

            if resolved_sql.strip().upper().startswith('SELECT'):
                rows = [dict(row) for row in cursor.fetchall()]
                result = {'rows': rows, 'count': len(rows)}
            else:
                conn.commit()
                result = {'rows_affected': cursor.rowcount, 'lastrowid': cursor.lastrowid}

            conn.close()
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message="SQL executed", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Execute SQL error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['sql']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': ':memory:', 'params': [], 'output_var': 'execute_result'}


class SqliteDescribeAction(BaseAction):
    """Describe SQLite table schema."""
    action_type = "sqlite_describe"
    display_name = "SQLite表结构"
    description = "查看表结构"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute describe."""
        database = params.get('database', ':memory:')
        table_name = params.get('table_name', '')
        output_var = params.get('output_var', 'table_schema')

        if not table_name:
            return ActionResult(success=False, message="table_name is required")

        try:
            resolved_db = context.resolve_value(database) if context else database
            resolved_table = context.resolve_value(table_name) if context else table_name

            conn = sqlite3.connect(resolved_db)
            cursor = conn.cursor()
            cursor.execute(f"PRAGMA table_info({resolved_table})")
            columns = [{'cid': row[0], 'name': row[1], 'type': row[2], 'notnull': row[3], 'default': row[4], 'pk': row[5]} for row in cursor.fetchall()]
            conn.close()

            result = {'table': resolved_table, 'columns': columns}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Schema for {resolved_table}", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"Describe error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ['table_name']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': ':memory:', 'output_var': 'table_schema'}


class SqliteListTablesAction(BaseAction):
    """List all tables in SQLite database."""
    action_type = "sqlite_list_tables"
    display_name = "SQLite表列表"
    description = "列出所有表"
    version = "1.0"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute list tables."""
        database = params.get('database', ':memory:')
        output_var = params.get('output_var', 'tables_list')

        try:
            resolved_db = context.resolve_value(database) if context else database

            conn = sqlite3.connect(resolved_db)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
            conn.close()

            result = {'tables': tables, 'count': len(tables)}
            if context:
                context.set(output_var, result)
            return ActionResult(success=True, message=f"Found {len(tables)} tables", data=result)
        except Exception as e:
            return ActionResult(success=False, message=f"List tables error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'database': ':memory:', 'output_var': 'tables_list'}
