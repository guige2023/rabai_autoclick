"""Sql action module for RabAI AutoClick.

Provides SQL database operations:
- SqlConnectAction: Connect to database
- SqlExecuteAction: Execute SQL query
- SqlSelectAction: Execute SELECT query
- SqlInsertAction: Execute INSERT query
- SqlDisconnectAction: Disconnect from database
"""

import sqlite3
from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SqlConnectAction(BaseAction):
    """Connect to database."""
    action_type = "sql_connect"
    display_name = "数据库连接"
    description = "连接到SQLite数据库"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute connect.

        Args:
            context: Execution context.
            params: Dict with database, output_var.

        Returns:
            ActionResult with connection status.
        """
        database = params.get('database', ':memory:')
        output_var = params.get('output_var', 'connection_status')

        try:
            resolved_db = context.resolve_value(database)
            conn = sqlite3.connect(resolved_db)
            context.set('__sql_connection__', conn)

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"数据库连接成功",
                data={
                    'database': resolved_db,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"数据库连接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['database']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'connection_status'}


class SqlExecuteAction(BaseAction):
    """Execute SQL query."""
    action_type = "sql_execute"
    display_name = "执行SQL"
    description = "执行SQL语句"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SQL.

        Args:
            context: Execution context.
            params: Dict with query, output_var.

        Returns:
            ActionResult with execution status.
        """
        query = params.get('query', '')
        output_var = params.get('output_var', 'execute_status')

        valid, msg = self.validate_type(query, str, 'query')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            conn = context.resolve_value('__sql_connection__')
            resolved_query = context.resolve_value(query)

            cursor = conn.cursor()
            cursor.execute(resolved_query)
            conn.commit()

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"SQL执行成功",
                data={
                    'query': resolved_query,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SQL执行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['query']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'execute_status'}


class SqlSelectAction(BaseAction):
    """Execute SELECT query."""
    action_type = "sql_select"
    display_name = "执行SELECT"
    description = "执行SELECT查询"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SELECT.

        Args:
            context: Execution context.
            params: Dict with query, output_var.

        Returns:
            ActionResult with query results.
        """
        query = params.get('query', '')
        output_var = params.get('output_var', 'select_result')

        valid, msg = self.validate_type(query, str, 'query')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            conn = context.resolve_value('__sql_connection__')
            resolved_query = context.resolve_value(query)

            cursor = conn.cursor()
            cursor.execute(resolved_query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            result = {
                'columns': columns,
                'rows': rows,
                'count': len(rows)
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SELECT查询成功: {len(rows)} 行",
                data={
                    'columns': columns,
                    'row_count': len(rows),
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SELECT查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['query']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'select_result'}


class SqlInsertAction(BaseAction):
    """Execute INSERT query."""
    action_type = "sql_insert"
    display_name = "执行INSERT"
    description = "执行INSERT语句"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute INSERT.

        Args:
            context: Execution context.
            params: Dict with query, output_var.

        Returns:
            ActionResult with insert status.
        """
        query = params.get('query', '')
        output_var = params.get('output_var', 'insert_status')

        valid, msg = self.validate_type(query, str, 'query')
        if not valid:
            return ActionResult(success=False, message=msg)

        try:
            conn = context.resolve_value('__sql_connection__')
            resolved_query = context.resolve_value(query)

            cursor = conn.cursor()
            cursor.execute(resolved_query)
            conn.commit()

            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"INSERT执行成功",
                data={
                    'query': resolved_query,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"INSERT执行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['query']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'insert_status'}


class SqlDisconnectAction(BaseAction):
    """Disconnect from database."""
    action_type = "sql_disconnect"
    display_name = "断开数据库连接"
    description = "断开数据库连接"
    version = "1.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute disconnect.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with disconnect status.
        """
        output_var = params.get('output_var', 'disconnect_status')

        try:
            conn = context.resolve_value('__sql_connection__')
            conn.close()

            context.set('__sql_connection__', None)
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"数据库连接已断开",
                data={
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"断开连接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'disconnect_status'}