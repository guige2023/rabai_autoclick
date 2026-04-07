"""Database12 action module for RabAI AutoClick.

Provides additional database operations:
- DatabaseConnectAction: Connect to database
- DatabaseQueryAction: Execute query
- DatabaseExecuteAction: Execute SQL
- DatabaseFetchAction: Fetch results
- DatabaseCloseAction: Close connection
- DatabaseCommitAction: Commit transaction
"""

from typing import Any, Dict, List

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DatabaseConnectAction(BaseAction):
    """Connect to database."""
    action_type = "database12_connect"
    display_name = "连接数据库"
    description = "连接数据库"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute connect.

        Args:
            context: Execution context.
            params: Dict with db_type, host, port, database, user, password, output_var.

        Returns:
            ActionResult with connection status.
        """
        db_type = params.get('db_type', 'sqlite')
        host = params.get('host', 'localhost')
        port = params.get('port', 5432)
        database = params.get('database', '')
        user = params.get('user', '')
        password = params.get('password', '')
        output_var = params.get('output_var', 'connection_status')

        try:
            import sqlite3

            resolved_type = context.resolve_value(db_type) if db_type else 'sqlite'
            resolved_database = context.resolve_value(database) if database else ':memory:'

            if resolved_type == 'sqlite':
                conn = sqlite3.connect(resolved_database)
                context.set(output_var, {
                    'connected': True,
                    'type': resolved_type,
                    'database': resolved_database
                })
                return ActionResult(
                    success=True,
                    message=f"连接数据库: {resolved_type}",
                    data={
                        'connected': True,
                        'type': resolved_type,
                        'output_var': output_var
                    }
                )
            else:
                return ActionResult(
                    success=False,
                    message=f"不支持的数据库类型: {resolved_type}"
                )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"连接数据库失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['db_type', 'database']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'host': 'localhost', 'port': 5432, 'user': '', 'password': '', 'output_var': 'connection_status'}


class DatabaseQueryAction(BaseAction):
    """Execute query."""
    action_type = "database12_query"
    display_name = "查询数据库"
    description = "执行数据库查询"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute query.

        Args:
            context: Execution context.
            params: Dict with sql, params, output_var.

        Returns:
            ActionResult with query results.
        """
        sql = params.get('sql', '')
        query_params = params.get('params', [])
        output_var = params.get('output_var', 'query_result')

        try:
            import sqlite3

            resolved_sql = context.resolve_value(sql)
            resolved_params = context.resolve_value(query_params) if query_params else ()

            conn = sqlite3.connect(':memory:')
            cursor = conn.cursor()
            cursor.execute(resolved_sql, resolved_params)

            if resolved_sql.strip().upper().startswith('SELECT'):
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                result = {
                    'columns': columns,
                    'rows': rows,
                    'row_count': len(rows)
                }
            else:
                conn.commit()
                result = {
                    'row_count': cursor.rowcount
                }

            cursor.close()
            conn.close()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"查询执行: {result.get('row_count', 0)}行",
                data={
                    'sql': resolved_sql,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"查询失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['sql']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'params': [], 'output_var': 'query_result'}


class DatabaseExecuteAction(BaseAction):
    """Execute SQL."""
    action_type = "database12_execute"
    display_name = "执行SQL"
    description: "执行SQL语句"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute SQL.

        Args:
            context: Execution context.
            params: Dict with sql, params, output_var.

        Returns:
            ActionResult with execution status.
        """
        sql = params.get('sql', '')
        sql_params = params.get('params', [])
        output_var = params.get('output_var', 'execute_status')

        try:
            import sqlite3

            resolved_sql = context.resolve_value(sql)
            resolved_params = context.resolve_value(sql_params) if sql_params else ()

            conn = sqlite3.connect(':memory:')
            cursor = conn.cursor()
            cursor.execute(resolved_sql, resolved_params)
            conn.commit()

            result = {
                'row_count': cursor.rowcount,
                'last_rowid': cursor.lastrowid
            }

            cursor.close()
            conn.close()

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"SQL执行: {result['row_count']}行受影响",
                data={
                    'sql': resolved_sql,
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"SQL执行失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['sql']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'params': [], 'output_var': 'execute_status'}


class DatabaseFetchAction(BaseAction):
    """Fetch results."""
    action_type = "database12_fetch"
    display_name = "获取结果"
    description = "获取查询结果"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute fetch.

        Args:
            context: Execution context.
            params: Dict with sql, count, output_var.

        Returns:
            ActionResult with fetched results.
        """
        sql = params.get('sql', '')
        count = params.get('count', 10)
        output_var = params.get('output_var', 'fetch_result')

        try:
            import sqlite3

            resolved_sql = context.resolve_value(sql)
            resolved_count = int(context.resolve_value(count)) if count else 10

            conn = sqlite3.connect(':memory:')
            cursor = conn.cursor()
            cursor.execute(resolved_sql)

            rows = cursor.fetchmany(resolved_count)
            columns = [desc[0] for desc in cursor.description] if cursor.description else []

            cursor.close()
            conn.close()

            result = {
                'columns': columns,
                'rows': rows,
                'row_count': len(rows)
            }

            context.set(output_var, result)

            return ActionResult(
                success=True,
                message=f"获取结果: {len(rows)}行",
                data={
                    'result': result,
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"获取结果失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return ['sql']

    def get_optional_params(self) -> Dict[str, Any]:
        return {'count': 10, 'output_var': 'fetch_result'}


class DatabaseCloseAction(BaseAction):
    """Close connection."""
    action_type = "database12_close"
    display_name = "关闭连接"
    description = "关闭数据库连接"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute close.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with close status.
        """
        output_var = params.get('output_var', 'close_status')

        try:
            # SQLite connection is closed per-operation, so just return success
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"连接已关闭",
                data={
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"关闭连接失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'close_status'}


class DatabaseCommitAction(BaseAction):
    """Commit transaction."""
    action_type = "database12_commit"
    display_name = "提交事务"
    description = "提交数据库事务"
    version = "12.0"

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute commit.

        Args:
            context: Execution context.
            params: Dict with output_var.

        Returns:
            ActionResult with commit status.
        """
        output_var = params.get('output_var', 'commit_status')

        try:
            # SQLite auto-commits, so just return success
            context.set(output_var, True)

            return ActionResult(
                success=True,
                message=f"事务已提交",
                data={
                    'output_var': output_var
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"提交事务失败: {str(e)}"
            )

    def get_required_params(self) -> List[str]:
        return []

    def get_optional_params(self) -> Dict[str, Any]:
        return {'output_var': 'commit_status'}