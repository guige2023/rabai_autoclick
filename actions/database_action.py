"""Database operations action module for RabAI AutoClick.

Provides database operations:
- DbConnectAction: Connect to database
- DbQueryAction: Execute query
- DbInsertAction: Insert records
- DbUpdateAction: Update records
- DbDeleteAction: Delete records
- DbTransactionAction: Transaction management
"""

import json
import sqlite3
from typing import Any, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DbConnectAction(BaseAction):
    """Connect to database."""
    action_type = "db_connect"
    display_name = "数据库连接"
    description = "连接数据库"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            db_path = params.get("db_path", ":memory:")
            timeout = params.get("timeout", 5)

            conn = sqlite3.connect(db_path, timeout=timeout)
            conn.row_factory = sqlite3.Row

            return ActionResult(
                success=True,
                message=f"Connected to {db_path}",
                data={"db_path": db_path, "connected": True}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Connect error: {str(e)}")


class DbQueryAction(BaseAction):
    """Execute query."""
    action_type = "db_query"
    display_name = "数据库查询"
    description = "执行SQL查询"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            db_path = params.get("db_path", ":memory:")
            query = params.get("query", "")
            params_list = params.get("params", [])
            fetch_size = params.get("fetch_size", 100)

            if not query:
                return ActionResult(success=False, message="query is required")

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            if params_list:
                cursor.execute(query, params_list)
            else:
                cursor.execute(query)

            if query.strip().upper().startswith(("SELECT", "PRAGMA", "EXPLAIN")):
                rows = cursor.fetchmany(fetch_size)
                results = [dict(row) for row in rows]
                has_more = len(cursor.fetchmany(1)) > 0
                conn.close()

                return ActionResult(
                    success=True,
                    message=f"Query returned {len(results)} rows",
                    data={"rows": results, "count": len(results), "has_more": has_more}
                )
            else:
                conn.commit()
                rows_affected = cursor.rowcount
                last_row_id = cursor.lastrowid
                conn.close()

                return ActionResult(
                    success=True,
                    message=f"Query executed: {rows_affected} rows affected",
                    data={"rows_affected": rows_affected, "last_row_id": last_row_id}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Query error: {str(e)}")


class DbInsertAction(BaseAction):
    """Insert records."""
    action_type = "db_insert"
    display_name = "数据库插入"
    description = "插入记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            db_path = params.get("db_path", ":memory:")
            table = params.get("table", "")
            records = params.get("records", [])
            or_mode = params.get("or_mode", "REPLACE")

            if not table:
                return ActionResult(success=False, message="table is required")

            if not records:
                return ActionResult(success=False, message="No records to insert")

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            inserted = 0
            for record in records:
                if not isinstance(record, dict):
                    continue

                columns = list(record.keys())
                placeholders = ["?"] * len(columns)
                values = list(record.values())

                query = f"INSERT OR {or_mode} INTO {table} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"

                try:
                    cursor.execute(query, values)
                    inserted += 1
                except Exception as e:
                    pass

            conn.commit()
            conn.close()

            return ActionResult(
                success=True,
                message=f"Inserted {inserted}/{len(records)} records",
                data={"inserted": inserted, "total": len(records)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Insert error: {str(e)}")


class DbUpdateAction(BaseAction):
    """Update records."""
    action_type = "db_update"
    display_name: "数据库更新"
    description = "更新记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            db_path = params.get("db_path", ":memory:")
            table = params.get("table", "")
            set_values = params.get("set", {})
            where = params.get("where", "")
            where_params = params.get("where_params", [])

            if not table:
                return ActionResult(success=False, message="table is required")

            if not set_values:
                return ActionResult(success=False, message="set values required")

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            set_clause = ", ".join([f"{k} = ?" for k in set_values.keys()])
            values = list(set_values.values()) + where_params

            query = f"UPDATE {table} SET {set_clause}"
            if where:
                query += f" WHERE {where}"

            cursor.execute(query, values)
            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()

            return ActionResult(
                success=True,
                message=f"Updated {rows_affected} rows",
                data={"rows_affected": rows_affected}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Update error: {str(e)}")


class DbDeleteAction(BaseAction):
    """Delete records."""
    action_type = "db_delete"
    display_name = "数据库删除"
    description = "删除记录"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            db_path = params.get("db_path", ":memory:")
            table = params.get("table", "")
            where = params.get("where", "")
            where_params = params.get("where_params", [])

            if not table:
                return ActionResult(success=False, message="table is required")

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            query = f"DELETE FROM {table}"
            if where:
                query += f" WHERE {where}"

            cursor.execute(query, where_params)
            conn.commit()
            rows_affected = cursor.rowcount
            conn.close()

            return ActionResult(
                success=True,
                message=f"Deleted {rows_affected} rows",
                data={"rows_affected": rows_affected}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Delete error: {str(e)}")


class DbTransactionAction(BaseAction):
    """Transaction management."""
    action_type = "db_transaction"
    display_name = "数据库事务"
    description = "事务管理"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            db_path = params.get("db_path", ":memory:")
            action = params.get("action", "begin")
            queries = params.get("queries", [])

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            if action == "begin":
                conn.isolation_level = None
                cursor.execute("BEGIN")
                return ActionResult(success=True, message="Transaction started")

            elif action == "commit":
                conn.isolation_level = None
                cursor.execute("COMMIT")
                conn.close()
                return ActionResult(success=True, message="Transaction committed")

            elif action == "rollback":
                conn.isolation_level = None
                cursor.execute("ROLLBACK")
                conn.close()
                return ActionResult(success=True, message="Transaction rolled back")

            elif action == "execute":
                if not queries:
                    conn.close()
                    return ActionResult(success=False, message="No queries to execute")

                conn.isolation_level = None
                cursor.execute("BEGIN")

                try:
                    for q in queries:
                        sql = q.get("sql", "")
                        q_params = q.get("params", [])
                        if sql:
                            cursor.execute(sql, q_params)

                    cursor.execute("COMMIT")
                    conn.close()

                    return ActionResult(
                        success=True,
                        message=f"Executed {len(queries)} queries",
                        data={"queries_executed": len(queries)}
                    )

                except Exception as e:
                    cursor.execute("ROLLBACK")
                    conn.close()
                    return ActionResult(success=False, message=f"Transaction failed: {str(e)}")

            conn.close()
            return ActionResult(success=False, message=f"Unknown action: {action}")

        except Exception as e:
            return ActionResult(success=False, message=f"Transaction error: {str(e)}")
