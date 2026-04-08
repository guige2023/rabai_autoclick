"""ClickHouse action module for RabAI AutoClick.

Provides analytics database operations via ClickHouse HTTP interface
for high-performance OLAP queries and data ingestion.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
from urllib.parse import urlencode

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ClickHouseAction(BaseAction):
    """ClickHouse HTTP interface integration for analytics.

    Supports query execution, table operations, data insertion,
    and database management.

    Args:
        config: ClickHouse configuration containing host, port, database,
                user, and password
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.host = self.config.get("host", "localhost")
        self.port = self.config.get("port", 8123)
        self.database = self.config.get("database", "default")
        self.user = self.config.get("user", "default")
        self.password = self.config.get("password", "")
        self.api_base = f"http://{self.host}:{self.port}"

    def _make_request(
        self,
        query: str,
        params: Optional[Dict] = None,
        data: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to ClickHouse."""
        url = f"{self.api_base}/?database={self.database}&user={self.user}"
        if self.password:
            url += f"&password={self.password}"

        if params:
            for k, v in params.items():
                url += f"&{k}={v}"

        headers = {}
        if data:
            headers["Content-Type"] = "text/plain"

        req = Request(
            url,
            data=data.encode("utf-8") if data else query.encode("utf-8"),
            headers=headers,
            method="POST" if data else "POST",
        )

        try:
            with urlopen(req, timeout=60) as response:
                result = response.read().decode("utf-8")
                return {"data": result, "success": True}
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def execute_query(
        self,
        query: str,
        params: Optional[Dict] = None,
        stream: bool = False,
    ) -> ActionResult:
        """Execute a ClickHouse SQL query.

        Args:
            query: SQL query string
            params: Optional query parameters
            stream: Whether to stream results

        Returns:
            ActionResult with query results
        """
        result = self._make_request(query, params=params)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"result": result["data"]})

    def select(
        self,
        table: str,
        columns: str = "*",
        where: Optional[str] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
    ) -> ActionResult:
        """Select data from a table.

        Args:
            table: Table name
            columns: Column list (default *)
            where: Optional WHERE clause
            order_by: Optional ORDER BY clause
            limit: Maximum rows to return

        Returns:
            ActionResult with rows
        """
        query = f"SELECT {columns} FROM {table}"
        if where:
            query += f" WHERE {where}"
        if order_by:
            query += f" ORDER BY {order_by}"
        query += f" LIMIT {limit}"

        return self.execute_query(query)

    def insert(
        self,
        table: str,
        columns: List[str],
        values: List[List[Any]],
    ) -> ActionResult:
        """Insert rows into a table.

        Args:
            table: Table name
            columns: List of column names
            values: List of row value lists

        Returns:
            ActionResult with insert status
        """
        columns_str = ", ".join(columns)
        values_str = "\n".join(
            "\t".join(str(v) for v in row) for row in values
        )
        query = f"INSERT INTO {table} ({columns_str}) FORMAT TabSeparated\n{values_str}"

        result = self._make_request(query, data=values_str)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"inserted": len(values)})

    def create_table(
        self,
        table: str,
        columns: Dict[str, str],
        engine: str = "MergeTree()",
        order_by: Optional[str] = None,
    ) -> ActionResult:
        """Create a table.

        Args:
            table: Table name
            columns: Dict of column_name -> type
            engine: Table engine (default MergeTree)
            order_by: ORDER BY key for MergeTree

        Returns:
            ActionResult with creation status
        """
        col_defs = ", ".join(f"{k} {v}" for k, v in columns.items())
        if order_by:
            query = f"CREATE TABLE {table} ({col_defs}) ENGINE {engine} ORDER BY {order_by}"
        else:
            query = f"CREATE TABLE {table} ({col_defs}) ENGINE {engine}"

        result = self._make_request(query)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"table": table, "created": True})

    def drop_table(self, table: str) -> ActionResult:
        """Drop a table.

        Args:
            table: Table name

        Returns:
            ActionResult with drop status
        """
        result = self._make_request(f"DROP TABLE {table}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"table": table, "dropped": True})

    def show_tables(self) -> ActionResult:
        """Show all tables in the database.

        Returns:
            ActionResult with table names
        """
        result = self._make_request("SHOW TABLES")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"tables": result["data"].splitlines()})

    def describe_table(self, table: str) -> ActionResult:
        """Describe table structure.

        Args:
            table: Table name

        Returns:
            ActionResult with column definitions
        """
        result = self._make_request(f"DESCRIBE TABLE {table}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"structure": result["data"]})

    def get_server_info(self) -> ActionResult:
        """Get ClickHouse server information.

        Returns:
            ActionResult with server info
        """
        result = self._make_request("SELECT version()")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"version": result["data"].strip()})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute ClickHouse operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "execute_query": self.execute_query,
            "select": self.select,
            "insert": self.insert,
            "create_table": self.create_table,
            "drop_table": self.drop_table,
            "show_tables": self.show_tables,
            "describe_table": self.describe_table,
            "get_server_info": self.get_server_info,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
