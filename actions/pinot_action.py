"""Apache Pinot action module for RabAI AutoClick.

Provides real-time OLAP database operations via Apache Pinot Controller API
for sub-second query latency on streaming data.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class PinotAction(BaseAction):
    """Apache Pinot Controller API integration for real-time OLAP.

    Supports PQL query execution, table management, schema operations,
    and segment management.

    Args:
        config: Pinot configuration containing controller_url
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.controller_url = self.config.get("controller_url", "http://localhost:9000")
        self.api_base = f"{self.controller_url}/api"

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Pinot."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = {"Content-Type": "application/json"}

        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def execute_pql(self, query: str) -> ActionResult:
        """Execute a Pinot PQL query.

        Args:
            query: PQL query string

        Returns:
            ActionResult with query results
        """
        result = self._make_request(
            "POST", "query", data={"pql": query}
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"result": result})

    def execute_sql(self, query: str) -> ActionResult:
        """Execute a Pinot SQL query.

        Args:
            query: SQL query string

        Returns:
            ActionResult with query results
        """
        result = self._make_request(
            "POST", "query/sql", data={"sql": query}
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"result": result})

    def list_tables(self) -> ActionResult:
        """List all tables.

        Returns:
            ActionResult with tables list
        """
        result = self._make_request("GET", "tables")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"tables": result.get("tables", [])})

    def get_table(self, table_name: str) -> ActionResult:
        """Get table configuration.

        Args:
            table_name: Table name

        Returns:
            ActionResult with table config
        """
        result = self._make_request("GET", f"tables/{table_name}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def list_schemas(self) -> ActionResult:
        """List all schemas.

        Returns:
            ActionResult with schemas list
        """
        result = self._make_request("GET", "schemas")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"schemas": result.get("schemas", [])})

    def get_schema(self, schema_name: str) -> ActionResult:
        """Get schema configuration.

        Args:
            schema_name: Schema name

        Returns:
            ActionResult with schema data
        """
        result = self._make_request("GET", f"schemas/{schema_name}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def get_table_segments(self, table_name: str) -> ActionResult:
        """Get segments for a table.

        Args:
            table_name: Table name

        Returns:
            ActionResult with segments list
        """
        result = self._make_request("GET", f"tables/{table_name}/segments")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={"segments": result.get("segments", [])},
        )

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Pinot operation."""
        operations = {
            "execute_pql": self.execute_pql,
            "execute_sql": self.execute_sql,
            "list_tables": self.list_tables,
            "get_table": self.get_table,
            "list_schemas": self.list_schemas,
            "get_schema": self.get_schema,
            "get_table_segments": self.get_table_segments,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
