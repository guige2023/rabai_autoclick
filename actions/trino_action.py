"""Trino action module for RabAI AutoClick.

Provides distributed SQL query engine operations via Trino API
for federated queries across diverse data sources.
"""

import json
import time
import sys
import os
from typing import Any, Dict, List, Optional, Union
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import hashlib
import hmac
import base64

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TrinoAction(BaseAction):
    """Trino/Presto distributed SQL integration.

    Supports SQL queries, catalog/table operations, session management,
    and query cancellation.

    Args:
        config: Trino configuration containing host, port, user, and catalog
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.host = self.config.get("host", "localhost")
        self.port = self.config.get("port", 8080)
        self.user = self.config.get("user", "user")
        self.catalog = self.config.get("catalog", "")
        self.schema = self.config.get("schema", "default")
        self.password = self.config.get("password", "")
        self.api_base = f"http://{self.host}:{self.port}/v1"
        self.headers = {
            "X-Trino-User": self.user,
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Trino."""
        url = f"{self.api_base}/{endpoint}"
        body = json.dumps(data).encode("utf-8") if data else None
        headers = dict(self.headers)
        if body:
            headers["Content-Type"] = "application/json"
        if self.password:
            credentials = f"{self.user}:{self.password}"
            headers["Authorization"] = f"Basic {base64.b64encode(credentials.encode()).decode()}"

        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=60) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def execute_query(
        self,
        query: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> ActionResult:
        """Execute a SQL query.

        Args:
            query: SQL query string
            catalog: Optional catalog to use
            schema: Optional schema to use

        Returns:
            ActionResult with query ID for polling
        """
        if not self.host:
            return ActionResult(success=False, error="Missing host")

        cat = catalog or self.catalog
        sch = schema or self.schema

        payload = {
            "query": query,
            "catalog": cat,
            "schema": sch,
        }

        result = self._make_request("POST", "statement", data=payload)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(
            success=True,
            data={
                "query_id": result.get("id"),
                "stats": result.get("stats"),
            },
        )

    def get_query_results(self, query_id: str) -> ActionResult:
        """Get query results by ID.

        Args:
            query_id: Query ID from execute_query

        Returns:
            ActionResult with results and status
        """
        result = self._make_request("GET", f"query/{query_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        data = result.get("data", [])
        columns = result.get("columns", [])

        return ActionResult(
            success=True,
            data={
                "columns": columns,
                "rows": data,
                "stats": result.get("stats"),
            },
        )

    def cancel_query(self, query_id: str) -> ActionResult:
        """Cancel a running query.

        Args:
            query_id: Query ID to cancel

        Returns:
            ActionResult with cancellation status
        """
        result = self._make_request("DELETE", f"query/{query_id}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"cancelled": True})

    def list_catalogs(self) -> ActionResult:
        """List available catalogs.

        Returns:
            ActionResult with catalog list
        """
        result = self._make_request("GET", "catalog")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"catalogs": result})

    def list_schemas(
        self,
        catalog: Optional[str] = None,
    ) -> ActionResult:
        """List schemas in a catalog.

        Args:
            catalog: Catalog name (uses config default)

        Returns:
            ActionResult with schema list
        """
        cat = catalog or self.catalog
        if not cat:
            return ActionResult(success=False, error="Missing catalog")

        result = self._make_request("GET", f"catalog/{cat}/schemas")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"schemas": result})

    def list_tables(
        self,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> ActionResult:
        """List tables in a schema.

        Args:
            catalog: Catalog name
            schema: Schema name

        Returns:
            ActionResult with table list
        """
        cat = catalog or self.catalog
        sch = schema or self.schema
        if not cat or not sch:
            return ActionResult(success=False, error="Missing catalog or schema")

        result = self._make_request("GET", f"catalog/{cat}/schemas/{sch}/tables")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"tables": result})

    def get_table_metadata(
        self,
        table: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> ActionResult:
        """Get table column metadata.

        Args:
            table: Table name
            catalog: Catalog name
            schema: Schema name

        Returns:
            ActionResult with column metadata
        """
        cat = catalog or self.catalog
        sch = schema or self.schema
        if not cat or not sch:
            return ActionResult(success=False, error="Missing catalog or schema")

        result = self._make_request(
            "GET", f"catalog/{cat}/schemas/{sch}/tables/{table}/columns"
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"columns": result})

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Trino operation.

        Args:
            operation: Operation name
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "execute_query": self.execute_query,
            "get_query_results": self.get_query_results,
            "cancel_query": self.cancel_query,
            "list_catalogs": self.list_catalogs,
            "list_schemas": self.list_schemas,
            "list_tables": self.list_tables,
            "get_table_metadata": self.get_table_metadata,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
