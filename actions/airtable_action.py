"""Airtable action module for RabAI AutoClick.

Provides integration with Airtable API for database operations,
record management, and automation workflows.
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


class AirtableAction(BaseAction):
    """Airtable API integration for record and table operations.

    Supports CRUD operations on Airtable tables, filtering,
    sorting, and batch operations.

    Args:
        config: Airtable configuration containing api_key and base_id
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.api_key = self.config.get("api_key", "")
        self.base_id = self.config.get("base_id", "")
        self.api_base = "https://api.airtable.com/v0"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Airtable API."""
        url = f"{self.api_base}/{endpoint}"
        if params:
            query = "&".join(f"{k}={v}" for k, v in params.items())
            url = f"{url}?{query}"

        body = json.dumps(data).encode("utf-8") if data else None
        req = Request(url, data=body, headers=self.headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def list_records(
        self,
        table: str,
        filter_by_formula: Optional[str] = None,
        sort: Optional[List[Dict]] = None,
        max_records: int = 100,
        page_size: int = 100,
        offset: Optional[str] = None,
    ) -> ActionResult:
        """List records from an Airtable table.

        Args:
            table: Table name or ID
            filter_by_formula: Airtable formula to filter records
            sort: List of sort dictionaries with 'field' and 'direction'
            max_records: Maximum number of records to return
            page_size: Records per page (max 100)
            offset: Pagination offset token

        Returns:
            ActionResult with records list
        """
        if not self.api_key or not self.base_id:
            return ActionResult(success=False, error="Missing api_key or base_id")

        params = {"pageSize": min(page_size, 100)}
        if filter_by_formula:
            params["filterByFormula"] = filter_by_formula
        if sort:
            params["sort"] = json.dumps(sort)
        if offset:
            params["offset"] = offset

        endpoint = f"{self.base_id}/{table}"
        result = self._make_request("GET", endpoint, params=params)

        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        records = result.get("records", [])
        return ActionResult(
            success=True,
            data={"records": records, "offset": result.get("offset")},
        )

    def get_record(self, table: str, record_id: str) -> ActionResult:
        """Get a single record by ID.

        Args:
            table: Table name or ID
            record_id: Airtable record ID

        Returns:
            ActionResult with record data
        """
        if not self.api_key or not self.base_id:
            return ActionResult(success=False, error="Missing api_key or base_id")

        endpoint = f"{self.base_id}/{table}/{record_id}"
        result = self._make_request("GET", endpoint)

        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def create_record(self, table: str, fields: Dict[str, Any]) -> ActionResult:
        """Create a new record in a table.

        Args:
            table: Table name or ID
            fields: Dictionary of field names and values

        Returns:
            ActionResult with created record
        """
        if not self.api_key or not self.base_id:
            return ActionResult(success=False, error="Missing api_key or base_id")

        endpoint = f"{self.base_id}/{table}"
        data = {"fields": fields}
        result = self._make_request("POST", endpoint, data=data)

        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def update_record(
        self, table: str, record_id: str, fields: Dict[str, Any]
    ) -> ActionResult:
        """Update an existing record.

        Args:
            table: Table name or ID
            record_id: Airtable record ID
            fields: Dictionary of fields to update

        Returns:
            ActionResult with updated record
        """
        if not self.api_key or not self.base_id:
            return ActionResult(success=False, error="Missing api_key or base_id")

        endpoint = f"{self.base_id}/{table}/{record_id}"
        data = {"fields": fields}
        result = self._make_request("PATCH", endpoint, data=data)

        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def delete_record(self, table: str, record_id: str) -> ActionResult:
        """Delete a record by ID.

        Args:
            table: Table name or ID
            record_id: Airtable record ID

        Returns:
            ActionResult with deletion confirmation
        """
        if not self.api_key or not self.base_id:
            return ActionResult(success=False, error="Missing api_key or base_id")

        endpoint = f"{self.base_id}/{table}/{record_id}"
        result = self._make_request("DELETE", endpoint)

        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def batch_create_records(
        self, table: str, records: List[Dict[str, Any]]
    ) -> ActionResult:
        """Create multiple records in a single request.

        Args:
            table: Table name or ID
            records: List of field dictionaries

        Returns:
            ActionResult with created records
        """
        if not self.api_key or not self.base_id:
            return ActionResult(success=False, error="Missing api_key or base_id")

        endpoint = f"{self.base_id}/{table}"
        data = {"records": [{"fields": r} for r in records]}
        result = self._make_request("POST", endpoint, data=data)

        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Airtable operation.

        Args:
            operation: Operation name (list_records, get_record, etc.)
            **kwargs: Operation-specific arguments

        Returns:
            ActionResult with operation result
        """
        operations = {
            "list_records": self.list_records,
            "get_record": self.get_record,
            "create_record": self.create_record,
            "update_record": self.update_record,
            "delete_record": self.delete_record,
            "batch_create": self.batch_create_records,
        }

        if operation not in operations:
            return ActionResult(
                success=False, error=f"Unknown operation: {operation}"
            )

        return operations[operation](**kwargs)
