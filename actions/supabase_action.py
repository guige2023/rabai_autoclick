"""Supabase action module for RabAI AutoClick.

Provides Postgres database and auth operations via Supabase API
for scalable backend services.
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


class SupabaseAction(BaseAction):
    """Supabase API integration for Postgres and auth operations.

    Supports table CRUD, auth management, storage, and edge functions.

    Args:
        config: Supabase configuration containing url and anon_key
    """

    def __init__(self, config: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.url = self.config.get("url", "")
        self.anon_key = self.config.get("anon_key", "")
        self.service_key = self.config.get("service_key", "")
        self.headers = {
            "apikey": self.anon_key,
            "Content-Type": "application/json",
        }

    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to Supabase."""
        url = f"{self.url}/rest/v1/{endpoint}"
        if params:
            url += "?" + "&".join(f"{k}={v}" for k, v in params.items())

        body = json.dumps(data).encode("utf-8") if data else None
        headers = dict(self.headers)
        if self.service_key:
            headers["Authorization"] = f"Bearer {self.service_key}"

        req = Request(url, data=body, headers=headers, method=method)

        try:
            with urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode("utf-8"))
                return result if isinstance(result, list) else result
        except HTTPError as e:
            error_body = e.read().decode("utf-8") if e.fp else ""
            return {"error": f"HTTP {e.code}: {error_body}", "success": False}
        except URLError as e:
            return {"error": f"URL error: {e.reason}", "success": False}

    def select(
        self,
        table: str,
        columns: str = "*",
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: int = 100,
    ) -> ActionResult:
        """Select rows from a table.

        Args:
            table: Table name
            columns: Column list (default *)
            filters: Dict of column -> value filters
            order_by: Ordering specification
            limit: Maximum rows

        Returns:
            ActionResult with rows
        """
        params = {"select": columns, "limit": limit}
        if order_by:
            params["order"] = order_by

        if filters:
            for col, val in filters.items():
                params[col] = f"eq.{val}"

        result = self._make_request("GET", table, params=params)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        rows = result if isinstance(result, list) else result.get("data", [])
        return ActionResult(success=True, data={"rows": rows})

    def insert(
        self,
        table: str,
        values: Union[Dict, List[Dict]],
        upsert: bool = False,
    ) -> ActionResult:
        """Insert rows into a table.

        Args:
            table: Table name
            values: Row dict or list of row dicts
            upsert: Whether to upsert on conflict

        Returns:
            ActionResult with inserted rows
        """
        headers = {"Prefer": "return=representation"}
        if upsert:
            headers["Prefer"] += ",resolution=merge-duplicates"

        body = values if isinstance(values, list) else [values]
        result = self._make_request("POST", table, data=body)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"inserted": len(body)})

    def update(
        self,
        table: str,
        values: Dict[str, Any],
        filters: Dict[str, Any],
    ) -> ActionResult:
        """Update rows in a table.

        Args:
            table: Table name
            values: Dict of columns to update
            filters: Dict of column -> value filters

        Returns:
            ActionResult with update status
        """
        params = {f"{k}=eq.{v}" for k, v in filters.items()}
        params_str = "&".join(params)

        result = self._make_request(
            "PATCH",
            f"{table}?{params_str}",
            data=values,
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"updated": True})

    def delete(
        self,
        table: str,
        filters: Dict[str, Any],
    ) -> ActionResult:
        """Delete rows from a table.

        Args:
            table: Table name
            filters: Dict of column -> value filters

        Returns:
            ActionResult with deletion status
        """
        params = {f"{k}=eq.{v}" for k, v in filters.items()}
        params_str = "&".join(params)

        result = self._make_request("DELETE", f"{table}?{params_str}")
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"deleted": True})

    def rpc(
        self,
        function: str,
        params: Optional[Dict] = None,
    ) -> ActionResult:
        """Call a Postgres stored procedure/function.

        Args:
            function: Function name
            params: Function arguments

        Returns:
            ActionResult with function result
        """
        result = self._make_request(
            "POST",
            f"rpc/{function}",
            data=params or {},
        )
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data={"result": result})

    def sign_up(
        self,
        email: str,
        password: str,
        metadata: Optional[Dict] = None,
    ) -> ActionResult:
        """Sign up a new user.

        Args:
            email: User email
            password: User password
            metadata: Optional user metadata

        Returns:
            ActionResult with auth result
        """
        data = {"email": email, "password": password}
        if metadata:
            data["user_metadata"] = metadata

        result = self._make_request("POST", "signup", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def sign_in(
        self,
        email: str,
        password: str,
    ) -> ActionResult:
        """Sign in a user.

        Args:
            email: User email
            password: User password

        Returns:
            ActionResult with session data
        """
        data = {"email": email, "password": password}
        result = self._make_request("POST", "token?grant_type=password", data=data)
        if "error" in result:
            return ActionResult(success=False, error=result["error"])

        return ActionResult(success=True, data=result)

    def execute(self, operation: str, **kwargs) -> ActionResult:
        """Execute Supabase operation."""
        operations = {
            "select": self.select,
            "insert": self.insert,
            "update": self.update,
            "delete": self.delete,
            "rpc": self.rpc,
            "sign_up": self.sign_up,
            "sign_in": self.sign_in,
        }
        if operation not in operations:
            return ActionResult(success=False, error=f"Unknown: {operation}")
        return operations[operation](**kwargs)
