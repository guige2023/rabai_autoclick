"""Materialized View Action Module.

Provides materialized view for cached
query results.
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MaterializedView:
    """Materialized view definition."""
    view_id: str
    name: str
    query_func: Callable
    data: Optional[Any] = None
    last_refresh: Optional[float] = None
    ttl_seconds: float = 300


class MaterializedViewManager:
    """Manages materialized views."""

    def __init__(self):
        self._views: Dict[str, MaterializedView] = {}
        self._lock = threading.RLock()

    def create_view(
        self,
        name: str,
        query_func: Callable,
        ttl_seconds: float = 300
    ) -> str:
        """Create materialized view."""
        view_id = f"view_{name.lower().replace(' ', '_')}"

        view = MaterializedView(
            view_id=view_id,
            name=name,
            query_func=query_func,
            ttl_seconds=ttl_seconds
        )

        with self._lock:
            self._views[view_id] = view

        return view_id

    def refresh(self, view_id: str) -> bool:
        """Refresh a view."""
        with self._lock:
            view = self._views.get(view_id)
            if not view:
                return False

            try:
                view.data = view.query_func()
                view.last_refresh = time.time()
                return True
            except Exception:
                return False

    def get_data(self, view_id: str) -> Optional[Any]:
        """Get view data."""
        with self._lock:
            view = self._views.get(view_id)
            if not view:
                return None

            if view.data is None or self._is_stale(view):
                view.data = view.query_func()
                view.last_refresh = time.time()

            return view.data

    def _is_stale(self, view: MaterializedView) -> bool:
        """Check if view is stale."""
        if view.last_refresh is None:
            return True
        return (time.time() - view.last_refresh) > view.ttl_seconds


class MaterializedViewAction(BaseAction):
    """Action for materialized view operations."""

    def __init__(self):
        super().__init__("materialized_view")
        self._manager = MaterializedViewManager()

    def execute(self, params: Dict) -> ActionResult:
        """Execute materialized view action."""
        try:
            operation = params.get("operation", "create")

            if operation == "create":
                return self._create(params)
            elif operation == "refresh":
                return self._refresh(params)
            elif operation == "get":
                return self._get(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _create(self, params: Dict) -> ActionResult:
        """Create view."""
        view_id = self._manager.create_view(
            name=params.get("name", ""),
            query_func=params.get("query_func") or (lambda: {}),
            ttl_seconds=params.get("ttl_seconds", 300)
        )
        return ActionResult(success=True, data={"view_id": view_id})

    def _refresh(self, params: Dict) -> ActionResult:
        """Refresh view."""
        success = self._manager.refresh(params.get("view_id", ""))
        return ActionResult(success=success)

    def _get(self, params: Dict) -> ActionResult:
        """Get view data."""
        data = self._manager.get_data(params.get("view_id", ""))
        if data is None:
            return ActionResult(success=False, message="View not found")
        return ActionResult(success=True, data={"data": data})
