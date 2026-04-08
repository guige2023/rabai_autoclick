"""Materialized View action module for RabAI AutoClick.

Provides materialized view management for caching complex
query results with automatic and manual refresh strategies.
"""

import sys
import os
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class RefreshStrategy(Enum):
    """View refresh strategies."""
    MANUAL = "manual"
    AUTOMATIC = "automatic"
    INCREMENTAL = "incremental"
    SCHEDULED = "scheduled"


class ViewStatus(Enum):
    """Materialized view status."""
    INITIAL = "initial"  # Never refreshed
    REFRESHING = "refreshing"
    CURRENT = "current"
    STALE = "stale"
    ERROR = "error"


@dataclass
class MaterializedView:
    """Represents a materialized view."""
    view_id: str
    name: str
    query: str  # Source query definition
    base_tables: List[str] = field(default_factory=list)
    refresh_strategy: RefreshStrategy = RefreshStrategy.MANUAL
    refresh_interval_seconds: float = 3600.0
    status: ViewStatus = ViewStatus.INITIAL
    last_refresh_at: Optional[float] = None
    last_refresh_duration: float = 0.0
    row_count: int = 0
    size_bytes: int = 0
    columns: List[str] = field(default_factory=list)
    description: str = ""


class MaterializedViewManager:
    """Manages materialized views."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._views: Dict[str, MaterializedView] = {}
        self._view_data: Dict[str, List[Dict]] = {}  # view_id -> data
        self._query_functions: Dict[str, Callable] = {}
        self._persistence_path = persistence_path
        self._load()
    
    def _load(self) -> None:
        """Load views from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for view_data in data.get("views", []):
                        view_data.pop('status', None)
                        view = MaterializedView(
                            status=ViewStatus(view_data.pop('_status')),
                            **view_data
                        )
                        self._views[view.view_id] = view
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist views."""
        if self._persistence_path:
            try:
                data = {
                    "views": [
                        {
                            "_status": v.status.value,
                            "view_id": v.view_id,
                            "name": v.name,
                            "query": v.query,
                            "base_tables": v.base_tables,
                            "refresh_strategy": v.refresh_strategy.value,
                            "refresh_interval_seconds": v.refresh_interval_seconds,
                            "last_refresh_at": v.last_refresh_at,
                            "last_refresh_duration": v.last_refresh_duration,
                            "row_count": v.row_count,
                            "size_bytes": v.size_bytes,
                            "columns": v.columns,
                            "description": v.description
                        }
                        for v in self._views.values()
                    ]
                }
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except OSError:
                pass
    
    def create_view(
        self,
        name: str,
        query: str,
        base_tables: List[str],
        refresh_strategy: RefreshStrategy = RefreshStrategy.MANUAL,
        refresh_interval_seconds: float = 3600.0
    ) -> str:
        """Create a new materialized view."""
        view_id = str(uuid.uuid4())
        view = MaterializedView(
            view_id=view_id,
            name=name,
            query=query,
            base_tables=base_tables,
            refresh_strategy=refresh_strategy,
            refresh_interval_seconds=refresh_interval_seconds,
            status=ViewStatus.INITIAL
        )
        self._views[view_id] = view
        self._persist()
        return view_id
    
    def register_query_function(self, view_id: str, func: Callable) -> None:
        """Register a function to execute the view's query."""
        self._query_functions[view_id] = func
    
    def refresh_view(self, view_id: str) -> tuple[bool, str]:
        """Refresh a materialized view."""
        view = self._views.get(view_id)
        if not view:
            return False, "View not found"
        
        view.status = ViewStatus.REFRESHING
        start_time = time.time()
        
        try:
            query_func = self._query_functions.get(view_id)
            if query_func:
                data = query_func()
                self._view_data[view_id] = data
                view.row_count = len(data)
                view.columns = list(data[0].keys()) if data else []
            else:
                # Simulate refresh
                self._view_data[view_id] = []
                view.row_count = 0
            
            view.status = ViewStatus.CURRENT
            view.last_refresh_at = time.time()
            view.last_refresh_duration = time.time() - start_time
            view.last_refresh_duration = view.last_refresh_duration
            self._persist()
            return True, f"Refreshed: {view.row_count} rows"
        
        except Exception as e:
            view.status = ViewStatus.ERROR
            self._persist()
            return False, str(e)
    
    def refresh_stale_views(self) -> Dict[str, Any]:
        """Refresh all stale views."""
        now = time.time()
        stats = {"refreshed": 0, "failed": 0, "skipped": 0}
        
        for view in self._views.values():
            if view.refresh_strategy == RefreshStrategy.MANUAL:
                stats["skipped"] += 1
                continue
            
            should_refresh = False
            if view.refresh_strategy == RefreshStrategy.AUTOMATIC:
                should_refresh = True
            elif view.refresh_strategy == RefreshStrategy.SCHEDULED:
                if not view.last_refresh_at or \
                   (now - view.last_refresh_at) >= view.refresh_interval_seconds:
                    should_refresh = True
            
            if should_refresh:
                success, _ = self.refresh_view(view.view_id)
                if success:
                    stats["refreshed"] += 1
                else:
                    stats["failed"] += 1
        
        return stats
    
    def get_view_data(
        self,
        view_id: str,
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 1000,
        offset: int = 0
    ) -> tuple[bool, List[Dict], str]:
        """Get data from a materialized view."""
        view = self._views.get(view_id)
        if not view:
            return False, [], "View not found"
        
        if view.status == ViewStatus.INITIAL:
            return False, [], "View has never been refreshed"
        
        data = self._view_data.get(view_id, [])
        
        # Apply filters
        if filters:
            filtered = []
            for row in data:
                match = all(row.get(k) == v for k, v in filters.items())
                if match:
                    filtered.append(row)
            data = filtered
        
        return True, data[offset:offset + limit], ""
    
    def get_view(self, view_id: str) -> Optional[MaterializedView]:
        """Get view metadata."""
        return self._views.get(view_id)
    
    def delete_view(self, view_id: str) -> bool:
        """Delete a materialized view."""
        if view_id in self._views:
            del self._views[view_id]
            self._view_data.pop(view_id, None)
            self._query_functions.pop(view_id, None)
            self._persist()
            return True
        return False
    
    def list_views(self) -> List[MaterializedView]:
        """List all views."""
        return list(self._views.values())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get view statistics."""
        total_rows = sum(v.row_count for v in self._views.values())
        by_status = {}
        for v in self._views.values():
            status_name = v.status.value
            by_status[status_name] = by_status.get(status_name, 0) + 1
        
        return {
            "total_views": len(self._views),
            "total_rows": total_rows,
            "by_status": by_status
        }


class MaterializedViewAction(BaseAction):
    """Materialized view management for cached query results.
    
    Supports manual, automatic, incremental, and scheduled refresh
    strategies with query result caching.
    """
    action_type = "materialized_view"
    display_name = "物化视图"
    description = "物化视图管理，缓存复杂查询结果"
    
    def __init__(self):
        super().__init__()
        self._manager = MaterializedViewManager()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute materialized view operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "create":
                return self._create(params)
            elif operation == "refresh":
                return self._refresh(params)
            elif operation == "refresh_all":
                return self._refresh_all(params)
            elif operation == "get_data":
                return self._get_data(params)
            elif operation == "get_view":
                return self._get_view(params)
            elif operation == "delete":
                return self._delete(params)
            elif operation == "list":
                return self._list(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _create(self, params: Dict[str, Any]) -> ActionResult:
        """Create a materialized view."""
        view_id = self._manager.create_view(
            name=params.get("name", ""),
            query=params.get("query", ""),
            base_tables=params.get("base_tables", []),
            refresh_strategy=RefreshStrategy(params.get("refresh_strategy", "manual")),
            refresh_interval_seconds=params.get("refresh_interval_seconds", 3600.0)
        )
        return ActionResult(success=True, message=f"View created: {view_id}",
                         data={"view_id": view_id})
    
    def _refresh(self, params: Dict[str, Any]) -> ActionResult:
        """Refresh a view."""
        view_id = params.get("view_id", "")
        success, msg = self._manager.refresh_view(view_id)
        return ActionResult(success=success, message=msg)
    
    def _refresh_all(self, params: Dict[str, Any]) -> ActionResult:
        """Refresh all stale views."""
        stats = self._manager.refresh_stale_views()
        return ActionResult(success=True, message="Refresh complete", data=stats)
    
    def _get_data(self, params: Dict[str, Any]) -> ActionResult:
        """Get view data."""
        view_id = params.get("view_id", "")
        success, data, error = self._manager.get_view_data(
            view_id,
            params.get("filters"),
            params.get("limit", 1000),
            params.get("offset", 0)
        )
        if not success:
            return ActionResult(success=False, message=error)
        return ActionResult(success=True, message=f"{len(data)} rows",
                         data={"data": data, "count": len(data)})
    
    def _get_view(self, params: Dict[str, Any]) -> ActionResult:
        """Get view metadata."""
        view_id = params.get("view_id", "")
        view = self._manager.get_view(view_id)
        if not view:
            return ActionResult(success=False, message="View not found")
        return ActionResult(success=True, message="View retrieved",
                         data={"view_id": view.view_id, "name": view.name,
                               "status": view.status.value, "row_count": view.row_count})
    
    def _delete(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a view."""
        view_id = params.get("view_id", "")
        deleted = self._manager.delete_view(view_id)
        return ActionResult(success=deleted, message="Deleted" if deleted else "Not found")
    
    def _list(self, params: Dict[str, Any]) -> ActionResult:
        """List all views."""
        views = self._manager.list_views()
        return ActionResult(success=True, message=f"{len(views)} views",
                         data={"views": [{"id": v.view_id, "name": v.name,
                                         "status": v.status.value} for v in views]})
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get statistics."""
        stats = self._manager.get_stats()
        return ActionResult(success=True, message="Stats", data=stats)
