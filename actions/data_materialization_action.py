"""
Data Materialization Action Module.

Manages materialized views with refresh strategies,
incremental updates, and query optimization.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class RefreshStrategy(Enum):
    """Materialization refresh strategies."""

    FULL = "full"
    INCREMENTAL = "incremental"
    ON_DEMAND = "on_demand"
    SCHEDULED = "scheduled"
    CACHED = "cached"


@dataclass
class MaterializedView:
    """Represents a materialized view."""

    name: str
    query: Callable
    data: list[dict[str, Any]] = field(default_factory=list)
    last_refresh: float = 0.0
    refresh_count: int = 0
    row_count: int = 0
    refresh_strategy: RefreshStrategy = RefreshStrategy.FULL
    incremental_key: str = "id"
    is_stale: bool = True


@dataclass
class MaterializationStats:
    """Statistics for materialization operations."""

    total_refreshes: int = 0
    total_rows_materialized: int = 0
    last_refresh_duration: float = 0.0
    total_refresh_time: float = 0.0
    incremental_updates: int = 0
    full_refreshes: int = 0


class DataMaterializationAction:
    """
    Manages data materialization for query optimization.

    Features:
    - Full and incremental refresh
    - Scheduled refresh
    - Query result caching
    - Staleness detection

    Example:
        mat = DataMaterializationAction()
        view = mat.create_view("active_users", query_fn, strategy=RefreshStrategy.INCREMENTAL)
        await mat.refresh("active_users")
        data = mat.get_view_data("active_users")
    """

    def __init__(
        self,
        default_ttl_seconds: float = 3600.0,
        enable_staleness_check: bool = True,
    ) -> None:
        """
        Initialize materialization action.

        Args:
            default_ttl_seconds: Default TTL for cached views.
            enable_staleness_check: Enable automatic staleness detection.
        """
        self.default_ttl_seconds = default_ttl_seconds
        self.enable_staleness_check = enable_staleness_check
        self._views: dict[str, MaterializedView] = {}
        self._stats = MaterializationStats()

    def create_view(
        self,
        name: str,
        query: Callable,
        refresh_strategy: RefreshStrategy = RefreshStrategy.FULL,
        incremental_key: str = "id",
    ) -> MaterializedView:
        """
        Create a new materialized view.

        Args:
            name: View name.
            query: Query function returning list of dicts.
            refresh_strategy: Refresh strategy to use.
            incremental_key: Key field for incremental updates.

        Returns:
            Created MaterializedView.
        """
        view = MaterializedView(
            name=name,
            query=query,
            refresh_strategy=refresh_strategy,
            incremental_key=incremental_key,
        )
        self._views[name] = view
        logger.info(f"Created materialized view: {name} (strategy={refresh_strategy.value})")
        return view

    async def refresh(
        self,
        name: str,
        incremental_data: Optional[list[dict[str, Any]]] = None,
        force: bool = False,
    ) -> bool:
        """
        Refresh a materialized view.

        Args:
            name: View name.
            incremental_data: New data for incremental refresh.
            force: Force full refresh even for incremental strategy.

        Returns:
            True if refresh was successful.
        """
        if name not in self._views:
            logger.error(f"View not found: {name}")
            return False

        view = self._views[name]
        start_time = time.time()

        try:
            if view.refresh_strategy == RefreshStrategy.INCREMENTAL and not force:
                await self._incremental_refresh(view, incremental_data)
            else:
                await self._full_refresh(view)

            duration = time.time() - start_time
            view.last_refresh = time.time()
            view.refresh_count += 1
            view.is_stale = False

            self._stats.total_refreshes += 1
            self._stats.last_refresh_duration = duration
            self._stats.total_refresh_time += duration

            logger.info(f"Refreshed view {name}: {view.row_count} rows in {duration:.3f}s")
            return True

        except Exception as e:
            logger.error(f"Refresh failed for {name}: {e}")
            return False

    async def _full_refresh(self, view: MaterializedView) -> None:
        """Perform full refresh of a view."""
        self._stats.full_refreshes += 1

        if asyncio.iscoroutinefunction(view.query):
            data = await view.query()
        else:
            data = view.query()

        view.data = data if data else []
        view.row_count = len(view.data)
        self._stats.total_rows_materialized += view.row_count

    async def _incremental_refresh(
        self,
        view: MaterializedView,
        new_data: Optional[list[dict[str, Any]]],
    ) -> None:
        """Perform incremental refresh of a view."""
        self._stats.incremental_updates += 1

        if not new_data:
            return

        key = view.incremental_key
        existing_map = {r[key]: r for r in view.data if key in r}
        existing_keys = set(existing_map.keys())

        for record in new_data:
            record_key = record.get(key)
            if record_key in existing_keys:
                existing_map[record_key] = record
            else:
                existing_map[record_key] = record

        view.data = list(existing_map.values())
        view.row_count = len(view.data)
        self._stats.total_rows_materialized += len(new_data)

    def get_view_data(
        self,
        name: str,
        filters: Optional[dict[str, Any]] = None,
        limit: Optional[int] = None,
    ) -> Optional[list[dict[str, Any]]]:
        """
        Get data from a materialized view.

        Args:
            name: View name.
            filters: Optional filters to apply.
            limit: Optional result limit.

        Returns:
            View data or None if not found.
        """
        if name not in self._views:
            return None

        view = self._views[name]
        data = view.data

        if filters:
            data = self._apply_filters(data, filters)

        if limit:
            data = data[:limit]

        return data

    def _apply_filters(
        self,
        data: list[dict[str, Any]],
        filters: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Apply filters to data."""
        result = data
        for key, value in filters.items():
            if isinstance(value, (list, tuple)):
                result = [r for r in result if r.get(key) in value]
            else:
                result = [r for r in result if r.get(key) == value]
        return result

    def is_stale(self, name: str) -> bool:
        """
        Check if a view is stale.

        Args:
            name: View name.

        Returns:
            True if view is stale.
        """
        if name not in self._views:
            return True

        if not self.enable_staleness_check:
            return False

        view = self._views[name]
        age = time.time() - view.last_refresh

        if view.refresh_strategy == RefreshStrategy.ON_DEMAND:
            return view.is_stale

        return age > self.default_ttl_seconds

    def get_stale_views(self) -> list[str]:
        """
        Get list of stale view names.

        Returns:
            List of stale view names.
        """
        return [name for name in self._views if self.is_stale(name)]

    def delete_view(self, name: str) -> bool:
        """
        Delete a materialized view.

        Args:
            name: View name.

        Returns:
            True if view was deleted.
        """
        if name in self._views:
            del self._views[name]
            logger.info(f"Deleted view: {name}")
            return True
        return False

    def get_stats(self) -> dict[str, Any]:
        """
        Get materialization statistics.

        Returns:
            Statistics dictionary.
        """
        avg_refresh_time = 0.0
        if self._stats.total_refreshes > 0:
            avg_refresh_time = self._stats.total_refresh_time / self._stats.total_refreshes

        return {
            "total_views": len(self._views),
            "total_refreshes": self._stats.total_refreshes,
            "total_rows_materialized": self._stats.total_rows_materialized,
            "avg_refresh_time": f"{avg_refresh_time:.3f}s",
            "incremental_updates": self._stats.incremental_updates,
            "full_refreshes": self._stats.full_refreshes,
            "stale_views": len(self.get_stale_views()),
        }

    def list_views(self) -> list[str]:
        """Get list of view names."""
        return list(self._views.keys())
