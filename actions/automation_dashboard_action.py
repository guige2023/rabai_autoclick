"""Automation Dashboard Action.

Provides a dashboard API for monitoring automation status.
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import time


@dataclass
class Widget:
    widget_id: str
    title: str
    widget_type: str
    data_fn: Callable
    refresh_interval_sec: float = 30.0
    metadata: Dict[str, Any] = field(default_factory=dict)


class AutomationDashboardAction:
    """Dashboard API for automation monitoring."""

    def __init__(self) -> None:
        self.widgets: Dict[str, Widget] = {}
        self.dashboards: Dict[str, List[str]] = {"default": []}
        self._cache: Dict[str, tuple[float, Any]] = {}

    def add_widget(
        self,
        widget_id: str,
        title: str,
        widget_type: str,
        data_fn: Callable,
        refresh_interval_sec: float = 30.0,
        dashboard: str = "default",
    ) -> Widget:
        widget = Widget(
            widget_id=widget_id,
            title=title,
            widget_type=widget_type,
            data_fn=data_fn,
            refresh_interval_sec=refresh_interval_sec,
        )
        self.widgets[widget_id] = widget
        self.dashboards.setdefault(dashboard, []).append(widget_id)
        return widget

    def get_widget_data(self, widget_id: str, force_refresh: bool = False) -> Optional[Any]:
        widget = self.widgets.get(widget_id)
        if not widget:
            return None
        now = time.time()
        if not force_refresh and widget_id in self._cache:
            cached_time, cached_data = self._cache[widget_id]
            if now - cached_time < widget.refresh_interval_sec:
                return cached_data
        try:
            data = widget.data_fn()
            self._cache[widget_id] = (now, data)
            return data
        except Exception as e:
            return {"error": str(e)}

    def get_dashboard(self, dashboard: str = "default") -> Dict[str, Any]:
        widget_ids = self.dashboards.get(dashboard, [])
        widgets_data = []
        for wid in widget_ids:
            widget = self.widgets.get(wid)
            if widget:
                widgets_data.append({
                    "widget_id": wid,
                    "title": widget.title,
                    "widget_type": widget.widget_type,
                    "data": self.get_widget_data(wid),
                    "refresh_interval_sec": widget.refresh_interval_sec,
                })
        return {
            "dashboard": dashboard,
            "widgets": widgets_data,
            "widget_count": len(widgets_data),
        }

    def list_dashboards(self) -> List[str]:
        return list(self.dashboards.keys())
