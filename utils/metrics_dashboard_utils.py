"""Metrics dashboard utilities: widget definitions, layout, and data binding."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

__all__ = [
    "Widget",
    "Dashboard",
    "MetricPanel",
    "TimeSeriesWidget",
    "GaugeWidget",
    "AlertWidget",
    "build_dashboard_json",
]


@dataclass
class Widget:
    """Base class for dashboard widgets."""

    id: str
    title: str
    x: int = 0
    y: int = 0
    width: int = 4
    height: int = 2
    widget_type: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "x": self.x,
            "y": self.y,
            "width": self.width,
            "height": self.height,
            "type": self.widget_type,
        }


class TimeSeriesWidget(Widget):
    """Time series chart widget."""

    def __init__(
        self,
        id: str,
        title: str,
        metrics: list[str],
        x: int = 0,
        y: int = 0,
        width: int = 6,
        height: int = 3,
        color: str = "#4CAF50",
        y_axis_min: float | None = None,
        y_axis_max: float | None = None,
    ) -> None:
        super().__init__(id, title, x, y, width, height, "timeseries")
        self.metrics = metrics
        self.color = color
        self.y_axis_min = y_axis_min
        self.y_axis_max = y_axis_max

    def to_dict(self) -> dict[str, Any]:
        d = super().to_dict()
        d.update({
            "metrics": self.metrics,
            "color": self.color,
            "y_axis_min": self.y_axis_min,
            "y_axis_max": self.y_axis_max,
        })
        return d


class GaugeWidget(Widget):
    """Gauge widget for single metric display."""

    def __init__(
        self,
        id: str,
        title: str,
        metric: str,
        x: int = 0,
        y: int = 0,
        width: int = 2,
        height: int = 2,
        min_val: float = 0.0,
        max_val: float = 100.0,
        thresholds: dict[str, float] | None = None,
    ) -> None:
        super().__init__(id, title, x, y, width, height, "gauge")
        self.metric = metric
        self.min_val = min_val
        self.max_val = max_val
        self.thresholds = thresholds or {"warning": 70, "critical": 90}

    def to_dict(self) -> dict[str, Any]:
        d = super().to_dict()
        d.update({
            "metric": self.metric,
            "min": self.min_val,
            "max": self.max_val,
            "thresholds": self.thresholds,
        })
        return d


class MetricPanel(Widget):
    """Single big number metric panel."""

    def __init__(
        self,
        id: str,
        title: str,
        metric: str,
        format_: str = "number",
        x: int = 0,
        y: int = 0,
        width: int = 2,
        height: int = 1,
        unit: str = "",
    ) -> None:
        super().__init__(id, title, x, y, width, height, "metric_panel")
        self.metric = metric
        self.format = format_
        self.unit = unit

    def to_dict(self) -> dict[str, Any]:
        d = super().to_dict()
        d.update({
            "metric": self.metric,
            "format": self.format,
            "unit": self.unit,
        })
        return d


class AlertWidget(Widget):
    """Alert list widget."""

    def __init__(
        self,
        id: str,
        title: str,
        alert_ids: list[str] | None = None,
        x: int = 0,
        y: int = 0,
        width: int = 4,
        height: int = 3,
    ) -> None:
        super().__init__(id, title, x, y, width, height, "alerts")
        self.alert_ids = alert_ids or []

    def to_dict(self) -> dict[str, Any]:
        d = super().to_dict()
        d["alert_ids"] = self.alert_ids
        return d


@dataclass
class Dashboard:
    """A dashboard containing multiple widgets."""

    name: str
    widgets: list[Widget] = field(default_factory=list)
    refresh_interval: int = 30
    tags: list[str] = field(default_factory=list)

    def add(self, widget: Widget) -> None:
        """Add a widget to the dashboard."""
        self.widgets.append(widget)

    def layout_grid(self, columns: int = 12) -> None:
        """Auto-layout widgets in a grid."""
        positions: dict[int, dict[int, bool]] = {}
        for w in self.widgets:
            placed = False
            for y in range(20):
                for x in range(columns - w.width + 1):
                    occupied = False
                    for wy in range(w.height):
                        row = positions.get(y + wy, {})
                        for wx in range(w.width):
                            if row.get(x + wx):
                                occupied = True
                                break
                        if occupied:
                            break
                    if not occupied:
                        w.x = x
                        w.y = y
                        for wy in range(w.height):
                            for wx in range(w.width):
                                positions.setdefault(y + wy, {})[x + wx] = True
                        placed = True
                        break
                if placed:
                    break

    def to_dict(self) -> dict[str, Any]:
        """Export dashboard as a dict."""
        return {
            "name": self.name,
            "refresh_interval": self.refresh_interval,
            "tags": self.tags,
            "widgets": [w.to_dict() for w in self.widgets],
            "generated_at": time.time(),
        }


def build_dashboard_json(
    name: str,
    metrics: list[tuple[str, str, str]],
) -> dict[str, Any]:
    """Build a dashboard from a list of (metric, title, type) tuples."""
    dashboard = Dashboard(name=name)
    x, y = 0, 0
    for metric, title, wtype in metrics:
        if wtype == "timeseries":
            w = TimeSeriesWidget(
                id=f"w_{x}_{y}",
                title=title,
                metrics=[metric],
                x=x,
                y=y,
            )
        elif wtype == "gauge":
            w = GaugeWidget(
                id=f"w_{x}_{y}",
                title=title,
                metric=metric,
                x=x,
                y=y,
            )
        else:
            w = MetricPanel(
                id=f"w_{x}_{y}",
                title=title,
                metric=metric,
                x=x,
                y=y,
            )
        dashboard.add(w)
        x += w.width
        if x >= 12:
            x = 0
            y += 2
    dashboard.layout_grid()
    return dashboard.to_dict()
