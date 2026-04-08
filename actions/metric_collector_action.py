"""
Metric collector module for aggregating and analyzing system metrics.

Supports counters, gauges, histograms, and custom metric collection with alerting.
"""
from __future__ import annotations

import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class MetricType(Enum):
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class AlertSeverity(Enum):
    """Alert severity levels."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: float
    value: float
    labels: dict = field(default_factory=dict)


@dataclass
class Metric:
    """A metric definition."""
    name: str
    metric_type: MetricType
    description: str = ""
    unit: str = ""
    buckets: Optional[list[float]] = None


@dataclass
class Alert:
    """An alert rule."""
    id: str
    name: str
    metric_name: str
    condition: str
    threshold: float
    severity: AlertSeverity
    window_seconds: int = 60
    enabled: bool = True


@dataclass
class AlertEvent:
    """A triggered alert event."""
    id: str
    alert_id: str
    alert_name: str
    metric_value: float
    threshold: float
    triggered_at: float = field(default_factory=time.time)
    resolved_at: Optional[float] = None


class MetricCollector:
    """
    Metric collector for aggregating system metrics.

    Supports counters, gauges, histograms, summaries,
    and alerting with multiple condition types.
    """

    def __init__(self):
        self._metrics: dict[str, Metric] = {}
        self._series: dict[str, list[MetricPoint]] = defaultdict(list)
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}
        self._alerts: dict[str, Alert] = {}
        self._alert_events: list[AlertEvent] = []
        self._alert_states: dict[str, bool] = {}
        self._max_points_per_series: int = 10000

    def register_metric(
        self,
        name: str,
        metric_type: MetricType,
        description: str = "",
        unit: str = "",
        buckets: Optional[list[float]] = None,
    ) -> Metric:
        """Register a new metric."""
        metric = Metric(
            name=name,
            metric_type=metric_type,
            description=description,
            unit=unit,
            buckets=buckets,
        )

        self._metrics[name] = metric
        return metric

    def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict] = None,
    ) -> None:
        """Increment a counter metric."""
        if name not in self._metrics:
            self.register_metric(name, MetricType.COUNTER)

        key = self._make_key(name, labels or {})
        self._counters[key] += value

        self._add_point(key, name, self._counters[key], labels)

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None,
    ) -> None:
        """Set a gauge metric."""
        if name not in self._metrics:
            self.register_metric(name, MetricType.GAUGE)

        key = self._make_key(name, labels or {})
        self._gauges[key] = value

        self._add_point(key, name, value, labels)

    def observe_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None,
    ) -> None:
        """Observe a value for histogram metric."""
        if name not in self._metrics:
            self.register_metric(name, MetricType.HISTOGRAM, buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0])

        key = self._make_key(name, labels or {})
        self._add_point(key, name, value, labels)

    def _make_key(self, name: str, labels: dict) -> str:
        """Create a unique key for a metric with labels."""
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}" if label_str else name

    def _add_point(
        self,
        key: str,
        name: str,
        value: float,
        labels: Optional[dict] = None,
    ) -> None:
        """Add a data point to a series."""
        point = MetricPoint(
            timestamp=time.time(),
            value=value,
            labels=labels or {},
        )

        self._series[key].append(point)

        if len(self._series[key]) > self._max_points_per_series:
            self._series[key] = self._series[key][-self._max_points_per_series // 2:]

    def query(
        self,
        name: str,
        labels: Optional[dict] = None,
        start: Optional[float] = None,
        end: Optional[float] = None,
    ) -> list[MetricPoint]:
        """Query metric data points."""
        key = self._make_key(name, labels or {})

        points = self._series.get(key, [])

        if start:
            points = [p for p in points if p.timestamp >= start]
        if end:
            points = [p for p in points if p.timestamp <= end]

        return points

    def get_latest(self, name: str, labels: Optional[dict] = None) -> Optional[float]:
        """Get the latest value for a metric."""
        points = self.query(name, labels)
        return points[-1].value if points else None

    def aggregate(
        self,
        name: str,
        labels: Optional[dict] = None,
        start: Optional[float] = None,
        end: Optional[float] = None,
        func: str = "avg",
    ) -> float:
        """Aggregate metric values."""
        points = self.query(name, labels, start, end)

        if not points:
            return 0.0

        values = [p.value for p in points]

        if func == "sum":
            return sum(values)
        elif func == "avg":
            return sum(values) / len(values)
        elif func == "min":
            return min(values)
        elif func == "max":
            return max(values)
        elif func == "count":
            return float(len(values))
        elif func == "last":
            return values[-1]

        return 0.0

    def create_alert(
        self,
        name: str,
        metric_name: str,
        condition: str,
        threshold: float,
        severity: AlertSeverity = AlertSeverity.WARNING,
        window_seconds: int = 60,
    ) -> Alert:
        """Create an alert rule."""
        alert = Alert(
            id=str(uuid.uuid4())[:8],
            name=name,
            metric_name=metric_name,
            condition=condition,
            threshold=threshold,
            severity=severity,
            window_seconds=window_seconds,
        )

        self._alerts[alert.id] = alert
        return alert

    def evaluate_alerts(self) -> list[AlertEvent]:
        """Evaluate all alert rules."""
        now = time.time()
        triggered = []

        for alert in self._alerts.values():
            if not alert.enabled:
                continue

            start = now - alert.window_seconds
            value = self.aggregate(alert.metric_name, start=start, end=now, func="avg")

            should_trigger = False

            if alert.condition == "above" and value > alert.threshold:
                should_trigger = True
            elif alert.condition == "below" and value < alert.threshold:
                should_trigger = True
            elif alert.condition == "equals" and abs(value - alert.threshold) < 0.001:
                should_trigger = True

            if should_trigger:
                if not self._alert_states.get(alert.id):
                    event = AlertEvent(
                        id=str(uuid.uuid4())[:8],
                        alert_id=alert.id,
                        alert_name=alert.name,
                        metric_value=value,
                        threshold=alert.threshold,
                    )
                    self._alert_events.append(event)
                    triggered.append(event)
                    self._alert_states[alert.id] = True
            else:
                if self._alert_states.get(alert.id):
                    self._alert_states[alert.id] = False

                    for evt in reversed(self._alert_events):
                        if evt.alert_id == alert.id and not evt.resolved_at:
                            evt.resolved_at = now
                            break

        return triggered

    def get_active_alerts(self) -> list[AlertEvent]:
        """Get currently active alerts."""
        return [
            evt for evt in self._alert_events
            if not evt.resolved_at and evt.alert_id in self._alert_states and self._alert_states[evt.alert_id]
        ]

    def list_metrics(self) -> list[Metric]:
        """List all registered metrics."""
        return list(self._metrics.values())

    def list_alerts(self, enabled: Optional[bool] = None) -> list[Alert]:
        """List alert rules."""
        alerts = list(self._alerts.values())
        if enabled is not None:
            alerts = [a for a in alerts if a.enabled == enabled]
        return alerts

    def get_stats(self) -> dict:
        """Get collector statistics."""
        return {
            "total_metrics": len(self._metrics),
            "total_series": len(self._series),
            "total_alerts": len(self._alerts),
            "active_alerts": len(self.get_active_alerts()),
            "total_points": sum(len(s) for s in self._series.values()),
        }
