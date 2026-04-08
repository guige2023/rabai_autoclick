"""
Metrics aggregation and monitoring module for system observability.

Collects, processes, and visualizes metrics with support for
counters, gauges, histograms, and summaries.
"""
from __future__ import annotations

import json
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Optional


class MetricType(Enum):
    """Type of metric."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


class AggregationType(Enum):
    """Aggregation functions."""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    PERCENTILE = "percentile"


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
    labels: list[str] = field(default_factory=list)
    buckets: Optional[list[float]] = None

    def __hash__(self):
        return hash(self.name)


@dataclass
class AlertRule:
    """Alert rule for metrics."""
    name: str
    metric_name: str
    condition: str
    threshold: float
    window_seconds: int
    severity: str = "warning"
    enabled: bool = True
    recipients: list[str] = field(default_factory=list)


@dataclass
class Alert:
    """A triggered alert."""
    rule: AlertRule
    current_value: float
    triggered_at: float = field(default_factory=time.time)
    resolved_at: Optional[float] = None
    status: str = "firing"


@dataclass
class TimeSeries:
    """A time series of metric data points."""
    metric_name: str
    labels: dict
    points: list[MetricPoint] = field(default_factory=list)

    def latest_value(self) -> Optional[float]:
        if not self.points:
            return None
        return self.points[-1].value

    def query_range(
        self,
        start: float,
        end: float,
    ) -> list[MetricPoint]:
        return [p for p in self.points if start <= p.timestamp <= end]


class MetricsAggregator:
    """
    Metrics aggregation and monitoring service.

    Collects metrics from multiple sources, provides time-series
    storage, aggregation, and alerting capabilities.
    """

    def __init__(self, retention_days: int = 30):
        self.retention_days = retention_days
        self._metrics: dict[str, Metric] = {}
        self._series: dict[str, TimeSeries] = {}
        self._count_values: dict[str, float] = defaultdict(float)
        self._gauge_values: dict[str, float] = {}
        self._histogram_values: dict[str, list[float]] = defaultdict(list)
        self._alert_rules: dict[str, AlertRule] = {}
        self._active_alerts: dict[str, Alert] = {}

    def register_metric(
        self,
        name: str,
        metric_type: MetricType,
        description: str = "",
        unit: str = "",
        labels: Optional[list[str]] = None,
        buckets: Optional[list[float]] = None,
    ) -> Metric:
        """Register a new metric."""
        metric = Metric(
            name=name,
            metric_type=metric_type,
            description=description,
            unit=unit,
            labels=labels or [],
            buckets=buckets,
        )
        self._metrics[name] = metric
        return metric

    def _make_series_key(self, metric_name: str, labels: dict) -> str:
        """Create a unique key for a time series."""
        label_str = json.dumps(labels, sort_keys=True)
        return f"{metric_name}:{hashlib_md5(label_str)}"

    def record_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict] = None,
        timestamp: Optional[float] = None,
    ) -> None:
        """Record a counter metric."""
        labels = labels or {}
        timestamp = timestamp or time.time()

        metric = self._metrics.get(name)
        if not metric or metric.metric_type != MetricType.COUNTER:
            metric = self.register_metric(name, MetricType.COUNTER)

        series_key = self._make_series_key(name, labels)
        self._count_values[series_key] += value

        self._add_point(series_key, name, timestamp, self._count_values[series_key], labels)

    def record_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None,
        timestamp: Optional[float] = None,
    ) -> None:
        """Record a gauge metric."""
        labels = labels or {}
        timestamp = timestamp or time.time()

        metric = self._metrics.get(name)
        if not metric or metric.metric_type != MetricType.GAUGE:
            metric = self.register_metric(name, MetricType.GAUGE)

        series_key = self._make_series_key(name, labels)
        self._gauge_values[series_key] = value

        self._add_point(series_key, name, timestamp, value, labels)

    def record_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None,
        timestamp: Optional[float] = None,
    ) -> None:
        """Record a histogram metric."""
        labels = labels or {}
        timestamp = timestamp or time.time()

        metric = self._metrics.get(name)
        if not metric or metric.metric_type != MetricType.HISTOGRAM:
            buckets = metric.buckets if metric else [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]
            metric = self.register_metric(name, MetricType.HISTOGRAM, buckets=buckets)

        series_key = self._make_series_key(name, labels)
        self._histogram_values[series_key].append(value)

        cumulative = 0
        bucket_values = {}
        for bucket in metric.buckets:
            count = sum(1 for v in self._histogram_values[series_key] if v <= bucket)
            cumulative = count
            bucket_values[bucket] = cumulative

        self._add_point(series_key, name, timestamp, value, labels)

    def record_summary(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None,
        timestamp: Optional[float] = None,
    ) -> None:
        """Record a summary metric."""
        labels = labels or {}
        timestamp = timestamp or time.time()

        metric = self._metrics.get(name)
        if not metric or metric.metric_type != MetricType.SUMMARY:
            metric = self.register_metric(name, MetricType.SUMMARY)

        series_key = self._make_series_key(name, labels)
        self._histogram_values[series_key].append(value)

        self._add_point(series_key, name, timestamp, value, labels)

    def _add_point(
        self,
        series_key: str,
        metric_name: str,
        timestamp: float,
        value: float,
        labels: dict,
    ) -> None:
        """Add a data point to a time series."""
        if series_key not in self._series:
            self._series[series_key] = TimeSeries(
                metric_name=metric_name,
                labels=labels,
            )

        point = MetricPoint(timestamp=timestamp, value=value, labels=labels)
        self._series[series_key].points.append(point)

        cutoff = time.time() - (self.retention_days * 86400)
        self._series[series_key].points = [
            p for p in self._series[series_key].points if p.timestamp >= cutoff
        ]

    def query(
        self,
        metric_name: str,
        labels: Optional[dict] = None,
        start: Optional[float] = None,
        end: Optional[float] = None,
        step: Optional[int] = None,
    ) -> list[TimeSeries]:
        """Query metrics with optional label filtering."""
        results = []

        for series_key, series in self._series.items():
            if series.metric_name != metric_name:
                continue

            if labels:
                if not all(series.labels.get(k) == v for k, v in labels.items()):
                    continue

            filtered_series = TimeSeries(
                metric_name=series.metric_name,
                labels=series.labels,
            )

            points = series.points
            if start:
                points = [p for p in points if p.timestamp >= start]
            if end:
                points = [p for p in points if p.timestamp <= end]

            if step and len(points) > 1:
                bucketed = self._bucket_points(points, step)
                filtered_series.points = bucketed
            else:
                filtered_series.points = points

            results.append(filtered_series)

        return results

    def _bucket_points(self, points: list[MetricPoint], step: int) -> list[MetricPoint]:
        """Bucket points into time intervals."""
        if not points:
            return []

        buckets = defaultdict(list)
        for point in points:
            bucket_time = int(point.timestamp / step) * step
            buckets[bucket_time].append(point.value)

        result = []
        for timestamp in sorted(buckets.keys()):
            avg_value = sum(buckets[timestamp]) / len(buckets[timestamp])
            result.append(MetricPoint(timestamp=timestamp, value=avg_value))

        return result

    def aggregate(
        self,
        metric_name: str,
        aggregation: AggregationType,
        labels: Optional[dict] = None,
        start: Optional[float] = None,
        end: Optional[float] = None,
    ) -> float:
        """Aggregate metric values."""
        series_list = self.query(metric_name, labels, start, end)

        all_values = []
        for series in series_list:
            for point in series.points:
                all_values.append(point.value)

        if not all_values:
            return 0.0

        if aggregation == AggregationType.SUM:
            return sum(all_values)
        elif aggregation == AggregationType.AVG:
            return sum(all_values) / len(all_values)
        elif aggregation == AggregationType.MIN:
            return min(all_values)
        elif aggregation == AggregationType.MAX:
            return max(all_values)
        elif aggregation == AggregationType.COUNT:
            return float(len(all_values))
        elif aggregation == AggregationType.PERCENTILE:
            sorted_values = sorted(all_values)
            idx = int(len(sorted_values) * 0.95)
            return sorted_values[min(idx, len(sorted_values) - 1)]

        return 0.0

    def create_alert_rule(
        self,
        name: str,
        metric_name: str,
        condition: str,
        threshold: float,
        window_seconds: int,
        severity: str = "warning",
        recipients: Optional[list[str]] = None,
    ) -> AlertRule:
        """Create an alert rule."""
        rule = AlertRule(
            name=name,
            metric_name=metric_name,
            condition=condition,
            threshold=threshold,
            window_seconds=window_seconds,
            severity=severity,
            recipients=recipients or [],
        )
        self._alert_rules[name] = rule
        return rule

    def evaluate_alerts(self) -> list[Alert]:
        """Evaluate all alert rules."""
        triggered_alerts = []
        now = time.time()

        for rule_name, rule in self._alert_rules.items():
            if not rule.enabled:
                continue

            start = now - rule.window_seconds
            current_value = self.aggregate(
                rule.metric_name,
                AggregationType.AVG,
                start=start,
                end=now,
            )

            should_trigger = False
            if rule.condition == "above" and current_value > rule.threshold:
                should_trigger = True
            elif rule.condition == "below" and current_value < rule.threshold:
                should_trigger = True
            elif rule.condition == "equals" and abs(current_value - rule.threshold) < 0.001:
                should_trigger = True

            if should_trigger:
                if rule_name not in self._active_alerts:
                    alert = Alert(rule=rule, current_value=current_value)
                    self._active_alerts[rule_name] = alert
                    triggered_alerts.append(alert)
                else:
                    triggered_alerts.append(self._active_alerts[rule_name])
            else:
                if rule_name in self._active_alerts:
                    self._active_alerts[rule_name].resolved_at = now
                    self._active_alerts[rule_name].status = "resolved"
                    del self._active_alerts[rule_name]

        return triggered_alerts

    def get_active_alerts(self) -> list[Alert]:
        """Get all currently firing alerts."""
        return list(self._active_alerts.values())

    def get_metric_info(self, metric_name: str) -> Optional[Metric]:
        """Get metric metadata."""
        return self._metrics.get(metric_name)

    def list_metrics(self) -> list[Metric]:
        """List all registered metrics."""
        return list(self._metrics.values())

    def get_top_values(
        self,
        metric_name: str,
        label: str,
        limit: int = 10,
        start: Optional[float] = None,
        end: Optional[float] = None,
    ) -> list[tuple[str, float]]:
        """Get top values for a label."""
        series_list = self.query(metric_name, start=start, end=end)

        label_values: dict[str, float] = defaultdict(float)
        for series in series_list:
            label_value = series.labels.get(label, "unknown")
            if series.points:
                label_values[label_value] += series.points[-1].value

        sorted_values = sorted(label_values.items(), key=lambda x: x[1], reverse=True)
        return sorted_values[:limit]


def hashlib_md5(data: str) -> str:
    """Simple MD5 hash helper."""
    import hashlib
    return hashlib.md5(data.encode()).hexdigest()
