"""
API Metrics Action Module.

Provides comprehensive API metrics collection and monitoring
including latency, throughput, error rates, and custom metrics.

Author: rabai_autoclick team
"""

import time
import logging
from typing import (
    Optional, Dict, Any, List, Callable, Set,
    Union
)
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta
from collections import defaultdict
from heapq import heappush, heappop
import threading
import asyncio

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    RATE = "rate"


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class TimeWindow:
    """Time window for aggregation."""
    window_seconds: int
    max_size: int = 1000


class MetricRegistry:
    """Registry for metric collectors."""

    def __init__(self):
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._labels: Dict[str, Dict[str, str]] = {}
        self._lock = threading.RLock()

    def counter(self, name: str, value: float = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter."""
        with self._lock:
            key = self._make_key(name, labels)
            self._counters[key] += value
            self._labels[key] = labels or {}

    def gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge value."""
        with self._lock:
            key = self._make_key(name, labels)
            self._gauges[key] = value
            self._labels[key] = labels or {}

    def histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a histogram value."""
        with self._lock:
            key = self._make_key(name, labels)
            self._histograms[key].append(value)
            self._labels[key] = labels or {}

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create metric key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


class APIMetricsAction:
    """
    API Metrics Collection and Monitoring.

    Provides comprehensive metrics including request counts,
    latency distributions, error rates, and custom business metrics.

    Example:
        >>> metrics = APIMetricsAction()
        >>> metrics.increment("api_requests_total", labels={"method": "GET"})
        >>> metrics.record("api_latency_seconds", 0.123)
        >>> stats = metrics.get_stats()
    """

    def __init__(
        self,
        service_name: str = "api",
        time_windows: Optional[List[TimeWindow]] = None,
    ):
        self.service_name = service_name
        self.registry = MetricRegistry()
        self._time_windows = time_windows or [
            TimeWindow(window_seconds=60),
            TimeWindow(window_seconds=300),
            TimeWindow(window_seconds=600),
        ]
        self._time_series: Dict[str, Dict[int, List[MetricPoint]]] = defaultdict(
            lambda: defaultdict(list)
        )
        self._percentiles = [50, 75, 90, 95, 99]
        self._request_start_times: Dict[str, float] = {}

    def increment(
        self,
        name: str,
        value: float = 1,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Increment a counter metric.

        Args:
            name: Metric name
            value: Value to add
            labels: Optional labels
        """
        self.registry.counter(name, value, labels)
        self._record_timeseries(name, value, labels)

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Set a gauge metric.

        Args:
            name: Metric name
            value: Gauge value
            labels: Optional labels
        """
        self.registry.gauge(name, value, labels)

    def record(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Record a histogram value.

        Args:
            name: Metric name
            value: Value to record
            labels: Optional labels
        """
        self.registry.histogram(name, value, labels)
        self._record_timeseries(name, value, labels)

    def _record_timeseries(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]],
    ) -> None:
        """Record value to time series."""
        now = time.time()
        point = MetricPoint(timestamp=now, value=value, labels=labels or {})

        for window in self._time_windows:
            bucket = int(now / window.window_seconds)
            key = f"{name}:{bucket}"
            self._time_series[name][window.window_seconds].append(point)

            if len(self._time_series[name][window.window_seconds]) > window.max_size:
                self._time_series[name][window.window_seconds] = self._time_series[name][
                    window.window_seconds
                ][-window.max_size:]

    def start_request(
        self,
        request_id: str,
        method: str,
        endpoint: str,
    ) -> None:
        """
        Mark the start of a request.

        Args:
            request_id: Unique request ID
            method: HTTP method
            endpoint: Request endpoint
        """
        self._request_start_times[request_id] = time.time()
        self.increment(
            "api_requests_total",
            labels={"method": method, "endpoint": endpoint, "status": "started"},
        )

    def end_request(
        self,
        request_id: str,
        method: str,
        endpoint: str,
        status_code: int,
        error: Optional[str] = None,
    ) -> None:
        """
        Mark the end of a request and record metrics.

        Args:
            request_id: Unique request ID
            method: HTTP method
            endpoint: Request endpoint
            status_code: HTTP status code
            error: Optional error message
        """
        start_time = self._request_start_times.pop(request_id, None)
        latency = time.time() - start_time if start_time else 0

        status_category = self._get_status_category(status_code)

        self.increment(
            "api_requests_total",
            labels={"method": method, "endpoint": endpoint, "status": status_category},
        )

        self.record(
            "api_latency_seconds",
            latency,
            labels={"method": method, "endpoint": endpoint, "status": status_category},
        )

        if error:
            self.increment(
                "api_errors_total",
                labels={"method": method, "endpoint": endpoint, "error_type": error},
            )

        self.set_gauge(
            "api_request_in_progress",
            len(self._request_start_times),
            labels={"method": method, "endpoint": endpoint},
        )

    def _get_status_category(self, status_code: int) -> str:
        """Get status code category."""
        if 200 <= status_code < 300:
            return "success"
        elif 300 <= status_code < 400:
            return "redirect"
        elif 400 <= status_code < 500:
            return "client_error"
        elif 500 <= status_code < 600:
            return "server_error"
        return "unknown"

    def get_percentile(self, values: List[float], percentile: float) -> float:
        """Calculate percentile from values."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile / 100)
        return sorted_values[min(index, len(sorted_values) - 1)]

    def get_stats(
        self,
        name: Optional[str] = None,
        window_seconds: int = 60,
    ) -> Dict[str, Any]:
        """
        Get metric statistics.

        Args:
            name: Optional metric name filter
            window_seconds: Time window in seconds

        Returns:
            Dictionary of statistics
        """
        now = time.time()
        stats = {}

        time_series = self._time_series.get(name, {})
        points = time_series.get(window_seconds, [])

        recent_points = [p for p in points if now - p.timestamp <= window_seconds]

        if name and recent_points:
            values = [p.value for p in recent_points]
            stats[name] = {
                "count": len(values),
                "sum": sum(values),
                "min": min(values) if values else 0,
                "max": max(values) if values else 0,
                "avg": sum(values) / len(values) if values else 0,
            }

            for p in self._percentiles:
                stats[name][f"p{p}"] = self.get_percentile(values, p)

        else:
            for metric_name, metric_series in self._time_series.items():
                points = metric_series.get(window_seconds, [])
                recent_points = [p for p in points if now - p.timestamp <= window_seconds]

                if recent_points:
                    values = [p.value for p in recent_points]
                    stats[metric_name] = {
                        "count": len(values),
                        "sum": sum(values),
                        "min": min(values) if values else 0,
                        "max": max(values) if values else 0,
                        "avg": sum(values) / len(values) if values else 0,
                    }

                    for p in self._percentiles:
                        stats[metric_name][f"p{p}"] = self.get_percentile(values, p)

        return stats

    def get_rate(
        self,
        name: str,
        window_seconds: int = 60,
        labels: Optional[Dict[str, str]] = None,
    ) -> float:
        """
        Calculate rate (requests per second) for a metric.

        Args:
            name: Metric name
            window_seconds: Time window
            labels: Optional label filter

        Returns:
            Rate per second
        """
        now = time.time()
        points = self._time_series.get(name, {}).get(window_seconds, [])

        if labels:
            recent = [
                p for p in points
                if now - p.timestamp <= window_seconds
                and all(p.labels.get(k) == v for k, v in labels.items())
            ]
        else:
            recent = [p for p in points if now - p.timestamp <= window_seconds]

        if not recent:
            return 0.0

        return len(recent) / window_seconds

    def get_summary(self, labels: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Get a summary of all metrics.

        Args:
            labels: Optional label filter

        Returns:
            Summary dictionary
        """
        return {
            "service": self.service_name,
            "timestamp": datetime.now().isoformat(),
            "requests_in_progress": len(self._request_start_times),
            "rates": {
                "api_requests_per_second": self.get_rate("api_requests_total"),
            },
            "latency": self.get_stats("api_latency_seconds"),
            "errors": self.get_stats("api_errors_total"),
        }

    def reset(self) -> None:
        """Reset all metrics."""
        self._time_series.clear()
        self._request_start_times.clear()
        self.registry = MetricRegistry()

    def export_prometheus(self) -> str:
        """
        Export metrics in Prometheus format.

        Returns:
            Prometheus-formatted metrics string
        """
        lines = []
        now = time.time()

        for name, value in self.registry._counters.items():
            labels_str = self._parse_labels_from_key(name)
            lines.append(f"{name}{labels_str} {value}")

        for name, value in self.registry._gauges.items():
            labels_str = self._parse_labels_from_key(name)
            lines.append(f"{name}{labels_str} {value}")

        for name, values in self.registry._histograms.items():
            if values:
                labels_str = self._parse_labels_from_key(name)
                avg = sum(values) / len(values)
                lines.append(f"{name}_sum{labels_str} {sum(values)}")
                lines.append(f"{name}_count{labels_str} {len(values)}")
                lines.append(f"{name}_avg{labels_str} {avg}")

        return "\n".join(lines)

    def _parse_labels_from_key(self, key: str) -> str:
        """Parse labels from metric key."""
        if "{" not in key:
            return ""
        label_str = key[key.index("{") : key.index("}") + 1]
        return label_str


class MetricsMiddleware:
    """Middleware for automatic metrics collection."""

    def __init__(self, metrics: APIMetricsAction):
        self.metrics = metrics

    async def __call__(self, request, call_next):
        """Process request and record metrics."""
        import uuid

        request_id = str(uuid.uuid4())
        method = request.get("method", "GET")
        endpoint = request.get("path", "/")

        self.metrics.start_request(request_id, method, endpoint)

        try:
            response = await call_next(request)
            status_code = response.get("status_code", 200)
            error = response.get("error")
        except Exception as e:
            status_code = 500
            error = type(e).__name__
            raise
        finally:
            self.metrics.end_request(request_id, method, endpoint, status_code, error)

        return response
