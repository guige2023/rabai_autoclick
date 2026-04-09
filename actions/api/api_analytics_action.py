"""
API Analytics Action Module.

Analytics and metrics collection for API services including
request tracking, performance analysis, and usage reporting.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of analytics metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class MetricPoint:
    """A single metric data point."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: float = field(default_factory=time.time)
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class AnalyticsReport:
    """Complete analytics report."""
    period_start: float
    period_end: float
    total_requests: int
    successful_requests: int
    failed_requests: int
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    top_endpoints: List[tuple[str, int]]
    error_breakdown: Dict[str, int]


class AnalyticsCollector:
    """Collects and aggregates analytics metrics."""

    def __init__(self) -> None:
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timers: Dict[str, List[float]] = defaultdict(list)
        self._labels: Dict[str, Dict[str, str]] = {}

    def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Increment a counter."""
        key = self._make_key(name, labels)
        self._counters[key] += value
        self._labels[key] = labels or {}

    def gauge(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Set a gauge value."""
        key = self._make_key(name, labels)
        self._gauges[key] = value
        self._labels[key] = labels or {}

    def histogram(
        self,
        name: str,
        value: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a histogram value."""
        key = self._make_key(name, labels)
        self._histograms[key].append(value)
        self._labels[key] = labels or {}

    def timer(
        self,
        name: str,
        duration_ms: float,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Record a timer duration."""
        key = self._make_key(name, labels)
        self._timers[key].append(duration_ms)
        self._labels[key] = labels or {}

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Create a metric key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def get_percentile(
        self,
        values: List[float],
        percentile: float,
    ) -> float:
        """Calculate percentile from a list of values."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * percentile)
        return sorted_values[min(index, len(sorted_values) - 1)]

    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of all collected metrics."""
        summary: Dict[str, Any] = {}

        # Counters
        for key, value in self._counters.items():
            summary[key] = {"type": "counter", "value": value}

        # Gauges
        for key, value in self._gauges.items():
            summary[key] = {"type": "gauge", "value": value}

        # Histograms
        for key, values in self._histograms.items():
            summary[key] = {
                "type": "histogram",
                "count": len(values),
                "sum": sum(values),
                "avg": sum(values) / len(values) if values else 0,
                "min": min(values) if values else 0,
                "max": max(values) if values else 0,
                "p50": self.get_percentile(values, 0.50),
                "p95": self.get_percentile(values, 0.95),
                "p99": self.get_percentile(values, 0.99),
            }

        # Timers
        for key, values in self._timers.items():
            summary[key] = {
                "type": "timer",
                "count": len(values),
                "avg_ms": sum(values) / len(values) if values else 0,
                "min_ms": min(values) if values else 0,
                "max_ms": max(values) if values else 0,
                "p50_ms": self.get_percentile(values, 0.50),
                "p95_ms": self.get_percentile(values, 0.95),
                "p99_ms": self.get_percentile(values, 0.99),
            }

        return summary

    def reset(self) -> None:
        """Reset all metrics."""
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()
        self._timers.clear()
        self._labels.clear()


class APIAnalyticsAction:
    """
    API analytics and metrics collection.

    Tracks API requests, latency, errors, and usage patterns.

    Example:
        analytics = APIAnalyticsAction()

        analytics.track_request("/api/users", method="GET", status=200, latency_ms=45)
        analytics.track_error("/api/users", method="POST", error_code=500)

        report = analytics.generate_report()
        print(f"Total requests: {report.total_requests}")
        print(f"P95 latency: {report.p95_latency_ms}ms")
    """

    def __init__(self) -> None:
        self.collector = AnalyticsCollector()
        self._request_latencies: Dict[str, List[float]] = defaultdict(list)
        self._endpoint_counts: Dict[str, int] = defaultdict(int)
        self._error_counts: Dict[str, int] = defaultdict(int)
        self._status_counts: Dict[int, int] = defaultdict(int)
        self._period_start = time.time()

    def track_request(
        self,
        endpoint: str,
        method: str = "GET",
        status: int = 200,
        latency_ms: float = 0.0,
        labels: Optional[Dict[str, str]] = None,
    ) -> None:
        """Track an API request."""
        lbls = {"endpoint": endpoint, "method": method, "status": str(status)}
        if labels:
            lbls.update(labels)

        self.collector.increment("api_requests_total", labels=lbls)
        self.collector.gauge("api_requests_in_progress", 1, labels=lbls)

        if latency_ms > 0:
            self.collector.timer("api_request_duration_ms", latency_ms, lbls)
            self._request_latencies[endpoint].append(latency_ms)

        self._endpoint_counts[endpoint] += 1
        self._status_counts[status] += 1

        if status >= 400:
            self._error_counts[endpoint] += 1

    def track_error(
        self,
        endpoint: str,
        method: str = "GET",
        error_code: int = 500,
        error_message: Optional[str] = None,
    ) -> None:
        """Track an API error."""
        lbls = {"endpoint": endpoint, "method": method, "error_code": str(error_code)}
        if error_message:
            lbls["error"] = error_message[:50]

        self.collector.increment("api_errors_total", labels=lbls)
        self._error_counts[f"{endpoint}:{error_code}"] += 1

    def track_payload_size(
        self,
        endpoint: str,
        request_size_bytes: int = 0,
        response_size_bytes: int = 0,
    ) -> None:
        """Track request/response payload sizes."""
        if request_size_bytes > 0:
            self.collector.histogram(
                "api_request_size_bytes",
                float(request_size_bytes),
                labels={"endpoint": endpoint},
            )
        if response_size_bytes > 0:
            self.collector.histogram(
                "api_response_size_bytes",
                float(response_size_bytes),
                labels={"endpoint": endpoint},
            )

    def generate_report(self) -> AnalyticsReport:
        """Generate an analytics report for the current period."""
        now = time.time()

        all_latencies = []
        for latencies in self._request_latencies.values():
            all_latencies.extend(latencies)

        total_requests = sum(self._status_counts.values())
        successful_requests = self._status_counts.get(200, 0) + self._status_counts.get(201, 0) + self._status_counts.get(204, 0)
        failed_requests = sum(c for s, c in self._status_counts.items() if s >= 400)

        # Sort endpoint counts for top endpoints
        top_endpoints = sorted(
            self._endpoint_counts.items(),
            key=lambda x: x[1],
            reverse=True,
        )[:10]

        return AnalyticsReport(
            period_start=self._period_start,
            period_end=now,
            total_requests=total_requests,
            successful_requests=successful_requests,
            failed_requests=failed_requests,
            avg_latency_ms=sum(all_latencies) / len(all_latencies) if all_latencies else 0,
            p50_latency_ms=self.collector.get_percentile(all_latencies, 0.50),
            p95_latency_ms=self.collector.get_percentile(all_latencies, 0.95),
            p99_latency_ms=self.collector.get_percentile(all_latencies, 0.99),
            top_endpoints=top_endpoints,
            error_breakdown=dict(self._error_counts),
        )

    def reset(self) -> None:
        """Reset analytics for a new period."""
        self.collector.reset()
        self._request_latencies.clear()
        self._endpoint_counts.clear()
        self._error_counts.clear()
        self._status_counts.clear()
        self._period_start = time.time()

    def get_summary(self) -> Dict[str, Any]:
        """Get raw metrics summary."""
        return self.collector.get_summary()
