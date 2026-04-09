"""API metrics collection and reporting.

This module provides metrics collection:
- Request/response metrics
- Latency tracking
- Error rate monitoring
- Custom metrics

Example:
    >>> from actions.api_metrics_action import MetricsCollector
    >>> collector = MetricsCollector()
    >>> collector.record_request("GET", "/api/users", 200, latency=0.05)
"""

from __future__ import annotations

import time
import threading
import logging
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
from enum import Enum

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Types of metrics."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class MetricValue:
    """A single metric value."""
    name: str
    value: float
    labels: dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class RequestMetric:
    """Request metric data."""
    method: str
    path: str
    status_code: int
    latency: float
    timestamp: float = field(default_factory=time.time)


class MetricsCollector:
    """Collect and aggregate API metrics.

    Example:
        >>> collector = MetricsCollector()
        >>> collector.record_request("GET", "/api/users", 200, latency=0.05)
        >>> stats = collector.get_stats()
    """

    def __init__(
        self,
        retention_period: float = 60.0,
        max_metrics: int = 10000,
    ) -> None:
        self.retention_period = retention_period
        self.max_metrics = max_metrics
        self._metrics: dict[str, deque[MetricValue]] = defaultdict(lambda: deque(maxlen=max_metrics))
        self._request_metrics: deque[RequestMetric] = deque(maxlen=max_metrics)
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}
        self._lock = threading.RLock()

    def record_request(
        self,
        method: str,
        path: str,
        status_code: int,
        latency: float,
    ) -> None:
        """Record a request metric.

        Args:
            method: HTTP method.
            path: Request path.
            status_code: Response status code.
            latency: Request latency in seconds.
        """
        metric = RequestMetric(
            method=method,
            path=path,
            status_code=status_code,
            latency=latency,
        )
        with self._lock:
            self._request_metrics.append(metric)
            self._metrics[f"request.{method}.{path}"].append(MetricValue(
                name=f"request.{method}.{path}",
                value=latency,
                labels={"method": method, "path": path, "status": str(status_code)},
            ))

    def increment_counter(self, name: str, value: float = 1.0) -> None:
        """Increment a counter metric.

        Args:
            name: Counter name.
            value: Value to add.
        """
        with self._lock:
            self._counters[name] += value

    def set_gauge(self, name: str, value: float) -> None:
        """Set a gauge metric.

        Args:
            name: Gauge name.
            value: Gauge value.
        """
        with self._lock:
            self._gauges[name] = value

    def record_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict[str, str]] = None,
    ) -> None:
        """Record a histogram value.

        Args:
            name: Histogram name.
            value: Observed value.
            labels: Optional labels.
        """
        with self._lock:
            self._metrics[name].append(MetricValue(
                name=name,
                value=value,
                labels=labels or {},
            ))

    def start_timer(self, name: str) -> Callable[[], float]:
        """Start a timer.

        Args:
            name: Timer name.

        Returns:
            Callable that returns elapsed time when called.
        """
        start = time.time()
        def elapsed() -> float:
            return time.time() - start
        return elapsed

    def get_stats(self) -> dict[str, Any]:
        """Get aggregated statistics.

        Returns:
            Dictionary of statistics.
        """
        with self._lock:
            now = time.time()
            cutoff = now - self.retention_period
            recent_requests = [m for m in self._request_metrics if m.timestamp > cutoff]
            total_requests = len(recent_requests)
            if total_requests == 0:
                return {"total_requests": 0}
            latencies = [m.latency for m in recent_requests]
            sorted_latencies = sorted(latencies)
            p50 = sorted_latencies[int(len(sorted_latencies) * 0.5)]
            p95 = sorted_latencies[int(len(sorted_latencies) * 0.95)]
            p99 = sorted_latencies[int(len(sorted_latencies) * 0.99)]
            error_count = sum(1 for m in recent_requests if m.status_code >= 400)
            return {
                "total_requests": total_requests,
                "requests_per_second": total_requests / self.retention_period,
                "avg_latency": sum(latencies) / len(latencies),
                "p50_latency": p50,
                "p95_latency": p95,
                "p99_latency": p99,
                "error_rate": error_count / total_requests if total_requests > 0 else 0,
                "error_count": error_count,
                "counters": dict(self._counters),
                "gauges": dict(self._gauges),
            }

    def get_request_stats(
        self,
        method: Optional[str] = None,
        path: Optional[str] = None,
    ) -> dict[str, Any]:
        """Get request-specific statistics.

        Args:
            method: Filter by HTTP method.
            path: Filter by path.

        Returns:
            Statistics dictionary.
        """
        with self._lock:
            metrics = list(self._request_metrics)
            if method:
                metrics = [m for m in metrics if m.method == method]
            if path:
                metrics = [m for m in metrics if m.path == path]
            if not metrics:
                return {"count": 0}
            latencies = [m.latency for m in metrics]
            status_codes = defaultdict(int)
            for m in metrics:
                status_codes[m.status_code] += 1
            return {
                "count": len(metrics),
                "avg_latency": sum(latencies) / len(latencies),
                "min_latency": min(latencies),
                "max_latency": max(latencies),
                "status_codes": dict(status_codes),
            }

    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics.clear()
            self._request_metrics.clear()
            self._counters.clear()
            self._gauges.clear()


class MetricsReporter:
    """Report metrics to various backends.

    Example:
        >>> reporter = MetricsReporter([ConsoleReporter(), PrometheusReporter()])
        >>> reporter.report(collector.get_stats())
    """

    def __init__(self, reporters: Optional[list[Any]] = None) -> None:
        self.reporters = reporters or []

    def add_reporter(self, reporter: Any) -> None:
        """Add a reporter."""
        self.reporters.append(reporter)

    def report(self, metrics: dict[str, Any]) -> None:
        """Report metrics to all backends.

        Args:
            metrics: Metrics dictionary to report.
        """
        for reporter in self.reporters:
            try:
                reporter.send(metrics)
            except Exception as e:
                logger.error(f"Reporter {reporter} failed: {e}")


class ConsoleReporter:
    """Print metrics to console."""

    def send(self, metrics: dict[str, Any]) -> None:
        """Print metrics."""
        print(f"[Metrics] {metrics}")
