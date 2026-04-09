"""
API Metrics Action Module.

API metrics collection with counters, gauges, histograms,
and automatic Prometheus/StatsD export.
"""

import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricValue:
    """Metric value with metadata."""
    name: str
    metric_type: MetricType
    value: float
    labels: dict
    timestamp: float


@dataclass
class APIMetrics:
    """API metrics summary."""
    total_requests: int
    success_count: int
    error_count: int
    avg_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float


class APIMetricsAction:
    """
    API metrics collection and reporting.

    Example:
        metrics = APIMetricsAction()
        metrics.increment_counter("http_requests_total", labels={"method": "GET"})
        metrics.record_histogram("http_request_duration_ms", 123.45)
        summary = metrics.get_summary()
    """

    def __init__(self, service_name: str = "api"):
        """
        Initialize API metrics.

        Args:
            service_name: Service identifier.
        """
        self.service_name = service_name
        self._counters: dict[str, float] = {}
        self._gauges: dict[str, float] = {}
        self._histograms: dict[str, list[float]] = {}
        self._metrics_index: dict[str, MetricType] = {}
        self._labels_index: dict[str, dict] = {}

    def increment_counter(
        self,
        name: str,
        value: float = 1.0,
        labels: Optional[dict] = None
    ) -> None:
        """
        Increment a counter metric.

        Args:
            name: Metric name.
            value: Increment value.
            labels: Metric labels.
        """
        key = self._make_key(name, labels)
        self._counters[key] = self._counters.get(key, 0.0) + value
        self._metrics_index[key] = MetricType.COUNTER
        self._labels_index[key] = labels or {}

    def set_gauge(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None
    ) -> None:
        """
        Set a gauge metric.

        Args:
            name: Metric name.
            value: Gauge value.
            labels: Metric labels.
        """
        key = self._make_key(name, labels)
        self._gauges[key] = value
        self._metrics_index[key] = MetricType.GAUGE
        self._labels_index[key] = labels or {}

    def record_histogram(
        self,
        name: str,
        value: float,
        labels: Optional[dict] = None
    ) -> None:
        """
        Record a histogram value.

        Args:
            name: Metric name.
            value: Observed value.
            labels: Metric labels.
        """
        key = self._make_key(name, labels)
        if key not in self._histograms:
            self._histograms[key] = []
        self._histograms[key].append(value)
        self._metrics_index[key] = MetricType.HISTOGRAM
        self._labels_index[key] = labels or {}

    def record_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float
    ) -> None:
        """
        Record API request metrics.

        Args:
            method: HTTP method.
            path: Request path.
            status_code: Response status code.
            duration_ms: Request duration in milliseconds.
        """
        labels = {"method": method, "path": path, "status": str(status_code)}

        self.increment_counter("http_requests_total", labels=labels)

        if status_code >= 200 and status_code < 400:
            self.increment_counter("http_requests_success", labels=labels)
        else:
            self.increment_counter("http_requests_errors", labels=labels)

        self.record_histogram("http_request_duration_ms", duration_ms, labels=labels)

    def get_counter(self, name: str, labels: Optional[dict] = None) -> float:
        """Get counter value."""
        key = self._make_key(name, labels)
        return self._counters.get(key, 0.0)

    def get_gauge(self, name: str, labels: Optional[dict] = None) -> Optional[float]:
        """Get gauge value."""
        key = self._make_key(name, labels)
        return self._gauges.get(key)

    def get_histogram_stats(
        self,
        name: str,
        labels: Optional[dict] = None
    ) -> Optional[dict]:
        """Get histogram statistics."""
        key = self._make_key(name, labels)
        values = self._histograms.get(key)

        if not values:
            return None

        sorted_values = sorted(values)
        count = len(sorted_values)

        return {
            "count": count,
            "sum": sum(sorted_values),
            "min": sorted_values[0],
            "max": sorted_values[-1],
            "mean": sum(sorted_values) / count,
            "p50": sorted_values[int(count * 0.50)],
            "p90": sorted_values[int(count * 0.90)] if count > 1 else sorted_values[0],
            "p95": sorted_values[int(count * 0.95)] if count > 1 else sorted_values[0],
            "p99": sorted_values[int(count * 0.99)] if count > 1 else sorted_values[0],
        }

    def get_summary(self) -> APIMetrics:
        """Get API metrics summary."""
        request_labels = self._get_matching_keys("http_requests_total", MetricType.COUNTER)

        total = sum(self._counters.get(k, 0.0) for k in request_labels)
        success = sum(self._counters.get(k, 0.0) for k in self._get_matching_keys("http_requests_success", MetricType.COUNTER))
        errors = sum(self._counters.get(k, 0.0) for k in self._get_matching_keys("http_requests_errors", MetricType.COUNTER))

        all_durations = []
        for k, vals in self._histograms.items():
            if "http_request_duration" in k:
                all_durations.extend(vals)

        if all_durations:
            sorted_durations = sorted(all_durations)
            count = len(sorted_durations)
            avg = sum(sorted_durations) / count
            p95 = sorted_durations[int(count * 0.95)] if count > 1 else sorted_durations[0]
            p99 = sorted_durations[int(count * 0.99)] if count > 1 else sorted_durations[0]
        else:
            avg = p95 = p99 = 0.0

        return APIMetrics(
            total_requests=int(total),
            success_count=int(success),
            error_count=int(errors),
            avg_latency_ms=avg,
            p95_latency_ms=p95,
            p99_latency_ms=p99
        )

    def _get_matching_keys(self, prefix: str, metric_type: MetricType) -> list[str]:
        """Get keys matching prefix and metric type."""
        return [k for k, t in self._metrics_index.items() if k.startswith(prefix) and t == metric_type]

    def _make_key(self, name: str, labels: Optional[dict]) -> str:
        """Create metric key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def export_prometheus(self) -> str:
        """
        Export metrics in Prometheus format.

        Returns:
            Prometheus-formatted metrics string.
        """
        lines = [f"# HELP {self.service_name}_metrics API metrics"]
        lines.append(f"# TYPE {self.service_name}_metrics counter")

        for key, value in self._counters.items():
            name = key.split("{")[0]
            lines.append(f'{self.service_name}_{name} {value}')

        for key, value in self._gauges.items():
            name = key.split("{")[0]
            lines.append(f'{self.service_name}_{name} {value}')

        return "\n".join(lines)

    def reset(self) -> None:
        """Reset all metrics."""
        self._counters.clear()
        self._gauges.clear()
        self._histograms.clear()
        self._metrics_index.clear()
        self._labels_index.clear()

    def get_all_metrics(self) -> list[MetricValue]:
        """Get all metric values."""
        metrics = []

        for key, value in self._counters.items():
            name = key.split("{")[0]
            metrics.append(MetricValue(
                name=name,
                metric_type=MetricType.COUNTER,
                value=value,
                labels=self._labels_index.get(key, {}),
                timestamp=time.time()
            ))

        for key, value in self._gauges.items():
            name = key.split("{")[0]
            metrics.append(MetricValue(
                name=name,
                metric_type=MetricType.GAUGE,
                value=value,
                labels=self._labels_index.get(key, {}),
                timestamp=time.time()
            ))

        for key, values in self._histograms.items():
            name = key.split("{")[0]
            if values:
                metrics.append(MetricValue(
                    name=name,
                    metric_type=MetricType.HISTOGRAM,
                    value=values[-1],
                    labels=self._labels_index.get(key, {}),
                    timestamp=time.time()
                ))

        return metrics
