"""
API Metrics Action Module.

Provides API metrics collection and monitoring
including latency, throughput, and error rates.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import time

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"
    SUMMARY = "summary"


@dataclass
class Metric:
    """Base metric."""
    name: str
    metric_type: MetricType
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class Counter:
    """Counter metric."""
    name: str
    value: int = 0
    labels: Dict[str, str] = field(default_factory=dict)

    def increment(self, amount: int = 1):
        """Increment counter."""
        self.value += amount


@dataclass
class Gauge:
    """Gauge metric."""
    name: str
    value: float = 0.0
    labels: Dict[str, str] = field(default_factory=dict)

    def set(self, value: float):
        """Set gauge value."""
        self.value = value

    def increment(self, amount: float = 1.0):
        """Increment gauge."""
        self.value += amount

    def decrement(self, amount: float = 1.0):
        """Decrement gauge."""
        self.value -= amount


@dataclass
class Histogram:
    """Histogram metric."""
    name: str
    buckets: Dict[float, int] = field(default_factory=dict)
    sum: float = 0.0
    count: int = 0
    labels: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if not self.buckets:
            self.buckets = {0.005: 0, 0.01: 0, 0.025: 0, 0.05: 0, 0.1: 0,
                           0.25: 0, 0.5: 0, 1.0: 0, 2.5: 0, 5.0: 0, 10.0: 0}

    def observe(self, value: float):
        """Observe a value."""
        self.sum += value
        self.count += 1
        for bucket_limit in sorted(self.buckets.keys()):
            if value <= bucket_limit:
                self.buckets[bucket_limit] += 1

    def get_percentile(self, percentile: float) -> float:
        """Calculate percentile."""
        if self.count == 0:
            return 0.0

        sorted_buckets = sorted(self.buckets.items())
        threshold = self.count * percentile / 100.0

        cumulative = 0
        for bucket_limit, count in sorted_buckets:
            cumulative += count
            if cumulative >= threshold:
                return bucket_limit

        return sorted_buckets[-1][0] if sorted_buckets else 0.0


class MetricsCollector:
    """Collects and manages metrics."""

    def __init__(self):
        self.counters: Dict[str, Counter] = {}
        self.gauges: Dict[str, Gauge] = {}
        self.histograms: Dict[str, Histogram] = {}
        self.timers: Dict[str, List[float]] = {}

    def get_counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> Counter:
        """Get or create counter."""
        key = self._make_key(name, labels or {})
        if key not in self.counters:
            self.counters[key] = Counter(name=name, labels=labels or {})
        return self.counters[key]

    def get_gauge(self, name: str, labels: Optional[Dict[str, str]] = None) -> Gauge:
        """Get or create gauge."""
        key = self._make_key(name, labels or {})
        if key not in self.gauges:
            self.gauges[key] = Gauge(name=name, labels=labels or {})
        return self.gauges[key]

    def get_histogram(self, name: str, labels: Optional[Dict[str, str]] = None) -> Histogram:
        """Get or create histogram."""
        key = self._make_key(name, labels or {})
        if key not in self.histograms:
            self.histograms[key] = Histogram(name=name, labels=labels or {})
        return self.histograms[key]

    def _make_key(self, name: str, labels: Dict[str, str]) -> str:
        """Make unique key for metric."""
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}" if label_str else name

    def increment(self, name: str, amount: int = 1, labels: Optional[Dict[str, str]] = None):
        """Increment counter."""
        counter = self.get_counter(name, labels)
        counter.increment(amount)

    def decrement(self, name: str, amount: int = 1, labels: Optional[Dict[str, str]] = None):
        """Decrement counter."""
        counter = self.get_counter(name, labels)
        counter.increment(-amount)

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set gauge value."""
        gauge = self.get_gauge(name, labels)
        gauge.set(value)

    def observe(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Observe value for histogram."""
        histogram = self.get_histogram(name, labels)
        histogram.observe(value)

    def start_timer(self, name: str, labels: Optional[Dict[str, str]] = None) -> float:
        """Start a timer."""
        key = self._make_key(name, labels or {})
        if key not in self.timers:
            self.timers[key] = []
        start_time = time.perf_counter()
        return start_time

    def stop_timer(self, name: str, start_time: float, labels: Optional[Dict[str, str]] = None):
        """Stop timer and record duration."""
        duration = time.perf_counter() - start_time
        key = self._make_key(name, labels or {})
        if key not in self.timers:
            self.timers[key] = []
        self.timers[key].append(duration)
        self.observe(name, duration, labels)


class APIMetrics:
    """API-specific metrics collection."""

    def __init__(self, collector: MetricsCollector):
        self.collector = collector
        self._active_requests: Dict[str, float] = {}

    def record_request(
        self,
        method: str,
        path: str,
        status_code: int,
        duration: float
    ):
        """Record API request metrics."""
        labels = {"method": method, "path": path, "status": str(status_code)}

        self.collector.increment("api_requests_total", labels=labels)
        self.collector.observe("api_request_duration_seconds", duration, labels=labels)

        if status_code >= 500:
            self.collector.increment("api_errors_total", labels={"method": method, "type": "server"})
        elif status_code >= 400:
            self.collector.increment("api_errors_total", labels={"method": method, "type": "client"})

    def record_active_requests(self, delta: int = 1):
        """Record active requests."""
        gauge = self.collector.get_gauge("api_active_requests")
        gauge.increment(delta)

    def record_request_size(self, method: str, path: str, size: int):
        """Record request size."""
        labels = {"method": method, "path": path}
        self.collector.observe("api_request_size_bytes", float(size), labels=labels)

    def record_response_size(self, method: str, path: str, size: int):
        """Record response size."""
        labels = {"method": method, "path": path}
        self.collector.observe("api_response_size_bytes", float(size), labels=labels)


class MetricsAggregator:
    """Aggregates metrics over time windows."""

    def __init__(self, collector: MetricsCollector):
        self.collector = collector

    def get_summary(self, window: timedelta = timedelta(minutes=5)) -> Dict[str, Any]:
        """Get metrics summary."""
        summary = {
            "timestamp": datetime.now(),
            "window": str(window),
            "counters": {},
            "gauges": {},
            "histograms": {}
        }

        for key, counter in self.collector.counters.items():
            summary["counters"][key] = counter.value

        for key, gauge in self.collector.gauges.items():
            summary["gauges"][key] = gauge.value

        for key, histogram in self.collector.histograms.items():
            summary["histograms"][key] = {
                "count": histogram.count,
                "sum": histogram.sum,
                "avg": histogram.sum / histogram.count if histogram.count > 0 else 0,
                "p50": histogram.get_percentile(50),
                "p90": histogram.get_percentile(90),
                "p99": histogram.get_percentile(99)
            }

        return summary


def main():
    """Demonstrate API metrics."""
    collector = MetricsCollector()
    api_metrics = APIMetrics(collector)

    api_metrics.record_request("GET", "/api/users", 200, 0.05)
    api_metrics.record_request("GET", "/api/users", 200, 0.03)
    api_metrics.record_request("POST", "/api/users", 201, 0.10)

    aggregator = MetricsAggregator(collector)
    summary = aggregator.get_summary()

    print(f"Total requests: {summary['counters'].get('api_requests_total', 0)}")
    print(f"Active requests: {summary['gauges'].get('api_active_requests', 0)}")


if __name__ == "__main__":
    main()
