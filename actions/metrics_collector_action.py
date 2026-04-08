"""Metrics collector action module for RabAI AutoClick.

Provides metrics collection:
- MetricsCollector: Collect and aggregate metrics
- Counter: Counter metric
- Gauge: Gauge metric
- Histogram: Histogram metric
- Timer: Timer metric
- MetricsExporter: Export metrics
"""

import time
import threading
from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from collections import defaultdict
from statistics import mean, median

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


@dataclass
class MetricPoint:
    """Single metric point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class Counter:
    """Counter metric."""
    name: str
    value: float = 0.0
    labels: Dict[str, str] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def inc(self, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Increment counter."""
        with self._lock:
            self.value += value

    def get(self) -> float:
        """Get current value."""
        return self.value

    def reset(self):
        """Reset counter."""
        with self._lock:
            self.value = 0.0


@dataclass
class Gauge:
    """Gauge metric."""
    name: str
    value: float = 0.0
    labels: Dict[str, str] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def set(self, value: float, labels: Optional[Dict[str, str]] = None):
        """Set gauge value."""
        with self._lock:
            self.value = value

    def inc(self, value: float = 1.0):
        """Increment gauge."""
        with self._lock:
            self.value += value

    def dec(self, value: float = 1.0):
        """Decrement gauge."""
        with self._lock:
            self.value -= value

    def get(self) -> float:
        """Get current value."""
        return self.value


@dataclass
class Histogram:
    """Histogram metric."""
    name: str
    buckets: List[float] = field(default_factory=lambda: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0])
    values: List[float] = field(default_factory=list)
    labels: Dict[str, str] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def observe(self, value: float, labels: Optional[Dict[str, str]] = None):
        """Observe a value."""
        with self._lock:
            self.values.append(value)

    def get_stats(self) -> Dict[str, float]:
        """Get histogram statistics."""
        with self._lock:
            if not self.values:
                return {"count": 0, "sum": 0, "avg": 0, "min": 0, "max": 0, "p50": 0, "p95": 0, "p99": 0}

            sorted_values = sorted(self.values)
            count = len(sorted_values)
            return {
                "count": count,
                "sum": sum(sorted_values),
                "avg": mean(sorted_values),
                "min": min(sorted_values),
                "max": max(sorted_values),
                "p50": sorted_values[int(count * 0.5)],
                "p95": sorted_values[int(count * 0.95)] if count > 1 else sorted_values[0],
                "p99": sorted_values[int(count * 0.99)] if count > 1 else sorted_values[0],
            }

    def get_bucket_counts(self) -> Dict[float, int]:
        """Get bucket counts."""
        with self._lock:
            result = {}
            for bucket in self.buckets:
                result[bucket] = sum(1 for v in self.values if v <= bucket)
            return result

    def reset(self):
        """Reset histogram."""
        with self._lock:
            self.values.clear()


@dataclass
class Timer:
    """Timer metric."""
    name: str
    histogram: Histogram = field(default_factory=lambda: Histogram(name=""))
    _start_time: Optional[float] = field(default=None, repr=False)

    def start(self):
        """Start timer."""
        self._start_time = time.time()

    def stop(self) -> float:
        """Stop timer and record duration."""
        if self._start_time is None:
            return 0.0
        duration = time.time() - self._start_time
        self.histogram.observe(duration)
        self._start_time = None
        return duration

    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, *args):
        """Context manager exit."""
        self.stop()


class MetricsCollector:
    """Central metrics collector."""

    def __init__(self):
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._timers: Dict[str, Timer] = {}
        self._lock = threading.RLock()

    def counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> Counter:
        """Get or create counter."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._counters:
                self._counters[key] = Counter(name=name, labels=labels or {})
            return self._counters[key]

    def gauge(self, name: str, labels: Optional[Dict[str, str]] = None) -> Gauge:
        """Get or create gauge."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._gauges:
                self._gauges[key] = Gauge(name=name, labels=labels or {})
            return self._gauges[key]

    def histogram(self, name: str, labels: Optional[Dict[str, str]] = None, buckets: Optional[List[float]] = None) -> Histogram:
        """Get or create histogram."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._histograms:
                h = Histogram(name=name, labels=labels or {})
                if buckets:
                    h.buckets = buckets
                self._histograms[key] = h
            return self._histograms[key]

    def timer(self, name: str, labels: Optional[Dict[str, str]] = None) -> Timer:
        """Get or create timer."""
        with self._lock:
            key = self._make_key(name, labels)
            if key not in self._timers:
                h = self.histogram(f"{name}_duration", labels)
                self._timers[key] = Timer(name=name, histogram=h)
            return self._timers[key]

    def inc_counter(self, name: str, value: float = 1.0, labels: Optional[Dict[str, str]] = None):
        """Increment counter."""
        self.counter(name, labels).inc(value)

    def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Set gauge value."""
        self.gauge(name, labels).set(value)

    def observe_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None):
        """Observe value for histogram."""
        self.histogram(name, labels).observe(value)

    def time_function(self, name: str, func: Callable, labels: Optional[Dict[str, str]] = None) -> Any:
        """Time a function execution."""
        t = self.timer(name, labels)
        t.start()
        try:
            return func()
        finally:
            t.stop()

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics."""
        with self._lock:
            result = {
                "counters": {},
                "gauges": {},
                "histograms": {},
            }

            for key, counter in self._counters.items():
                result["counters"][key] = {"name": counter.name, "value": counter.value, "labels": counter.labels}

            for key, gauge in self._gauges.items():
                result["gauges"][key] = {"name": gauge.name, "value": gauge.value, "labels": gauge.labels}

            for key, hist in self._histograms.items():
                result["histograms"][key] = {
                    "name": hist.name,
                    "stats": hist.get_stats(),
                    "labels": hist.labels,
                }

            return result

    def reset_all(self):
        """Reset all metrics."""
        with self._lock:
            for counter in self._counters.values():
                counter.reset()
            for gauge in self._gauges.values():
                gauge.set(0.0)
            for hist in self._histograms.values():
                hist.reset()

    def _make_key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Make metric key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"


class MetricsExporter:
    """Export metrics in various formats."""

    @staticmethod
    def to_prometheus(metrics: Dict[str, Any]) -> str:
        """Export to Prometheus format."""
        lines = []

        for key, data in metrics.get("counters", {}).items():
            lines.append(f'# TYPE {data["name"]} counter')
            lines.append(f'{data["name"]}{{labels="{data["labels"]}"}} {data["value"]}')

        for key, data in metrics.get("gauges", {}).items():
            lines.append(f'# TYPE {data["name"]} gauge')
            lines.append(f'{data["name"]}{{labels="{data["labels"]}"}} {data["value"]}')

        for key, data in metrics.get("histograms", {}).items():
            lines.append(f'# TYPE {data["name"]} histogram')
            stats = data["stats"]
            lines.append(f'{data["name"]}{{quantile="0.5"}} {stats.get("p50", 0)}')
            lines.append(f'{data["name"]}{{quantile="0.95"}} {stats.get("p95", 0)}')
            lines.append(f'{data["name"]}{{quantile="0.99"}} {stats.get("p99", 0)}')

        return "\n".join(lines)

    @staticmethod
    def to_json(metrics: Dict[str, Any]) -> str:
        """Export to JSON."""
        import json
        return json.dumps(metrics, indent=2)


class MetricsCollectorAction(BaseAction):
    """Metrics collector action."""
    action_type = "metrics_collector"
    display_name = "指标收集器"
    description = "系统指标收集和聚合"

    def __init__(self):
        super().__init__()
        self._collector = MetricsCollector()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "inc")

            if operation == "inc":
                return self._inc_counter(params)
            elif operation == "set":
                return self._set_gauge(params)
            elif operation == "observe":
                return self._observe_histogram(params)
            elif operation == "time":
                return self._time_operation(params)
            elif operation == "get":
                return self._get_metrics(params)
            elif operation == "export":
                return self._export_metrics(params)
            elif operation == "reset":
                return self._reset_metrics()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Metrics error: {str(e)}")

    def _inc_counter(self, params: Dict) -> ActionResult:
        """Increment counter."""
        name = params.get("name", "counter")
        value = params.get("value", 1.0)
        labels = params.get("labels", {})

        self._collector.inc_counter(name, value, labels)
        return ActionResult(success=True, message=f"Counter '{name}' incremented by {value}")

    def _set_gauge(self, params: Dict) -> ActionResult:
        """Set gauge value."""
        name = params.get("name", "gauge")
        value = params.get("value", 0.0)
        labels = params.get("labels", {})

        self._collector.set_gauge(name, value, labels)
        return ActionResult(success=True, message=f"Gauge '{name}' set to {value}")

    def _observe_histogram(self, params: Dict) -> ActionResult:
        """Observe histogram value."""
        name = params.get("name", "histogram")
        value = params.get("value", 0.0)
        labels = params.get("labels", {})

        self._collector.observe_histogram(name, value, labels)
        return ActionResult(success=True, message=f"Observed {value} for '{name}'")

    def _time_operation(self, params: Dict) -> ActionResult:
        """Time an operation."""
        name = params.get("name", "operation")
        func = params.get("func")

        if not func:
            return ActionResult(success=False, message="func is required")

        duration = self._collector.time_function(name, func)
        return ActionResult(
            success=True,
            message=f"Operation '{name}' took {duration:.4f}s",
            data={"duration": duration},
        )

    def _get_metrics(self, params: Dict) -> ActionResult:
        """Get all metrics."""
        metrics = self._collector.get_all_metrics()
        return ActionResult(success=True, message="Metrics retrieved", data=metrics)

    def _export_metrics(self, params: Dict) -> ActionResult:
        """Export metrics."""
        format_type = params.get("format", "json")
        metrics = self._collector.get_all_metrics()

        if format_type == "prometheus":
            output = MetricsExporter.to_prometheus(metrics)
        else:
            output = MetricsExporter.to_json(metrics)

        return ActionResult(success=True, message=f"Exported as {format_type}", data={"output": output})

    def _reset_metrics(self) -> ActionResult:
        """Reset all metrics."""
        self._collector.reset_all()
        return ActionResult(success=True, message="All metrics reset")
