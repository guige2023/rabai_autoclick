"""Metric action module for RabAI AutoClick.

Provides metrics collection utilities:
- MetricCollector: Collect metrics
- Counters, Gauges, Histograms: Metric types
- MetricRegistry: Manage metrics
"""

from typing import Any, Callable, Dict, List, Optional
import threading
import time
import uuid

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class Counter:
    """Simple counter metric."""

    def __init__(self, name: str):
        self.name = name
        self._value = 0
        self._lock = threading.RLock()

    def increment(self, value: float = 1) -> None:
        """Increment counter."""
        with self._lock:
            self._value += value

    def get(self) -> float:
        """Get current value."""
        with self._lock:
            return self._value

    def reset(self) -> None:
        """Reset counter."""
        with self._lock:
            self._value = 0


class Gauge:
    """Simple gauge metric."""

    def __init__(self, name: str):
        self.name = name
        self._value = 0.0
        self._lock = threading.RLock()

    def set(self, value: float) -> None:
        """Set gauge value."""
        with self._lock:
            self._value = value

    def get(self) -> float:
        """Get current value."""
        with self._lock:
            return self._value

    def increment(self, value: float = 1) -> None:
        """Increment gauge."""
        with self._lock:
            self._value += value

    def decrement(self, value: float = 1) -> None:
        """Decrement gauge."""
        with self._lock:
            self._value -= value


class Histogram:
    """Simple histogram metric."""

    def __init__(self, name: str, buckets: Optional[List[float]] = None):
        self.name = name
        self.buckets = buckets or [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
        self._values: List[float] = []
        self._lock = threading.RLock()

    def observe(self, value: float) -> None:
        """Record an observation."""
        with self._lock:
            self._values.append(value)

    def get_stats(self) -> Dict[str, float]:
        """Get histogram statistics."""
        with self._lock:
            if not self._values:
                return {"count": 0, "sum": 0, "min": 0, "max": 0, "avg": 0}

            sorted_values = sorted(self._values)
            count = len(sorted_values)

            return {
                "count": count,
                "sum": sum(sorted_values),
                "min": sorted_values[0],
                "max": sorted_values[-1],
                "avg": sum(sorted_values) / count,
                "p50": sorted_values[int(count * 0.5)],
                "p95": sorted_values[int(count * 0.95)],
                "p99": sorted_values[int(count * 0.99)],
            }


class MetricRegistry:
    """Central metric registry."""

    def __init__(self):
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._lock = threading.RLock()

    def counter(self, name: str) -> Counter:
        """Get or create counter."""
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name)
            return self._counters[name]

    def gauge(self, name: str) -> Gauge:
        """Get or create gauge."""
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name)
            return self._gauges[name]

    def histogram(self, name: str, buckets: Optional[List[float]] = None) -> Histogram:
        """Get or create histogram."""
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, buckets)
            return self._histograms[name]

    def get_all(self) -> Dict[str, Dict[str, Any]]:
        """Get all metrics."""
        with self._lock:
            result = {}

            for name, c in self._counters.items():
                result[name] = {"type": "counter", "value": c.get()}

            for name, g in self._gauges.items():
                result[name] = {"type": "gauge", "value": g.get()}

            for name, h in self._histograms.items():
                result[name] = {"type": "histogram", "stats": h.get_stats()}

            return result


class MetricAction(BaseAction):
    """Metric collection action."""
    action_type = "metric"
    display_name = "指标收集"
    description = "监控指标"

    def __init__(self):
        super().__init__()
        self._registry = MetricRegistry()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "inc_counter")

            if operation == "inc_counter":
                return self._inc_counter(params)
            elif operation == "set_gauge":
                return self._set_gauge(params)
            elif operation == "observe":
                return self._observe(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "list":
                return self._list()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Metric error: {str(e)}")

    def _inc_counter(self, params: Dict[str, Any]) -> ActionResult:
        """Increment counter."""
        name = params.get("name")
        value = params.get("value", 1)

        if not name:
            return ActionResult(success=False, message="name is required")

        counter = self._registry.counter(name)
        counter.increment(value)

        return ActionResult(success=True, message=f"Incremented: {name}", data={"name": name, "value": counter.get()})

    def _set_gauge(self, params: Dict[str, Any]) -> ActionResult:
        """Set gauge value."""
        name = params.get("name")
        value = params.get("value", 0)

        if not name:
            return ActionResult(success=False, message="name is required")

        gauge = self._registry.gauge(name)
        gauge.set(value)

        return ActionResult(success=True, message=f"Set: {name}", data={"name": name, "value": gauge.get()})

    def _observe(self, params: Dict[str, Any]) -> ActionResult:
        """Observe histogram value."""
        name = params.get("name")
        value = params.get("value", 0)

        if not name:
            return ActionResult(success=False, message="name is required")

        histogram = self._registry.histogram(name)
        histogram.observe(value)

        return ActionResult(success=True, message=f"Observed: {name}", data={"name": name})

    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get metric value."""
        name = params.get("name")

        if not name:
            return ActionResult(success=False, message="name is required")

        all_metrics = self._registry.get_all()

        if name not in all_metrics:
            return ActionResult(success=False, message=f"Metric not found: {name}")

        return ActionResult(success=True, message=f"Got: {name}", data={"metric": all_metrics[name]})

    def _list(self) -> ActionResult:
        """List all metrics."""
        metrics = self._registry.get_all()
        return ActionResult(success=True, message=f"{len(metrics)} metrics", data={"metrics": metrics})
