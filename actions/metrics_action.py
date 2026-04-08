"""Metrics Action Module.

Provides metrics collection and aggregation
with time series support.
"""

import time
import threading
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MetricPoint:
    """A single metric point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


class MetricsCollector:
    """Collects and aggregates metrics."""

    def __init__(self):
        self._metrics: Dict[str, List[MetricPoint]] = defaultdict(list)
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._lock = threading.Lock()
        self._max_points = 10000

    def record(
        self,
        name: str,
        value: float,
        labels: Optional[Dict] = None
    ) -> None:
        """Record a metric point."""
        with self._lock:
            self._metrics[name].append(MetricPoint(
                timestamp=time.time(),
                value=value,
                labels=labels or {}
            ))

            if len(self._metrics[name]) > self._max_points:
                self._metrics[name] = self._metrics[name][-self._max_points // 2:]

    def increment(self, name: str, amount: float = 1.0) -> None:
        """Increment a counter."""
        with self._lock:
            self._counters[name] += amount

    def gauge(self, name: str, value: float) -> None:
        """Set a gauge value."""
        with self._lock:
            self._gauges[name] = value

    def get(self, name: str, since: Optional[float] = None) -> List[Dict]:
        """Get metric points."""
        points = self._metrics.get(name, [])

        if since:
            points = [p for p in points if p.timestamp >= since]

        return [
            {
                "timestamp": p.timestamp,
                "value": p.value,
                "labels": p.labels
            }
            for p in points
        ]

    def counter(self, name: str) -> float:
        """Get counter value."""
        return self._counters.get(name, 0.0)

    def gauge_value(self, name: str) -> Optional[float]:
        """Get gauge value."""
        return self._gauges.get(name)

    def aggregate(
        self,
        name: str,
        operation: str,
        since: Optional[float] = None
    ) -> Optional[float]:
        """Aggregate metric values."""
        points = self.get(name, since)
        if not points:
            return None

        values = [p["value"] for p in points]

        if operation == "sum":
            return sum(values)
        elif operation == "avg":
            return sum(values) / len(values)
        elif operation == "min":
            return min(values)
        elif operation == "max":
            return max(values)
        elif operation == "count":
            return len(values)

        return None

    def list_metrics(self) -> List[str]:
        """List all metric names."""
        with self._lock:
            return list(self._metrics.keys())


class MetricsAction(BaseAction):
    """Action for metrics operations."""

    def __init__(self):
        super().__init__("metrics")
        self._collector = MetricsCollector()

    def execute(self, params: Dict) -> ActionResult:
        """Execute metrics action."""
        try:
            operation = params.get("operation", "record")

            if operation == "record":
                return self._record(params)
            elif operation == "increment":
                return self._increment(params)
            elif operation == "gauge":
                return self._gauge(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "aggregate":
                return self._aggregate(params)
            elif operation == "list":
                return self._list(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=str(e))

    def _record(self, params: Dict) -> ActionResult:
        """Record a metric."""
        self._collector.record(
            params.get("name", ""),
            params.get("value", 0),
            params.get("labels")
        )
        return ActionResult(success=True)

    def _increment(self, params: Dict) -> ActionResult:
        """Increment a counter."""
        self._collector.increment(
            params.get("name", ""),
            params.get("amount", 1)
        )
        return ActionResult(success=True)

    def _gauge(self, params: Dict) -> ActionResult:
        """Set a gauge."""
        self._collector.gauge(
            params.get("name", ""),
            params.get("value", 0)
        )
        return ActionResult(success=True)

    def _get(self, params: Dict) -> ActionResult:
        """Get metric points."""
        since = params.get("since")
        if since:
            since = float(since)

        points = self._collector.get(params.get("name", ""), since)
        return ActionResult(success=True, data={"points": points})

    def _aggregate(self, params: Dict) -> ActionResult:
        """Aggregate metrics."""
        since = params.get("since")
        if since:
            since = float(since)

        value = self._collector.aggregate(
            params.get("name", ""),
            params.get("operation", "avg"),
            since
        )
        return ActionResult(success=True, data={"value": value})

    def _list(self, params: Dict) -> ActionResult:
        """List metrics."""
        metrics = self._collector.list_metrics()
        return ActionResult(success=True, data={"metrics": metrics})
