"""Observability Metrics Action Module.

Collect and expose application metrics for monitoring.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .aggregation_engine_action import AggregationEngine


class MetricType(Enum):
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"


@dataclass
class MetricValue:
    """Single metric value."""
    value: float
    timestamp: float
    labels: dict[str, str] = field(default_factory=dict)


@dataclass
class MetricDefinition:
    """Metric definition."""
    name: str
    metric_type: MetricType
    description: str = ""
    unit: str = ""
    labels: list[str] = field(default_factory=list)


class MetricsCollector:
    """Collect application metrics."""

    def __init__(self) -> None:
        self._definitions: dict[str, MetricDefinition] = {}
        self._values: dict[str, list[MetricValue]] = defaultdict(list)
        self._counters: dict[str, float] = defaultdict(float)
        self._gauges: dict[str, float] = {}
        self._lock = asyncio.Lock()
        self._max_values_per_metric = 1000

    def register(
        self,
        name: str,
        metric_type: MetricType,
        description: str = "",
        unit: str = "",
        labels: list[str] | None = None
    ) -> None:
        """Register a metric."""
        self._definitions[name] = MetricDefinition(
            name=name,
            metric_type=metric_type,
            description=description,
            unit=unit,
            labels=labels or []
        )

    async def increment(
        self,
        name: str,
        value: float = 1.0,
        labels: dict[str, str] | None = None
    ) -> None:
        """Increment a counter metric."""
        async with self._lock:
            self._counters[name] += value
            self._record_value(name, self._counters[name], labels)

    async def set(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None
    ) -> None:
        """Set a gauge metric."""
        async with self._lock:
            self._gauges[name] = value
            self._record_value(name, value, labels)

    async def observe(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None = None
    ) -> None:
        """Observe a value for histogram/summary."""
        async with self._lock:
            self._record_value(name, value, labels)

    def _record_value(
        self,
        name: str,
        value: float,
        labels: dict[str, str] | None
    ) -> None:
        """Record metric value."""
        metric_value = MetricValue(
            value=value,
            timestamp=time.time(),
            labels=labels or {}
        )
        self._values[name].append(metric_value)
        if len(self._values[name]) > self._max_values_per_metric:
            self._values[name] = self._values[name][-self._max_values_per_metric // 2:]

    async def get_value(
        self,
        name: str,
        labels: dict[str, str] | None = None
    ) -> float | None:
        """Get current metric value."""
        async with self._lock:
            if name in self._counters:
                return self._counters[name]
            if name in self._gauges:
                return self._gauges[name]
            values = self._values.get(name, [])
            if not values:
                return None
            return values[-1].value

    async def get_histogram_stats(
        self,
        name: str,
        percentiles: list[float] | None = None
    ) -> dict[str, float]:
        """Get histogram statistics."""
        percentiles = percentiles or [0.5, 0.9, 0.99]
        async with self._lock:
            values = [v.value for v in self._values.get(name, [])]
            if not values:
                return {}
            sorted_values = sorted(values)
            result = {
                "count": len(values),
                "sum": sum(values),
                "min": min(values),
                "max": max(values),
                "avg": sum(values) / len(values),
            }
            for p in percentiles:
                idx = int(len(sorted_values) * p)
                result[f"p{int(p * 100)}"] = sorted_values[min(idx, len(sorted_values) - 1)]
            return result

    def get_all_metrics(self) -> dict[str, Any]:
        """Get all metrics in Prometheus format."""
        lines = []
        for name, defn in self._definitions.items():
            help_line = f"# HELP {name} {defn.description}"
            type_line = f"# TYPE {name} {defn.metric_type.value}"
            lines.append(help_line)
            lines.append(type_line)
            if defn.metric_type == MetricType.COUNTER and name in self._counters:
                label_str = ""
                if defn.labels:
                    label_str = "{" + ",".join(f'{l}=""' for l in defn.labels) + "}"
                lines.append(f"{name}{label_str} {self._counters[name]}")
            elif defn.metric_type == MetricType.GAUGE and name in self._gauges:
                label_str = ""
                if defn.labels:
                    label_str = "{" + ",".join(f'{l}=""' for l in defn.labels) + "}"
                lines.append(f"{name}{label_str} {self._gauges[name]}")
        return {"metrics": "\n".join(lines)}
