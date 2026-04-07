"""
Prometheus metrics utilities for monitoring and alerting.

Provides metric collectors, counters, gauges, histograms, summaries,
label management, and Pushgateway integration.
"""

from __future__ import annotations

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional

import httpx

logger = logging.getLogger(__name__)


class MetricType(Enum):
    COUNTER = auto()
    GAUGE = auto()
    HISTOGRAM = auto()
    SUMMARY = auto()


@dataclass
class MetricSample:
    """A single metric sample."""
    name: str
    value: float
    labels: dict[str, str] = field(default_factory=dict)
    timestamp: Optional[float] = None


@dataclass
class HistogramBucket:
    """Histogram bucket definition."""
    le: float  # less-than-or-equal threshold
    count: int = 0


class MetricCollector:
    """In-memory metric collector compatible with Prometheus format."""

    def __init__(self, namespace: str = "") -> None:
        self.namespace = namespace
        self._counters: dict[str, float] = defaultdict(float)
        self._gauge_values: dict[str, float] = defaultdict(float)
        self._histograms: dict[str, list[tuple[float, int]]] = defaultdict(
            lambda: [(0.005, 0), (0.01, 0), (0.025, 0), (0.05, 0), (0.1, 0),
                     (0.25, 0), (0.5, 0), (1.0, 0), (2.5, 0), (5.0, 0), (10.0, 0)]
        )
        self._summaries: dict[str, list[float]] = defaultdict(list)
        self._labels: dict[str, dict[str, str]] = defaultdict(dict)
        self._descriptions: dict[str, str] = {}

    def counter(self, name: str, description: str = "", labels: Optional[dict[str, str]] = None) -> Callable:
        """Decorator to count events."""
        full_name = f"{self.namespace}_{name}" if self.namespace else name
        self._descriptions[full_name] = description
        if labels:
            self._labels[full_name] = labels

        def decorator(func: Callable) -> Callable:
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                result = func(*args, **kwargs)
                self.inc_counter(full_name, 1, labels or {})
                return result
            return wrapper
        return decorator

    def inc_counter(self, name: str, value: float = 1, labels: Optional[dict[str, str]] = None) -> None:
        key = self._make_key(name, labels or {})
        self._counters[key] += value

    def set_gauge(self, name: str, value: float, labels: Optional[dict[str, str]] = None) -> None:
        key = self._make_key(name, labels or {})
        self._gauge_values[key] = value

    def observe_histogram(self, name: str, value: float, labels: Optional[dict[str, str]] = None) -> None:
        key = self._make_key(name, labels or {})
        buckets = self._histograms[key]
        for i, (le, _) in enumerate(buckets):
            if value <= le:
                buckets[i] = (le, buckets[i][1] + 1)

    def observe_summary(self, name: str, value: float, labels: Optional[dict[str, str]] = None) -> None:
        key = self._make_key(name, labels or {})
        self._summaries[key].append(value)

    def _make_key(self, name: str, labels: dict[str, str]) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def render(self) -> str:
        """Render all metrics in Prometheus text format."""
        lines = []

        for key, value in sorted(self._counters.items()):
            metric_name = key.split("{")[0]
            if metric_name in self._descriptions:
                lines.append(f"# HELP {metric_name} {self._descriptions[metric_name]}")
            lines.append(f"# TYPE {metric_name} counter")
            lines.append(f"{key} {value}")

        for key, value in sorted(self._gauge_values.items()):
            metric_name = key.split("{")[0]
            if metric_name in self._descriptions:
                lines.append(f"# HELP {metric_name} {self._descriptions[metric_name]}")
            lines.append(f"# TYPE {metric_name} gauge")
            lines.append(f"{key} {value}")

        for key, buckets in sorted(self._histograms.items()):
            metric_name = key.split("{")[0]
            lines.append(f"# HELP {metric_name} {self._descriptions.get(metric_name, '')}")
            lines.append(f"# TYPE {metric_name} histogram")
            cumulative = 0
            for le, count in buckets:
                cumulative += count
                label_part = key.split("{")[1] if "{" in key else ""
                label_str = f"{{{label_part}}}" if label_part else ""
                lines.append(f'{metric_name}_bucket{{le="{le}"{label_str}}} {cumulative}')
            lines.append(f'{metric_name}_sum{key.split("{")[1] if "{" in key else ""} {sum(c for _, c in buckets)}')
            lines.append(f'{metric_name}_count{key.split("{")[1] if "{" in key else ""} {sum(c for _, c in buckets)}')

        for key, values in sorted(self._summaries.items()):
            metric_name = key.split("{")[0]
            lines.append(f"# HELP {metric_name} {self._descriptions.get(metric_name, '')}")
            lines.append(f"# TYPE {metric_name} summary")
            sorted_vals = sorted(values)
            for quantile in [0.5, 0.9, 0.99]:
                idx = int(len(sorted_vals) * quantile)
                label_part = key.split("{")[1] if "{" in key else ""
                label_str = f"{{{label_part}}}" if label_part else ""
                lines.append(f'{metric_name}{{quantile="{quantile}"{label_str}}} {sorted_vals[idx] if sorted_vals else 0}')
            lines.append(f'{metric_name}_sum{key.split("{")[1] if "{" in key else ""} {sum(values)}')
            lines.append(f'{metric_name}_count{key.split("{")[1] if "{" in key else ""} {len(values)}')

        return "\n".join(lines) + "\n"


class Timer:
    """Context manager for timing operations."""

    def __init__(self, collector: MetricCollector, metric_name: str, labels: Optional[dict[str, str]] = None) -> None:
        self.collector = collector
        self.metric_name = metric_name
        self.labels = labels or {}
        self.start_time: Optional[float] = None

    def __enter__(self) -> "Timer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        if self.start_time is not None:
            elapsed = time.perf_counter() - self.start_time
            self.collector.observe_histogram(self.metric_name, elapsed, self.labels)

    async def __aenter__(self) -> "Timer":
        self.start_time = time.perf_counter()
        return self

    async def __aexit__(self, *args: Any) -> None:
        if self.start_time is not None:
            elapsed = time.perf_counter() - self.start_time
            self.collector.observe_histogram(self.metric_name, elapsed, self.labels)


class PrometheusPusher:
    """Pushes metrics to Prometheus Pushgateway."""

    def __init__(self, pushgateway_url: str = "http://localhost:9091", job: str = "batch_job") -> None:
        self.pushgateway_url = pushgateway_url.rstrip("/")
        self.job = job
        self._client: Optional[httpx.AsyncClient] = None

    async def push(self, collector: MetricCollector, grouping_key: Optional[dict[str, str]] = None) -> bool:
        """Push metrics to Pushgateway."""
        metrics = collector.render()
        url = f"{self.pushgateway_url}/metrics/job/{self.job}"
        if grouping_key:
            for k, v in grouping_key.items():
                url += f"/{k}/{v}"

        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(url, content=metrics, headers={"Content-Type": "text/plain"})
                return response.status_code == 200
        except Exception as e:
            logger.error("Failed to push metrics: %s", e)
            return False


def exponential_buckets(start: float, factor: float, count: int) -> list[float]:
    """Generate exponential histogram buckets."""
    return [start * (factor ** i) for i in range(count)]


def linear_buckets(start: float, width: float, count: int) -> list[float]:
    """Generate linear histogram buckets."""
    return [start + width * i for i in range(count)]
