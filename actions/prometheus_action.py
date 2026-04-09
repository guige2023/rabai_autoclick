"""Prometheus metrics action module.

Provides Prometheus client functionality for metrics collection,
including counters, gauges, histograms, and summaries.
"""

from __future__ import annotations

import time
import threading
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class MetricType(Enum):
    """Prometheus metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    UNTYPED = "untypeded"


@dataclass
class MetricLabel:
    """Metric label."""
    name: str
    value: str


@dataclass
class MetricSample:
    """Represents a metric sample."""
    name: str
    value: float
    labels: dict[str, str] = field(default_factory=dict)
    timestamp: Optional[float] = None


@dataclass
class MetricConfig:
    """Metric configuration."""
    name: str
    description: str
    unit: str = ""
    metric_type: MetricType = MetricType.UNTYPED


class Counter:
    """Prometheus counter metric."""

    def __init__(self, config: MetricConfig, labels: Optional[dict[str, str]] = None):
        """Initialize counter.

        Args:
            config: Metric configuration
            labels: Default labels
        """
        self.config = config
        self._value = 0.0
        self._labels = labels or {}
        self._lock = threading.Lock()

    def inc(self, value: float = 1.0, labels: Optional[dict[str, str]] = None) -> None:
        """Increment counter.

        Args:
            value: Value to increment
            labels: Additional labels
        """
        with self._lock:
            self._value += value

    def get(self) -> float:
        """Get current value."""
        return self._value


class Gauge:
    """Prometheus gauge metric."""

    def __init__(self, config: MetricConfig, labels: Optional[dict[str, str]] = None):
        """Initialize gauge.

        Args:
            config: Metric configuration
            labels: Default labels
        """
        self.config = config
        self._value = 0.0
        self._labels = labels or {}
        self._lock = threading.Lock()

    def set(self, value: float, labels: Optional[dict[str, str]] = None) -> None:
        """Set gauge value.

        Args:
            value: Value to set
            labels: Additional labels
        """
        with self._lock:
            self._value = value

    def inc(self, value: float = 1.0, labels: Optional[dict[str, str]] = None) -> None:
        """Increment gauge.

        Args:
            value: Value to increment
            labels: Additional labels
        """
        with self._lock:
            self._value += value

    def dec(self, value: float = 1.0, labels: Optional[dict[str, str]] = None) -> None:
        """Decrement gauge.

        Args:
            value: Value to decrement
            labels: Additional labels
        """
        with self._lock:
            self._value -= value

    def get(self) -> float:
        """Get current value."""
        return self._value


class Histogram:
    """Prometheus histogram metric."""

    def __init__(
        self,
        config: MetricConfig,
        buckets: tuple[float, ...] = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
        labels: Optional[dict[str, str]] = None,
    ):
        """Initialize histogram.

        Args:
            config: Metric configuration
            buckets: Histogram buckets
            labels: Default labels
        """
        self.config = config
        self.buckets = buckets
        self._labels = labels or {}
        self._buckets: dict[float, int] = {b: 0 for b in buckets}
        self._sum = 0.0
        self._count = 0
        self._lock = threading.Lock()

    def observe(self, value: float, labels: Optional[dict[str, str]] = None) -> None:
        """Observe a value.

        Args:
            value: Observed value
            labels: Additional labels
        """
        with self._lock:
            self._count += 1
            self._sum += value
            for bucket in self.buckets:
                if value <= bucket:
                    self._buckets[bucket] += 1


class Summary:
    """Prometheus summary metric."""

    def __init__(
        self,
        config: MetricConfig,
        objectives: Optional[dict[float, float]] = None,
        labels: Optional[dict[str, str]] = None,
    ):
        """Initialize summary.

        Args:
            config: Metric configuration
            objectives: Quantile objectives
            labels: Default labels
        """
        self.config = config
        self.objectives = objectives or {}
        self._labels = labels or {}
        self._sum = 0.0
        self._count = 0
        self._lock = threading.Lock()

    def observe(self, value: float, labels: Optional[dict[str, str]] = None) -> None:
        """Observe a value.

        Args:
            value: Observed value
            labels: Additional labels
        """
        with self._lock:
            self._count += 1
            self._sum += value


class PrometheusRegistry:
    """Registry for Prometheus metrics."""

    def __init__(self):
        """Initialize registry."""
        self._metrics: dict[str, Any] = {}
        self._lock = threading.Lock()

    def register(self, metric: Any) -> None:
        """Register a metric.

        Args:
            metric: Metric to register
        """
        with self._lock:
            self._metrics[metric.config.name] = metric

    def get(self, name: str) -> Optional[Any]:
        """Get metric by name."""
        return self._metrics.get(name)

    def collect(self) -> list[MetricSample]:
        """Collect all metrics.

        Returns:
            List of metric samples
        """
        samples: list[MetricSample] = []
        with self._lock:
            for metric in self._metrics.values():
                if isinstance(metric, Counter):
                    samples.append(MetricSample(
                        name=metric.config.name,
                        value=metric.get(),
                        labels=metric._labels,
                    ))
                elif isinstance(metric, Gauge):
                    samples.append(MetricSample(
                        name=metric.config.name,
                        value=metric.get(),
                        labels=metric._labels,
                    ))
        return samples


class PrometheusClient:
    """Prometheus metrics client."""

    def __init__(self, registry: Optional[PrometheusRegistry] = None):
        """Initialize Prometheus client.

        Args:
            registry: Metrics registry
        """
        self.registry = registry or PrometheusRegistry()

    def counter(
        self,
        name: str,
        description: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Counter:
        """Create or get counter metric.

        Args:
            name: Metric name
            description: Metric description
            labels: Default labels

        Returns:
            Counter metric
        """
        counter = Counter(
            config=MetricConfig(
                name=name,
                description=description,
                metric_type=MetricType.COUNTER,
            ),
            labels=labels,
        )
        self.registry.register(counter)
        return counter

    def gauge(
        self,
        name: str,
        description: str,
        labels: Optional[dict[str, str]] = None,
    ) -> Gauge:
        """Create or get gauge metric.

        Args:
            name: Metric name
            description: Metric description
            labels: Default labels

        Returns:
            Gauge metric
        """
        gauge = Gauge(
            config=MetricConfig(
                name=name,
                description=description,
                metric_type=MetricType.GAUGE,
            ),
            labels=labels,
        )
        self.registry.register(gauge)
        return gauge

    def histogram(
        self,
        name: str,
        description: str,
        buckets: tuple[float, ...] = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
        labels: Optional[dict[str, str]] = None,
    ) -> Histogram:
        """Create or get histogram metric.

        Args:
            name: Metric name
            description: Metric description
            buckets: Histogram buckets
            labels: Default labels

        Returns:
            Histogram metric
        """
        histogram = Histogram(
            config=MetricConfig(
                name=name,
                description=description,
                metric_type=MetricType.HISTOGRAM,
            ),
            buckets=buckets,
            labels=labels,
        )
        self.registry.register(histogram)
        return histogram

    def summary(
        self,
        name: str,
        description: str,
        objectives: Optional[dict[float, float]] = None,
        labels: Optional[dict[str, str]] = None,
    ) -> Summary:
        """Create or get summary metric.

        Args:
            name: Metric name
            description: Metric description
            objectives: Quantile objectives
            labels: Default labels

        Returns:
            Summary metric
        """
        summary = Summary(
            config=MetricConfig(
                name=name,
                description=description,
                metric_type=MetricType.SUMMARY,
            ),
            objectives=objectives,
            labels=labels,
        )
        self.registry.register(summary)
        return summary

    def collect(self) -> list[MetricSample]:
        """Collect all metrics."""
        return self.registry.collect()


def create_prometheus_client() -> PrometheusClient:
    """Create Prometheus client instance.

    Returns:
        PrometheusClient instance
    """
    return PrometheusClient()
