"""Prometheus Metrics Action Module.

Provides Prometheus metrics collection, aggregation, and export
capabilities including counters, gauges, histograms, and summaries.
"""
from __future__ import annotations

import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)


class MetricType(Enum):
    """Prometheus metric type."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"
    UNtyped = "untyped"


@dataclass
class LabelSet:
    """Set of labels for a metric."""
    labels: Dict[str, str] = field(default_factory=dict)

    def __hash__(self):
        return hash(tuple(sorted(self.labels.items())))

    def __eq__(self, other):
        if not isinstance(other, LabelSet):
            return False
        return self.labels == other.labels


@dataclass
class MetricSample:
    """Single metric sample."""
    name: str
    value: float
    timestamp: float
    labels: Dict[str, str]
    metric_type: MetricType


@dataclass
class Counter:
    """Prometheus counter metric."""
    name: str
    description: str
    value: float = 0.0
    labels: Dict[LabelSet, float] = field(default_factory=lambda: defaultdict(float))
    total: float = 0.0

    def inc(self, value: float = 1.0, **label_values: str) -> None:
        """Increment counter."""
        label_set = LabelSet(label_values)
        self.labels[label_set] += value
        self.total += value

    def get(self, **label_values: str) -> float:
        """Get counter value for labels."""
        label_set = LabelSet(label_values)
        return self.labels.get(label_set, 0.0)

    def collect(self, timestamp: float) -> List[MetricSample]:
        """Collect metric samples."""
        samples = []
        for label_set, value in self.labels.items():
            samples.append(MetricSample(
                name=self.name,
                value=value,
                timestamp=timestamp,
                labels=label_set.labels,
                metric_type=MetricType.COUNTER
            ))
        return samples


@dataclass
class Gauge:
    """Prometheus gauge metric."""
    name: str
    description: str
    value: float = 0.0
    labels: Dict[LabelSet, float] = field(default_factory=lambda: defaultdict(float))

    def inc(self, value: float = 1.0, **label_values: str) -> None:
        """Increment gauge."""
        label_set = LabelSet(label_values)
        self.labels[label_set] += value

    def dec(self, value: float = 1.0, **label_values: str) -> None:
        """Decrement gauge."""
        label_set = LabelSet(label_values)
        self.labels[label_set] -= value

    def set(self, value: float, **label_values: str) -> None:
        """Set gauge value."""
        label_set = LabelSet(label_values)
        self.labels[label_set] = value

    def get(self, **label_values: str) -> float:
        """Get gauge value for labels."""
        label_set = LabelSet(label_values)
        return self.labels.get(label_set, 0.0)

    def collect(self, timestamp: float) -> List[MetricSample]:
        """Collect metric samples."""
        samples = []
        for label_set, value in self.labels.items():
            samples.append(MetricSample(
                name=self.name,
                value=value,
                timestamp=timestamp,
                labels=label_set.labels,
                metric_type=MetricType.GAUGE
            ))
        return samples


@dataclass
class Histogram:
    """Prometheus histogram metric."""
    name: str
    description: str
    buckets: List[float]
    labels: Dict[LabelSet, Dict[str, float]] = field(
        default_factory=lambda: defaultdict(lambda: {
            "sum": 0.0, "count": 0.0, "inf": 0.0
        })
    )
    bucket_counts: Dict[LabelSet, Dict[float, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )

    def observe(self, value: float, **label_values: str) -> None:
        """Observe a value for histogram."""
        label_set = LabelSet(label_values)
        self.labels[label_set]["sum"] += value
        self.labels[label_set]["count"] += 1
        for bucket in self.buckets:
            if value <= bucket:
                self.bucket_counts[label_set][bucket] += 1
        self.bucket_counts[label_set][float("inf")] += 1

    def collect(self, timestamp: float) -> List[MetricSample]:
        """Collect histogram samples."""
        samples = []
        for label_set, data in self.labels.items():
            for bucket in self.buckets:
                samples.append(MetricSample(
                    name=f"{self.name}_bucket",
                    value=self.bucket_counts[label_set][bucket],
                    timestamp=timestamp,
                    labels={**label_set.labels, "le": str(bucket)},
                    metric_type=MetricType.HISTOGRAM
                ))
            samples.append(MetricSample(
                name=f"{self.name}_bucket",
                value=self.bucket_counts[label_set][float("inf")],
                timestamp=timestamp,
                labels={**label_set.labels, "le": "+Inf"},
                metric_type=MetricType.HISTOGRAM
            ))
            samples.append(MetricSample(
                name=f"{self.name}_sum",
                value=data["sum"],
                timestamp=timestamp,
                labels=label_set.labels,
                metric_type=MetricType.HISTOGRAM
            ))
            samples.append(MetricSample(
                name=f"{self.name}_count",
                value=data["count"],
                timestamp=timestamp,
                labels=label_set.labels,
                metric_type=MetricType.HISTOGRAM
            ))
        return samples


@dataclass
class Summary:
    """Prometheus summary metric."""
    name: str
    description: str
    quantiles: List[float]
    labels: Dict[LabelSet, List[float]] = field(
        default_factory=lambda: defaultdict(list)
    )
    totals: Dict[LabelSet, Dict[str, float]] = field(
        default_factory=lambda: defaultdict(lambda: {"sum": 0.0, "count": 0.0})
    )

    def observe(self, value: float, **label_values: str) -> None:
        """Observe a value for summary."""
        label_set = LabelSet(label_values)
        self.labels[label_set].append(value)
        self.totals[label_set]["sum"] += value
        self.totals[label_set]["count"] += 1

    def collect(self, timestamp: float) -> List[MetricSample]:
        """Collect summary samples."""
        samples = []
        for label_set, values in self.labels.items():
            sorted_values = sorted(values)
            for quantile in self.quantiles:
                idx = int(len(sorted_values) * quantile)
                idx = min(idx, len(sorted_values) - 1)
                samples.append(MetricSample(
                    name=f"{self.name}",
                    value=sorted_values[idx],
                    timestamp=timestamp,
                    labels={**label_set.labels, "quantile": str(quantile)},
                    metric_type=MetricType.SUMMARY
                ))
            samples.append(MetricSample(
                name=f"{self.name}_sum",
                value=self.totals[label_set]["sum"],
                timestamp=timestamp,
                labels=label_set.labels,
                metric_type=MetricType.SUMMARY
            ))
            samples.append(MetricSample(
                name=f"{self.name}_count",
                value=self.totals[label_set]["count"],
                timestamp=timestamp,
                labels=label_set.labels,
                metric_type=MetricType.SUMMARY
            ))
        return samples


class PrometheusRegistry:
    """Central metrics registry."""

    def __init__(self):
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._summaries: Dict[str, Summary] = {}

    def counter(self, name: str, description: str = "") -> Counter:
        """Get or create counter."""
        if name not in self._counters:
            self._counters[name] = Counter(name=name, description=description)
        return self._counters[name]

    def gauge(self, name: str, description: str = "") -> Gauge:
        """Get or create gauge."""
        if name not in self._gauges:
            self._gauges[name] = Gauge(name=name, description=description)
        return self._gauges[name]

    def histogram(self, name: str, description: str = "",
                  buckets: Optional[List[float]] = None) -> Histogram:
        """Get or create histogram."""
        if name not in self._histograms:
            if buckets is None:
                buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
            self._histograms[name] = Histogram(
                name=name, description=description, buckets=buckets
            )
        return self._histograms[name]

    def summary(self, name: str, description: str = "",
                quantiles: Optional[List[float]] = None) -> Summary:
        """Get or create summary."""
        if name not in self._summaries:
            if quantiles is None:
                quantiles = [0.5, 0.9, 0.99]
            self._summaries[name] = Summary(
                name=name, description=description, quantiles=quantiles
            )
        return self._summaries[name]

    def collect(self) -> List[MetricSample]:
        """Collect all metrics."""
        timestamp = time.time()
        samples = []
        for counter in self._counters.values():
            samples.extend(counter.collect(timestamp))
        for gauge in self._gauges.values():
            samples.extend(gauge.collect(timestamp))
        for histogram in self._histograms.values():
            samples.extend(histogram.collect(timestamp))
        for summary in self._summaries.values():
            samples.extend(summary.collect(timestamp))
        return samples

    def export_text(self) -> str:
        """Export metrics in Prometheus text format."""
        lines = []
        for sample in self.collect():
            label_str = ""
            if sample.labels:
                label_parts = [f'{k}="{v}"' for k, v in sample.labels.items()]
                label_str = "{" + ",".join(label_parts) + "}"
            lines.append(f"{sample.name}{label_str} {sample.value}")
        return "\n".join(lines) + "\n"


_global_registry = PrometheusRegistry()


class PrometheusAction:
    """Prometheus metrics collection action.

    Example:
        action = PrometheusAction()

        action.register_counter("http_requests_total", "Total HTTP requests")
        action.inc_counter("http_requests_total", labels={"method": "GET", "status": "200"})

        action.register_gauge("active_connections", "Active connections")
        action.set_gauge("active_connections", 42)

        action.register_histogram("request_duration_seconds", buckets=[0.1, 0.5, 1.0, 5.0])
        action.observe_histogram("request_duration_seconds", 0.35)

        output = action.export()
    """

    def __init__(self, registry: Optional[PrometheusRegistry] = None):
        self._registry = registry or _global_registry

    def register_counter(self, name: str, description: str = "",
                         labels: Optional[List[str]] = None) -> Dict[str, Any]:
        """Register a counter metric."""
        try:
            counter = self._registry.counter(name, description)
            return {"success": True, "name": name, "type": "counter"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def register_gauge(self, name: str, description: str = "") -> Dict[str, Any]:
        """Register a gauge metric."""
        try:
            gauge = self._registry.gauge(name, description)
            return {"success": True, "name": name, "type": "gauge"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def register_histogram(self, name: str, description: str = "",
                          buckets: Optional[List[float]] = None) -> Dict[str, Any]:
        """Register a histogram metric."""
        try:
            histogram = self._registry.histogram(name, description, buckets)
            return {"success": True, "name": name, "type": "histogram"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def register_summary(self, name: str, description: str = "",
                        quantiles: Optional[List[float]] = None) -> Dict[str, Any]:
        """Register a summary metric."""
        try:
            summary = self._registry.summary(name, description, quantiles)
            return {"success": True, "name": name, "type": "summary"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def inc_counter(self, name: str, value: float = 1.0,
                   **labels: str) -> Dict[str, Any]:
        """Increment a counter."""
        try:
            counter = self._registry.counter(name)
            counter.inc(value, **labels)
            return {"success": True, "value": counter.get(**labels)}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def set_gauge(self, name: str, value: float, **labels: str) -> Dict[str, Any]:
        """Set a gauge value."""
        try:
            gauge = self._registry.gauge(name)
            gauge.set(value, **labels)
            return {"success": True, "value": value}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def inc_gauge(self, name: str, value: float = 1.0,
                  **labels: str) -> Dict[str, Any]:
        """Increment a gauge."""
        try:
            gauge = self._registry.gauge(name)
            gauge.inc(value, **labels)
            return {"success": True, "value": gauge.get(**labels)}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def dec_gauge(self, name: str, value: float = 1.0,
                  **labels: str) -> Dict[str, Any]:
        """Decrement a gauge."""
        try:
            gauge = self._registry.gauge(name)
            gauge.dec(value, **labels)
            return {"success": True, "value": gauge.get(**labels)}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def observe_histogram(self, name: str, value: float,
                         **labels: str) -> Dict[str, Any]:
        """Observe a value for histogram."""
        try:
            histogram = self._registry.histogram(name)
            histogram.observe(value, **labels)
            return {"success": True, "value": value}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def observe_summary(self, name: str, value: float,
                        **labels: str) -> Dict[str, Any]:
        """Observe a value for summary."""
        try:
            summary = self._registry.summary(name)
            summary.observe(value, **labels)
            return {"success": True, "value": value}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def get(self, name: str, metric_type: str, **labels: str) -> Dict[str, Any]:
        """Get current value of a metric."""
        try:
            if metric_type == "counter":
                return {"success": True, "value": self._registry.counter(name).get(**labels)}
            elif metric_type == "gauge":
                return {"success": True, "value": self._registry.gauge(name).get(**labels)}
            else:
                return {"success": False, "message": f"Unknown type: {metric_type}"}
        except Exception as e:
            return {"success": False, "message": str(e)}

    def export(self, format: str = "text") -> Dict[str, Any]:
        """Export all metrics.

        Args:
            format: Output format (text, json)

        Returns:
            Dict with success, output, sample_count
        """
        try:
            if format == "text":
                output = self._registry.export_text()
            elif format == "json":
                samples = self._registry.collect()
                output = [
                    {
                        "name": s.name,
                        "value": s.value,
                        "timestamp": s.timestamp,
                        "labels": s.labels,
                        "type": s.metric_type.value
                    }
                    for s in samples
                ]
            else:
                return {"success": False, "message": f"Unknown format: {format}"}

            return {
                "success": True,
                "output": output,
                "sample_count": len(output) if isinstance(output, list) else len(output.split("\n")),
                "message": "Metrics exported"
            }
        except Exception as e:
            return {"success": False, "message": str(e)}


def execute(context: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Execute Prometheus metrics action.

    Args:
        context: Execution context
        params: Dict with keys:
            - operation: "register", "inc_counter", "set_gauge", "inc_gauge",
                         "dec_gauge", "observe_histogram", "observe_summary",
                         "get", "export"
            - name: Metric name
            - description: Metric description
            - value: Value to set/inc/dec/observe
            - labels: Dict of label key-values
            - buckets: List of histogram buckets
            - quantiles: List of summary quantiles
            - format: Export format (text, json)

    Returns:
        Dict with success, data, message
    """
    operation = params.get("operation", "export")
    action = PrometheusAction()

    try:
        if operation == "register":
            name = params.get("name", "")
            description = params.get("description", "")
            metric_type = params.get("type", "counter")
            buckets = params.get("buckets")
            quantiles = params.get("quantiles")

            if not name:
                return {"success": False, "message": "name required"}

            if metric_type == "counter":
                return action.register_counter(name, description)
            elif metric_type == "gauge":
                return action.register_gauge(name, description)
            elif metric_type == "histogram":
                return action.register_histogram(name, description, buckets)
            elif metric_type == "summary":
                return action.register_summary(name, description, quantiles)
            else:
                return {"success": False, "message": f"Unknown type: {metric_type}"}

        elif operation == "inc_counter":
            name = params.get("name", "")
            value = params.get("value", 1.0)
            labels = params.get("labels", {})
            if not name:
                return {"success": False, "message": "name required"}
            return action.inc_counter(name, value, **labels)

        elif operation == "set_gauge":
            name = params.get("name", "")
            value = params.get("value", 0.0)
            labels = params.get("labels", {})
            if not name:
                return {"success": False, "message": "name required"}
            return action.set_gauge(name, value, **labels)

        elif operation == "inc_gauge":
            name = params.get("name", "")
            value = params.get("value", 1.0)
            labels = params.get("labels", {})
            if not name:
                return {"success": False, "message": "name required"}
            return action.inc_gauge(name, value, **labels)

        elif operation == "dec_gauge":
            name = params.get("name", "")
            value = params.get("value", 1.0)
            labels = params.get("labels", {})
            if not name:
                return {"success": False, "message": "name required"}
            return action.dec_gauge(name, value, **labels)

        elif operation == "observe_histogram":
            name = params.get("name", "")
            value = params.get("value", 0.0)
            labels = params.get("labels", {})
            if not name:
                return {"success": False, "message": "name required"}
            return action.observe_histogram(name, value, **labels)

        elif operation == "observe_summary":
            name = params.get("name", "")
            value = params.get("value", 0.0)
            labels = params.get("labels", {})
            if not name:
                return {"success": False, "message": "name required"}
            return action.observe_summary(name, value, **labels)

        elif operation == "get":
            name = params.get("name", "")
            metric_type = params.get("type", "counter")
            labels = params.get("labels", {})
            if not name:
                return {"success": False, "message": "name required"}
            return action.get(name, metric_type, **labels)

        elif operation == "export":
            format = params.get("format", "text")
            return action.export(format)

        else:
            return {"success": False, "message": f"Unknown operation: {operation}"}

    except Exception as e:
        return {"success": False, "message": f"Prometheus error: {str(e)}"}
