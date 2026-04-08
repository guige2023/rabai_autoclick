"""Metrics action module for RabAI AutoClick.

Provides metrics collection and reporting operations including
counters, gauges, histograms, and export to various formats.
"""

import os
import sys
import time
import json
import threading
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class MetricValue:
    """Represents a single metric value with timestamp.
    
    Attributes:
        value: The metric value.
        timestamp: Unix timestamp of the value.
        labels: Optional labels/tags for the metric.
    """
    value: float
    timestamp: float
    labels: Dict[str, str] = field(default_factory=dict)


class Counter:
    """Metric counter that only increments.
    
    A counter is a cumulative metric that represents a single
    monotonically increasing counter whose value can only increase.
    """
    
    def __init__(self, name: str, description: str = "", labels: Optional[Dict[str, str]] = None) -> None:
        """Initialize counter.
        
        Args:
            name: Metric name.
            description: Metric description.
            labels: Default labels.
        """
        self.name = name
        self.description = description
        self._labels = labels or {}
        self._value: float = 0.0
        self._lock = threading.Lock()
    
    def inc(self, amount: float = 1.0, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment the counter.
        
        Args:
            amount: Amount to increment by.
            labels: Additional labels to include.
        """
        with self._lock:
            self._value += amount
    
    @property
    def value(self) -> float:
        """Get current counter value."""
        return self._value
    
    def get(self) -> float:
        """Get current value."""
        return self._value


class Gauge:
    """Metric gauge that can go up or down.
    
    A gauge is a metric that represents a single numerical value
    that can arbitrarily go up and down.
    """
    
    def __init__(self, name: str, description: str = "", labels: Optional[Dict[str, str]] = None) -> None:
        """Initialize gauge.
        
        Args:
            name: Metric name.
            description: Metric description.
            labels: Default labels.
        """
        self.name = name
        self.description = description
        self._labels = labels or {}
        self._value: float = 0.0
        self._lock = threading.Lock()
    
    def inc(self, amount: float = 1.0) -> None:
        """Increment the gauge.
        
        Args:
            amount: Amount to increment by.
        """
        with self._lock:
            self._value += amount
    
    def dec(self, amount: float = 1.0) -> None:
        """Decrement the gauge.
        
        Args:
            amount: Amount to decrement by.
        """
        with self._lock:
            self._value -= amount
    
    def set(self, value: float) -> None:
        """Set the gauge to a specific value.
        
        Args:
            value: Value to set.
        """
        with self._lock:
            self._value = value
    
    @property
    def value(self) -> float:
        """Get current gauge value."""
        return self._value
    
    def get(self) -> float:
        """Get current value."""
        return self._value


class Histogram:
    """Metric histogram for observing distributions.
    
    A histogram samples observations and counts them in configurable
    buckets. It is used for things like request duration or response sizes.
    """
    
    DEFAULT_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0)
    
    def __init__(
        self,
        name: str,
        description: str = "",
        buckets: Optional[List[float]] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> None:
        """Initialize histogram.
        
        Args:
            name: Metric name.
            description: Metric description.
            buckets: Custom bucket boundaries.
            labels: Default labels.
        """
        self.name = name
        self.description = description
        self.buckets = buckets or list(self.DEFAULT_BUCKETS)
        self._labels = labels or {}
        self._sum: float = 0.0
        self._count: int = 0
        self._bucket_counts: Dict[float, int] = {b: 0 for b in self.buckets}
        self._lock = threading.Lock()
    
    def observe(self, value: float) -> None:
        """Observe a value.
        
        Args:
            value: Value to observe.
        """
        with self._lock:
            self._sum += value
            self._count += 1
            
            for bucket in self.buckets:
                if value <= bucket:
                    self._bucket_counts[bucket] += 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get histogram statistics.
        
        Returns:
            Dictionary with sum, count, mean, and bucket information.
        """
        with self._lock:
            return {
                "sum": self._sum,
                "count": self._count,
                "mean": self._sum / self._count if self._count > 0 else 0.0,
                "buckets": dict(self._bucket_counts)
            }


class MetricsCollector:
    """Central metrics collector managing all metrics.
    
    Provides a centralized registry for metrics and methods for
    exporting metrics in various formats (Prometheus, JSON, etc.).
    """
    
    def __init__(self) -> None:
        """Initialize metrics collector."""
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._lock = threading.Lock()
        self._start_time = time.time()
    
    def register_counter(
        self,
        name: str,
        description: str = "",
        labels: Optional[Dict[str, str]] = None
    ) -> Counter:
        """Register a new counter.
        
        Args:
            name: Metric name.
            description: Metric description.
            labels: Default labels.
            
        Returns:
            Registered counter.
        """
        with self._lock:
            if name in self._counters:
                return self._counters[name]
            
            counter = Counter(name, description, labels)
            self._counters[name] = counter
            return counter
    
    def register_gauge(
        self,
        name: str,
        description: str = "",
        labels: Optional[Dict[str, str]] = None
    ) -> Gauge:
        """Register a new gauge.
        
        Args:
            name: Metric name.
            description: Metric description.
            labels: Default labels.
            
        Returns:
            Registered gauge.
        """
        with self._lock:
            if name in self._gauges:
                return self._gauges[name]
            
            gauge = Gauge(name, description, labels)
            self._gauges[name] = gauge
            return gauge
    
    def register_histogram(
        self,
        name: str,
        description: str = "",
        buckets: Optional[List[float]] = None,
        labels: Optional[Dict[str, str]] = None
    ) -> Histogram:
        """Register a new histogram.
        
        Args:
            name: Metric name.
            description: Metric description.
            buckets: Custom bucket boundaries.
            labels: Default labels.
            
        Returns:
            Registered histogram.
        """
        with self._lock:
            if name in self._histograms:
                return self._histograms[name]
            
            histogram = Histogram(name, description, buckets, labels)
            self._histograms[name] = histogram
            return histogram
    
    def get_counter(self, name: str) -> Optional[Counter]:
        """Get a counter by name."""
        return self._counters.get(name)
    
    def get_gauge(self, name: str) -> Optional[Gauge]:
        """Get a gauge by name."""
        return self._gauges.get(name)
    
    def get_histogram(self, name: str) -> Optional[Histogram]:
        """Get a histogram by name."""
        return self._histograms.get(name)
    
    def list_metrics(self) -> Dict[str, List[str]]:
        """List all registered metrics.
        
        Returns:
            Dictionary mapping metric type to list of metric names.
        """
        with self._lock:
            return {
                "counters": list(self._counters.keys()),
                "gauges": list(self._gauges.keys()),
                "histograms": list(self._histograms.keys())
            }
    
    def export_prometheus(self) -> str:
        """Export all metrics in Prometheus text format.
        
        Returns:
            Prometheus-formatted metrics string.
        """
        lines: List[str] = []
        lines.append(f"# Metrics exported at {time.time()}")
        lines.append("")
        
        with self._lock:
            for counter in self._counters.values():
                if counter.description:
                    lines.append(f"# HELP {counter.name} {counter.description}")
                lines.append(f"# TYPE {counter.name} counter")
                lines.append(f"{counter.name} {counter.value}")
            
            for gauge in self._gauges.values():
                if gauge.description:
                    lines.append(f"# HELP {gauge.name} {gauge.description}")
                lines.append(f"# TYPE {gauge.name} gauge")
                lines.append(f"{gauge.name} {gauge.value}")
            
            for histogram in self._histograms.values():
                if histogram.description:
                    lines.append(f"# HELP {histogram.name} {histogram.description}")
                lines.append(f"# TYPE {histogram.name} histogram")
                
                stats = histogram.get_stats()
                for bucket, count in stats["buckets"].items():
                    lines.append(f'{histogram.name}_bucket{{le="{bucket}"}} {count}')
                lines.append(f'{histogram.name}_bucket{{le="+Inf"}} {stats["count"]}')
                lines.append(f"{histogram.name}_sum {stats['sum']}")
                lines.append(f"{histogram.name}_count {stats['count']}")
        
        return "\n".join(lines)
    
    def export_json(self) -> Dict[str, Any]:
        """Export all metrics in JSON format.
        
        Returns:
            JSON-serializable dictionary of all metrics.
        """
        with self._lock:
            result: Dict[str, Any] = {
                "timestamp": time.time(),
                "uptime": time.time() - self._start_time,
                "counters": {},
                "gauges": {},
                "histograms": {}
            }
            
            for name, counter in self._counters.items():
                result["counters"][name] = {
                    "value": counter.value,
                    "description": counter.description
                }
            
            for name, gauge in self._gauges.items():
                result["gauges"][name] = {
                    "value": gauge.value,
                    "description": gauge.description
                }
            
            for name, histogram in self._histograms.items():
                result["histograms"][name] = {
                    **histogram.get_stats(),
                    "description": histogram.description,
                    "buckets": histogram.buckets
                }
            
            return result
    
    def reset(self) -> None:
        """Reset all metrics to initial state."""
        with self._lock:
            for counter in self._counters.values():
                counter._value = 0.0
            for gauge in self._gauges.values():
                gauge._value = 0.0
            for histogram in self._histograms.values():
                histogram._sum = 0.0
                histogram._count = 0
                histogram._bucket_counts = {b: 0 for b in histogram.buckets}
        
        self._start_time = time.time()


class MetricsAction(BaseAction):
    """Metrics action for collecting and reporting metrics.
    
    Supports counters, gauges, histograms, and export to Prometheus/JSON.
    """
    action_type: str = "metrics"
    display_name: str = "指标动作"
    description: str = "指标收集和报告，支持计数器、仪表和直方图"
    
    def __init__(self) -> None:
        super().__init__()
        self._collector = MetricsCollector()
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute metrics operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "list")
            
            if operation == "register":
                return self._register_metric(params, start_time)
            elif operation == "inc":
                return self._inc_metric(params, start_time)
            elif operation == "dec":
                return self._dec_metric(params, start_time)
            elif operation == "set":
                return self._set_metric(params, start_time)
            elif operation == "observe":
                return self._observe_histogram(params, start_time)
            elif operation == "get":
                return self._get_metric(params, start_time)
            elif operation == "list":
                return self._list_metrics(start_time)
            elif operation == "export":
                return self._export_metrics(params, start_time)
            elif operation == "reset":
                return self._reset_metrics(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Metrics operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _register_metric(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Register a new metric."""
        name = params.get("name", "")
        metric_type = params.get("type", "gauge")
        description = params.get("description", "")
        buckets = params.get("buckets")
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        try:
            if metric_type == "counter":
                self._collector.register_counter(name, description)
            elif metric_type == "histogram":
                self._collector.register_histogram(name, description, buckets)
            else:
                self._collector.register_gauge(name, description)
            
            return ActionResult(
                success=True,
                message=f"Registered {metric_type} metric: {name}",
                data={"name": name, "type": metric_type},
                duration=time.time() - start_time
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Failed to register metric: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _inc_metric(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Increment a counter or gauge."""
        name = params.get("name", "")
        amount = params.get("amount", 1.0)
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        counter = self._collector.get_counter(name)
        gauge = self._collector.get_gauge(name)
        
        if counter:
            counter.inc(amount)
            return ActionResult(
                success=True,
                message=f"Incremented counter {name} by {amount}",
                data={"name": name, "value": counter.value},
                duration=time.time() - start_time
            )
        elif gauge:
            gauge.inc(amount)
            return ActionResult(
                success=True,
                message=f"Incremented gauge {name} by {amount}",
                data={"name": name, "value": gauge.value},
                duration=time.time() - start_time
            )
        else:
            return ActionResult(
                success=False,
                message=f"Metric not found: {name}",
                duration=time.time() - start_time
            )
    
    def _dec_metric(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Decrement a gauge."""
        name = params.get("name", "")
        amount = params.get("amount", 1.0)
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        gauge = self._collector.get_gauge(name)
        
        if gauge:
            gauge.dec(amount)
            return ActionResult(
                success=True,
                message=f"Decremented gauge {name} by {amount}",
                data={"name": name, "value": gauge.value},
                duration=time.time() - start_time
            )
        else:
            return ActionResult(
                success=False,
                message=f"Gauge not found: {name}",
                duration=time.time() - start_time
            )
    
    def _set_metric(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Set a gauge to a specific value."""
        name = params.get("name", "")
        value = params.get("value", 0.0)
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        gauge = self._collector.get_gauge(name)
        
        if gauge:
            gauge.set(value)
            return ActionResult(
                success=True,
                message=f"Set gauge {name} to {value}",
                data={"name": name, "value": value},
                duration=time.time() - start_time
            )
        else:
            return ActionResult(
                success=False,
                message=f"Gauge not found: {name}",
                duration=time.time() - start_time
            )
    
    def _observe_histogram(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Observe a value in a histogram."""
        name = params.get("name", "")
        value = params.get("value", 0.0)
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        histogram = self._collector.get_histogram(name)
        
        if histogram:
            histogram.observe(value)
            stats = histogram.get_stats()
            return ActionResult(
                success=True,
                message=f"Observed {value} in histogram {name}",
                data={"name": name, "stats": stats},
                duration=time.time() - start_time
            )
        else:
            return ActionResult(
                success=False,
                message=f"Histogram not found: {name}",
                duration=time.time() - start_time
            )
    
    def _get_metric(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Get current value of a metric."""
        name = params.get("name", "")
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        counter = self._collector.get_counter(name)
        gauge = self._collector.get_gauge(name)
        histogram = self._collector.get_histogram(name)
        
        if counter:
            return ActionResult(
                success=True,
                message=f"Retrieved counter {name}",
                data={"name": name, "type": "counter", "value": counter.value},
                duration=time.time() - start_time
            )
        elif gauge:
            return ActionResult(
                success=True,
                message=f"Retrieved gauge {name}",
                data={"name": name, "type": "gauge", "value": gauge.value},
                duration=time.time() - start_time
            )
        elif histogram:
            return ActionResult(
                success=True,
                message=f"Retrieved histogram {name}",
                data={"name": name, "type": "histogram", **histogram.get_stats()},
                duration=time.time() - start_time
            )
        else:
            return ActionResult(
                success=False,
                message=f"Metric not found: {name}",
                duration=time.time() - start_time
            )
    
    def _list_metrics(self, start_time: float) -> ActionResult:
        """List all registered metrics."""
        metrics = self._collector.list_metrics()
        
        return ActionResult(
            success=True,
            message=f"Found {sum(len(v) for v in metrics.values())} metrics",
            data=metrics,
            duration=time.time() - start_time
        )
    
    def _export_metrics(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Export metrics in specified format."""
        format_type = params.get("format", "json")
        
        if format_type == "prometheus":
            output = self._collector.export_prometheus()
            return ActionResult(
                success=True,
                message="Exported metrics in Prometheus format",
                data={"format": "prometheus", "content": output},
                duration=time.time() - start_time
            )
        else:
            output = self._collector.export_json()
            return ActionResult(
                success=True,
                message="Exported metrics in JSON format",
                data={"format": "json", "metrics": output},
                duration=time.time() - start_time
            )
    
    def _reset_metrics(self, start_time: float) -> ActionResult:
        """Reset all metrics."""
        self._collector.reset()
        
        return ActionResult(
            success=True,
            message="All metrics reset",
            duration=time.time() - start_time
        )
