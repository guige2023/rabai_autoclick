"""Metrics collector action module for RabAI AutoClick.

Provides metrics collection with counters, gauges, histograms,
and time series aggregation for monitoring.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from threading import Lock
from collections import defaultdict
from statistics import mean, median

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MetricType(Enum) if False else object:
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class MetricPoint:
    """A single metric data point."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


class MetricsCollectorAction(BaseAction):
    """Collect and aggregate metrics.
    
    Supports counters, gauges, histograms, and timers
    with labels and time series storage.
    """
    action_type = "metrics_collector"
    display_name = "指标收集"
    description = "监控指标收集和聚合"
    
    def __init__(self):
        super().__init__()
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timers: Dict[str, List[float]] = defaultdict(list)
        self._series: Dict[str, List[MetricPoint]] = defaultdict(list)
        self._labels: Dict[str, Dict[str, str]] = {}
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute metrics operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'inc', 'dec', 'set', 'record', 'timing', 'get', 'snapshot'
                - metric: Metric name
                - value: Metric value
                - labels: Metric labels
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'inc').lower()
        
        if operation == 'inc':
            return self._increment(params)
        elif operation == 'dec':
            return self._decrement(params)
        elif operation == 'set':
            return self._set_gauge(params)
        elif operation == 'record':
            return self._record_histogram(params)
        elif operation == 'timing':
            return self._record_timing(params)
        elif operation == 'get':
            return self._get_metric(params)
        elif operation == 'snapshot':
            return self._snapshot(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _increment(self, params: Dict[str, Any]) -> ActionResult:
        """Increment a counter."""
        metric = params.get('metric')
        value = params.get('value', 1)
        labels = params.get('labels', {})
        
        if not metric:
            return ActionResult(success=False, message="metric is required")
        
        key = self._make_key(metric, labels)
        
        with self._lock:
            self._counters[key] += value
            self._labels[key] = labels
            self._add_to_series(key, self._counters[key], labels)
        
        return ActionResult(
            success=True,
            message=f"Incremented {metric}",
            data={'metric': metric, 'value': self._counters[key]}
        )
    
    def _decrement(self, params: Dict[str, Any]) -> ActionResult:
        """Decrement a counter."""
        metric = params.get('metric')
        value = params.get('value', 1)
        labels = params.get('labels', {})
        
        key = self._make_key(metric, labels)
        
        with self._lock:
            self._counters[key] -= value
        
        return ActionResult(
            success=True,
            message=f"Decremented {metric}",
            data={'metric': metric, 'value': self._counters[key]}
        )
    
    def _set_gauge(self, params: Dict[str, Any]) -> ActionResult:
        """Set a gauge value."""
        metric = params.get('metric')
        value = params.get('value')
        labels = params.get('labels', {})
        
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        key = self._make_key(metric, labels)
        
        with self._lock:
            self._gauges[key] = float(value)
            self._labels[key] = labels
            self._add_to_series(key, self._gauges[key], labels)
        
        return ActionResult(
            success=True,
            message=f"Set gauge {metric}",
            data={'metric': metric, 'value': value}
        )
    
    def _record_histogram(self, params: Dict[str, Any]) -> ActionResult:
        """Record a histogram value."""
        metric = params.get('metric')
        value = params.get('value')
        labels = params.get('labels', {})
        
        if value is None:
            return ActionResult(success=False, message="value is required")
        
        key = self._make_key(metric, labels)
        
        with self._lock:
            self._histograms[key].append(float(value))
            self._labels[key] = labels
        
        return ActionResult(
            success=True,
            message=f"Recorded histogram {metric}",
            data={'metric': metric, 'count': len(self._histograms[key])}
        )
    
    def _record_timing(self, params: Dict[str, Any]) -> ActionResult:
        """Record a timing value (in milliseconds)."""
        metric = params.get('metric')
        duration_ms = params.get('duration_ms')
        labels = params.get('labels', {})
        
        if duration_ms is None:
            return ActionResult(success=False, message="duration_ms is required")
        
        key = self._make_key(metric, labels)
        
        with self._lock:
            self._timers[key].append(float(duration_ms))
        
        return ActionResult(
            success=True,
            message=f"Recorded timing {metric}",
            data={'metric': metric, 'duration_ms': duration_ms}
        )
    
    def _get_metric(self, params: Dict[str, Any]) -> ActionResult:
        """Get current metric value(s)."""
        metric = params.get('metric')
        labels = params.get('labels')
        
        with self._lock:
            if metric:
                key = self._make_key(metric, labels or {})
                result = {}
                
                # Counter
                if key in self._counters:
                    result['counter'] = self._counters[key]
                # Gauge
                if key in self._gauges:
                    result['gauge'] = self._gauges[key]
                # Histogram
                if key in self._histograms:
                    values = self._histograms[key]
                    result['histogram'] = {
                        'count': len(values),
                        'sum': sum(values),
                        'min': min(values),
                        'max': max(values),
                        'mean': mean(values),
                        'median': median(values)
                    }
                # Timer
                if key in self._timers:
                    values = self._timers[key]
                    result['timer'] = {
                        'count': len(values),
                        'sum': sum(values),
                        'min': min(values),
                        'max': max(values),
                        'mean': mean(values),
                        'p95': self._percentile(values, 0.95),
                        'p99': self._percentile(values, 0.99)
                    }
                
                return ActionResult(
                    success=True,
                    message=f"Metric {metric}",
                    data={'metric': metric, 'labels': labels, 'values': result}
                )
            else:
                # Return all metrics
                return ActionResult(
                    success=True,
                    message="All metrics",
                    data={
                        'counters': dict(self._counters),
                        'gauges': dict(self._gauges),
                        'histogram_count': len(self._histograms),
                        'timer_count': len(self._timers)
                    }
                )
    
    def _snapshot(self, params: Dict[str, Any]) -> ActionResult:
        """Get a snapshot of all metrics."""
        window = params.get('window', 300)  # seconds
        
        with self._lock:
            snapshot = {
                'timestamp': time.time(),
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'histograms': {
                    k: {
                        'count': len(v),
                        'sum': sum(v),
                        'min': min(v) if v else 0,
                        'max': max(v) if v else 0,
                        'mean': mean(v) if v else 0
                    }
                    for k, v in self._histograms.items()
                },
                'timers': {
                    k: {
                        'count': len(v),
                        'sum': sum(v),
                        'min': min(v) if v else 0,
                        'max': max(v) if v else 0,
                        'mean': mean(v) if v else 0,
                        'p95': self._percentile(v, 0.95),
                        'p99': self._percentile(v, 0.99)
                    }
                    for k, v in self._timers.items()
                }
            }
        
        return ActionResult(
            success=True,
            message="Metrics snapshot",
            data=snapshot
        )
    
    def _make_key(self, metric: str, labels: Dict[str, str]) -> str:
        """Create a metric key from name and labels."""
        if not labels:
            return metric
        
        label_str = ','.join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{metric}{{{label_str}}}"
    
    def _add_to_series(
        self,
        key: str,
        value: float,
        labels: Dict[str, str]
    ) -> None:
        """Add point to time series."""
        self._series[key].append(MetricPoint(
            timestamp=time.time(),
            value=value,
            labels=labels
        ))
        
        # Limit series size
        if len(self._series[key]) > 1000:
            self._series[key] = self._series[key][-1000:]
    
    def _percentile(self, values: List[float], p: float) -> float:
        """Calculate percentile of values."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * p)
        return sorted_values[min(index, len(sorted_values) - 1)]


class PercentileTrackerAction(BaseAction):
    """Track percentiles over a sliding window."""
    action_type = "percentile_tracker"
    display_name = "百分位追踪"
    description = "滑动窗口百分位数追踪"
    
    def __init__(self):
        super().__init__()
        self._values: Dict[str, List[float]] = defaultdict(list)
        self._window_size = 1000
        self._lock = Lock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Track and get percentiles."""
        metric = params.get('metric')
        value = params.get('value')
        percentiles = params.get('percentiles', [0.5, 0.9, 0.95, 0.99])
        
        if value is not None:
            with self._lock:
                self._values[metric].append(float(value))
                if len(self._values[metric]) > self._window_size:
                    self._values[metric] = self._values[metric][-self._window_size:]
        
        if metric and metric in self._values:
            with self._lock:
                values = self._values[metric]
                result = {
                    f"p{int(p*100)}": self._calc_percentile(values, p)
                    for p in percentiles
                }
                result['count'] = len(values)
            
            return ActionResult(
                success=True,
                message=f"Percentiles for {metric}",
                data=result
            )
        
        return ActionResult(
            success=False,
            message="No data for metric"
        )
    
    def _calc_percentile(self, values: List[float], p: float) -> float:
        """Calculate percentile."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int(len(sorted_values) * p)
        return sorted_values[min(index, len(sorted_values) - 1)]
