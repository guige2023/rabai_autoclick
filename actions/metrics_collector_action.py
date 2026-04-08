"""Metrics collector action module for RabAI AutoClick.

Provides metrics collection with counters, gauges, histograms,
and time series tracking for monitoring.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from collections import defaultdict, deque
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class MetricType(Enum):
    """Metric types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


from enum import Enum


@dataclass
class MetricValue:
    """A single metric value."""
    timestamp: float
    value: float
    labels: Dict[str, str] = field(default_factory=dict)


class MetricsCollectorAction(BaseAction):
    """Metrics collector action for monitoring and observability.
    
    Supports counters, gauges, histograms, and timers with
    configurable retention and aggregation.
    """
    action_type = "metrics_collector"
    display_name = "指标收集器"
    description = "监控指标收集与聚合"
    
    def __init__(self):
        super().__init__()
        self._metrics: Dict[str, Dict[str, Any]] = defaultdict(lambda: {'type': None, 'values': deque(maxlen=10000)})
        self._lock = threading.RLock()
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = defaultdict(float)
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timers: Dict[str, List[float]] = defaultdict(list)
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute metrics operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: inc|dec|set|gauge|record|get|stats
                name: Metric name
                value: Metric value
                labels: Metric labels.
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'get')
        
        if operation == 'inc':
            return self._inc(params)
        elif operation == 'dec':
            return self._dec(params)
        elif operation == 'set':
            return self._set(params)
        elif operation == 'gauge':
            return self._gauge(params)
        elif operation == 'record':
            return self._record(params)
        elif operation == 'get':
            return self._get(params)
        elif operation == 'stats':
            return self._stats(params)
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _inc(self, params: Dict[str, Any]) -> ActionResult:
        """Increment counter."""
        name = params.get('name')
        value = params.get('value', 1)
        labels = params.get('labels', {})
        
        if not name:
            return ActionResult(success=False, message="Metric name required")
        
        key = self._make_key(name, labels)
        
        with self._lock:
            self._counters[key] += value
            self._record_metric(name, MetricType.COUNTER, self._counters[key], labels)
        
        return ActionResult(
            success=True,
            message=f"Incremented {name} by {value}",
            data={'name': name, 'value': self._counters[key], 'labels': labels}
        )
    
    def _dec(self, params: Dict[str, Any]) -> ActionResult:
        """Decrement counter."""
        name = params.get('name')
        value = params.get('value', 1)
        labels = params.get('labels', {})
        
        if not name:
            return ActionResult(success=False, message="Metric name required")
        
        key = self._make_key(name, labels)
        
        with self._lock:
            self._counters[key] -= value
            self._record_metric(name, MetricType.COUNTER, self._counters[key], labels)
        
        return ActionResult(
            success=True,
            message=f"Decremented {name} by {value}",
            data={'name': name, 'value': self._counters[key], 'labels': labels}
        )
    
    def _set(self, params: Dict[str, Any]) -> ActionResult:
        """Set gauge value."""
        name = params.get('name')
        value = params.get('value')
        labels = params.get('labels', {})
        
        if not name or value is None:
            return ActionResult(success=False, message="Name and value required")
        
        key = self._make_key(name, labels)
        
        with self._lock:
            self._gauges[key] = value
            self._record_metric(name, MetricType.GAUGE, value, labels)
        
        return ActionResult(
            success=True,
            message=f"Set {name} to {value}",
            data={'name': name, 'value': value, 'labels': labels}
        )
    
    def _gauge(self, params: Dict[str, Any]) -> ActionResult:
        """Alias for set."""
        return self._set(params)
    
    def _record(self, params: Dict[str, Any]) -> ActionResult:
        """Record histogram value or timer."""
        name = params.get('name')
        value = params.get('value')
        metric_type = params.get('type', 'histogram')
        labels = params.get('labels', {})
        
        if not name or value is None:
            return ActionResult(success=False, message="Name and value required")
        
        key = self._make_key(name, labels)
        
        with self._lock:
            if metric_type == 'histogram':
                self._histograms[key].append(value)
                self._record_metric(name, MetricType.HISTOGRAM, value, labels)
            elif metric_type == 'timer':
                self._timers[key].append(value)
                self._record_metric(name, MetricType.TIMER, value, labels)
        
        return ActionResult(
            success=True,
            message=f"Recorded {metric_type} {name}={value}",
            data={'name': name, 'value': value, 'type': metric_type, 'labels': labels}
        )
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get current metric value."""
        name = params.get('name')
        labels = params.get('labels', {})
        
        if not name:
            return ActionResult(success=False, message="Metric name required")
        
        key = self._make_key(name, labels)
        
        with self._lock:
            if key in self._counters:
                value = self._counters[key]
            elif key in self._gauges:
                value = self._gauges[key]
            else:
                return ActionResult(success=False, message=f"Metric {name} not found")
        
        return ActionResult(
            success=True,
            message=f"Got {name}={value}",
            data={'name': name, 'value': value, 'labels': labels}
        )
    
    def _stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get statistics for all metrics."""
        name = params.get('name')
        labels = params.get('labels', {})
        
        with self._lock:
            stats = {}
            
            if name:
                keys = [self._make_key(name, labels)]
            else:
                keys = list(set(list(self._counters.keys()) + list(self._gauges.keys())))
            
            for key in keys:
                metric_name = key.split('::')[0] if '::' in key else key
                label_str = key.split('::')[1] if '::' in key else '{}'
                
                if key in self._counters:
                    stats[metric_name] = {
                        'type': 'counter',
                        'value': self._counters[key],
                        'labels': self._parse_labels(label_str)
                    }
                elif key in self._gauges:
                    stats[metric_name] = {
                        'type': 'gauge',
                        'value': self._gauges[key],
                        'labels': self._parse_labels(label_str)
                    }
                elif key in self._histograms:
                    values = self._histograms[key]
                    stats[metric_name] = {
                        'type': 'histogram',
                        'count': len(values),
                        'sum': sum(values),
                        'avg': sum(values) / len(values) if values else 0,
                        'min': min(values) if values else 0,
                        'max': max(values) if values else 0,
                        'p50': self._percentile(values, 50),
                        'p90': self._percentile(values, 90),
                        'p99': self._percentile(values, 99),
                        'labels': self._parse_labels(label_str)
                    }
                elif key in self._timers:
                    values = self._timers[key]
                    stats[metric_name] = {
                        'type': 'timer',
                        'count': len(values),
                        'sum': sum(values),
                        'avg': sum(values) / len(values) if values else 0,
                        'min': min(values) if values else 0,
                        'max': max(values) if values else 0,
                        'p50': self._percentile(values, 50),
                        'p90': self._percentile(values, 90),
                        'p99': self._percentile(values, 99),
                        'labels': self._parse_labels(label_str)
                    }
        
        return ActionResult(
            success=True,
            message=f"Stats for {len(stats)} metrics",
            data={'metrics': stats, 'count': len(stats)}
        )
    
    def _record_metric(
        self,
        name: str,
        metric_type: MetricType,
        value: float,
        labels: Dict[str, str]
    ) -> None:
        """Record metric value in time series."""
        key = f"{name}"
        self._metrics[key]['type'] = metric_type.value
        self._metrics[key]['values'].append(MetricValue(
            timestamp=time.time(),
            value=value,
            labels=labels
        ))
    
    def _make_key(self, name: str, labels: Dict[str, str]) -> str:
        """Create metric key from name and labels."""
        if not labels:
            return name
        label_str = ','.join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}::{label_str}"
    
    def _parse_labels(self, label_str: str) -> Dict[str, str]:
        """Parse labels from string."""
        if not label_str or label_str == '{}':
            return {}
        labels = {}
        for pair in label_str.split(','):
            if '=' in pair:
                k, v = pair.split('=', 1)
                labels[k] = v
        return labels
    
    def _percentile(self, values: List[float], p: int) -> float:
        """Calculate percentile of values."""
        if not values:
            return 0
        sorted_values = sorted(values)
        idx = int(len(sorted_values) * p / 100)
        return sorted_values[min(idx, len(sorted_values) - 1)]
