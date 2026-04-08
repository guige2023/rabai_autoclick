"""
Automation metrics collection and monitoring utilities.

Provides metrics tracking for automation workflows including
timing, success rates, and performance monitoring.
"""

from __future__ import annotations

import time
import psutil
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from collections import defaultdict
import threading


class MetricType(Enum):
    """Metric value types."""
    COUNTER = "counter"
    GAUGE = "gauge"
    TIMER = "timer"
    HISTOGRAM = "histogram"


@dataclass
class Metric:
    """Single metric data point."""
    name: str
    value: float
    metric_type: MetricType
    timestamp: float
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class AutomationMetrics:
    """Aggregated automation metrics."""
    total_runs: int
    successful_runs: int
    failed_runs: int
    total_duration: float
    average_duration: float
    success_rate: float
    timestamp: float


class MetricsCollector:
    """Collects and aggregates automation metrics."""
    
    def __init__(self, name: str = "automation"):
        """
        Initialize metrics collector.
        
        Args:
            name: Metrics namespace.
        """
        self.name = name
        self._metrics: List[Metric] = []
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._timers: Dict[str, List[float]] = defaultdict(list)
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._lock = threading.Lock()
        self._start_time = time.time()
    
    def increment(self, name: str, value: float = 1.0, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Increment a counter metric.
        
        Args:
            name: Metric name.
            value: Value to add.
            tags: Optional tags.
        """
        with self._lock:
            self._counters[name] += value
            self._metrics.append(Metric(
                name=name,
                value=self._counters[name],
                metric_type=MetricType.COUNTER,
                timestamp=time.time(),
                tags=tags or {}
            ))
    
    def gauge(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Set a gauge metric.
        
        Args:
            name: Metric name.
            value: Current value.
            tags: Optional tags.
        """
        with self._lock:
            self._gauges[name] = value
            self._metrics.append(Metric(
                name=name,
                value=value,
                metric_type=MetricType.GAUGE,
                timestamp=time.time(),
                tags=tags or {}
            ))
    
    def timing(self, name: str, duration: float, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Record a timing metric.
        
        Args:
            name: Metric name.
            duration: Duration in seconds.
            tags: Optional tags.
        """
        with self._lock:
            self._timers[name].append(duration)
            self._metrics.append(Metric(
                name=name,
                value=duration,
                metric_type=MetricType.TIMER,
                timestamp=time.time(),
                tags=tags or {}
            ))
    
    def histogram(self, name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Record a histogram value.
        
        Args:
            name: Metric name.
            value: Observed value.
            tags: Optional tags.
        """
        with self._lock:
            self._histograms[name].append(value)
            self._metrics.append(Metric(
                name=name,
                value=value,
                metric_type=MetricType.HISTOGRAM,
                timestamp=time.time(),
                tags=tags or {}
            ))
    
    def get_counter(self, name: str) -> float:
        """Get counter value."""
        with self._lock:
            return self._counters.get(name, 0.0)
    
    def get_gauge(self, name: str) -> Optional[float]:
        """Get gauge value."""
        with self._lock:
            return self._gauges.get(name)
    
    def get_timer_stats(self, name: str) -> Dict[str, float]:
        """
        Get timing statistics.
        
        Returns:
            Dict with min, max, mean, median, p95, p99.
        """
        with self._lock:
            values = sorted(self._timers.get(name, []))
            if not values:
                return {}
            
            n = len(values)
            return {
                'count': n,
                'min': values[0],
                'max': values[-1],
                'mean': sum(values) / n,
                'median': values[n // 2],
                'p95': values[int(n * 0.95)] if n > 1 else values[0],
                'p99': values[int(n * 0.99)] if n > 1 else values[0],
            }
    
    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics summary."""
        with self._lock:
            return {
                'namespace': self.name,
                'uptime': time.time() - self._start_time,
                'counters': dict(self._counters),
                'gauges': dict(self._gauges),
                'timers': {
                    name: self.get_timer_stats(name)
                    for name in self._timers
                },
                'total_metrics': len(self._metrics)
            }
    
    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._gauges.clear()
            self._timers.clear()
            self._histograms.clear()
            self._start_time = time.time()


class Timer:
    """Context manager for timing code blocks."""
    
    def __init__(self, collector: MetricsCollector, name: str, tags: Optional[Dict[str, str]] = None):
        self.collector = collector
        self.name = name
        self.tags = tags
        self.start_time = 0.0
        self.duration = 0.0
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, *args):
        self.duration = time.time() - self.start_time
        self.collector.timing(self.name, self.duration, self.tags)


def get_system_metrics() -> Dict[str, Any]:
    """
    Get current system metrics.
    
    Returns:
        Dict with CPU, memory, disk usage.
    """
    return {
        'cpu_percent': psutil.cpu_percent(interval=0.1),
        'memory_percent': psutil.virtual_memory().percent,
        'memory_available_mb': psutil.virtual_memory().available / (1024 * 1024),
        'disk_percent': psutil.disk_usage('/').percent,
        'timestamp': time.time()
    }


def get_process_metrics() -> Dict[str, Any]:
    """
    Get current process metrics.
    
    Returns:
        Dict with process CPU, memory usage.
    """
    process = psutil.Process()
    with process.oneshot():
        return {
            'pid': process.pid,
            'cpu_percent': process.cpu_percent(),
            'memory_mb': process.memory_info().rss / (1024 * 1024),
            'num_threads': process.num_threads(),
            'timestamp': time.time()
        }
