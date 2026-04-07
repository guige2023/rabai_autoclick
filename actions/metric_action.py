"""
Metric collection and monitoring actions.
"""
from __future__ import annotations

import time
import psutil
from typing import Dict, Any, Optional, List
from collections import defaultdict
from datetime import datetime


class MetricCollector:
    """Collect and store metrics."""

    def __init__(self):
        """Initialize collector."""
        self.metrics: Dict[str, List[float]] = defaultdict(list)
        self.counters: Dict[str, int] = defaultdict(int)
        self.gauges: Dict[str, float] = {}
        self.timestamps: Dict[str, float] = {}

    def record(self, name: str, value: float) -> None:
        """Record a metric value."""
        self.metrics[name].append(value)
        self.timestamps[name] = time.time()

    def increment(self, name: str, amount: int = 1) -> int:
        """Increment a counter."""
        self.counters[name] += amount
        return self.counters[name]

    def decrement(self, name: str, amount: int = 1) -> int:
        """Decrement a counter."""
        self.counters[name] -= amount
        return self.counters[name]

    def set_gauge(self, name: str, value: float) -> None:
        """Set a gauge value."""
        self.gauges[name] = value

    def get_counter(self, name: str) -> int:
        """Get counter value."""
        return self.counters.get(name, 0)

    def get_gauge(self, name: str) -> Optional[float]:
        """Get gauge value."""
        return self.gauges.get(name)

    def get_metric_stats(self, name: str) -> Dict[str, Any]:
        """Get statistics for a metric."""
        values = self.metrics.get(name, [])

        if not values:
            return {'error': 'No data'}

        sorted_values = sorted(values)
        count = len(sorted_values)

        return {
            'count': count,
            'min': sorted_values[0],
            'max': sorted_values[-1],
            'mean': sum(sorted_values) / count,
            'median': sorted_values[count // 2],
            'p95': sorted_values[int(count * 0.95)] if count >= 20 else sorted_values[-1],
            'p99': sorted_values[int(count * 0.99)] if count >= 100 else sorted_values[-1],
        }

    def reset(self, name: Optional[str] = None) -> None:
        """Reset metrics."""
        if name:
            self.metrics.pop(name, None)
            self.counters.pop(name, None)
            self.gauges.pop(name, None)
        else:
            self.metrics.clear()
            self.counters.clear()
            self.gauges.clear()


_collector = MetricCollector()


def record_metric(name: str, value: float) -> None:
    """Record a metric value."""
    _collector.record(name, value)


def increment_counter(name: str, amount: int = 1) -> int:
    """Increment a counter."""
    return _collector.increment(name, amount)


def decrement_counter(name: str, amount: int = 1) -> int:
    """Decrement a counter."""
    return _collector.decrement(name, amount)


def set_gauge(name: str, value: float) -> None:
    """Set a gauge value."""
    _collector.set_gauge(name, value)


def get_metric_summary(name: str) -> Dict[str, Any]:
    """Get summary statistics for a metric."""
    return _collector.get_metric_stats(name)


def get_system_metrics() -> Dict[str, Any]:
    """
    Get current system metrics.

    Returns:
        Dictionary of system metrics.
    """
    cpu_percent = psutil.cpu_percent(interval=0.1)
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')

    return {
        'cpu_percent': cpu_percent,
        'cpu_count': psutil.cpu_count(),
        'memory_total_gb': round(memory.total / (1024 ** 3), 2),
        'memory_used_gb': round(memory.used / (1024 ** 3), 2),
        'memory_percent': memory.percent,
        'disk_total_gb': round(disk.total / (1024 ** 3), 2),
        'disk_used_gb': round(disk.used / (1024 ** 3), 2),
        'disk_percent': disk.percent,
    }


def get_cpu_metrics() -> Dict[str, Any]:
    """
    Get CPU metrics.

    Returns:
        CPU metrics.
    """
    cpu_percent = psutil.cpu_percent(interval=0.5, percpu=True)
    load_avg = psutil.getloadavg() if hasattr(psutil, 'getloadavg') else (0, 0, 0)

    return {
        'cpu_percent_per_core': cpu_percent,
        'cpu_percent_total': sum(cpu_percent) / len(cpu_percent) if cpu_percent else 0,
        'load_average_1m': load_avg[0],
        'load_average_5m': load_avg[1],
        'load_average_15m': load_avg[2],
        'cpu_count': psutil.cpu_count(),
        'cpu_freq_mhz': psutil.cpu_freq().current if psutil.cpu_freq() else None,
    }


def get_memory_metrics() -> Dict[str, Any]:
    """
    Get memory metrics.

    Returns:
        Memory metrics.
    """
    memory = psutil.virtual_memory()
    swap = psutil.swap_memory()

    return {
        'total_gb': round(memory.total / (1024 ** 3), 2),
        'available_gb': round(memory.available / (1024 ** 3), 2),
        'used_gb': round(memory.used / (1024 ** 3), 2),
        'free_gb': round(memory.free / (1024 ** 3), 2),
        'percent_used': memory.percent,
        'swap_total_gb': round(swap.total / (1024 ** 3), 2),
        'swap_used_gb': round(swap.used / (1024 ** 3), 2),
        'swap_percent': swap.percent,
    }


def get_disk_metrics(path: str = '/') -> Dict[str, Any]:
    """
    Get disk metrics.

    Args:
        path: Path to check.

    Returns:
        Disk metrics.
    """
    usage = psutil.disk_usage(path)
    io_counters = psutil.disk_io_counters() if hasattr(psutil, 'disk_io_counters') else None

    metrics = {
        'path': path,
        'total_gb': round(usage.total / (1024 ** 3), 2),
        'used_gb': round(usage.used / (1024 ** 3), 2),
        'free_gb': round(usage.free / (1024 ** 3), 2),
        'percent_used': usage.percent,
    }

    if io_counters:
        metrics['read_count'] = io_counters.read_count
        metrics['write_count'] = io_counters.write_count
        metrics['read_mb'] = round(io_counters.read_bytes / (1024 ** 2), 2)
        metrics['write_mb'] = round(io_counters.write_bytes / (1024 ** 2), 2)

    return metrics


def get_network_metrics() -> Dict[str, Any]:
    """
    Get network metrics.

    Returns:
        Network metrics.
    """
    io_counters = psutil.net_io_counters()

    return {
        'bytes_sent_mb': round(io_counters.bytes_sent / (1024 ** 2), 2),
        'bytes_recv_mb': round(io_counters.bytes_recv / (1024 ** 2), 2),
        'packets_sent': io_counters.packets_sent,
        'packets_recv': io_counters.packets_recv,
        'errin': io_counters.errin,
        'errout': io_counters.errout,
        'dropin': io_counters.dropin,
        'dropout': io_counters.dropout,
    }


def get_process_metrics() -> List[Dict[str, Any]]:
    """
    Get metrics for running processes.

    Returns:
        List of process metrics.
    """
    processes = []

    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
        try:
            processes.append({
                'pid': proc.info['pid'],
                'name': proc.info['name'],
                'cpu_percent': proc.info['cpu_percent'],
                'memory_percent': proc.info['memory_percent'],
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    return sorted(processes, key=lambda x: x.get('cpu_percent', 0), reverse=True)[:10]


def get_top_processes(limit: int = 10) -> List[Dict[str, Any]]:
    """
    Get top processes by CPU usage.

    Args:
        limit: Number of processes to return.

    Returns:
        Top processes.
    """
    processes = []

    for proc in psutil.process_iter(['pid', 'name', 'cpu_percent', 'memory_percent']):
        try:
            info = proc.info
            info['memory_mb'] = round(proc.memory_info().rss / (1024 ** 2), 2)
            processes.append(info)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass

    return sorted(processes, key=lambda x: x.get('cpu_percent', 0), reverse=True)[:limit]


def measure_execution_time(func: callable) -> tuple:
    """
    Measure function execution time.

    Args:
        func: Function to measure.

    Returns:
        Tuple of (result, duration_seconds).
    """
    start = time.time()
    result = func()
    duration = time.time() - start

    return (result, round(duration, 4))


class Timer:
    """Context manager for timing code blocks."""

    def __init__(self, name: str = 'operation'):
        """Initialize timer."""
        self.name = name
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.duration: Optional[float] = None

    def __enter__(self):
        """Start timer."""
        self.start_time = time.time()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop timer."""
        self.end_time = time.time()
        self.duration = round(self.end_time - self.start_time, 4)

    def elapsed(self) -> float:
        """Get elapsed time."""
        if self.start_time is None:
            return 0.0

        end = self.end_time or time.time()
        return round(end - self.start_time, 4)


def timed_operation(name: str):
    """Decorator for timing functions."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with Timer(name) as timer:
                result = func(*args, **kwargs)
            record_metric(f'{name}_duration_seconds', timer.duration or 0)
            return result
        return wrapper
    return decorator


def get_uptime() -> Dict[str, Any]:
    """
    Get system uptime.

    Returns:
        Uptime information.
    """
    boot_time = datetime.fromtimestamp(psutil.boot_time())
    now = datetime.now()
    uptime_delta = now - boot_time

    return {
        'boot_time': boot_time.isoformat(),
        'uptime_seconds': int(uptime_delta.total_seconds()),
        'uptime_days': uptime_delta.days,
        'uptime_hours': uptime_delta.seconds // 3600,
        'uptime_minutes': (uptime_delta.seconds % 3600) // 60,
    }


def check_resource_usage(thresholds: Dict[str, float]) -> Dict[str, Any]:
    """
    Check if resource usage exceeds thresholds.

    Args:
        thresholds: Dictionary of metric -> threshold percentage.

    Returns:
        Alert information.
    """
    metrics = get_system_metrics()
    alerts = []

    if 'cpu_percent' in thresholds and metrics['cpu_percent'] > thresholds['cpu_percent']:
        alerts.append({
            'metric': 'cpu_percent',
            'value': metrics['cpu_percent'],
            'threshold': thresholds['cpu_percent'],
        })

    if 'memory_percent' in thresholds and metrics['memory_percent'] > thresholds['memory_percent']:
        alerts.append({
            'metric': 'memory_percent',
            'value': metrics['memory_percent'],
            'threshold': thresholds['memory_percent'],
        })

    if 'disk_percent' in thresholds and metrics['disk_percent'] > thresholds['disk_percent']:
        alerts.append({
            'metric': 'disk_percent',
            'value': metrics['disk_percent'],
            'threshold': thresholds['disk_percent'],
        })

    return {
        'has_alerts': len(alerts) > 0,
        'alerts': alerts,
    }
