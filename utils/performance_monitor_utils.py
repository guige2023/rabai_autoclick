"""
Performance monitoring utilities for automation workflows.

Provides utilities for monitoring CPU, memory, disk, and network usage,
as well as timing and profiling for automation scripts.
"""

from __future__ import annotations

import time
import subprocess
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import deque


@dataclass
class SystemMetrics:
    """System performance metrics snapshot."""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_gb: float
    memory_total_gb: float
    disk_read_bytes: int
    disk_write_bytes: int
    network_sent_bytes: int
    network_recv_bytes: int
    

@dataclass
class ProcessMetrics:
    """Per-process performance metrics."""
    pid: int
    name: str
    cpu_percent: float
    memory_mb: float
    num_threads: int
    timestamp: datetime


@dataclass
class TimingMeasurement:
    """A single timing measurement."""
    name: str
    start_time: float
    end_time: Optional[float] = None
    duration_ms: Optional[float] = None
    
    def complete(self) -> None:
        """Mark measurement as complete and calculate duration."""
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
    
    @property
    def is_complete(self) -> bool:
        return self.end_time is not None


class PerformanceMonitor:
    """Monitors system and process performance."""
    
    def __init__(self):
        """Initialize performance monitor."""
        self._metrics_history: deque = deque(maxlen=1000)
        self._process_history: Dict[int, deque] = {}
    
    def get_current_metrics(self) -> SystemMetrics:
        """Get current system metrics.
        
        Returns:
            SystemMetrics snapshot
        """
        # Get CPU usage
        cpu_percent = 0.0
        try:
            result = subprocess.run(
                ["ps", "-axco", "%cpu,pid"],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")[1:]  # Skip header
                cpu_values = [float(line.split()[0]) for line in lines if line.strip()]
                cpu_percent = sum(cpu_values) / len(cpu_values) if cpu_values else 0
        except Exception:
            pass
        
        # Get memory info
        memory_percent = 0.0
        memory_used = 0
        memory_total = 0
        try:
            result = subprocess.run(
                ["vm_stat"],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")
                for line in lines:
                    if "Pages active:" in line or "Pages wired down:" in line:
                        parts = line.split(":")
                        if len(parts) > 1:
                            value = parts[1].strip().rstrip(".")
                            memory_used += int(value)
                    elif "Pages free:" in line:
                        parts = line.split(":")
                        if len(parts) > 1:
                            value = parts[1].strip().rstrip(".")
                            memory_free = int(value)
        except Exception:
            pass
        
        try:
            result = subprocess.run(
                ["sysctl", "-n", "hw.memsize"],
                capture_output=True,
                text=True,
                timeout=2
            )
            if result.returncode == 0:
                memory_total = int(result.stdout.strip()) / (1024**3)
        except Exception:
            pass
        
        if memory_total > 0:
            page_size = 4096  # Default page size
            memory_used_gb = (memory_used * page_size) / (1024**3)
            memory_percent = (memory_used_gb / memory_total) * 100
        else:
            memory_used_gb = 0
        
        return SystemMetrics(
            timestamp=datetime.now(),
            cpu_percent=cpu_percent,
            memory_percent=memory_percent,
            memory_used_gb=memory_used_gb,
            memory_total_gb=memory_total,
            disk_read_bytes=0,
            disk_write_bytes=0,
            network_sent_bytes=0,
            network_recv_bytes=0,
        )
    
    def get_process_metrics(self, pid: Optional[int] = None) -> List[ProcessMetrics]:
        """Get metrics for processes.
        
        Args:
            pid: Optional specific PID to query
            
        Returns:
            List of ProcessMetrics
        """
        metrics = []
        
        try:
            cmd = ["ps", "-axco", "pid,%cpu,rss,threadcount,name"]
            if pid:
                cmd.insert(2, f"-p {pid}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=2)
            
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")[1:]
                for line in lines:
                    parts = line.split(None, 4)
                    if len(parts) >= 4:
                        try:
                            metrics.append(ProcessMetrics(
                                pid=int(parts[0]),
                                name=parts[4] if len(parts) > 4 else "unknown",
                                cpu_percent=float(parts[1]),
                                memory_mb=float(parts[2]) / 1024,
                                num_threads=int(parts[3]),
                                timestamp=datetime.now()
                            ))
                        except (ValueError, IndexError):
                            pass
        except Exception:
            pass
        
        return metrics
    
    def record_metrics(self) -> SystemMetrics:
        """Record current metrics to history.
        
        Returns:
            The recorded metrics
        """
        metrics = self.get_current_metrics()
        self._metrics_history.append(metrics)
        return metrics
    
    def get_average_metrics(
        self,
        duration_seconds: Optional[float] = None
    ) -> Dict[str, float]:
        """Get average metrics over a time period.
        
        Args:
            duration_seconds: Time period to average over
            
        Returns:
            Dictionary of averaged metrics
        """
        if not self._metrics_history:
            return {}
        
        cutoff = None
        if duration_seconds:
            cutoff = datetime.now() - timedelta(seconds=duration_seconds)
        
        relevant = [
            m for m in self._metrics_history
            if cutoff is None or m.timestamp >= cutoff
        ]
        
        if not relevant:
            return {}
        
        return {
            "cpu_percent_avg": sum(m.cpu_percent for m in relevant) / len(relevant),
            "memory_percent_avg": sum(m.memory_percent for m in relevant) / len(relevant),
            "memory_used_gb_avg": sum(m.memory_used_gb for m in relevant) / len(relevant),
        }
    
    def get_peak_metrics(self) -> Dict[str, float]:
        """Get peak (maximum) metrics from history.
        
        Returns:
            Dictionary of peak metrics
        """
        if not self._metrics_history:
            return {}
        
        return {
            "cpu_percent_peak": max(m.cpu_percent for m in self._metrics_history),
            "memory_percent_peak": max(m.memory_percent for m in self._metrics_history),
        }


class Timer:
    """Context manager for timing code execution."""
    
    def __init__(self, name: str = "operation", verbose: bool = True):
        """Initialize timer.
        
        Args:
            name: Name for this timing
            verbose: Whether to print results
        """
        self.name = name
        self.verbose = verbose
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.duration_ms: Optional[float] = None
    
    def __enter__(self) -> "Timer":
        self.start_time = time.time()
        return self
    
    def __exit__(self, *args) -> None:
        self.end_time = time.time()
        self.duration_ms = (self.end_time - self.start_time) * 1000
        
        if self.verbose:
            print(f"[Timer] {self.name}: {self.duration_ms:.2f}ms")
    
    def elapsed_ms(self) -> float:
        """Get elapsed time so far in milliseconds."""
        if self.start_time is None:
            return 0
        current = time.time()
        return (current - self.start_time) * 1000


class Profiler:
    """Simple profiler for tracking multiple timing measurements."""
    
    def __init__(self):
        """Initialize profiler."""
        self._measurements: Dict[str, List[float]] = {}
        self._active: Dict[str, TimingMeasurement] = {}
    
    def start(self, name: str) -> None:
        """Start a named measurement.
        
        Args:
            name: Measurement name
        """
        self._active[name] = TimingMeasurement(
            name=name,
            start_time=time.time()
        )
    
    def end(self, name: str) -> Optional[float]:
        """End a named measurement.
        
        Args:
            name: Measurement name
            
        Returns:
            Duration in milliseconds, or None if not started
        """
        if name not in self._active:
            return None
        
        measurement = self._active[name]
        measurement.complete()
        
        duration = measurement.duration_ms
        
        if name not in self._measurements:
            self._measurements[name] = []
        self._measurements[name].append(duration)
        
        del self._active[name]
        return duration
    
    def get_stats(self, name: str) -> Optional[Dict[str, float]]:
        """Get statistics for a measurement.
        
        Args:
            name: Measurement name
            
        Returns:
            Dictionary with min, max, avg, count, or None
        """
        if name not in self._measurements:
            return None
        
        values = self._measurements[name]
        return {
            "count": len(values),
            "min_ms": min(values),
            "max_ms": max(values),
            "avg_ms": sum(values) / len(values),
            "total_ms": sum(values),
        }
    
    def get_all_stats(self) -> Dict[str, Dict[str, float]]:
        """Get statistics for all measurements.
        
        Returns:
            Dictionary of name -> stats
        """
        return {
            name: self.get_stats(name)
            for name in self._measurements
        }
    
    def reset(self) -> None:
        """Clear all measurements."""
        self._measurements.clear()
        self._active.clear()
    
    def print_summary(self) -> None:
        """Print a summary of all measurements."""
        print("\n=== Profiler Summary ===")
        for name, stats in sorted(self.get_all_stats().items()):
            print(f"{name}:")
            print(f"  count: {stats['count']}")
            print(f"  min: {stats['min_ms']:.2f}ms")
            print(f"  max: {stats['max_ms']:.2f}ms")
            print(f"  avg: {stats['avg_ms']:.2f}ms")
            print(f"  total: {stats['total_ms']:.2f}ms")


# Global profiler instance
_global_profiler = Profiler()


def profile(name: str) -> Callable:
    """Decorator to profile a function.
    
    Args:
        name: Profile name
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        def wrapper(*args, **kwargs):
            _global_profiler.start(name)
            try:
                return func(*args, **kwargs)
            finally:
                _global_profiler.end(name)
        return wrapper
    return decorator


def get_profiler() -> Profiler:
    """Get the global profiler instance."""
    return _global_profiler


class RateTracker:
    """Tracks the rate of operations over time."""
    
    def __init__(self, window_seconds: float = 60.0):
        """Initialize rate tracker.
        
        Args:
            window_seconds: Time window for rate calculation
        """
        self.window_seconds = window_seconds
        self._events: deque = deque()
    
    def record(self) -> None:
        """Record an event."""
        self._events.append(time.time())
        self._prune()
    
    def _prune(self) -> None:
        """Remove old events outside the window."""
        cutoff = time.time() - self.window_seconds
        while self._events and self._events[0] < cutoff:
            self._events.popleft()
    
    @property
    def rate(self) -> float:
        """Get current rate (events per second)."""
        self._prune()
        if not self._events:
            return 0.0
        duration = self._events[-1] - self._events[0] if len(self._events) > 1 else 1
        return len(self._events) / duration
    
    @property
    def count(self) -> int:
        """Get total count in window."""
        self._prune()
        return len(self._events)
    
    def reset(self) -> None:
        """Clear all events."""
        self._events.clear()


def format_bytes(num_bytes: int) -> str:
    """Format bytes as human-readable string.
    
    Args:
        num_bytes: Number of bytes
        
    Returns:
        Formatted string (e.g., "1.5 GB")
    """
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f} PB"


def format_duration(seconds: float) -> str:
    """Format duration as human-readable string.
    
    Args:
        seconds: Duration in seconds
        
    Returns:
        Formatted string (e.g., "1h 23m 45s")
    """
    if seconds < 1:
        return f"{seconds*1000:.0f}ms"
    
    parts = []
    
    hours = int(seconds // 3600)
    if hours > 0:
        parts.append(f"{hours}h")
        seconds %= 3600
    
    minutes = int(seconds // 60)
    if minutes > 0:
        parts.append(f"{minutes}m")
        seconds %= 60
    
    if seconds > 0 or not parts:
        parts.append(f"{seconds:.0f}s")
    
    return " ".join(parts)
