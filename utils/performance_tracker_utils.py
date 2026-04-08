"""Performance tracking utilities for monitoring execution metrics."""

from typing import Optional, Dict, Any, List, Callable
import time
import threading
import statistics


class PerformanceTracker:
    """Track performance metrics over time."""

    def __init__(self, window_size: int = 1000):
        """Initialize performance tracker.
        
        Args:
            window_size: Number of samples to keep for rolling statistics.
        """
        self.window_size = window_size
        self._durations: Dict[str, List[float]] = {}
        self._counts: Dict[str, int] = {}
        self._errors: Dict[str, int] = {}
        self._lock = threading.RLock()
        self._active_timers: Dict[str, float] = {}

    def start(self, operation: str) -> None:
        """Start tracking an operation.
        
        Args:
            operation: Operation name.
        """
        with self._lock:
            self._active_timers[operation] = time.perf_counter()

    def end(self, operation: str, success: bool = True) -> Optional[float]:
        """End tracking an operation.
        
        Args:
            operation: Operation name.
            success: Whether operation succeeded.
        
        Returns:
            Duration in seconds, or None if not started.
        """
        duration = None
        with self._lock:
            start_time = self._active_timers.pop(operation, None)
            if start_time is not None:
                duration = time.perf_counter() - start_time
                if operation not in self._durations:
                    self._durations[operation] = []
                self._durations[operation].append(duration)
                if len(self._durations[operation]) > self.window_size:
                    self._durations[operation] = self._durations[operation][-self.window_size:]
                self._counts[operation] = self._counts.get(operation, 0) + 1
                if not success:
                    self._errors[operation] = self._errors.get(operation, 0) + 1
        return duration

    def track(self, operation: str, func: Callable) -> Any:
        """Track a function execution.
        
        Args:
            operation: Operation name.
            func: Function to execute.
        
        Returns:
            Function result.
        """
        self.start(operation)
        try:
            return func()
        finally:
            self.end(operation)

    def get_stats(self, operation: str) -> Optional[Dict[str, Any]]:
        """Get statistics for an operation.
        
        Args:
            operation: Operation name.
        
        Returns:
            Statistics dict or None if not tracked.
        """
        with self._lock:
            durations = list(self._durations.get(operation, []))
            if not durations:
                return None
            return {
                "count": self._counts.get(operation, 0),
                "errors": self._errors.get(operation, 0),
                "total_time": sum(durations),
                "mean_time": statistics.mean(durations),
                "median_time": statistics.median(durations),
                "min_time": min(durations),
                "max_time": max(durations),
                "stdev": statistics.stdev(durations) if len(durations) > 1 else 0.0,
                "p95": self._percentile(durations, 0.95),
                "p99": self._percentile(durations, 0.99),
            }

    def _percentile(self, data: List[float], p: float) -> float:
        """Calculate percentile."""
        if not data:
            return 0.0
        sorted_data = sorted(data)
        idx = int(len(sorted_data) * p)
        return sorted_data[min(idx, len(sorted_data) - 1)]

    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all operations."""
        with self._lock:
            operations = set(self._durations.keys())
        return {op: self.get_stats(op) for op in operations if self.get_stats(op)}

    def reset(self, operation: Optional[str] = None) -> None:
        """Reset tracking data.
        
        Args:
            operation: Specific operation to reset, or None for all.
        """
        with self._lock:
            if operation:
                self._durations.pop(operation, None)
                self._counts.pop(operation, None)
                self._errors.pop(operation, None)
            else:
                self._durations.clear()
                self._counts.clear()
                self._errors.clear()


class ProfilerContext:
    """Context manager for profiling code blocks."""

    def __init__(self, tracker: PerformanceTracker, operation: str):
        """Initialize profiler context.
        
        Args:
            tracker: Performance tracker instance.
            operation: Operation name.
        """
        self.tracker = tracker
        self.operation = operation
        self.duration: Optional[float] = None

    def __enter__(self) -> "ProfilerContext":
        self.tracker.start(self.operation)
        return self

    def __exit__(self, *args: Any) -> None:
        self.duration = self.tracker.end(self.operation)
