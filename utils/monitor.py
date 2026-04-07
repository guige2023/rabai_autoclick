"""Monitoring utilities for RabAI AutoClick.

Provides:
- Performance monitoring
- Health checks
- Metrics collection
"""

import gc
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional


@dataclass
class PerformanceMetrics:
    """Performance metrics snapshot."""
    timestamp: float
    cpu_percent: float
    memory_mb: float
    thread_count: int
    gc_counts: tuple


@dataclass
class HealthCheck:
    """Health check result."""
    name: str
    healthy: bool
    message: str = ""
    latency_ms: float = 0


class HealthChecker:
    """Application health checker.

    Runs health checks and reports overall status.
    """

    def __init__(self) -> None:
        self._checks: Dict[str, Callable[[], HealthCheck]] = {}

    def register(self, name: str, check: Callable[[], HealthCheck]) -> None:
        """Register a health check.

        Args:
            name: Check name.
            check: Function that returns HealthCheck.
        """
        self._checks[name] = check

    def check_all(self) -> tuple:
        """Run all health checks.

        Returns:
            Tuple of (overall_healthy, List[HealthCheck]).
        """
        results = []
        all_healthy = True

        for name, check in self._checks.items():
            try:
                result = check()
                results.append(result)
                if not result.healthy:
                    all_healthy = False
            except Exception as e:
                results.append(HealthCheck(
                    name=name,
                    healthy=False,
                    message=str(e),
                ))
                all_healthy = False

        return all_healthy, results

    @property
    def check_names(self) -> List[str]:
        """Get registered check names."""
        return list(self._checks.keys())


class PerformanceMonitor:
    """Monitor application performance."""

    def __init__(self, interval: float = 60) -> None:
        """Initialize monitor.

        Args:
            interval: Sampling interval in seconds.
        """
        self.interval = interval
        self._samples: List[PerformanceMetrics] = []
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()

    def _collect_sample(self) -> PerformanceMetrics:
        """Collect a performance sample."""
        try:
            import psutil
            process = psutil.Process()
            cpu = process.cpu_percent(interval=0.1)
            mem = process.memory_info().rss / 1024 / 1024
        except ImportError:
            cpu = 0
            mem = 0

        gc_counts = gc.get_count()

        return PerformanceMetrics(
            timestamp=time.time(),
            cpu_percent=cpu,
            memory_mb=mem,
            thread_count=threading.active_count(),
            gc_counts=gc_counts,
        )

    def sample(self) -> None:
        """Take a performance sample."""
        sample = self._collect_sample()
        with self._lock:
            self._samples.append(sample)

    def start(self) -> None:
        """Start monitoring."""
        if self._running:
            return

        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop monitoring."""
        if not self._running:
            return

        self._running = False
        if self._thread:
            self._thread.join(timeout=5)

    def _run(self) -> None:
        """Main monitoring loop."""
        while self._running:
            self.sample()
            time.sleep(self.interval)

    def get_samples(self) -> List[PerformanceMetrics]:
        """Get collected samples."""
        with self._lock:
            return self._samples.copy()

    def get_average(self) -> Dict[str, float]:
        """Get average metrics."""
        with self._lock:
            if not self._samples:
                return {}

            return {
                "avg_cpu": sum(s.cpu_percent for s in self._samples) / len(self._samples),
                "avg_memory": sum(s.memory_mb for s in self._samples) / len(self._samples),
                "avg_threads": sum(s.thread_count for s in self._samples) / len(self._samples),
            }


class ResourceTracker:
    """Track resource usage over time."""

    def __init__(self) -> None:
        self._snapshots: List[Dict[str, Any]] = []

    def take_snapshot(self, label: str = "") -> Dict[str, Any]:
        """Take a resource usage snapshot.

        Args:
            label: Optional label for snapshot.

        Returns:
            Snapshot dictionary.
        """
        try:
            import psutil
            process = psutil.Process()
            snapshot = {
                "timestamp": time.time(),
                "label": label,
                "cpu_percent": process.cpu_percent(),
                "memory_mb": process.memory_info().rss / 1024 / 1024,
                "num_threads": process.num_threads(),
            }
        except ImportError:
            snapshot = {
                "timestamp": time.time(),
                "label": label,
            }

        self._snapshots.append(snapshot)
        return snapshot

    def get_snapshots(self) -> List[Dict[str, Any]]:
        """Get all snapshots."""
        return self._snapshots.copy()

    def clear(self) -> None:
        """Clear all snapshots."""
        self._snapshots.clear()


class Watchdog:
    """Watchdog timer for detecting hangs.

    If the watched operation takes too long, it's considered hung.
    """

    def __init__(self, timeout: float) -> None:
        """Initialize watchdog.

        Args:
            timeout: Timeout in seconds.
        """
        self.timeout = timeout
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def watch(self, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Watch a function execution.

        Args:
            func: Function to watch.
            *args: Function arguments.
            **kwargs: Function keyword arguments.

        Returns:
            Function result.

        Raises:
            TimeoutError: If function takes too long.
        """
        result = [None]
        error = [None]
        finished = threading.Event()

        def target():
            try:
                result[0] = func(*args, **kwargs)
            except Exception as e:
                error[0] = e
            finally:
                finished.set()

        thread = threading.Thread(target=target)
        thread.start()

        if not finished.wait(self.timeout):
            thread.join(timeout=0.1)
            raise TimeoutError(f"Operation timed out after {self.timeout}s")

        if error[0]:
            raise error[0]

        return result[0]