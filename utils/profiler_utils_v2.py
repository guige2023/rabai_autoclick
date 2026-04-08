"""
Profiling and performance measurement utilities.

Provides decorators and context managers for timing,
memory profiling, and performance analysis.

Example:
    >>> from utils.profiler_utils_v2 import profile, timer
    >>> with timer("operation"):
    ...     do_work()
"""

from __future__ import annotations

import functools
import gc
import sys
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Union


@dataclass
class ProfileResult:
    """Result of a profiled operation."""
    name: str
    call_count: int
    total_time: float
    min_time: float
    max_time: float
    avg_time: float
    stddev_time: float
    memory_delta: int


@dataclass
class TimingResult:
    """Result of a timed operation."""
    name: str
    duration: float
    start_time: float
    end_time: float
    metadata: Dict[str, Any] = field(default_factory=dict)


class Timer:
    """
    Context manager and decorator for timing operations.

    Provides nanosecond precision timing with result tracking.
    """

    def __init__(
        self,
        name: str = "operation",
        precision: int = 6,
        log: bool = False,
    ) -> None:
        """
        Initialize the timer.

        Args:
            name: Name of the operation being timed.
            precision: Decimal precision for output.
            log: If True, print timing on exit.
        """
        self.name = name
        self.precision = precision
        self.log = log
        self._start: Optional[float] = None
        self._end: Optional[float] = None
        self._duration: Optional[float] = None

    def __enter__(self) -> "Timer":
        """Start timing."""
        self._start = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        """Stop timing."""
        self._end = time.perf_counter()
        self._duration = self._end - self._start
        if self.log:
            print(f"{self.name}: {self._duration:.{self.precision}f}s")

    @property
    def duration(self) -> Optional[float]:
        """Get elapsed duration in seconds."""
        return self._duration

    def result(self) -> TimingResult:
        """Get timing result."""
        return TimingResult(
            name=self.name,
            duration=self._duration or 0.0,
            start_time=self._start or 0.0,
            end_time=self._end or 0.0,
        )


def timer(
    name: str = "operation",
    log: bool = False,
) -> Callable:
    """
    Decorator for timing function execution.

    Args:
        name: Name for the timing record.
        log: If True, print timing on completion.

    Returns:
        Decorated function.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration = time.perf_counter() - start
                if log:
                    print(f"{name or func.__name__}: {duration:.6f}s")

        return wrapper
    return decorator


class MemoryProfiler:
    """
    Memory profiler for tracking memory usage.

    Tracks memory allocations and deallocations during execution.
    """

    def __init__(self) -> None:
        """Initialize the memory profiler."""
        self._snapshots: List[Dict[str, Any]] = []
        self._start_memory: Optional[int] = None

    def snapshot(self, label: str = "") -> Dict[str, Any]:
        """
        Take a memory snapshot.

        Args:
            label: Label for the snapshot.

        Returns:
            Snapshot dictionary.
        """
        gc.collect()
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()

        snapshot = {
            "label": label,
            "timestamp": time.time(),
            "rss": memory_info.rss,
            "vms": memory_info.vms,
        }

        if self._snapshots:
            prev = self._snapshots[-1]
            snapshot["rss_delta"] = snapshot["rss"] - prev["rss"]
            snapshot["vms_delta"] = snapshot["vms"] - prev["vms"]

        if self._start_memory is None:
            self._start_memory = memory_info.rss
            snapshot["rss_from_start"] = 0
            snapshot["vms_from_start"] = 0
        else:
            snapshot["rss_from_start"] = snapshot["rss"] - self._start_memory
            snapshot["vms_from_start"] = snapshot["vms"] - self._start_memory

        self._snapshots.append(snapshot)
        return snapshot

    def get_snapshots(self) -> List[Dict[str, Any]]:
        """Get all memory snapshots."""
        return list(self._snapshots)

    def get_summary(self) -> Dict[str, Any]:
        """Get memory profiling summary."""
        if not self._snapshots:
            return {}

        gc.collect()
        import psutil
        process = psutil.Process()
        current = process.memory_info()

        return {
            "num_snapshots": len(self._snapshots),
            "first_rss": self._snapshots[0]["rss"],
            "last_rss": self._snapshots[-1]["rss"],
            "peak_rss": max(s["rss"] for s in self._snapshots),
            "current_rss": current.rss,
            "total_delta": self._snapshots[-1]["rss"] - self._snapshots[0]["rss"],
        }


class Profiler:
    """
    Function call profiler.

    Tracks function call counts and execution times
    for performance analysis.
    """

    def __init__(self) -> None:
        """Initialize the profiler."""
        self._stats: Dict[str, ProfileResult] = {}
        self._call_stack: List[float] = []

    def record(
        self,
        name: str,
        duration: float,
        memory_delta: int = 0,
    ) -> None:
        """
        Record a function execution.

        Args:
            name: Function name.
            duration: Execution duration.
            memory_delta: Change in memory usage.
        """
        if name not in self._stats:
            self._stats[name] = ProfileResult(
                name=name,
                call_count=0,
                total_time=0.0,
                min_time=float("inf"),
                max_time=0.0,
                avg_time=0.0,
                stddev_time=0.0,
                memory_delta=0,
            )

        stat = self._stats[name]
        stat.call_count += 1
        stat.total_time += duration
        stat.min_time = min(stat.min_time, duration)
        stat.max_time = max(stat.max_time, duration)
        stat.memory_delta += memory_delta

        if stat.call_count > 1:
            avg = stat.total_time / stat.call_count
            variance = sum(
                (t - avg) ** 2 for t in [stat.min_time, stat.max_time]
            ) / 2
            stat.stddev_time = math.sqrt(variance)

        stat.avg_time = stat.total_time / stat.call_count

    def profile(self, func: Callable) -> Callable:
        """
        Decorator to profile a function.

        Args:
            func: Function to profile.

        Returns:
            Wrapped function.
        """
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                return func(*args, **kwargs)
            finally:
                duration = time.perf_counter() - start
                self.record(func.__name__, duration)

        return wrapper

    def get_stats(self) -> List[ProfileResult]:
        """Get all profiling statistics."""
        return sorted(
            self._stats.values(),
            key=lambda x: x.total_time,
            reverse=True,
        )

    def print_stats(self, top_n: int = 20) -> None:
        """Print profiling statistics."""
        stats = self.get_stats()[:top_n]

        print(f"{'Name':<30} {'Calls':>8} {'Total(s)':>12} {'Avg(s)':>12} {'Min(s)':>12} {'Max(s)':>12}")
        print("-" * 90)

        for stat in stats:
            print(
                f"{stat.name:<30} "
                f"{stat.call_count:>8} "
                f"{stat.total_time:>12.6f} "
                f"{stat.avg_time:>12.6f} "
                f"{stat.min_time:>12.6f} "
                f"{stat.max_time:>12.6f}"
            )

    def reset(self) -> None:
        """Reset all profiling data."""
        self._stats.clear()
        self._call_stack.clear()


def profile(func: Callable) -> Callable:
    """
    Decorator for profiling function execution.

    Args:
        func: Function to profile.

    Returns:
        Decorated function.
    """
    profiler = Profiler()
    return profiler.profile(func)


def timeit(
    iterations: int = 1,
    warmup: int = 0,
    log: bool = True,
) -> Callable:
    """
    Decorator for benchmarking function execution.

    Args:
        iterations: Number of iterations to run.
        warmup: Number of warmup iterations.
        log: If True, print timing results.

    Returns:
        Decorator function.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            for _ in range(warmup):
                func(*args, **kwargs)

            times: List[float] = []
            for _ in range(iterations):
                start = time.perf_counter()
                result = func(*args, **kwargs)
                times.append(time.perf_counter() - start)

            if log:
                avg = sum(times) / len(times)
                min_t = min(times)
                max_t = max(times)
                print(f"{func.__name__}: avg={avg:.6f}s min={min_t:.6f}s max={max_t:.6f}s over {iterations} runs")

            return result

        return wrapper
    return decorator


import math
