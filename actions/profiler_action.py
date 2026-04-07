"""profiler action module for rabai_autoclick.

Provides performance profiling utilities: CPU profiling, memory profiling,
timing decorators, call tracing, and performance metrics collection.
"""

from __future__ import annotations

import functools
import gc
import os
import sys
import threading
import time
import tracemalloc
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple
from contextlib import contextmanager

__all__ = [
    "ProfileResult",
    "TimingStats",
    "MemorySnapshot",
    "Profiler",
    "Timer",
    "profile",
    "timeit",
    "memit",
    "trace_calls",
    "get_memory_usage",
    "get_cpu_usage",
    "timing_decorator",
    "memory_decorator",
    "ProfileMode",
]


class ProfileMode(Enum):
    """Profiling modes."""
    CPU = auto()
    MEMORY = auto()
    WALL = auto()
    ALL = auto()


@dataclass
class TimingStats:
    """Timing statistics for a measured operation."""
    name: str
    calls: int = 0
    total_time: float = 0.0
    min_time: float = float("inf")
    max_time: float = 0.0
    mean_time: float = 0.0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def record(self, elapsed: float) -> None:
        """Record a timing measurement."""
        with self._lock:
            self.calls += 1
            self.total_time += elapsed
            self.min_time = min(self.min_time, elapsed)
            self.max_time = max(self.max_time, elapsed)
            if self.calls > 0:
                self.mean_time = self.total_time / self.calls

    def reset(self) -> None:
        """Reset all statistics."""
        with self._lock:
            self.calls = 0
            self.total_time = 0.0
            self.min_time = float("inf")
            self.max_time = 0.0
            self.mean_time = 0.0

    def report(self) -> dict:
        """Return statistics as dict."""
        with self._lock:
            return {
                "name": self.name,
                "calls": self.calls,
                "total_time": self.total_time,
                "min_time": self.min_time if self.min_time != float("inf") else 0.0,
                "max_time": self.max_time,
                "mean_time": self.mean_time,
            }


@dataclass
class MemorySnapshot:
    """Memory usage snapshot."""
    timestamp: float
    rss: int = 0
    vms: int = 0
    user: float = 0.0
    system: float = 0.0
    peak_rss: int = 0
    available: int = 0

    @classmethod
    def capture(cls) -> "MemorySnapshot":
        """Capture current memory usage."""
        import resource
        rusage = resource.getrusage(resource.RUSAGE_SELF)
        mem = cls(
            timestamp=time.time(),
            rss=get_rss(),
            vms=get_vms(),
            user=rusage.ru_utime,
            system=rusage.ru_stime,
            peak_rss=rusage.ru_maxrss * 1024,
            available=get_available_memory(),
        )
        return mem


@dataclass
class ProfileResult:
    """Result of a profiling session."""
    function_name: str
    calls: int
    total_time: float
    per_call: float
    cumulative_time: float
    file_name: str
    line_number: int
    primitive_calls: int = 0


class Profiler:
    """Context-aware profiler with multiple modes."""

    def __init__(
        self,
        mode: ProfileMode = ProfileMode.ALL,
        interval: float = 0.001,
    ) -> None:
        self.mode = mode
        self.interval = interval
        self._timings: Dict[str, TimingStats] = defaultdict(lambda: TimingStats(name=""))
        self._call_counts: Dict[str, int] = defaultdict(int)
        self._memory_snapshots: deque = deque(maxlen=1000)
        self._running = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start profiling."""
        self._running = True
        if self.mode in (ProfileMode.MEMORY, ProfileMode.ALL):
            tracemalloc.start()

    def stop(self) -> None:
        """Stop profiling."""
        self._running = False
        if tracemalloc.is_tracing():
            tracemalloc.stop()

    @contextmanager
    def profile(self):
        """Context manager for profiling a block."""
        self.start()
        try:
            yield self
        finally:
            self.stop()

    def time(self, name: str) -> Callable[[Callable], Callable]:
        """Decorator to time a function."""
        def decorator(fn: Callable) -> Callable:
            stats = self._timings[name]

            @functools.wraps(fn)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                start = time.perf_counter()
                try:
                    return fn(*args, **kwargs)
                finally:
                    elapsed = time.perf_counter() - start
                    stats.record(elapsed)

            wrapper._stats = stats
            return wrapper
        return decorator

    def timing(self, name: str, func: Callable, *args: Any, **kwargs: Any) -> Any:
        """Time a single function call."""
        stats = self._timings[name]
        start = time.perf_counter()
        try:
            return func(*args, **kwargs)
        finally:
            elapsed = time.perf_counter() - start
            stats.record(elapsed)

    def get_timings(self) -> Dict[str, dict]:
        """Get all timing statistics."""
        return {name: stats.report() for name, stats in self._timings.items()}

    def reset(self) -> None:
        """Reset all profiling data."""
        with self._lock:
            self._timings.clear()
            self._call_counts.clear()
            self._memory_snapshots.clear()


class Timer:
    """High-precision timer context manager."""

    def __init__(self, name: str = "timer") -> None:
        self.name = name
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
        self.elapsed: float = 0.0

    def __enter__(self) -> "Timer":
        self.start_time = time.perf_counter()
        return self

    def __exit__(self, *args: Any) -> None:
        self.end_time = time.perf_counter()
        if self.start_time is not None:
            self.elapsed = self.end_time - self.start_time

    def reset(self) -> None:
        """Reset the timer."""
        self.start_time = None
        self.end_time = None
        self.elapsed = 0.0

    @property
    def running(self) -> bool:
        """Check if timer is currently running."""
        return self.start_time is not None and self.end_time is None


def profile(fn: Optional[Callable] = None, mode: ProfileMode = ProfileMode.WALL) -> Callable:
    """Decorator to profile a function.

    Args:
        fn: Function to profile.
        mode: Profiling mode.

    Returns:
        Decorated function or decorator.
    """
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            profiler = Profiler(mode=mode)
            profiler.start()
            try:
                return f(*args, **kwargs)
            finally:
                profiler.stop()
                timings = profiler.get_timings()
                if timings:
                    for name, stats in timings.items():
                        print(f"[Profile] {name}: {stats['total_time']:.4f}s")
        return wrapper

    if fn is None:
        return decorator
    return decorator(fn)


def timeit(fn: Optional[Callable] = None, iterations: int = 1) -> Callable:
    """Decorator to measure execution time.

    Args:
        fn: Function to time.
        iterations: Number of iterations to average.

    Returns:
        Decorated function or decorator.
    """
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            times = []
            for _ in range(iterations):
                start = time.perf_counter()
                result = f(*args, **kwargs)
                elapsed = time.perf_counter() - start
                times.append(elapsed)
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            print(f"[timeit] {f.__name__}: avg={avg_time:.6f}s min={min_time:.6f}s max={max_time:.6f}s")
            return result
        return wrapper

    if fn is None:
        return decorator
    return decorator(fn)


def memit(fn: Optional[Callable] = None) -> Callable:
    """Decorator to measure memory usage.

    Args:
        fn: Function to measure.

    Returns:
        Decorated function or decorator.
    """
    def decorator(f: Callable) -> Callable:
        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            gc.collect()
            tracemalloc.start()
            snapshot_before = tracemalloc.take_snapshot()
            result = f(*args, **kwargs)
            snapshot_after = tracemalloc.take_snapshot()
            tracemalloc.stop()
            top_stats = snapshot_after.compare_to(snapshot_before, "lineno")
            total_diff = sum(stat.size_diff for stat in top_stats)
            print(f"[memit] {f.__name__}: {total_diff / 1024 / 1024:.2f} MB")
            return result
        return wrapper

    if fn is None:
        return decorator
    return decorator(fn)


class trace_calls:
    """Context manager / decorator to trace function calls."""

    def __init__(self, stream: Optional[Any] = None, pattern: str = "*") -> None:
        self.stream = stream or sys.stdout
        self.pattern = pattern
        self._trace_func: Optional[Callable] = None
        self._old_trace: Optional[Callable] = None

    def _trace(self, frame: Any, event: str, arg: Any) -> Optional[Callable]:
        """Trace function events."""
        if event not in ("call", "return"):
            return self._trace
        code = frame.f_code
        name = code.co_name
        if self.pattern != "*" and not self._matches(name):
            return self._trace
        lineno = frame.f_lineno
        filename = code.co_filename
        if event == "call":
            print(f">>> {name}() at {filename}:{lineno}", file=self.stream)
        else:
            print(f"<<< {name}() returned", file=self.stream)
        return self._trace

    def _matches(self, name: str) -> bool:
        """Check if name matches pattern."""
        import fnmatch
        return fnmatch.fnmatch(name, self.pattern)

    def __enter__(self) -> "trace_calls":
        self._old_trace = sys.gettrace()
        sys.settrace(self._trace)
        return self

    def __exit__(self, *args: Any) -> None:
        sys.settrace(self._old_trace)

    def __call__(self, fn: Callable) -> Callable:
        """Use as decorator."""
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with self:
                return fn(*args, **kwargs)
        return wrapper


def get_memory_usage() -> Dict[str, int]:
    """Get current process memory usage in bytes."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        mem = process.memory_info()
        return {
            "rss": mem.rss,
            "vms": mem.vms,
            "shared": getattr(mem, "shared", 0),
            "data": getattr(mem, "data", 0),
        }
    except ImportError:
        return {"rss": get_rss(), "vms": get_vms()}


def get_cpu_usage() -> Dict[str, float]:
    """Get CPU usage statistics."""
    try:
        import psutil
        process = psutil.Process(os.getpid())
        return {
            "percent": process.cpu_percent(interval=0.1),
            "user": process.cpu_times().user,
            "system": process.cpu_times().system,
        }
    except ImportError:
        return {"percent": 0.0, "user": 0.0, "system": 0.0}


def get_rss() -> int:
    """Get resident set size in bytes."""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    except Exception:
        return 0


def get_vms() -> int:
    """Get virtual memory size in bytes."""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * 1024
    except Exception:
        return 0


def get_available_memory() -> int:
    """Get available system memory in bytes."""
    try:
        import psutil
        return psutil.virtual_memory().available
    except ImportError:
        return 0


def timing_decorator(name: Optional[str] = None) -> Callable:
    """Create a timing decorator for a specific name."""
    def decorator(fn: Callable) -> Callable:
        func_name = name or fn.__name__
        stats = TimingStats(name=func_name)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            start = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                elapsed = time.perf_counter() - start
                stats.record(elapsed)

        wrapper._timing_stats = stats
        return wrapper
    return decorator


def memory_decorator(name: Optional[str] = None) -> Callable:
    """Create a memory profiling decorator."""
    def decorator(fn: Callable) -> Callable:
        func_name = name or fn.__name__

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            gc.collect()
            tracemalloc.start()
            result = fn(*args, **kwargs)
            snapshot = tracemalloc.take_snapshot()
            tracemalloc.stop()
            print(f"[memory] {func_name}: {snapshot}")
            return result

        return wrapper
    return decorator
