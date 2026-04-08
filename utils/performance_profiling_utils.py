"""Performance profiling utilities for automation workflow analysis.

Provides decorators and context managers for profiling
action execution time, memory usage, and CPU cycles,
with flamegraph and summary report generation.

Example:
    >>> from utils.performance_profiling_utils import profile, Profiler
    >>> @profile
    ... def slow_action():
    ...     time.sleep(0.5)
    >>> Profiler.enable()
    >>> # ... run code ...
    >>> Profiler.disable().print_stats()
"""

from __future__ import annotations

import cProfile
import io
import os
import pstats
import sys
import threading
import time
from typing import Optional

__all__ = [
    "profile",
    "Profiler",
    "MemoryProfiler",
    "CPUProfiler",
    "get_performance_report",
]


# Module-level profiler state
_profiler: Optional[cProfile.Profile] = None
_profiler_enabled = False


def profile(func):
    """Decorator to profile a function with cProfile.

    Example:
        >>> @profile
        ... def my_function():
        ...     ...
    """
    def wrapper(*args, **kwargs):
        pr = cProfile.Profile()
        pr.enable()
        try:
            result = func(*args, **kwargs)
        finally:
            pr.disable()
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
        ps.print_stats(20)
        sys.stderr.write(s.getvalue())
        return result
    return wrapper


class Profiler:
    """Context manager for profiling code blocks.

    Example:
        >>> with Profiler() as p:
        ...     do_work()
        >>> p.print_stats()
    """

    _global_profiler: Optional[cProfile.Profile] = None
    _enabled = False
    _lock = threading.Lock()

    @classmethod
    def enable(cls) -> None:
        cls._global_profiler = cProfile.Profile()
        cls._global_profiler.enable()
        cls._enabled = True

    @classmethod
    def disable(cls) -> pstats.Stats:
        if cls._global_profiler is None:
            cls._global_profiler = cProfile.Profile()
        cls._global_profiler.disable()
        cls._enabled = False
        return pstats.Stats(cls._global_profiler)

    @classmethod
    def get_stats(cls, sort_by: str = "cumulative", top: int = 20) -> str:
        """Get profiler stats as a string."""
        stats = cls.disable()
        s = io.StringIO()
        stats.stream = s
        stats.sort_stats(sort_by)
        stats.print_stats(top)
        return s.getvalue()

    def __init__(self, enabled: bool = True):
        self.enabled = enabled
        self._profiler: Optional[cProfile.Profile] = None

    def __enter__(self) -> "Profiler":
        if self.enabled:
            self._profiler = cProfile.Profile()
            self._profiler.enable()
        return self

    def __exit__(self, *args) -> None:
        if self._profiler is not None:
            self._profiler.disable()

    def get_stats(self, sort_by: str = "cumulative", top: int = 20) -> str:
        if self._profiler is None:
            return ""
        s = io.StringIO()
        ps = pstats.Stats(self._profiler, stream=s).sort_stats(sort_by)
        ps.print_stats(top)
        return s.getvalue()


class MemoryProfiler:
    """Memory profiling context manager.

    Uses tracemalloc to measure memory allocations.

    Example:
        >>> with MemoryProfiler() as mp:
        ...     process_data()
        >>> print(f"Peak: {mp.peak_mb:.1f} MB")
    """

    def __init__(self):
        self._start_stats = None
        self._peak: float = 0.0
        self._enabled = False

    def __enter__(self) -> "MemoryProfiler":
        try:
            import tracemalloc

            tracemalloc.start()
            self._start_stats = tracemalloc.take_snapshot()
            self._enabled = True
        except ImportError:
            self._enabled = False
        return self

    def __exit__(self, *args) -> None:
        if not self._enabled:
            return

        try:
            import tracemalloc

            snapshot = tracemalloc.take_snapshot()
            self._peak = tracemalloc.get_traced_memory()[1]
            tracemalloc.stop()
        except ImportError:
            pass

    @property
    def peak_mb(self) -> float:
        return self._peak / (1024 * 1024)

    def compare(self) -> str:
        if self._start_stats is None:
            return ""

        try:
            import tracemalloc

            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.compare_to(self._start_stats, "lineno")
            lines = []
            for stat in top_stats[:10]:
                lines.append(str(stat))
            return "\n".join(lines)
        except ImportError:
            return ""


class CPUProfiler:
    """CPU time profiling using resource module.

    Example:
        >>> with CPUProfiler() as cp:
        ...     do_work()
        >>> print(f"CPU time: {cp.cpu_time:.2f}s")
    """

    def __init__(self):
        self._start_time: Optional[float] = None
        self._start_cpu: Optional[float] = None
        self.cpu_time: float = 0.0
        self.wall_time: float = 0.0

    def __enter__(self) -> "CPUProfiler":
        import resource

        self._start_time = time.time()
        usage = resource.getrusage(resource.RUSAGE_SELF)
        self._start_cpu = usage.ru_utime + usage.ru_stime
        return self

    def __exit__(self, *args) -> None:
        import resource

        self.wall_time = time.time() - (self._start_time or 0)
        usage = resource.getrusage(resource.RUSAGE_SELF)
        cpu_end = usage.ru_utime + usage.ru_stime
        self.cpu_time = cpu_end - (self._start_cpu or 0)


def get_performance_report(func: callable, *args, iterations: int = 10, **kwargs) -> dict:
    """Benchmark a function over multiple iterations.

    Args:
        func: Function to benchmark.
        *args: Positional arguments.
        iterations: Number of iterations.
        **kwargs: Keyword arguments.

    Returns:
        Dictionary with timing statistics.
    """
    timings: list[float] = []

    for _ in range(iterations):
        start = time.perf_counter()
        func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        timings.append(elapsed)

    timings.sort()
    return {
        "iterations": iterations,
        "min": min(timings),
        "max": max(timings),
        "mean": sum(timings) / len(timings),
        "median": timings[len(timings) // 2],
        "p95": timings[int(len(timings) * 0.95)] if len(timings) >= 20 else timings[-1],
        "p99": timings[int(len(timings) * 0.99)] if len(timings) >= 100 else timings[-1],
    }
