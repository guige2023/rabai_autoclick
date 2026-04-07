"""Profiler and observability utilities: CPU/memory profiling, performance monitoring."""

from __future__ import annotations

import gc
import sys
import threading
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable

__all__ = [
    "ProfileStats",
    "FunctionProfiler",
    "MemoryProfiler",
    "LineProfiler",
    "profile",
]


@dataclass
class ProfileStats:
    """Profiling statistics for a function."""
    name: str
    calls: int = 0
    total_time_ms: float = 0.0
    min_time_ms: float = float("inf")
    max_time_ms: float = 0.0
    avg_time_ms: float = 0.0
    per_call_times: list[float] = field(default_factory=list)

    def add_call(self, elapsed_ms: float) -> None:
        self.calls += 1
        self.total_time_ms += elapsed_ms
        self.min_time_ms = min(self.min_time_ms, elapsed_ms)
        self.max_time_ms = max(self.max_time_ms, elapsed_ms)
        self.avg_time_ms = self.total_time_ms / self.calls
        if len(self.per_call_times) < 1000:
            self.per_call_times.append(elapsed_ms)


class FunctionProfiler:
    """Profiler that tracks function call statistics."""

    def __init__(self) -> None:
        self._stats: dict[str, ProfileStats] = {}
        self._lock = threading.Lock()
        self._enabled = True

    def record(self, name: str, elapsed_ms: float) -> None:
        if not self._enabled:
            return
        with self._lock:
            if name not in self._stats:
                self._stats[name] = ProfileStats(name=name)
            self._stats[name].add_call(elapsed_ms)

    def get_stats(self, name: str) -> ProfileStats | None:
        return self._stats.get(name)

    def report(self) -> list[ProfileStats]:
        with self._lock:
            return sorted(
                self._stats.values(),
                key=lambda s: s.total_time_ms,
                reverse=True,
            )

    def summary(self) -> dict[str, Any]:
        stats = self.report()
        return {
            "total_functions": len(stats),
            "total_calls": sum(s.calls for s in stats),
            "total_time_ms": sum(s.total_time_ms for s in stats),
            "top_functions": [
                {"name": s.name, "calls": s.calls, "total_ms": s.total_time_ms, "avg_ms": s.avg_time_ms}
                for s in stats[:10]
            ],
        }

    def disable(self) -> None:
        self._enabled = False

    def enable(self) -> None:
        self._enabled = True

    def reset(self) -> None:
        with self._lock:
            self._stats.clear()


_global_profiler = FunctionProfiler()


def profile(fn: Callable[..., T]) -> Callable[..., T]:
    """Decorator to profile a function."""
    name = f"{fn.__module__}.{fn.__qualname__}"

    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            elapsed = (time.perf_counter() - start) * 1000
            _global_profiler.record(name, elapsed)

    return wrapper  # type: ignore


class MemoryProfiler:
    """Memory profiling utility."""

    @staticmethod
    def snapshot() -> dict[str, Any]:
        import psutil
        process = psutil.Process()
        mem = process.memory_info()
        return {
            "rss_mb": mem.rss / 1024 / 1024,
            "vms_mb": mem.vms / 1024 / 1024,
            "percent": process.memory_percent(),
            "gc_stats": {
                f"gen_{i}": len(gc.get_count()[i]) for i in range(3)
            },
        }

    @staticmethod
    def diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
        return {
            "rss_delta_mb": after["rss_mb"] - before["rss_mb"],
            "vms_delta_mb": after["vms_mb"] - before["vms_mb"],
        }

    @staticmethod
    def top_objects(limit: int = 10) -> list[tuple[str, int]]:
        gc.collect()
        objs = gc.get_objects()
        type_counts: dict[str, int] = defaultdict(int)
        for obj in objs:
            t = type(obj).__name__
            type_counts[t] += 1
        return sorted(type_counts.items(), key=lambda x: -x[1])[:limit]


class LineProfiler:
    """Line-by-line profiler using sys.settrace."""

    def __init__(self) -> None:
        self._line_times: dict[str, float] = defaultdict(float)
        self._line_counts: dict[str, int] = defaultdict(int)
        self._enabled = False
        self._local = threading.local()

    def _trace(self, frame: Any, event: str, arg: Any) -> Any:
        if event == "line":
            code = frame.f_code
            lineno = frame.f_lineno
            key = f"{code.co_filename}:{lineno}"
            self._line_times[key] += 1
        return self._trace

    def start(self) -> None:
        self._enabled = True
        sys.settrace(self._trace)

    def stop(self) -> None:
        self._enabled = False
        sys.settrace(None)

    def report(self, limit: int = 20) -> list[tuple[str, float]]:
        sorted_lines = sorted(
            self._line_times.items(),
            key=lambda x: -x[1],
        )
        return sorted_lines[:limit]
