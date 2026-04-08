"""
Heap/memory monitoring utilities.

Provides heap profiling, memory usage tracking,
and garbage collection helpers.
"""

from __future__ import annotations

import gc
import threading
import time
import tracemalloc
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class MemorySnapshot:
    """Point-in-time memory measurement."""
    timestamp: float
    rss_mb: float
    vms_mb: float
    python_allocated_mb: float | None = None
    python_peak_mb: float | None = None


class MemoryMonitor:
    """
    Monitor memory usage over time.

    Tracks RSS, VMS, and optionally Python heap.
    """

    def __init__(self, interval: float = 1.0, track_python: bool = False):
        self.interval = interval
        self.track_python = track_python
        self._snapshots: list[MemorySnapshot] = []
        self._running = False
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()

        if self.track_python:
            tracemalloc.start()

    def _get_memory_usage(self) -> MemorySnapshot:
        import resource
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if hasattr(resource, "RUSAGE_SELF"):
            rss_kb = rss
        else:
            rss_kb = rss / 1024

        python_allocated = tracemalloc.get_traced_memory()[0] if self.track_python else 0
        python_peak = tracemalloc.get_traced_memory()[1] if self.track_python else 0

        return MemorySnapshot(
            timestamp=time.time(),
            rss_mb=rss_kb / 1024,
            vms_mb=0,
            python_allocated_mb=python_allocated / (1024 * 1024),
            python_peak_mb=python_peak / (1024 * 1024),
        )

    def _monitor_loop(self) -> None:
        while self._running:
            snapshot = self._get_memory_usage()
            with self._lock:
                self._snapshots.append(snapshot)
            time.sleep(self.interval)

    def start(self) -> None:
        """Start monitoring."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Stop monitoring."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def get_snapshots(self, count: int | None = None) -> list[MemorySnapshot]:
        """Get recent snapshots."""
        with self._lock:
            if count is None:
                return list(self._snapshots)
            return list(self._snapshots[-count:])

    @property
    def current_rss_mb(self) -> float:
        return self._get_memory_usage().rss_mb

    @property
    def peak_rss_mb(self) -> float:
        with self._lock:
            if not self._snapshots:
                return 0.0
            return max(s.rss_mb for s in self._snapshots)

    def get_stats(self) -> dict:
        """Get memory statistics."""
        with self._lock:
            if not self._snapshots:
                return {}
            return {
                "current_mb": self._snapshots[-1].rss_mb,
                "peak_mb": max(s.rss_mb for s in self._snapshots),
                "avg_mb": sum(s.rss_mb for s in self._snapshots) / len(self._snapshots),
                "samples": len(self._snapshots),
            }


def get_memory_usage_mb() -> float:
    """Get current RSS memory usage in MB."""
    import resource
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if hasattr(resource, "RUSAGE_SELF"):
        return rss / 1024
    return rss / (1024 * 1024)


def get_object_counts() -> dict[str, int]:
    """Get counts of objects by type."""
    counts: dict[str, int] = {}
    for obj in gc.get_objects():
        name = type(obj).__name__
        counts[name] = counts.get(name, 0) + 1
    return counts


def get_gc_stats() -> dict:
    """Get garbage collection statistics."""
    gc_stats = gc.get_stats()
    thresholds = gc.get_threshold()
    return {
        "collections": gc.get_count(),
        "thresholds": thresholds,
        "enabled": gc.isenabled(),
        "stats": gc_stats,
    }


def force_garbage_collection() -> dict:
    """Force garbage collection and return stats."""
    before = get_object_counts()
    collected = gc.collect()
    after = get_object_counts()
    freed = {k: before.get(k, 0) - after.get(k, 0) for k in set(before)}
    return {
        "objects_collected": collected,
        "freed_by_type": {k: v for k, v in freed.items() if v > 0},
    }


class MemoryThresholdWatcher:
    """
    Watch memory usage and trigger callbacks on threshold.

    Useful for logging, alerting, or triggering cleanup
    when memory usage exceeds limits.
    """

    def __init__(
        self,
        threshold_mb: float,
        on_exceed: Callable[[], None] | None = None,
        check_interval: float = 5.0,
    ):
        self.threshold_mb = threshold_mb
        self.on_exceed = on_exceed
        self.check_interval = check_interval
        self._running = False
        self._thread: threading.Thread | None = None
        self._triggered = False

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._watch_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._running = False
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None

    def _watch_loop(self) -> None:
        while self._running:
            usage = get_memory_usage_mb()
            if usage > self.threshold_mb and not self._triggered:
                self._triggered = True
                if self.on_exceed:
                    self.on_exceed()
            elif usage < self.threshold_mb * 0.8:
                self._triggered = False
            time.sleep(self.check_interval)

    @property
    def is_triggered(self) -> bool:
        return self._triggered
