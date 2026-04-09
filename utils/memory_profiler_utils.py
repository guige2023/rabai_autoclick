"""
Memory profiler utilities for tracking memory usage in automation workflows.

Provides memory profiling, leak detection, and allocation tracking
for identifying memory issues in long-running automation processes.

Example:
    >>> from memory_profiler_utils import MemoryProfiler, track_allocations
    >>> profiler = MemoryProfiler()
    >>> profiler.start()
    >>> # run code
    >>> report = profiler.stop()
"""

from __future__ import annotations

import gc
import sys
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set


# =============================================================================
# Types
# =============================================================================


@dataclass
class MemorySnapshot:
    """A snapshot of memory usage at a point in time."""
    timestamp: float
    rss: int  # Resident Set Size in bytes
    vms: int  # Virtual Memory Size in bytes
    heap_used: int
    heap_free: int
    gc_counts: tuple  # (gen0, gen1, gen2)
    thread_count: int
    object_counts: Dict[str, int] = field(default_factory=dict)


@dataclass
class MemoryProfile:
    """Complete memory profile report."""
    start_time: float
    end_time: float
    duration: float
    snapshots: List[MemorySnapshot]
    peak_rss: int
    peak_heap: int
    leaks_detected: List[str]
    allocations: Dict[str, int]


class MemoryLeakDetector:
    """Detects potential memory leaks by tracking object growth."""

    def __init__(self, interval: float = 5.0):
        self.interval = interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._baseline: Dict[str, int] = {}
        self._snapshots: List[Dict[str, int]] = []
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start leak detection."""
        self._running = True
        self._thread = threading.Thread(target=self._track, daemon=True)
        self._thread.start()

    def stop(self) -> List[str]:
        """Stop leak detection and return detected leaks."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

        return self._detect_leaks()

    def _track(self) -> None:
        """Track object counts periodically."""
        gc.collect()
        self._baseline = self._get_object_counts()

        while self._running:
            time.sleep(self.interval)
            gc.collect()
            snapshot = self._get_object_counts()
            with self._lock:
                self._snapshots.append(snapshot)

    def _get_object_counts(self) -> Dict[str, int]:
        """Get counts of all reachable objects by type."""
        counts: Dict[str, int] = defaultdict(int)
        for obj in gc.get_objects():
            t = type(obj).__name__
            counts[t] += 1
        return dict(counts)

    def _detect_leaks(self) -> List[str]:
        """Detect objects with consistent growth patterns."""
        leaks: List[str] = []

        if len(self._snapshots) < 2:
            return leaks

        for obj_type in self._baseline:
            values = [s.get(obj_type, 0) for s in self._snapshots]
            baseline_val = self._baseline[obj_type]

            # Check if consistently growing
            if all(v >= baseline_val for v in values):
                growth_rate = (values[-1] - baseline_val) / max(baseline_val, 1)
                if growth_rate > 0.5:  # 50% growth threshold
                    leaks.append(
                        f"{obj_type}: {baseline_val} -> {values[-1]} "
                        f"(+{growth_rate * 100:.1f}%)"
                    )

        return leaks


# =============================================================================
# Memory Profiler
# =============================================================================


class MemoryProfiler:
    """
    Context manager for profiling memory usage of a code block.

    Example:
        >>> profiler = MemoryProfiler()
        >>> profiler.start()
        >>> # code to profile
        >>> report = profiler.stop()
        >>> print(report.peak_rss / 1024 / 1024, "MB peak")
    """

    def __init__(self, snapshot_interval: float = 1.0):
        self.snapshot_interval = snapshot_interval
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._snapshots: List[MemorySnapshot] = []
        self._start_time: float = 0.0
        self._leak_detector = MemoryLeakDetector(interval=snapshot_interval)
        self._allocations: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()

    def start(self) -> None:
        """Start profiling."""
        self._running = True
        self._start_time = time.monotonic()
        self._snapshots.clear()
        gc.collect()
        self._take_snapshot()
        self._leak_detector.start()
        self._thread = threading.Thread(target=self._track, daemon=True)
        self._thread.start()

    def stop(self) -> MemoryProfile:
        """Stop profiling and return results."""
        self._running = False
        if self._thread:
            self._thread.join(timeout=2.0)

        leaks = self._leak_detector.stop()
        gc.collect()
        self._take_snapshot()

        end_time = time.monotonic()
        duration = end_time - self._start_time

        peak_rss = max(s.rss for s in self._snapshots) if self._snapshots else 0
        peak_heap = max(s.heap_used for s in self._snapshots) if self._snapshots else 0

        return MemoryProfile(
            start_time=self._start_time,
            end_time=end_time,
            duration=duration,
            snapshots=self._snapshots,
            peak_rss=peak_rss,
            peak_heap=peak_heap,
            leaks_detected=leaks,
            allocations=dict(self._allocations),
        )

    def _track(self) -> None:
        """Periodically take memory snapshots."""
        while self._running:
            time.sleep(self.snapshot_interval)
            self._take_snapshot()

    def _take_snapshot(self) -> None:
        """Capture a memory snapshot."""
        try:
            import psutil
            process = psutil.Process()
            mem_info = process.memory_info()
            rss = mem_info.rss
            vms = mem_info.vms
        except ImportError:
            rss = vms = 0

        gc_counts = gc.get_count()
        thread_count = threading.active_count()

        # Heap info
        try:
            import tracemalloc
            if tracemalloc.is_tracing():
                current, peak = tracemalloc.get_traced_memory()
                heap_used, heap_free = current, peak - current
            else:
                heap_used = heap_free = 0
        except Exception:
            heap_used = heap_free = 0

        # Object counts for top types
        obj_counts: Dict[str, int] = {}
        try:
            gc.collect()
            type_counts: Dict[str, int] = defaultdict(int)
            for obj in gc.get_objects():
                type_counts[type(obj).__name__] += 1
            obj_counts = dict(sorted(
                type_counts.items(),
                key=lambda x: x[1],
                reverse=True
            )[:20])
        except Exception:
            pass

        snapshot = MemorySnapshot(
            timestamp=time.monotonic() - self._start_time,
            rss=rss,
            vms=vms,
            heap_used=heap_used,
            heap_free=heap_free,
            gc_counts=gc_counts,
            thread_count=thread_count,
            object_counts=obj_counts,
        )

        with self._lock:
            self._snapshots.append(snapshot)

    def __enter__(self) -> "MemoryProfiler":
        self.start()
        return self

    def __exit__(self, *args: Any) -> None:
        self.stop()


# =============================================================================
# Allocation Tracker
# =============================================================================


class AllocationTracker:
    """
    Track memory allocations by type or custom keys.

    Example:
        >>> tracker = AllocationTracker()
        >>> tracker.start()
        >>> x = [1] * 1000
        >>> print(tracker.get_allocations())
    """

    def __init__(self):
        self._original_new = None
        self._original_del = None
        self._allocations: Dict[str, int] = defaultdict(int)
        self._counts: Dict[str, int] = defaultdict(int)
        self._lock = threading.Lock()
        self._enabled = False

    def start(self) -> None:
        """Start tracking allocations."""
        self._enabled = True

    def stop(self) -> None:
        """Stop tracking allocations."""
        self._enabled = False

    def record_allocation(self, name: str, size: int) -> None:
        """Record an allocation."""
        if not self._enabled:
            return
        with self._lock:
            self._allocations[name] += size
            self._counts[name] += 1

    def record_deallocation(self, name: str, size: int) -> None:
        """Record a deallocation."""
        if not self._enabled:
            return
        with self._lock:
            self._allocations[name] -= size

    def get_allocations(self) -> Dict[str, int]:
        """Get current allocation sizes by name."""
        with self._lock:
            return dict(self._allocations)

    def get_counts(self) -> Dict[str, int]:
        """Get allocation counts by name."""
        with self._lock:
            return dict(self._counts)

    def get_net_size(self, name: str) -> int:
        """Get net memory size for a given allocation type."""
        with self._lock:
            return self._allocations.get(name, 0)


# =============================================================================
# Decorators
# =============================================================================


def profile_memory(func: Callable) -> Callable:
    """
    Decorator to profile memory usage of a function.

    Example:
        >>> @profile_memory
        >>> def my_function():
        ...     pass
    """
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        profiler = MemoryProfiler()
        profiler.start()
        try:
            result = func(*args, **kwargs)
        finally:
            report = profiler.stop()
            print(f"Memory: peak RSS={report.peak_rss / 1024 / 1024:.1f} MB")
        return result
    return wrapper


# =============================================================================
# Memory Utilities
# =============================================================================


def get_memory_usage() -> Dict[str, int]:
    """
    Get current memory usage statistics.

    Returns:
        Dict with rss, vms, heap_used, heap_free in bytes.
    """
    result: Dict[str, int] = {}

    try:
        import psutil
        process = psutil.Process()
        mem = process.memory_info()
        result["rss"] = mem.rss
        result["vms"] = mem.vms
    except ImportError:
        result["rss"] = result["vms"] = 0

    try:
        import tracemalloc
        if tracemalloc.is_tracing():
            current, peak = tracemalloc.get_traced_memory()
            result["heap_used"] = current
            result["heap_free"] = peak - current
        else:
            result["heap_used"] = result["heap_free"] = 0
    except Exception:
        result["heap_used"] = result["heap_free"] = 0

    return result


def force_garbage_collection() -> Dict[str, int]:
    """
    Force garbage collection and return pre/post collection counts.

    Returns:
        Dict with collected counts per generation.
    """
    before = gc.get_count()
    collected = gc.collect()
    after = gc.get_count()

    return {
        "collected_objects": collected,
        "before_gen0": before[0],
        "before_gen1": before[1],
        "before_gen2": before[2],
        "after_gen0": after[0],
        "after_gen1": after[1],
        "after_gen2": after[2],
    }


def get_object_count_by_type(top_n: int = 20) -> List[tuple]:
    """
    Get counts of objects by type.

    Args:
        top_n: Number of top types to return.

    Returns:
        List of (type_name, count) tuples sorted by count.
    """
    gc.collect()
    counts: Dict[str, int] = defaultdict(int)

    for obj in gc.get_objects():
        t = type(obj).__name__
        counts[t] += 1

    return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
