"""
Input profiling and performance analysis utilities.

Tracks input latency, throughput, and patterns to help optimize
UI automation performance.

Author: Auto-generated
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Callable


class InputEventType(Enum):
    """Types of input events."""
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_DRAG = auto()
    MOUSE_SCROLL = auto()
    KEYBOARD_KEY = auto()
    KEYBOARD_COMBO = auto()
    TOUCH_TAP = auto()
    TOUCH_SWIPE = auto()
    TOUCH_PINCH = auto()


@dataclass
class InputEvent:
    """A single input event with timing information."""
    event_type: InputEventType
    timestamp: float
    x: float = 0
    y: float = 0
    key: str = ""
    duration_ms: float = 0
    metadata: dict = field(default_factory=dict)


@dataclass
class PerformanceMetrics:
    """Performance metrics for input operations."""
    total_events: int
    avg_latency_ms: float
    p50_latency_ms: float
    p95_latency_ms: float
    p99_latency_ms: float
    throughput_per_second: float
    metadata: dict = field(default_factory=dict)


@dataclass
class LatencySample:
    """A single latency measurement."""
    operation: str
    start_time: float
    end_time: float
    
    @property
    def latency_ms(self) -> float:
        return (self.end_time - self.start_time) * 1000


class InputProfiler:
    """
    Profiles input operations to track performance.
    
    Example:
        profiler = InputProfiler()
        with profiler.profile("click"):
            perform_click()
        metrics = profiler.get_metrics()
    """
    
    def __init__(self, max_samples: int = 10000):
        self._samples: deque[LatencySample] = deque(maxlen=max_samples)
        self._events: deque[InputEvent] = deque(maxlen=max_samples)
        self._operation_stack: list[tuple[str, float]] = []
        self._event_counts: dict[InputEventType, int] = {}
        self._active = True
    
    def profile(self, operation: str) -> Callable:
        """
        Context manager to profile an operation.
        
        Args:
            operation: Name of the operation
            
        Returns:
            Context manager for the operation
        """
        return _ProfileContext(self, operation)
    
    def start_operation(self, operation: str) -> None:
        """Start timing an operation."""
        if not self._active:
            return
        self._operation_stack.append((operation, time.perf_counter()))
    
    def end_operation(self, operation: str | None = None) -> float:
        """
        End timing the most recent operation.
        
        Args:
            operation: Optional operation name to validate
            
        Returns:
            Latency in milliseconds
        """
        if not self._active or not self._operation_stack:
            return 0.0
        
        op, start_time = self._operation_stack.pop()
        if operation is not None and op != operation:
            raise ValueError(f"Operation mismatch: expected {operation}, got {op}")
        
        end_time = time.perf_counter()
        latency_ms = (end_time - start_time) * 1000
        
        self._samples.append(LatencySample(op, start_time, end_time))
        
        return latency_ms
    
    def record_event(self, event: InputEvent) -> None:
        """Record an input event."""
        if not self._active:
            return
        self._events.append(event)
        self._event_counts[event.event_type] = \
            self._event_counts.get(event.event_type, 0) + 1
    
    def record_latency(self, operation: str, latency_ms: float) -> None:
        """
        Record a pre-measured latency.
        
        Args:
            operation: Name of the operation
            latency_ms: Latency in milliseconds
        """
        if not self._active:
            return
        now = time.perf_counter()
        start = now - (latency_ms / 1000)
        self._samples.append(LatencySample(operation, start, now))
    
    def get_metrics(self, operation: str | None = None) -> PerformanceMetrics:
        """
        Get performance metrics.
        
        Args:
            operation: Optional filter by operation name
            
        Returns:
            PerformanceMetrics with latency percentiles
        """
        samples = [
            s for s in self._samples
            if operation is None or s.operation == operation
        ]
        
        if not samples:
            return PerformanceMetrics(
                total_events=0,
                avg_latency_ms=0,
                p50_latency_ms=0,
                p95_latency_ms=0,
                p99_latency_ms=0,
                throughput_per_second=0,
            )
        
        latencies = sorted(s.latency_ms for s in samples)
        n = len(latencies)
        
        total_time = (
            samples[-1].end_time - samples[0].start_time
            if len(samples) > 1 else 1.0
        )
        
        return PerformanceMetrics(
            total_events=n,
            avg_latency_ms=sum(latencies) / n,
            p50_latency_ms=latencies[int(n * 0.50)],
            p95_latency_ms=latencies[int(n * 0.95)] if n >= 20 else latencies[-1],
            p99_latency_ms=latencies[int(n * 0.99)] if n >= 100 else latencies[-1],
            throughput_per_second=n / max(total_time, 1.0),
            metadata={
                "min_latency_ms": min(latencies),
                "max_latency_ms": max(latencies),
                "operation": operation,
            },
        )
    
    def get_event_distribution(self) -> dict[InputEventType, int]:
        """Get count of events by type."""
        return dict(self._event_counts)
    
    def get_recent_events(
        self, event_type: InputEventType | None = None, limit: int = 100
    ) -> list[InputEvent]:
        """
        Get recent input events.
        
        Args:
            event_type: Optional filter by event type
            limit: Maximum number of events to return
            
        Returns:
            List of recent InputEvents
        """
        events = list(self._events)
        if event_type is not None:
            events = [e for e in events if e.event_type == event_type]
        return events[-limit:]
    
    def reset(self) -> None:
        """Clear all profiling data."""
        self._samples.clear()
        self._events.clear()
        self._operation_stack.clear()
        self._event_counts.clear()
    
    def pause(self) -> None:
        """Pause profiling."""
        self._active = False
    
    def resume(self) -> None:
        """Resume profiling."""
        self._active = True


class _ProfileContext:
    """Internal context manager for profiling."""
    
    def __init__(self, profiler: InputProfiler, operation: str):
        self._profiler = profiler
        self._operation = operation
    
    def __enter__(self) -> None:
        self._profiler.start_operation(self._operation)
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._profiler.end_operation(self._operation)


class RollingPerformanceWindow:
    """
    Rolling window for tracking performance over time.
    
    Maintains a fixed-size window of latency samples and
    computes rolling metrics.
    """
    
    def __init__(self, window_size: int = 1000):
        self._window_size = window_size
        self._samples: deque[LatencySample] = deque(maxlen=window_size)
        self._window_start: float | None = None
    
    def add(self, sample: LatencySample) -> None:
        """Add a latency sample."""
        if self._window_start is None:
            self._window_start = sample.start_time
        self._samples.append(sample)
    
    def add_operation(self, operation: str, latency_ms: float) -> None:
        """Add an operation with pre-measured latency."""
        now = time.perf_counter()
        start = now - (latency_ms / 1000)
        self.add(LatencySample(operation, start, now))
    
    def get_metrics(self) -> PerformanceMetrics:
        """Get rolling performance metrics."""
        if not self._samples:
            return PerformanceMetrics(
                total_events=0, avg_latency_ms=0,
                p50_latency_ms=0, p95_latency_ms=0, p99_latency_ms=0,
                throughput_per_second=0,
            )
        
        latencies = sorted(s.latency_ms for s in self._samples)
        n = len(latencies)
        
        window_duration = (
            self._samples[-1].start_time - self._window_start
            if self._window_start and len(self._samples) > 1 else 1.0
        )
        
        return PerformanceMetrics(
            total_events=n,
            avg_latency_ms=sum(latencies) / n,
            p50_latency_ms=latencies[n // 2],
            p95_latency_ms=latencies[int(n * 0.95)] if n >= 20 else latencies[-1],
            p99_latency_ms=latencies[int(n * 0.99)] if n >= 100 else latencies[-1],
            throughput_per_second=n / max(window_duration, 1.0),
        )
    
    def reset(self) -> None:
        """Clear the window."""
        self._samples.clear()
        self._window_start = None


# Global profiler instance
_default_profiler: InputProfiler | None = None


def get_global_profiler() -> InputProfiler:
    """Get the global profiler instance."""
    global _default_profiler
    if _default_profiler is None:
        _default_profiler = InputProfiler()
    return _default_profiler


def profile(operation: str) -> Callable:
    """Decorator/context to profile an operation."""
    return get_global_profiler().profile(operation)
