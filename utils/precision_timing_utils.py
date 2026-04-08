"""Precision timing utilities for high-resolution time measurement.

This module provides utilities for precise timing and performance
measurement in automation tasks.
"""

from __future__ import annotations

import time
import platform
from typing import Callable, Optional
from dataclasses import dataclass


IS_MACOS = platform.system() == "Darwin"


@dataclass
class TimingResult:
    """Result of a timing measurement."""
    elapsed_ns: int
    elapsed_ms: float
    elapsed_s: float
    
    def __str__(self) -> str:
        if self.elapsed_ms < 1.0:
            return f"{self.elapsed_ns}ns"
        elif self.elapsed_s < 1.0:
            return f"{self.elapsed_ms:.3f}ms"
        return f"{self.elapsed_s:.3f}s"


class PrecisionTimer:
    """High-precision timer using platform-specific methods."""
    
    def __init__(self):
        self._start_time: Optional[float] = None
        self._start_ns: Optional[int] = None
    
    def start(self) -> None:
        """Start the timer."""
        self._start_time = time.perf_counter()
        try:
            self._start_ns = time.perf_counter_ns()
        except AttributeError:
            self._start_ns = None
    
    def stop(self) -> TimingResult:
        """Stop the timer and return elapsed time.
        
        Returns:
            TimingResult with elapsed time in various units.
        
        Raises:
            RuntimeError: If timer was not started.
        """
        if self._start_time is None:
            raise RuntimeError("Timer was not started")
        
        elapsed = time.perf_counter() - self._start_time
        self._start_time = None
        
        elapsed_ns = int(elapsed * 1_000_000_000)
        elapsed_ms = elapsed * 1000
        elapsed_s = elapsed
        
        return TimingResult(
            elapsed_ns=elapsed_ns,
            elapsed_ms=elapsed_ms,
            elapsed_s=elapsed_s,
        )
    
    def elapsed(self) -> float:
        """Get elapsed time without stopping.
        
        Returns:
            Elapsed time in seconds.
        """
        if self._start_time is None:
            return 0.0
        return time.perf_counter() - self._start_time


def measure_ns(func: Callable[[], None]) -> int:
    """Measure execution time of a function in nanoseconds.
    
    Args:
        func: Function to measure.
    
    Returns:
        Execution time in nanoseconds.
    """
    start = time.perf_counter()
    func()
    return int((time.perf_counter() - start) * 1_000_000_000)


def measure_ms(func: Callable[[], None]) -> float:
    """Measure execution time of a function in milliseconds.
    
    Args:
        func: Function to measure.
    
    Returns:
        Execution time in milliseconds.
    """
    start = time.perf_counter()
    func()
    return (time.perf_counter() - start) * 1000


def wait_until(
    deadline: float,
    poll_interval: float = 0.001,
) -> bool:
    """Busy-wait until a deadline is reached.
    
    Args:
        deadline: Target time (from time.monotonic()).
        poll_interval: Polling interval in seconds.
    
    Returns:
        True when deadline is reached.
    """
    while time.monotonic() < deadline:
        time.sleep(poll_interval)
    return True


def sleep_until_ns(target_ns: int) -> None:
    """Sleep until a target time specified in nanoseconds.
    
    Args:
        target_ns: Target time in nanoseconds (from time.monotonic_ns()).
    """
    while time.monotonic_ns() < target_ns:
        pass


class RateCounter:
    """Counts events and calculates rate over time."""
    
    def __init__(self, window_size: float = 1.0):
        """Initialize rate counter.
        
        Args:
            window_size: Time window for rate calculation in seconds.
        """
        self.window_size = window_size
        self._events: list[float] = []
    
    def record(self) -> None:
        """Record an event at the current time."""
        self._events.append(time.monotonic())
        self._prune()
    
    def _prune(self) -> None:
        """Remove events outside the time window."""
        cutoff = time.monotonic() - self.window_size
        self._events = [t for t in self._events if t > cutoff]
    
    def rate(self) -> float:
        """Get the current event rate (events per second).
        
        Returns:
            Events per second.
        """
        self._prune()
        if not self._events:
            return 0.0
        return len(self._events) / self.window_size
    
    def count(self) -> int:
        """Get the number of events in the window."""
        self._prune()
        return len(self._events)


class Benchmark:
    """Utility for benchmarking function performance."""
    
    def __init__(
        self,
        name: str = "benchmark",
        warmup_runs: int = 3,
        measurement_runs: int = 10,
    ):
        self.name = name
        self.warmup_runs = warmup_runs
        self.measurement_runs = measurement_runs
        self._results: list[float] = []
    
    def run(self, func: Callable[[], None]) -> "Benchmark":
        """Run the benchmark.
        
        Args:
            func: Function to benchmark.
        
        Returns:
            Self for chaining.
        """
        # Warmup
        for _ in range(self.warmup_runs):
            measure_ms(func)
        
        # Measurement
        self._results = []
        for _ in range(self.measurement_runs):
            elapsed = measure_ms(func)
            self._results.append(elapsed)
        
        return self
    
    def report(self) -> str:
        """Get a formatted benchmark report.
        
        Returns:
            Formatted string with benchmark statistics.
        """
        if not self._results:
            return f"{self.name}: No data"
        
        import statistics
        mean = statistics.mean(self._results)
        median = statistics.median(self._results)
        stdev = statistics.stdev(self._results) if len(self._results) > 1 else 0
        
        return (
            f"{self.name}:\n"
            f"  mean:   {mean:.4f}ms\n"
            f"  median: {median:.4f}ms\n"
            f"  stdev:  {stdev:.4f}ms\n"
            f"  runs:   {len(self._results)}"
        )
