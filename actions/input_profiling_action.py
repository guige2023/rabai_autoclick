"""Input profiling action for UI automation.

Profiles input latency and performance:
- Input latency measurement
- Touch/mouse event timing
- Frame rate monitoring
- Performance profiling
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class LatencySample:
    """Single latency measurement."""
    timestamp: float
    input_type: str  # "touch", "mouse", "keyboard"
    action: str  # "down", "up", "move", "click", "type"
    latency_ms: float
    success: bool = True
    metadata: dict = field(default_factory=dict)


@dataclass
class PerformanceProfile:
    """Aggregated performance profile."""
    total_samples: int
    avg_latency_ms: float
    p50_latency_ms: float
    p90_latency_ms: float
    p99_latency_ms: float
    min_latency_ms: float
    max_latency_ms: float
    success_rate: float
    samples_per_minute: float


class InputProfiler:
    """Profiles input performance for UI automation.

    Features:
    - Input latency tracking
    - Performance statistics
    - Real-time monitoring
    - Bottleneck detection
    """

    def __init__(
        self,
        window_size: int = 1000,
        profile_interval: float = 60.0,
    ):
        self.window_size = window_size
        self.profile_interval = profile_interval
        self._samples: deque[LatencySample] = deque(maxlen=window_size)
        self._callbacks: list[Callable[[PerformanceProfile], None]] = []
        self._is_profiling = False
        self._start_time: float = 0.0
        self._record_func: Callable | None = None

    def set_record_func(self, func: Callable) -> None:
        """Set function to record performance data.

        Args:
            func: Function(action, latency_ms, success) -> None
        """
        self._record_func = func

    def start(self) -> None:
        """Start profiling."""
        self._is_profiling = True
        self._start_time = time.time()
        self._samples.clear()

    def stop(self) -> None:
        """Stop profiling."""
        self._is_profiling = False

    @property
    def is_profiling(self) -> bool:
        return self._is_profiling

    def record(
        self,
        input_type: str,
        action: str,
        latency_ms: float,
        success: bool = True,
        metadata: dict | None = None,
    ) -> None:
        """Record an input latency sample.

        Args:
            input_type: Type of input ("touch", "mouse", "keyboard")
            action: Action type
            latency_ms: Latency in milliseconds
            success: Whether action succeeded
            metadata: Additional metadata
        """
        sample = LatencySample(
            timestamp=time.time(),
            input_type=input_type,
            action=action,
            latency_ms=latency_ms,
            success=success,
            metadata=metadata or {},
        )
        self._samples.append(sample)

        if self._record_func:
            self._record_func(action, latency_ms, success)

    def record_touch(self, action: str, latency_ms: float, success: bool = True) -> None:
        """Record touch input."""
        self.record("touch", action, latency_ms, success)

    def record_mouse(self, action: str, latency_ms: float, success: bool = True) -> None:
        """Record mouse input."""
        self.record("mouse", action, latency_ms, success)

    def record_keyboard(self, action: str, latency_ms: float, success: bool = True) -> None:
        """Record keyboard input."""
        self.record("keyboard", action, latency_ms, success)

    def get_profile(self) -> PerformanceProfile:
        """Get current performance profile.

        Returns:
            Aggregated performance statistics
        """
        if not self._samples:
            return PerformanceProfile(
                total_samples=0,
                avg_latency_ms=0.0,
                p50_latency_ms=0.0,
                p90_latency_ms=0.0,
                p99_latency_ms=0.0,
                min_latency_ms=0.0,
                max_latency_ms=0.0,
                success_rate=1.0,
                samples_per_minute=0.0,
            )

        latencies = sorted(s.latency_ms for s in self._samples if s.success)
        total = len(self._samples)
        successes = sum(1 for s in self._samples if s.success)

        # Calculate percentiles
        def percentile(data: list, p: float) -> float:
            if not data:
                return 0.0
            idx = int(len(data) * p)
            idx = min(idx, len(data) - 1)
            return data[idx]

        p50 = percentile(latencies, 0.50)
        p90 = percentile(latencies, 0.90)
        p99 = percentile(latencies, 0.99)

        # Samples per minute
        duration = time.time() - self._start_time
        spm = (total / duration * 60) if duration > 0 else 0.0

        return PerformanceProfile(
            total_samples=total,
            avg_latency_ms=sum(latencies) / len(latencies) if latencies else 0.0,
            p50_latency_ms=p50,
            p90_latency_ms=p90,
            p99_latency_ms=p99,
            min_latency_ms=latencies[0] if latencies else 0.0,
            max_latency_ms=latencies[-1] if latencies else 0.0,
            success_rate=successes / total if total > 0 else 1.0,
            samples_per_minute=spm,
        )

    def get_profile_by_type(self, input_type: str) -> PerformanceProfile:
        """Get performance profile for specific input type.

        Args:
            input_type: Type to filter by

        Returns:
            Performance profile for type
        """
        type_samples = [s for s in self._samples if s.input_type == input_type]
        if not type_samples:
            return PerformanceProfile(
                total_samples=0,
                avg_latency_ms=0.0,
                p50_latency_ms=0.0,
                p90_latency_ms=0.0,
                p99_latency_ms=0.0,
                min_latency_ms=0.0,
                max_latency_ms=0.0,
                success_rate=1.0,
                samples_per_minute=0.0,
            )

        # Temporarily set samples
        old_samples = self._samples
        self._samples = deque(type_samples, maxlen=self.window_size)
        profile = self.get_profile()
        self._samples = old_samples
        return profile

    def detect_slow_inputs(self, threshold_ms: float = 100.0) -> list[LatencySample]:
        """Detect inputs exceeding threshold.

        Args:
            threshold_ms: Latency threshold

        Returns:
            List of slow samples
        """
        return [s for s in self._samples if s.latency_ms > threshold_ms]

    def get_latency_trend(self, bucket_count: int = 10) -> list[tuple[float, float]]:
        """Get latency trend over time.

        Args:
            bucket_count: Number of time buckets

        Returns:
            List of (timestamp, avg_latency) tuples
        """
        if not self._samples:
            return []

        samples = sorted(self._samples, key=lambda s: s.timestamp)
        duration = samples[-1].timestamp - samples[0].timestamp
        bucket_size = duration / bucket_count if bucket_count > 0 else 1.0

        trends = []
        for i in range(bucket_count):
            start = samples[0].timestamp + i * bucket_size
            end = start + bucket_size
            bucket_samples = [
                s for s in samples
                if start <= s.timestamp < end
            ]
            avg = sum(s.latency_ms for s in bucket_samples) / len(bucket_samples) if bucket_samples else 0.0
            trends.append((start, avg))

        return trends

    def register_callback(self, callback: Callable[[PerformanceProfile], None]) -> None:
        """Register for profile updates.

        Args:
            callback: Function(PerformanceProfile) to call periodically
        """
        self._callbacks.append(callback)

    def clear(self) -> None:
        """Clear all samples."""
        self._samples.clear()
        self._start_time = time.time()

    def __repr__(self) -> str:
        p = self.get_profile()
        return (f"InputProfiler(samples={p.total_samples}, "
                f"avg={p.avg_latency_ms:.1f}ms, "
                f"p90={p.p90_latency_ms:.1f}ms)")


def create_input_profiler(window_size: int = 1000) -> InputProfiler:
    """Create input profiler."""
    return InputProfiler(window_size)
