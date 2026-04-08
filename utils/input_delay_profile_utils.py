"""
Input delay profile utilities for modeling and compensating input latency.

Provides input delay profiling, modeling, and compensation
to improve automation timing accuracy.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class DelayProfile:
    """Profile of input delays."""
    name: str
    avg_delay_ms: float = 0.0
    std_dev_ms: float = 0.0
    min_delay_ms: float = 0.0
    max_delay_ms: float = 0.0
    percentile_95_ms: float = 0.0
    percentile_99_ms: float = 0.0


@dataclass
class DelaySample:
    """A single delay measurement."""
    expected_time_ms: float
    actual_time_ms: float
    delay_ms: float
    timestamp_ms: float


class InputDelayProfiler:
    """Profiles and models input delays."""

    def __init__(self, name: str = "default"):
        self.name = name
        self._samples: list[DelaySample] = []
        self._profile: Optional[DelayProfile] = None

    def record(self, expected_time_ms: float, actual_time_ms: float, timestamp_ms: float) -> None:
        """Record a delay measurement."""
        delay = actual_time_ms - expected_time_ms
        self._samples.append(DelaySample(
            expected_time_ms=expected_time_ms,
            actual_time_ms=actual_time_ms,
            delay_ms=delay,
            timestamp_ms=timestamp_ms,
        ))
        self._profile = None  # Invalidate cached profile

    def compute_profile(self) -> DelayProfile:
        """Compute the delay profile from samples."""
        if not self._samples:
            return DelayProfile(name=self.name)

        if self._profile:
            return self._profile

        delays = sorted([s.delay_ms for s in self._samples])

        n = len(delays)
        avg = sum(delays) / n
        variance = sum((d - avg) ** 2 for d in delays) / n
        std_dev = math.sqrt(variance)

        p95_idx = int(n * 0.95)
        p99_idx = int(n * 0.99)

        self._profile = DelayProfile(
            name=self.name,
            avg_delay_ms=avg,
            std_dev_ms=std_dev,
            min_delay_ms=delays[0],
            max_delay_ms=delays[-1],
            percentile_95_ms=delays[p95_idx] if p95_idx < n else delays[-1],
            percentile_99_ms=delays[p99_idx] if p99_idx < n else delays[-1],
        )

        return self._profile

    def expected_delay(self, confidence_level: float = 0.95) -> float:
        """Get expected delay at a confidence level."""
        profile = self.compute_profile()
        if confidence_level >= 0.99:
            return profile.percentile_99_ms
        elif confidence_level >= 0.95:
            return profile.percentile_95_ms
        else:
            return profile.avg_delay_ms

    def compensate_timing(
        self,
        target_time_ms: float,
        confidence_level: float = 0.95,
    ) -> float:
        """Compensate a target time using the delay profile.

        Returns an adjusted time that accounts for expected delays.
        """
        expected_delay = self.expected_delay(confidence_level)
        return target_time_ms + expected_delay

    def clear(self) -> None:
        """Clear all samples."""
        self._samples.clear()
        self._profile = None

    def sample_count(self) -> int:
        """Get the number of samples."""
        return len(self._samples)


class DelayCompensator:
    """Applies delay compensation to input timing."""

    def __init__(self, profiler: Optional[InputDelayProfiler] = None):
        self._profiler = profiler or InputDelayProfiler()

    def set_profiler(self, profiler: InputDelayProfiler) -> None:
        self._profiler = profiler

    def schedule_input(
        self,
        input_time_ms: float,
        confidence_level: float = 0.95,
    ) -> float:
        """Schedule an input to account for delays.

        Returns the adjusted time to send the input.
        """
        return self._profiler.compensate_timing(input_time_ms, confidence_level)

    def get_profile(self) -> DelayProfile:
        """Get the current delay profile."""
        return self._profiler.compute_profile()


__all__ = ["InputDelayProfiler", "DelayCompensator", "DelayProfile", "DelaySample"]
