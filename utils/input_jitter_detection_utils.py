"""
Input Jitter Detection Utilities

Detect jitter in input event streams, which can indicate
hardware issues, driver problems, or virtual machine instability.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass
class JitterAnalysisResult:
    """Result of jitter analysis on an input stream."""
    is_jittery: bool
    jitter_score: float  # 0.0 (stable) to 1.0 (very jittery)
    mean_interval_ms: float
    interval_std_dev_ms: float
    max_jitter_ms: float
    flagged_indices: list[int]


class InputJitterDetector:
    """
    Detect jitter in the timing of input events.

    Jitter is defined as variation in inter-arrival times beyond
    what is expected for regular input. High jitter can cause
    automation to fail silently.
    """

    def __init__(
        self,
        jitter_threshold_ms: float = 10.0,
        window_size: int = 50,
        min_events: int = 10,
    ):
        self.jitter_threshold_ms = jitter_threshold_ms
        self.min_events = min_events
        self._timestamps: deque[float] = deque(maxlen=window_size)
        self._intervals: deque[float] = deque(maxlen=window_size - 1)

    def add_event(self, timestamp_ms: float) -> JitterAnalysisResult:
        """Add an event timestamp and analyze jitter."""
        if self._timestamps:
            interval = timestamp_ms - self._timestamps[-1]
            self._intervals.append(interval)

        self._timestamps.append(timestamp_ms)
        return self.analyze()

    def analyze(self) -> JitterAnalysisResult:
        """Analyze the current stream for jitter."""
        intervals = list(self._intervals)
        if len(intervals) < self.min_events:
            return JitterAnalysisResult(
                is_jittery=False,
                jitter_score=0.0,
                mean_interval_ms=0.0,
                interval_std_dev_ms=0.0,
                max_jitter_ms=0.0,
                flagged_indices=[],
            )

        mean_interval = sum(intervals) / len(intervals)
        variance = sum((i - mean_interval) ** 2 for i in intervals) / len(intervals)
        std_dev = math.sqrt(variance)

        flagged = []
        for i, interval in enumerate(intervals):
            deviation = abs(interval - mean_interval)
            if deviation > self.jitter_threshold_ms:
                flagged.append(i)

        # Normalize jitter score
        jitter_score = min(1.0, std_dev / (mean_interval * 0.2 + 1.0))
        is_jittery = jitter_score > 0.5 or len(flagged) > len(intervals) * 0.3

        return JitterAnalysisResult(
            is_jittery=is_jittery,
            jitter_score=jitter_score,
            mean_interval_ms=mean_interval,
            interval_std_dev_ms=std_dev,
            max_jitter_ms=max(abs(i - mean_interval) for i in intervals),
            flagged_indices=flagged,
        )

    def reset(self) -> None:
        """Reset detector state."""
        self._timestamps.clear()
        self._intervals.clear()
