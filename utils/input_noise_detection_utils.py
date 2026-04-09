"""
Input Noise Detection Utilities

Detect and filter noisy or spurious input events caused by
system jitter, input device issues, or external interference.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass
class NoiseDetectionResult:
    """Result of noise detection analysis."""
    is_noisy: bool
    noise_score: float  # 0.0 (clean) to 1.0 (very noisy)
    confidence: float
    flagged_indices: list[int]


class InputNoiseDetector:
    """Detect noisy input events in a stream."""

    def __init__(
        self,
        noise_threshold: float = 0.5,
        min_events: int = 10,
        position_deviation_threshold_px: float = 5.0,
        timing_deviation_threshold_ms: float = 20.0,
    ):
        self.noise_threshold = noise_threshold
        self.min_events = min_events
        self.position_deviation_threshold_px = position_deviation_threshold_px
        self.timing_deviation_threshold_ms = timing_deviation_threshold_ms

        self._positions: deque[tuple[float, float]] = deque(maxlen=200)
        self._timestamps: deque[float] = deque(maxlen=200)

    def add_event(self, x: float, y: float, timestamp_ms: float) -> NoiseDetectionResult:
        """Add an event and check if the stream is noisy."""
        self._positions.append((x, y))
        self._timestamps.append(timestamp_ms)
        return self.analyze()

    def analyze(self) -> NoiseDetectionResult:
        """Analyze the current event stream for noise."""
        n = len(self._positions)
        if n < self.min_events:
            return NoiseDetectionResult(
                is_noisy=False,
                noise_score=0.0,
                confidence=0.0,
                flagged_indices=[],
            )

        flagged = []
        position_noise = 0
        timing_noise = 0

        # Check position jitter
        for i in range(1, n):
            dx = self._positions[i][0] - self._positions[i - 1][0]
            dy = self._positions[i][1] - self._positions[i - 1][1]
            dist = math.sqrt(dx * dx + dy * dy)
            if dist < self.position_deviation_threshold_px:
                position_noise += 1
                flagged.append(i)

        # Check timing regularity
        intervals = [self._timestamps[i] - self._timestamps[i - 1] for i in range(1, n)]
        if intervals:
            mean_interval = sum(intervals) / len(intervals)
            for interval in intervals:
                if abs(interval - mean_interval) > self.timing_deviation_threshold_ms:
                    timing_noise += 1

        position_noise_ratio = position_noise / max(1, n - 1)
        timing_noise_ratio = timing_noise / max(1, n - 1)
        noise_score = (position_noise_ratio + timing_noise_ratio) / 2.0
        is_noisy = noise_score > self.noise_threshold

        return NoiseDetectionResult(
            is_noisy=is_noisy,
            noise_score=min(1.0, noise_score),
            confidence=min(1.0, n / 50.0),
            flagged_indices=flagged,
        )

    def reset(self) -> None:
        """Reset detector state."""
        self._positions.clear()
        self._timestamps.clear()
