"""
Input Simulation Quality Utilities

Assess the quality and realism of simulated input events
(mouse, keyboard, touch) to detect bot-like patterns.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass, field
from typing import List, Optional, Deque


@dataclass
class SimulationQualityReport:
    """Quality report for simulated input."""
    is_realistic: bool
    overall_score: float  # 0.0 to 1.0
    temporal_score: float
    spatial_score: float
    patterns_detected: List[str]
    suggestions: List[str] = field(default_factory=list)


class InputSimulationQualityAnalyzer:
    """Analyze simulated input for human-likeness."""

    def __init__(
        self,
        max_velocity_px_per_s: float = 3000.0,
        max_acceleration_px_per_s2: float = 5000.0,
        min_inter_event_ms: float = 5.0,
    ):
        self.max_velocity_px_per_s = max_velocity_px_per_s
        self.max_acceleration_px_per_s2 = max_acceleration_px_per_s2
        self.min_inter_event_ms = min_inter_event_ms

        self._event_timestamps: Deque[float] = deque(maxlen=1000)
        self._event_positions: Deque[tuple[float, float]] = deque(maxlen=1000)
        self._scores: List[float] = []

    def record_event(self, timestamp_ms: float, x: float, y: float) -> None:
        """Record a simulated input event."""
        self._event_timestamps.append(timestamp_ms)
        self._event_positions.append((x, y))

    def analyze(self) -> SimulationQualityReport:
        """Analyze recorded events and return a quality report."""
        patterns = []
        suggestions = []
        temporal_score = 1.0
        spatial_score = 1.0

        if len(self._event_timestamps) < 2:
            return SimulationQualityReport(
                is_realistic=True,
                overall_score=1.0,
                temporal_score=1.0,
                spatial_score=1.0,
                patterns_detected=[],
            )

        # Check inter-event timing
        for i in range(1, len(self._event_timestamps)):
            interval = self._event_timestamps[i] - self._event_timestamps[i - 1]
            if interval < self.min_inter_event_ms:
                patterns.append("too_fast_events")
                temporal_score *= 0.7
                suggestions.append("Increase minimum inter-event delay")

        # Check velocity
        for i in range(1, len(self._event_positions)):
            t_delta = max(1.0, (self._event_timestamps[i] - self._event_timestamps[i - 1]) / 1000.0)
            dx = self._event_positions[i][0] - self._event_positions[i - 1][0]
            dy = self._event_positions[i][1] - self._event_positions[i - 1][1]
            distance = math.sqrt(dx * dx + dy * dy)
            velocity = distance / t_delta
            if velocity > self.max_velocity_px_per_s:
                patterns.append("unrealistic_velocity")
                spatial_score *= 0.8

        overall = (temporal_score + spatial_score) / 2.0
        is_realistic = overall > 0.5 and len(patterns) < 3

        return SimulationQualityReport(
            is_realistic=is_realistic,
            overall_score=max(0.0, overall),
            temporal_score=max(0.0, temporal_score),
            spatial_score=max(0.0, spatial_score),
            patterns_detected=list(set(patterns)),
            suggestions=suggestions,
        )
