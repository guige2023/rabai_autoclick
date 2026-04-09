"""
Touch Fidelity Utilities for UI Automation.

This module provides utilities for analyzing and improving
touch input fidelity in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple


@dataclass
class FidelityMetrics:
    """Metrics for touch input fidelity."""
    spatial_accuracy: float = 1.0
    temporal_accuracy: float = 1.0
    jitter_score: float = 0.0
    latency_ms: float = 0.0
    overall_score: float = 1.0


@dataclass
class FidelityConfig:
    """Configuration for fidelity analysis."""
    jitter_window_size: int = 5
    max_acceptable_jitter: float = 3.0
    max_acceptable_latency: float = 50.0
    accuracy_weight: float = 0.4
    latency_weight: float = 0.3
    jitter_weight: float = 0.3


class TouchFidelityAnalyzer:
    """Analyzes touch input fidelity and quality metrics."""

    def __init__(self, config: Optional[FidelityConfig] = None) -> None:
        self._config = config or FidelityConfig()
        self._position_history: List[Tuple[float, float, float]] = []
        self._expected_positions: List[Tuple[float, float, float]] = []
        self._max_history: int = 100

    def record_actual(
        self,
        x: float,
        y: float,
        timestamp: Optional[float] = None,
    ) -> None:
        """Record an actual touch position."""
        if timestamp is None:
            timestamp = time.time()

        self._position_history.append((x, y, timestamp))
        if len(self._position_history) > self._max_history:
            self._position_history.pop(0)

    def record_expected(
        self,
        x: float,
        y: float,
        timestamp: Optional[float] = None,
    ) -> None:
        """Record an expected/desired touch position."""
        if timestamp is None:
            timestamp = time.time()

        self._expected_positions.append((x, y, timestamp))
        if len(self._expected_positions) > self._max_history:
            self._expected_positions.pop(0)

    def analyze(self) -> FidelityMetrics:
        """Analyze fidelity metrics from recorded data."""
        spatial = self._calculate_spatial_accuracy()
        temporal = self._calculate_temporal_accuracy()
        jitter = self._calculate_jitter_score()
        latency = self._calculate_latency()

        overall = (
            spatial * self._config.accuracy_weight +
            (1.0 - jitter) * self._config.jitter_weight +
            max(0.0, 1.0 - latency / self._config.max_acceptable_latency) * self._config.latency_weight
        )

        return FidelityMetrics(
            spatial_accuracy=spatial,
            temporal_accuracy=temporal,
            jitter_score=jitter,
            latency_ms=latency,
            overall_score=overall,
        )

    def _calculate_spatial_accuracy(self) -> float:
        """Calculate spatial accuracy from actual vs expected positions."""
        if not self._position_history or not self._expected_positions:
            return 1.0

        total_error = 0.0
        matched = 0

        for actual in self._position_history:
            nearest_expected = self._find_nearest_expected(actual[0], actual[1])
            if nearest_expected is not None:
                dx = actual[0] - nearest_expected[0]
                dy = actual[1] - nearest_expected[1]
                error = math.sqrt(dx * dx + dy * dy)
                total_error += error
                matched += 1

        if matched == 0:
            return 1.0

        avg_error = total_error / matched
        return max(0.0, 1.0 - avg_error / 50.0)

    def _calculate_temporal_accuracy(self) -> float:
        """Calculate temporal accuracy of touch events."""
        if len(self._position_history) < 2:
            return 1.0

        intervals_actual = []
        for i in range(1, len(self._position_history)):
            dt = self._position_history[i][2] - self._position_history[i - 1][2]
            intervals_actual.append(dt)

        if not intervals_actual:
            return 1.0

        avg_actual = sum(intervals_actual) / len(intervals_actual)
        expected_interval = 0.016

        accuracy = min(1.0, expected_interval / (avg_actual + 0.001))
        return accuracy

    def _calculate_jitter_score(self) -> float:
        """Calculate jitter score from position deviations."""
        if len(self._position_history) < self._config.jitter_window_size:
            return 0.0

        window = self._position_history[-self._config.jitter_window_size:]
        displacements = []

        for i in range(1, len(window)):
            dx = window[i][0] - window[i - 1][0]
            dy = window[i][1] - window[i - 1][1]
            disp = math.sqrt(dx * dx + dy * dy)
            displacements.append(disp)

        if not displacements:
            return 0.0

        avg_disp = sum(displacements) / len(displacements)
        variance = sum((d - avg_disp) ** 2 for d in displacements) / len(displacements)
        std_dev = math.sqrt(variance)

        return min(1.0, std_dev / self._config.max_acceptable_jitter)

    def _calculate_latency(self) -> float:
        """Calculate average touch latency."""
        if not self._position_history or not self._expected_positions:
            return 0.0

        latencies = []
        for actual in self._position_history[-10:]:
            nearest = self._find_nearest_expected(actual[0], actual[1])
            if nearest is not None:
                latency = abs(actual[2] - nearest[2]) * 1000.0
                latencies.append(latency)

        return sum(latencies) / len(latencies) if latencies else 0.0

    def _find_nearest_expected(
        self,
        x: float,
        y: float,
    ) -> Optional[Tuple[float, float, float]]:
        """Find the nearest expected position to the given coordinates."""
        if not self._expected_positions:
            return None

        nearest = None
        min_dist = float('inf')

        for expected in self._expected_positions:
            dx = x - expected[0]
            dy = y - expected[1]
            dist = math.sqrt(dx * dx + dy * dy)
            if dist < min_dist:
                min_dist = dist
                nearest = expected

        return nearest

    def reset(self) -> None:
        """Reset all recorded data."""
        self._position_history.clear()
        self._expected_positions.clear()

    def get_position_count(self) -> int:
        """Get the number of recorded positions."""
        return len(self._position_history)
