"""
Click Quality Utilities

Utilities for assessing and scoring the quality of simulated clicks
in an automation context. Includes accuracy scoring, click validation,
and quality reporting.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional, Tuple


@dataclass
class Point:
    """2D point representation."""
    x: float
    y: float

    def distance_to(self, other: Point) -> float:
        """Euclidean distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)


@dataclass
class ClickQualityScore:
    """Quality score for a simulated click."""
    overall_score: float  # 0.0 to 1.0
    spatial_accuracy: float  # 0.0 to 1.0
    temporal_accuracy: float  # 0.0 to 1.0
    is_acceptable: bool
    details: dict = field(default_factory=dict)


class ClickQualityAssessor:
    """Assess the quality of simulated clicks against ground truth."""

    def __init__(
        self,
        spatial_threshold_px: float = 10.0,
        temporal_threshold_ms: float = 50.0,
    ):
        self.spatial_threshold_px = spatial_threshold_px
        self.temporal_threshold_ms = temporal_threshold_ms
        self._scores: list[float] = []

    def assess(
        self,
        expected: Point,
        actual: Point,
        expected_time_ms: float,
        actual_time_ms: float,
    ) -> ClickQualityScore:
        """Assess click quality against expected values."""
        spatial_error = expected.distance_to(actual)
        spatial_ok = spatial_error <= self.spatial_threshold_px
        spatial_accuracy = max(0.0, 1.0 - spatial_error / (self.spatial_threshold_px * 2))

        temporal_error = abs(actual_time_ms - expected_time_ms)
        temporal_ok = temporal_error <= self.temporal_threshold_ms
        temporal_accuracy = max(0.0, 1.0 - temporal_error / (self.temporal_threshold_ms * 2))

        overall_score = (spatial_accuracy + temporal_accuracy) / 2.0
        is_acceptable = spatial_ok and temporal_ok

        score = ClickQualityScore(
            overall_score=overall_score,
            spatial_accuracy=spatial_accuracy,
            temporal_accuracy=temporal_accuracy,
            is_acceptable=is_acceptable,
            details={
                "spatial_error_px": spatial_error,
                "temporal_error_ms": temporal_error,
                "expected": (expected.x, expected.y),
                "actual": (actual.x, actual.y),
            },
        )

        self._scores.append(overall_score)
        return score

    def get_average_score(self) -> float:
        """Return the average quality score from recorded assessments."""
        if not self._scores:
            return 0.0
        return sum(self._scores) / len(self._scores)


def validate_click_target(
    target: Point,
    bounds: Tuple[float, float, float, float],  # x, y, width, height
    margin: float = 0.0,
) -> bool:
    """Validate that a click target is within bounds with optional margin."""
    bx, by, bw, bh = bounds
    return (bx + margin <= target.x <= bx + bw - margin
            and by + margin <= target.y <= by + bh - margin)
