"""
Input Confidence Scoring Utilities

Compute confidence scores for input events based on
spatial, temporal, and contextual features. Used to
filter out low-confidence events in automation pipelines.

Author: rabai_autoclick-agent3
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class ConfidenceScore:
    """A computed confidence score with breakdown."""
    overall: float  # 0.0 to 1.0
    spatial_score: float
    temporal_score: float
    context_score: float
    is_acceptable: bool


class InputConfidenceScorer:
    """
    Score input events on their likelihood of being genuine user input.

    Lower scores may indicate bot-generated events, system noise,
    or automation artifacts that should be filtered.
    """

    def __init__(
        self,
        spatial_weight: float = 0.4,
        temporal_weight: float = 0.3,
        context_weight: float = 0.3,
        acceptance_threshold: float = 0.5,
    ):
        self.weights = {
            "spatial": spatial_weight,
            "temporal": temporal_weight,
            "context": context_weight,
        }
        self.acceptance_threshold = acceptance_threshold

    def score(
        self,
        x: float,
        y: float,
        timestamp_ms: float,
        expected_area_min_x: float = 0,
        expected_area_max_x: float = 10000,
        expected_area_min_y: float = 0,
        expected_area_max_y: float = 10000,
        is_within_interactive_element: bool = True,
        last_event_timestamp_ms: Optional[float] = None,
        last_event_x: Optional[float] = None,
        last_event_y: Optional[float] = None,
    ) -> ConfidenceScore:
        """Compute confidence score for an input event."""
        spatial_score = self._spatial_score(x, y, expected_area_min_x, expected_area_max_x, expected_area_min_y, expected_area_max_y, is_within_interactive_element)
        temporal_score = self._temporal_score(timestamp_ms, last_event_timestamp_ms)
        context_score = self._context_score(x, y, last_event_x, last_event_y)

        overall = (
            self.weights["spatial"] * spatial_score
            + self.weights["temporal"] * temporal_score
            + self.weights["context"] * context_score
        )

        return ConfidenceScore(
            overall=max(0.0, min(1.0, overall)),
            spatial_score=spatial_score,
            temporal_score=temporal_score,
            context_score=context_score,
            is_acceptable=overall >= self.acceptance_threshold,
        )

    def _spatial_score(
        self,
        x: float,
        y: float,
        min_x: float,
        max_x: float,
        min_y: float,
        max_y: float,
        within_element: bool,
    ) -> float:
        """Score based on spatial validity."""
        in_bounds = min_x <= x <= max_x and min_y <= y <= max_y
        if not in_bounds:
            return 0.0
        if not within_element:
            return 0.5
        return 1.0

    def _temporal_score(
        self,
        current_ms: float,
        last_ms: Optional[float],
    ) -> float:
        """Score based on inter-event timing."""
        if last_ms is None:
            return 0.8  # No history, assume acceptable
        interval = current_ms - last_ms
        if interval < 5:
            return 0.2  # Too fast, suspicious
        if interval > 10000:
            return 0.5  # Very slow, might be stale
        return 1.0

    def _context_score(
        self,
        x: float,
        y: float,
        last_x: Optional[float],
        last_y: Optional[float],
    ) -> float:
        """Score based on contextual consistency."""
        if last_x is None or last_y is None:
            return 0.8
        distance = math.sqrt((x - last_x) ** 2 + (y - last_y) ** 2)
        # Very large jumps (> 500px) are suspicious
        if distance > 500:
            return 0.3
        return 1.0
