"""Gesture Classifier Utilities.

Classifies gestures from touch/mouse input sequences using pattern matching
and machine learning-style heuristics.

Example:
    >>> from gesture_classifier_utils import GestureClassifier
    >>> classifier = GestureClassifier()
    >>> points = [(0, 0), (10, 10), (20, 20)]
    >>> result = classifier.classify(points)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Tuple, Optional


class GestureType(Enum):
    """Supported gesture types."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    PINCH = auto()
    SPREAD = auto()
    PAN = auto()
    ROTATE = auto()
    UNKNOWN = auto()


@dataclass
class GestureResult:
    """Result of gesture classification."""
    gesture_type: GestureType
    confidence: float
    metadata: dict


class GestureClassifier:
    """Classifies gestures from input points."""

    TAP_THRESHOLD_DISTANCE = 20
    TAP_THRESHOLD_TIME = 0.3
    SWIPE_MIN_DISTANCE = 50
    LONG_PRESS_THRESHOLD = 0.5

    def classify(
        self, points: List[Tuple[float, float]], timestamps: Optional[List[float]] = None
    ) -> GestureResult:
        """Classify a gesture from a list of points.

        Args:
            points: List of (x, y) coordinates.
            timestamps: Optional list of timestamps for each point.

        Returns:
            GestureResult with type and confidence.
        """
        if len(points) < 1:
            return GestureResult(GestureType.UNKNOWN, 0.0, {})

        if len(points) == 1:
            return GestureResult(GestureType.TAP, 1.0, {})

        if len(points) == 2:
            return self._classify_two_point(points, timestamps)

        return self._classify_multi_point(points, timestamps)

    def _classify_two_point(
        self, points: List[Tuple[float, float]], timestamps: Optional[List[float]]
    ) -> GestureResult:
        """Classify gestures with exactly two points."""
        dx = points[1][0] - points[0][0]
        dy = points[1][1] - points[0][1]
        distance = math.sqrt(dx * dx + dy * dy)

        if distance < self.TAP_THRESHOLD_DISTANCE:
            duration = 0.0
            if timestamps and len(timestamps) == 2:
                duration = timestamps[1] - timestamps[0]
            if duration > self.LONG_PRESS_THRESHOLD:
                return GestureResult(GestureType.LONG_PRESS, 0.9, {"duration": duration})
            return GestureResult(GestureType.TAP, 0.95, {})

        return self._classify_swipe_direction(dx, dy)

    def _classify_multi_point(
        self, points: List[Tuple[float, float]], timestamps: Optional[List[float]]
    ) -> GestureResult:
        """Classify gestures with multiple points."""
        start = points[0]
        end = points[-1]
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        distance = math.sqrt(dx * dx + dy * dy)

        if distance < self.SWIPE_MIN_DISTANCE:
            return GestureResult(GestureType.TAP, 0.7, {})

        return self._classify_swipe_direction(dx, dy)

    def _classify_swipe_direction(self, dx: float, dy: float) -> GestureResult:
        """Classify swipe direction from delta values."""
        abs_dx = abs(dx)
        abs_dy = abs(dy)

        if abs_dx > abs_dy:
            if dx > 0:
                return GestureResult(GestureType.SWIPE_RIGHT, 0.95, {"dx": dx, "dy": dy})
            return GestureResult(GestureType.SWIPE_LEFT, 0.95, {"dx": dx, "dy": dy})
        else:
            if dy > 0:
                return GestureResult(GestureType.SWIPE_DOWN, 0.95, {"dx": dx, "dy": dy})
            return GestureResult(GestureType.SWIPE_UP, 0.95, {"dx": dx, "dy": dy})


def classify_swipe(points: List[Tuple[float, float]]) -> GestureType:
    """Quick swipe classification from points.

    Args:
        points: List of (x, y) coordinates.

    Returns:
        GestureType of the swipe.
    """
    if len(points) < 2:
        return GestureType.UNKNOWN
    start, end = points[0], points[-1]
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    abs_dx, abs_dy = abs(dx), abs(dy)
    if abs_dx > abs_dy:
        return GestureType.SWIPE_RIGHT if dx > 0 else GestureType.SWIPE_LEFT
    return GestureType.SWIPE_DOWN if dy > 0 else GestureType.SWIPE_UP
