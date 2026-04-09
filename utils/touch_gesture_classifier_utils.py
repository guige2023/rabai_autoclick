"""Touch Gesture Classifier Utilities.

Advanced touch gesture classification using velocity and direction analysis.

Example:
    >>> from touch_gesture_classifier_utils import TouchGestureClassifier
    >>> clf = TouchGestureClassifier()
    >>> result = clf.classify([(0, 0, 0), (100, 50, 100), (200, 100, 200)])
    >>> print(result.gesture)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Tuple, Optional


class TouchGesture(Enum):
    """Touch gesture types."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE = auto()
    PAN = auto()
    PINCH_IN = auto()
    PINCH_OUT = auto()
    ROTATE = auto()
    UNKNOWN = auto()


@dataclass
class TouchPoint:
    """A touch point with timestamp."""
    x: float
    y: float
    timestamp: float


@dataclass
class GestureClassification:
    """Result of gesture classification."""
    gesture: TouchGesture
    confidence: float
    direction: Optional[str] = None
    distance: float = 0.0
    duration: float = 0.0
    velocity: float = 0.0


class TouchGestureClassifier:
    """Classifies touch gestures from point sequences."""

    TAP_DISTANCE_MAX = 25.0
    SWIPE_DISTANCE_MIN = 50.0
    SWIPE_VELOCITY_MIN = 200.0
    LONG_PRESS_TIME = 0.5

    def classify(
        self, points: List[Tuple[float, float, float]]
    ) -> GestureClassification:
        """Classify a gesture from touch points.

        Args:
            points: List of (x, y, timestamp_ms) tuples.

        Returns:
            GestureClassification with result.
        """
        if not points:
            return GestureClassification(TouchGesture.UNKNOWN, 0.0)

        if len(points) == 1:
            return GestureClassification(TouchGesture.TAP, 1.0)

        touch_points = [TouchPoint(x, y, t) for x, y, t in points]
        total_duration = touch_points[-1].timestamp - touch_points[0].timestamp
        total_distance = self._compute_distance(touch_points)

        if total_duration > self.LONG_PRESS_TIME and total_distance < self.TAP_DISTANCE_MAX:
            return GestureClassification(
                TouchGesture.LONG_PRESS, 0.95, duration=total_duration
            )

        if total_distance >= self.SWIPE_DISTANCE_MIN:
            velocity = total_distance / max(total_duration / 1000.0, 0.001)
            direction = self._compute_direction(touch_points)
            if velocity >= self.SWIPE_VELOCITY_MIN:
                return GestureClassification(
                    TouchGesture.SWIPE, 0.9,
                    direction=direction, distance=total_distance,
                    velocity=velocity
                )
            return GestureClassification(
                TouchGesture.PAN, 0.85,
                direction=direction, distance=total_distance,
                velocity=velocity
            )

        return GestureClassification(TouchGesture.TAP, 0.8, distance=total_distance)

    def _compute_distance(self, points: List[TouchPoint]) -> float:
        """Compute total path distance."""
        total = 0.0
        for i in range(1, len(points)):
            dx = points[i].x - points[i - 1].x
            dy = points[i].y - points[i - 1].y
            total += math.sqrt(dx * dx + dy * dy)
        return total

    def _compute_direction(self, points: List[TouchPoint]) -> str:
        """Compute overall swipe direction."""
        dx = points[-1].x - points[0].x
        dy = points[-1].y - points[0].y
        if abs(dx) > abs(dy):
            return "right" if dx > 0 else "left"
        return "down" if dy > 0 else "up"
