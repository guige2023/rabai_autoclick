"""
Gesture Recognition Utilities

Provides utilities for recognizing and classifying
gestures in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from enum import Enum, auto
import math


class GestureType(Enum):
    """Types of recognized gestures."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    PINCH = auto()
    ZOOM = auto()
    ROTATE = auto()
    PAN = auto()
    UNKNOWN = auto()


@dataclass
class Gesture:
    """Represents a recognized gesture."""
    gesture_type: GestureType
    points: list[tuple[int, int]]
    duration_ms: float
    velocity: float = 0.0
    metadata: dict[str, Any] | None = None


class GestureRecognizer:
    """
    Recognizes gestures from input data.
    
    Analyzes touch/mouse points to classify
    gestures and extract parameters.
    """

    def __init__(self) -> None:
        self._min_swipe_distance = 50
        self._max_tap_duration = 300
        self._max_double_tap_interval = 300

    def recognize(
        self,
        points: list[tuple[int, int]],
        duration_ms: float,
    ) -> Gesture:
        """
        Recognize a gesture from points and duration.
        
        Args:
            points: List of (x, y) points.
            duration_ms: Gesture duration in milliseconds.
            
        Returns:
            Recognized Gesture.
        """
        if len(points) == 1 and duration_ms < self._max_tap_duration:
            return Gesture(
                gesture_type=GestureType.TAP,
                points=points,
                duration_ms=duration_ms,
            )

        if len(points) >= 2:
            start = points[0]
            end = points[-1]
            dx = end[0] - start[0]
            dy = end[1] - start[1]
            distance = math.sqrt(dx * dx + dy * dy)
            velocity = distance / (duration_ms / 1000.0) if duration_ms > 0 else 0

            if distance > self._min_swipe_distance:
                if abs(dx) > abs(dy):
                    direction = GestureType.SWIPE_RIGHT if dx > 0 else GestureType.SWIPE_LEFT
                else:
                    direction = GestureType.SWIPE_DOWN if dy > 0 else GestureType.SWIPE_UP
                return Gesture(
                    gesture_type=direction,
                    points=points,
                    duration_ms=duration_ms,
                    velocity=velocity,
                )

        return Gesture(
            gesture_type=GestureType.UNKNOWN,
            points=points,
            duration_ms=duration_ms,
        )

    def set_swipe_threshold(self, distance: int) -> None:
        """Set minimum swipe distance."""
        self._min_swipe_distance = distance
