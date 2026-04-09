"""
Touch Gesture Action Module

Provides advanced touch gesture recognition and
multi-touch simulation for mobile automation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class Gesture(Enum):
    """Touch gesture types."""

    TAP = "tap"
    DOUBLE_TAP = "double_tap"
    LONG_PRESS = "long_press"
    SWIPE = "swipe"
    PINCH = "pinch"
    ROTATE = "rotate"
    DRAG = "drag"


@dataclass
class TouchPoint:
    """Touch point data."""

    x: float
    y: float
    pressure: float = 1.0
    finger_id: int = 0
    timestamp: float = field(default_factory=time.time)


@dataclass
class GestureRecognizer:
    """Recognizes touch gestures from input points."""

    min_tap_distance: float = 20.0
    min_swipe_distance: float = 100.0
    long_press_duration: float = 0.5
    double_tap_interval: float = 0.3


class TouchGestureRecognizer:
    """
    Recognizes touch gestures from point sequences.

    Supports tap, swipe, pinch, rotate, and custom gestures
    with configurable parameters.
    """

    def __init__(
        self,
        config: Optional[GestureRecognizer] = None,
        gesture_callback: Optional[Callable[[Gesture, Dict], None]] = None,
    ):
        self.config = config or GestureRecognizer()
        self.gesture_callback = gesture_callback
        self._active_touches: Dict[int, List[TouchPoint]] = {}
        self._gesture_history: List[Tuple[Gesture, Dict]] = []

    def add_touch(
        self,
        finger_id: int,
        x: float,
        y: float,
        pressure: float = 1.0,
    ) -> None:
        """Add a touch point."""
        if finger_id not in self._active_touches:
            self._active_touches[finger_id] = []

        self._active_touches[finger_id].append(
            TouchPoint(x=x, y=y, pressure=pressure, finger_id=finger_id)
        )

    def remove_touch(self, finger_id: int) -> Optional[Tuple[Gesture, Dict]]:
        """Remove touch and recognize gesture."""
        if finger_id not in self._active_touches:
            return None

        points = self._active_touches[finger_id]
        del self._active_touches[finger_id]

        if len(points) < 2:
            return None

        gesture = self._recognize_gesture(points)

        if gesture:
            self._gesture_history.append(gesture)

            if self.gesture_callback:
                self.gesture_callback(gesture[0], gesture[1])

        return gesture

    def _recognize_gesture(
        self,
        points: List[TouchPoint],
    ) -> Optional[Tuple[Gesture, Dict]]:
        """Recognize gesture from points."""
        if len(points) == 2:
            return self._recognize_two_finger(points)

        return self._recognize_single_finger(points)

    def _recognize_single_finger(
        self,
        points: List[TouchPoint],
    ) -> Optional[Tuple[Gesture, Dict]]:
        """Recognize single finger gesture."""
        start = points[0]
        end = points[-1]

        dx = end.x - start.x
        dy = end.y - start.y
        distance = math.sqrt(dx * dx + dy * dy)
        duration = end.timestamp - start.timestamp

        if distance < self.config.min_tap_distance:
            if duration < 0.2:
                return (Gesture.TAP, {"x": end.x, "y": end.y})
            else:
                return (Gesture.LONG_PRESS, {"x": end.x, "y": end.y})

        if distance > self.config.min_swipe_distance:
            angle = math.degrees(math.atan2(dy, dx))
            return (
                Gesture.SWIPE,
                {
                    "x": end.x,
                    "y": end.y,
                    "dx": dx,
                    "dy": dy,
                    "angle": angle,
                    "distance": distance,
                },
            )

        return (
            Gesture.DRAG,
            {"x": end.x, "y": end.y, "dx": dx, "dy": dy},
        )

    def _recognize_two_finger(
        self,
        points: List[TouchPoint],
    ) -> Optional[Tuple[Gesture, Dict]]:
        """Recognize two-finger gesture."""
        return (Gesture.PINCH, {})

    def clear(self) -> None:
        """Clear all active touches."""
        self._active_touches.clear()

    def get_history(self) -> List[Tuple[Gesture, Dict]]:
        """Get gesture history."""
        return self._gesture_history.copy()


def create_touch_gesture_recognizer(
    config: Optional[GestureRecognizer] = None,
) -> TouchGestureRecognizer:
    """Factory function."""
    return TouchGestureRecognizer(config=config)
