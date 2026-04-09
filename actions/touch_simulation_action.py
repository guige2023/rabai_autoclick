"""
Touch Simulation Action Module

Provides multi-touch gesture simulation, pinch-zoom, swipe detection,
and touch point management for UI automation on touch-capable devices.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class GestureType(Enum):
    """Supported gesture types."""

    TAP = "tap"
    DOUBLE_TAP = "double_tap"
    LONG_PRESS = "long_press"
    SWIPE_UP = "swipe_up"
    SWIPE_DOWN = "swipe_down"
    SWIPE_LEFT = "swipe_left"
    SWIPE_RIGHT = "swipe_right"
    PINCH_IN = "pinch_in"
    PINCH_OUT = "pinch_out"
    ROTATE = "rotate"
    DRAG = "drag"


@dataclass
class TouchPoint:
    """Represents a single touch point."""

    x: float
    y: float
    pressure: float = 1.0
    timestamp: float = field(default_factory=time.time)
    finger_id: int = 0

    def distance_to(self, other: TouchPoint) -> float:
        """Calculate Euclidean distance to another touch point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def midpoint_to(self, other: TouchPoint) -> Tuple[float, float]:
        """Get midpoint between two touch points."""
        return ((self.x + other.x) / 2, (self.y + other.y) / 2)


@dataclass
class GestureConfig:
    """Configuration for gesture simulation."""

    swipe_duration: float = 0.3
    tap_interval: float = 0.1
    long_press_duration: float = 0.5
    pinch_velocity: float = 1.0
    rotation_sensitivity: float = 1.0
    min_swipe_distance: float = 50.0


class TouchSimulator:
    """
    Simulates touch gestures for touch-enabled UI automation.

    Supports single-finger, multi-finger, and compound gestures
    including pinch-zoom, rotation, and complex swipe patterns.
    """

    def __init__(
        self,
        config: Optional[GestureConfig] = None,
        touch_handler: Optional[Callable[[List[TouchPoint]], None]] = None,
    ):
        self.config = config or GestureConfig()
        self.touch_handler = touch_handler or self._default_touch_handler
        self._active_touches: Dict[int, TouchPoint] = {}
        self._gesture_history: List[GestureType] = []

    def _default_touch_handler(self, points: List[TouchPoint]) -> None:
        """Default handler logs touch points."""
        logger.debug(f"Touch points: {[f'({p.x:.1f},{p.y:.1f})' for p in points]}")

    def tap(self, x: float, y: float, finger_id: int = 0) -> bool:
        """
        Simulate a single tap gesture.

        Args:
            x: X coordinate
            y: Y coordinate
            finger_id: Identifier for this finger track

        Returns:
            True if tap was simulated successfully
        """
        try:
            point = TouchPoint(x=x, y=y, finger_id=finger_id)
            self.touch_handler([point])
            self._gesture_history.append(GestureType.TAP)
            return True
        except Exception as e:
            logger.error(f"Tap failed: {e}")
            return False

    def double_tap(self, x: float, y: float, finger_id: int = 0) -> bool:
        """
        Simulate a double tap gesture.

        Args:
            x: X coordinate
            y: Y coordinate
            finger_id: Identifier for this finger track

        Returns:
            True if double tap was simulated successfully
        """
        try:
            point1 = TouchPoint(x=x, y=y, finger_id=finger_id)
            self.touch_handler([point1])
            time.sleep(self.config.tap_interval)
            point2 = TouchPoint(x=x, y=y, finger_id=finger_id)
            self.touch_handler([point2])
            self._gesture_history.append(GestureType.DOUBLE_TAP)
            return True
        except Exception as e:
            logger.error(f"Double tap failed: {e}")
            return False

    def swipe(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        finger_id: int = 0,
        duration: Optional[float] = None,
    ) -> bool:
        """
        Simulate a swipe gesture from start to end coordinates.

        Args:
            start_x: Starting X coordinate
            start_y: Starting Y coordinate
            end_x: Ending X coordinate
            end_y: Ending Y coordinate
            finger_id: Identifier for this finger track
            duration: Swipe duration in seconds (uses config default if None)

        Returns:
            True if swipe was simulated successfully
        """
        try:
            duration = duration or self.config.swipe_duration
            steps = max(int(duration * 60), 10)
            start = TouchPoint(x=start_x, y=start_y, finger_id=finger_id)
            end = TouchPoint(x=end_x, y=end_y, finger_id=finger_id)

            for i in range(steps + 1):
                t = i / steps
                interpolated = TouchPoint(
                    x=start.x + (end.x - start.x) * t,
                    y=start.y + (end.y - start.y) * t,
                    finger_id=finger_id,
                )
                self.touch_handler([interpolated])
                if i < steps:
                    time.sleep(duration / steps)

            self._gesture_history.append(self._detect_swipe_direction(start, end))
            return True
        except Exception as e:
            logger.error(f"Swipe failed: {e}")
            return False

    def _detect_swipe_direction(
        self, start: TouchPoint, end: TouchPoint
    ) -> GestureType:
        """Detect swipe direction from start and end points."""
        dx = end.x - start.x
        dy = end.y - start.y

        if abs(dx) < self.config.min_swipe_distance and abs(dy) < self.config.min_swipe_distance:
            return GestureType.TAP

        if abs(dx) > abs(dy):
            return GestureType.SWIPE_RIGHT if dx > 0 else GestureType.SWIPE_LEFT
        else:
            return GestureType.SWIPE_DOWN if dy > 0 else GestureType.SWIPE_UP

    def pinch(
        self,
        center_x: float,
        center_y: float,
        start_distance: float,
        scale: float,
        finger_id_1: int = 0,
        finger_id_2: int = 1,
    ) -> bool:
        """
        Simulate a pinch gesture for zoom in/out.

        Args:
            center_x: Center X coordinate of pinch
            center_y: Center Y coordinate of pinch
            start_distance: Initial distance between two fingers
            scale: Scale factor (>1 for zoom out, <1 for zoom in)
            finger_id_1: Identifier for first finger
            finger_id_2: Identifier for second finger

        Returns:
            True if pinch was simulated successfully
        """
        try:
            angle = math.atan2(center_y, center_x)
            end_distance = start_distance * scale

            for i in range(20):
                t = i / 19
                current_distance = start_distance + (end_distance - start_distance) * t
                offset = current_distance / 2

                p1 = TouchPoint(
                    x=center_x - offset * math.cos(angle),
                    y=center_y - offset * math.sin(angle),
                    finger_id=finger_id_1,
                )
                p2 = TouchPoint(
                    x=center_x + offset * math.cos(angle),
                    y=center_y + offset * math.sin(angle),
                    finger_id=finger_id_2,
                )
                self.touch_handler([p1, p2])
                time.sleep(0.02)

            self._gesture_history.append(
                GestureType.PINCH_OUT if scale > 1 else GestureType.PINCH_IN
            )
            return True
        except Exception as e:
            logger.error(f"Pinch failed: {e}")
            return False

    def rotate(
        self,
        center_x: float,
        center_y: float,
        start_angle: float,
        delta_angle: float,
        finger_id_1: int = 0,
        finger_id_2: int = 1,
    ) -> bool:
        """
        Simulate a rotation gesture.

        Args:
            center_x: Center X coordinate of rotation
            center_y: Center Y coordinate of rotation
            start_angle: Starting angle in radians
            delta_angle: Rotation angle in radians
            finger_id_1: Identifier for first finger
            finger_id_2: Identifier for second finger

        Returns:
            True if rotation was simulated successfully
        """
        try:
            radius = 75.0
            steps = max(int(abs(delta_angle) * 30), 10)

            for i in range(steps + 1):
                t = i / steps
                current_angle = start_angle + delta_angle * t

                p1 = TouchPoint(
                    x=center_x + radius * math.cos(current_angle),
                    y=center_y + radius * math.sin(current_angle),
                    finger_id=finger_id_1,
                )
                p2 = TouchPoint(
                    x=center_x + radius * math.cos(current_angle + math.pi),
                    y=center_y + radius * math.sin(current_angle + math.pi),
                    finger_id=finger_id_2,
                )
                self.touch_handler([p1, p2])
                if i < steps:
                    time.sleep(0.015)

            self._gesture_history.append(GestureType.ROTATE)
            return True
        except Exception as e:
            logger.error(f"Rotate failed: {e}")
            return False

    def multi_touch(
        self, points: List[Tuple[float, float]], finger_ids: Optional[List[int]] = None
    ) -> bool:
        """
        Simulate multiple simultaneous touch points.

        Args:
            points: List of (x, y) coordinate tuples
            finger_ids: Optional list of finger identifiers

        Returns:
            True if multi-touch was simulated successfully
        """
        try:
            if finger_ids is None:
                finger_ids = list(range(len(points)))

            touch_points = [
                TouchPoint(x=x, y=y, finger_id=fid)
                for (x, y), fid in zip(points, finger_ids)
            ]
            self.touch_handler(touch_points)
            return True
        except Exception as e:
            logger.error(f"Multi-touch failed: {e}")
            return False

    def gesture_sequence(
        self, gestures: List[Tuple[GestureType, Dict[str, Any]]]
    ) -> bool:
        """
        Execute a sequence of gestures.

        Args:
            gestures: List of (gesture_type, kwargs) tuples

        Returns:
            True if all gestures were executed successfully
        """
        gesture_methods = {
            GestureType.TAP: lambda kwargs: self.tap(**kwargs),
            GestureType.DOUBLE_TAP: lambda kwargs: self.double_tap(**kwargs),
            GestureType.SWIPE_UP: lambda kwargs: self.swipe(**kwargs),
            GestureType.SWIPE_DOWN: lambda kwargs: self.swipe(**kwargs),
            GestureType.SWIPE_LEFT: lambda kwargs: self.swipe(**kwargs),
            GestureType.SWIPE_RIGHT: lambda kwargs: self.swipe(**kwargs),
        }

        for gesture_type, kwargs in gestures:
            method = gesture_methods.get(gesture_type)
            if method:
                if not method(kwargs):
                    return False
            else:
                logger.warning(f"Unsupported gesture in sequence: {gesture_type}")

        return True

    def get_gesture_history(self) -> List[GestureType]:
        """Return the history of executed gestures."""
        return self._gesture_history.copy()

    def clear_history(self) -> None:
        """Clear the gesture history."""
        self._gesture_history.clear()


def create_touch_simulator(
    config: Optional[GestureConfig] = None,
    touch_handler: Optional[Callable[[List[TouchPoint]], None]] = None,
) -> TouchSimulator:
    """
    Factory function to create a TouchSimulator instance.

    Args:
        config: Optional gesture configuration
        touch_handler: Optional custom touch event handler

    Returns:
        Configured TouchSimulator instance
    """
    return TouchSimulator(config=config, touch_handler=touch_handler)
