"""
Mouse Gesture Action Module

Provides mouse gesture recognition, stroke detection,
and gesture-to-action mapping for UI automation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import math
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class GestureDirection(Enum):
    """Mouse gesture directions."""

    UP = "up"
    DOWN = "down"
    LEFT = "left"
    RIGHT = "right"
    UP_LEFT = "up_left"
    UP_RIGHT = "up_right"
    DOWN_LEFT = "down_left"
    DOWN_RIGHT = "down_right"
    CIRCLE_CW = "circle_cw"
    CIRCLE_CCW = "circle_ccw"


@dataclass
class GestureStroke:
    """Represents a mouse gesture stroke."""

    points: List[Tuple[float, float]] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    direction: Optional[GestureDirection] = None

    def add_point(self, x: float, y: float) -> None:
        """Add a point to the stroke."""
        self.points.append((x, y))

    def close(self) -> None:
        """Close the stroke."""
        self.end_time = time.time()
        self.direction = self._compute_direction()

    def _compute_direction(self) -> Optional[GestureDirection]:
        """Compute overall direction of the stroke."""
        if len(self.points) < 2:
            return None

        start = self.points[0]
        end = self.points[-1]

        dx = end[0] - start[0]
        dy = end[1] - start[1]

        threshold = 30

        if abs(dx) < threshold and abs(dy) < threshold:
            return None

        abs_dx = abs(dx)
        abs_dy = abs(dy)

        if abs_dx > abs_dy * 2:
            return GestureDirection.RIGHT if dx > 0 else GestureDirection.LEFT
        elif abs_dy > abs_dx * 2:
            return GestureDirection.DOWN if dy > 0 else GestureDirection.UP
        else:
            if dx > 0 and dy < 0:
                return GestureDirection.UP_RIGHT
            elif dx > 0 and dy > 0:
                return GestureDirection.DOWN_RIGHT
            elif dx < 0 and dy < 0:
                return GestureDirection.UP_LEFT
            else:
                return GestureDirection.DOWN_LEFT

    @property
    def length(self) -> float:
        """Get total length of stroke."""
        if len(self.points) < 2:
            return 0.0

        total = 0.0
        for i in range(1, len(self.points)):
            dx = self.points[i][0] - self.points[i - 1][0]
            dy = self.points[i][1] - self.points[i - 1][1]
            total += math.sqrt(dx * dx + dy * dy)

        return total

    @property
    def duration(self) -> float:
        """Get duration of stroke in seconds."""
        if not self.end_time:
            return time.time() - self.start_time
        return self.end_time - self.start_time


@dataclass
class GestureBinding:
    """Binds a gesture pattern to an action."""

    pattern: List[GestureDirection]
    callback: Callable[[], Any]
    description: str = ""
    min_stroke_length: float = 50.0
    strict_mode: bool = False


@dataclass
class GestureConfig:
    """Configuration for mouse gesture recognition."""

    recognition_threshold: float = 0.7
    min_stroke_length: float = 50.0
    max_stroke_duration: float = 2.0
    stroke_timeout: float = 0.5
    enable_circle_detection: bool = True
    circle_threshold: float = 0.8
    track_velocity: bool = True


class MouseGestureRecognizer:
    """
    Recognizes mouse gestures and maps them to actions.

    Supports directional gestures, circle gestures,
    and custom gesture patterns with configurable sensitivity.
    """

    def __init__(
        self,
        config: Optional[GestureConfig] = None,
        action_executor: Optional[Callable[[str], None]] = None,
    ):
        self.config = config or GestureConfig()
        self.action_executor = action_executor or self._default_executor
        self._bindings: Dict[str, GestureBinding] = {}
        self._current_stroke: Optional[GestureStroke] = None
        self._is_tracking: bool = False
        self._velocity_history: deque = deque(maxlen=10)

    def _default_executor(self, gesture_name: str) -> None:
        """Default action executor."""
        logger.debug(f"Executing gesture action: {gesture_name}")

    def register_gesture(
        self,
        name: str,
        pattern: List[GestureDirection],
        callback: Callable[[], Any],
        description: str = "",
        min_length: float = 50.0,
    ) -> bool:
        """
        Register a gesture pattern.

        Args:
            name: Gesture identifier
            pattern: List of gesture directions
            callback: Function to call when gesture is recognized
            description: Human-readable description
            min_length: Minimum stroke length to trigger

        Returns:
            True if registration succeeded
        """
        binding = GestureBinding(
            pattern=pattern,
            callback=callback,
            description=description,
            min_stroke_length=min_length,
        )

        self._bindings[name] = binding
        logger.debug(f"Registered gesture: {name} ({len(pattern)} directions)")
        return True

    def start_tracking(self) -> None:
        """Start tracking mouse movement for gesture."""
        self._current_stroke = GestureStroke()
        self._is_tracking = True
        self._velocity_history.clear()

    def on_move(self, x: float, y: float) -> None:
        """
        Handle mouse move event.

        Args:
            x: X coordinate
            y: Y coordinate
        """
        if not self._is_tracking or not self._current_stroke:
            return

        if self._current_stroke.points:
            last_x, last_y = self._current_stroke.points[-1]
            distance = math.sqrt((x - last_x) ** 2 + (y - last_y) ** 2)

            if distance < 3:
                return

        if self.config.track_velocity and self._current_stroke.points:
            last_time = self._current_stroke.points[-1] if len(self._current_stroke.points) > 1 else time.time()
            self._velocity_history.append(distance / (time.time() - last_time))

        self._current_stroke.add_point(x, y)

    def end_tracking(self) -> Optional[str]:
        """
        End tracking and attempt to recognize gesture.

        Returns:
            Name of recognized gesture or None
        """
        if not self._is_tracking or not self._current_stroke:
            return None

        self._is_tracking = False
        self._current_stroke.close()

        if self._current_stroke.length < self.config.min_stroke_length:
            self._current_stroke = None
            return None

        for name, binding in self._bindings.items():
            if self._match_pattern(binding):
                try:
                    self.action_executor(name)
                    binding.callback()
                    return name
                except Exception as e:
                    logger.error(f"Gesture callback failed: {e}")

        self._current_stroke = None
        return None

    def _match_pattern(self, binding: GestureBinding) -> bool:
        """Check if current stroke matches binding pattern."""
        if not self._current_stroke or not self._current_stroke.direction:
            return False

        if self._current_stroke.length < binding.min_stroke_length:
            return False

        if len(binding.pattern) == 1:
            return self._current_stroke.direction == binding.pattern[0]

        score = self._compute_similarity(binding.pattern)
        return score >= self.config.recognition_threshold

    def _compute_similarity(
        self,
        pattern: List[GestureDirection],
    ) -> float:
        """Compute similarity between stroke and pattern."""
        if not self._current_stroke or not self._current_stroke.points:
            return 0.0

        if len(pattern) == 1:
            return 1.0 if self._current_stroke.direction == pattern[0] else 0.0

        angles = self._get_stroke_angles()

        if len(angles) < len(pattern):
            return 0.0

        total_diff = 0.0
        for i, expected in enumerate(pattern):
            actual = angles[i] if i < len(angles) else angles[-1]
            expected_rad = self._direction_to_angle(expected)
            diff = abs(actual - expected_rad)
            if diff > math.pi:
                diff = 2 * math.pi - diff
            total_diff += diff

        max_diff = len(pattern) * math.pi
        return 1.0 - (total_diff / max_diff)

    def _get_stroke_angles(self) -> List[float]:
        """Get angles between consecutive stroke segments."""
        if len(self._current_stroke.points) < 3:
            return []

        angles = []
        for i in range(1, len(self._current_stroke.points) - 1):
            p1 = self._current_stroke.points[i - 1]
            p2 = self._current_stroke.points[i]
            p3 = self._current_stroke.points[i + 1]

            angle1 = math.atan2(p2[1] - p1[1], p2[0] - p1[0])
            angle2 = math.atan2(p3[1] - p2[1], p3[0] - p2[0])

            diff = angle2 - angle1
            if diff > math.pi:
                diff -= 2 * math.pi
            elif diff < -math.pi:
                diff += 2 * math.pi

            angles.append(abs(diff))

        return angles

    def _direction_to_angle(self, direction: GestureDirection) -> float:
        """Convert direction to angle in radians."""
        angle_map = {
            GestureDirection.RIGHT: 0,
            GestureDirection.DOWN_RIGHT: math.pi / 4,
            GestureDirection.DOWN: math.pi / 2,
            GestureDirection.DOWN_LEFT: 3 * math.pi / 4,
            GestureDirection.LEFT: math.pi,
            GestureDirection.UP_LEFT: -3 * math.pi / 4,
            GestureDirection.UP: -math.pi / 2,
            GestureDirection.UP_RIGHT: -math.pi / 4,
        }
        return angle_map.get(direction, 0.0)

    def cancel_tracking(self) -> None:
        """Cancel current gesture tracking."""
        self._is_tracking = False
        self._current_stroke = None
        self._velocity_history.clear()

    def get_current_stroke(self) -> Optional[GestureStroke]:
        """Get the current stroke being tracked."""
        return self._current_stroke

    def get_average_velocity(self) -> float:
        """Get average mouse velocity."""
        if not self._velocity_history:
            return 0.0
        return sum(self._velocity_history) / len(self._velocity_history)

    def list_gestures(self) -> List[Dict[str, Any]]:
        """List all registered gestures."""
        return [
            {
                "name": name,
                "pattern": [d.value for d in binding.pattern],
                "description": binding.description,
                "min_length": binding.min_stroke_length,
            }
            for name, binding in self._bindings.items()
        ]

    def unregister_gesture(self, name: str) -> bool:
        """Unregister a gesture."""
        if name in self._bindings:
            del self._bindings[name]
            return True
        return False


def create_gesture_recognizer(
    config: Optional[GestureConfig] = None,
) -> MouseGestureRecognizer:
    """Factory function to create a MouseGestureRecognizer."""
    return MouseGestureRecognizer(config=config)
