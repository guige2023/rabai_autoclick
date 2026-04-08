"""
Touch Simulation Builder.

Build complex multi-touch gesture simulations using a fluent API.
Supports pinch, swipe, rotate, and compound gestures.

Usage:
    from utils.touch_simulation_builder import TouchSimBuilder, Gesture

    builder = TouchSimBuilder()
    gesture = builder.pinch(scale=0.5).at(x=100, y=100).duration(0.5).build()
    builder.play()
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Tuple, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum, auto

if TYPE_CHECKING:
    pass


class GestureType(Enum):
    """Types of touch gestures."""
    TAP = auto()
    DOUBLE_TAP = auto()
    LONG_PRESS = auto()
    SWIPE = auto()
    PINCH = auto()
    ROTATE = auto()
    DRAG = auto()
    COMPOUND = auto()


@dataclass
class TouchPoint:
    """A single touch point in a gesture."""
    x: float
    y: float
    pressure: float = 1.0
    timestamp: float = 0.0


@dataclass
class TouchStroke:
    """A stroke of touch points forming part of a gesture."""
    points: List[TouchPoint] = field(default_factory=list)
    finger_id: int = 0


@dataclass
class Gesture:
    """A complete gesture with one or more strokes."""
    gesture_type: GestureType
    strokes: List[TouchStroke]
    center: Optional[Tuple[float, float]] = None
    scale: float = 1.0
    rotation: float = 0.0
    duration: float = 0.5


class TouchSimBuilder:
    """
    Builder for complex touch gesture simulations.

    Provides a fluent API for constructing multi-touch gestures
    including pinch, rotate, swipe, and compound gestures.

    Example:
        gesture = (TouchSimBuilder()
            .pinch(scale=0.5)
            .at(100, 200)
            .duration(0.5)
            .build())
    """

    def __init__(self) -> None:
        self._gesture_type: GestureType = GestureType.TAP
        self._strokes: List[TouchStroke] = []
        self._center: Tuple[float, float] = (0, 0)
        self._scale: float = 1.0
        self._rotation: float = 0.0
        self._duration: float = 0.5
        self._steps: int = 10

    def tap(self) -> "TouchSimBuilder":
        """Set gesture type to single tap."""
        self._gesture_type = GestureType.TAP
        return self

    def double_tap(self) -> "TouchSimBuilder":
        """Set gesture type to double tap."""
        self._gesture_type = GestureType.DOUBLE_TAP
        return self

    def long_press(self) -> "TouchSimBuilder":
        """Set gesture type to long press."""
        self._gesture_type = GestureType.LONG_PRESS
        self._duration = 1.0
        return self

    def swipe(
        self,
        direction: str = "up",
        distance: float = 200,
    ) -> "TouchSimBuilder":
        """
        Configure a swipe gesture.

        Args:
            direction: "up", "down", "left", "right".
            distance: Distance in points.

        Returns:
            Self for chaining.
        """
        self._gesture_type = GestureType.SWIPE

        start = TouchPoint(x=0, y=0)
        end = TouchPoint(x=0, y=0)

        if direction == "up":
            start.y = distance / 2
            end.y = -distance / 2
        elif direction == "down":
            start.y = -distance / 2
            end.y = distance / 2
        elif direction == "left":
            start.x = distance / 2
            end.x = -distance / 2
        elif direction == "right":
            start.x = -distance / 2
            end.x = distance / 2

        stroke = TouchStroke(finger_id=0)
        stroke.points = [start, end]
        self._strokes = [stroke]

        return self

    def pinch(
        self,
        scale: float = 1.0,
    ) -> "TouchSimBuilder":
        """
        Configure a pinch gesture.

        Args:
            scale: Scale factor (0.5 = zoom out, 2.0 = zoom in).

        Returns:
            Self for chaining.
        """
        self._gesture_type = GestureType.PINCH
        self._scale = scale
        return self

    def rotate(
        self,
        degrees: float = 90,
    ) -> "TouchSimBuilder":
        """
        Configure a rotation gesture.

        Args:
            degrees: Rotation angle in degrees.

        Returns:
            Self for chaining.
        """
        self._gesture_type = GestureType.ROTATE
        self._rotation = degrees
        return self

    def drag(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float],
    ) -> "TouchSimBuilder":
        """
        Configure a drag gesture.

        Args:
            start: Starting (x, y) coordinates.
            end: Ending (x, y) coordinates.

        Returns:
            Self for chaining.
        """
        self._gesture_type = GestureType.DRAG

        stroke = TouchStroke(finger_id=0)
        stroke.points = [
            TouchPoint(x=start[0], y=start[1]),
            TouchPoint(x=end[0], y=end[1]),
        ]
        self._strokes = [stroke]
        return self

    def at(
        self,
        x: float,
        y: float,
    ) -> "TouchSimBuilder":
        """
        Set the gesture center point.

        Args:
            x: X coordinate of center.
            y: Y coordinate of center.

        Returns:
            Self for chaining.
        """
        self._center = (x, y)
        return self

    def duration(
        self,
        seconds: float,
    ) -> "TouchSimBuilder":
        """
        Set the gesture duration.

        Args:
            seconds: Duration in seconds.

        Returns:
            Self for chaining.
        """
        self._duration = seconds
        return self

    def steps(
        self,
        n: int,
    ) -> "TouchSimBuilder":
        """
        Set the number of interpolation steps.

        Args:
            n: Number of steps.

        Returns:
            Self for chaining.
        """
        self._steps = n
        return self

    def build(self) -> Gesture:
        """
        Build the gesture object.

        Returns:
            Gesture object ready to play.
        """
        if self._gesture_type in (GestureType.PINCH, GestureType.ROTATE):
            self._build_pinch_rotate_strokes()

        return Gesture(
            gesture_type=self._gesture_type,
            strokes=self._strokes,
            center=self._center,
            scale=self._scale,
            rotation=self._rotation,
            duration=self._duration,
        )

    def _build_pinch_rotate_strokes(self) -> None:
        """Build pinch/rotate two-finger strokes."""
        cx, cy = self._center
        separation = 100.0

        angle = self._rotation * 3.14159 / 180.0

        finger1_x = cx - separation * 0.5
        finger1_y = cy
        finger2_x = cx + separation * 0.5
        finger2_y = cy

        if self._gesture_type == GestureType.PINCH:
            factor = self._scale
            finger1_x = cx - separation * factor * 0.5
            finger2_x = cx + separation * factor * 0.5

        stroke1 = TouchStroke(finger_id=0)
        stroke2 = TouchStroke(finger_id=1)

        import time
        now = time.time()

        for i in range(self._steps + 1):
            t = i / self._steps
            p1x = finger1_x
            p1y = finger1_y
            p2x = finger2_x
            p2y = finger2_y

            stroke1.points.append(TouchPoint(x=p1x, y=p1y, timestamp=now + t * self._duration))
            stroke2.points.append(TouchPoint(x=p2x, y=p2y, timestamp=now + t * self._duration))

        self._strokes = [stroke1, stroke2]

    def play(self) -> bool:
        """
        Build and play the gesture.

        Returns:
            True if successful.
        """
        gesture = self.build()
        return self._execute_gesture(gesture)

    def _execute_gesture(self, gesture: Gesture) -> bool:
        """Execute a gesture (requires platform implementation)."""
        return True


def interpolate_points(
    start: Tuple[float, float],
    end: Tuple[float, float],
    steps: int,
) -> List[Tuple[float, float]]:
    """
    Interpolate points between start and end.

    Args:
        start: Starting (x, y).
        end: Ending (x, y).
        steps: Number of interpolation steps.

    Returns:
        List of interpolated (x, y) points.
    """
    points = []
    for i in range(steps + 1):
        t = i / steps
        x = start[0] + (end[0] - start[0]) * t
        y = start[1] + (end[1] - start[1]) * t
        points.append((x, y))
    return points
