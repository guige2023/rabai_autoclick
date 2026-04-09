"""
Automation Touch Gesture Action Module

Simulates touch gestures for mobile automation.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from enum import Enum
import time
import math


class GestureType(Enum):
    """Touch gesture types."""
    TAP = "tap"
    DOUBLE_TAP = "double_tap"
    LONG_PRESS = "long_press"
    SWIPE = "swipe"
    DRAG = "drag"
    PINCH = "pinch"
    PAN = "pan"
    ROTATE = "rotate"
    MULTI_TOUCH = "multi_touch"


@dataclass
class TouchPoint:
    """Single touch point."""
    x: float
    y: float
    pressure: float = 1.0
    timestamp: float = 0.0


@dataclass
class GestureConfig:
    """Gesture execution configuration."""
    duration_ms: float = 200
    steps: int = 20
    pressure: float = 1.0
    interpolation: str = "linear"  # linear, ease_in, ease_out, ease_in_out
    hold_duration_ms: float = 500


@dataclass
class GesturePath:
    """Path of a gesture."""
    points: List[TouchPoint]
    gesture_type: GestureType
    start_time: float = 0.0


class BezierInterpolator:
    """Bezier curve interpolator for smooth gestures."""

    @staticmethod
    def cubic(p0: float, p1: float, p2: float, p3: float, t: float) -> float:
        """Cubic Bezier interpolation."""
        u = 1 - t
        return u*u*u*p0 + 3*u*u*t*p1 + 3*u*t*t*p2 + t*t*t*p3

    @staticmethod
    def ease_in(t: float) -> float:
        return t * t * t

    @staticmethod
    def ease_out(t: float) -> float:
        return 1 - (1 - t) ** 3

    @staticmethod
    def ease_in_out(t: float) -> float:
        return 2 * t * t if t < 0.5 else 1 - ((-2 * t + 2) ** 3) / 2


class GestureRecognizer:
    """Recognizes gesture type from touch points."""

    TAP_THRESHOLD_DIST = 50
    TAP_THRESHOLD_TIME = 0.3
    LONG_PRESS_THRESHOLD = 0.5
    SWIPE_THRESHOLD_DIST = 100

    @classmethod
    def recognize(
        cls,
        points: List[TouchPoint],
        duration: float
    ) -> GestureType:
        """Recognize gesture type from points."""
        if len(points) < 1:
            return GestureType.TAP

        start = points[0]
        end = points[-1]

        dx = end.x - start.x
        dy = end.y - start.y
        distance = math.sqrt(dx*dx + dy*dy)
        elapsed = points[-1].timestamp - points[0].timestamp

        if len(points) == 2 and distance < cls.TAP_THRESHOLD_DIST:
            if elapsed < cls.TAP_THRESHOLD_TIME:
                return GestureType.TAP
            elif elapsed > cls.LONG_PRESS_THRESHOLD:
                return GestureType.LONG_PRESS

        if distance > cls.SWIPE_THRESHOLD_DIST:
            return GestureType.SWIPE

        return GestureType.DRAG


class TouchGestureSimulator:
    """Simulates touch gestures on screen."""

    def __init__(self, screen_width: int = 1080, screen_height: int = 1920):
        self.screen_width = screen_width
        self.screen_height = screen_height
        self.active_touches: Dict[int, TouchPoint] = {}
        self.gesture_history: List[GesturePath] = []
        self.config = GestureConfig()

    def set_config(self, config: GestureConfig) -> None:
        """Set gesture configuration."""
        self.config = config

    def _interpolate_point(
        self,
        p1: TouchPoint,
        p2: TouchPoint,
        t: float
    ) -> TouchPoint:
        """Interpolate between two points."""
        interp = BezierInterpolator()

        if self.config.interpolation == "ease_in":
            t = interp.ease_in(t)
        elif self.config.interpolation == "ease_out":
            t = interp.ease_out(t)
        elif self.config.interpolation == "ease_in_out":
            t = interp.ease_in_out(t)

        return TouchPoint(
            x=p1.x + (p2.x - p1.x) * t,
            y=p1.y + (p2.y - p1.y) * t,
            pressure=p1.pressure + (p2.pressure - p1.pressure) * t
        )

    def _generate_path(
        self,
        start: Tuple[float, float],
        end: Tuple[float, float]
    ) -> List[TouchPoint]:
        """Generate interpolated path between two points."""
        points = []
        start_time = time.time()

        for i in range(self.config.steps + 1):
            t = i / self.config.steps
            p = self._interpolate_point(
                TouchPoint(start[0], start[1], timestamp=0),
                TouchPoint(end[0], end[1], timestamp=1),
                t
            )
            p.timestamp = start_time + (self.config.duration_ms / 1000) * t
            points.append(p)

        return points

    def tap(
        self,
        x: float,
        y: float,
        pressure: float = 1.0
    ) -> GesturePath:
        """Simulate a tap gesture."""
        points = [TouchPoint(x, y, pressure, time.time())]
        path = GesturePath(points, GestureType.TAP, time.time())
        self.gesture_history.append(path)
        return path

    def double_tap(
        self,
        x: float,
        y: float,
        interval_ms: float = 200
    ) -> GesturePath:
        """Simulate a double tap gesture."""
        t1 = TouchPoint(x, y, 1.0, time.time())
        t2 = TouchPoint(x, y, 1.0, time.time() + interval_ms / 1000)
        points = [t1, t2]
        path = GesturePath(points, GestureType.DOUBLE_TAP, time.time())
        self.gesture_history.append(path)
        return path

    def long_press(
        self,
        x: float,
        y: float,
        duration_ms: Optional[float] = None
    ) -> GesturePath:
        """Simulate a long press gesture."""
        duration = duration_ms or self.config.hold_duration_ms
        start_time = time.time()
        points = [
            TouchPoint(x, y, 1.0, start_time),
            TouchPoint(x, y, 1.0, start_time + duration / 1000)
        ]
        path = GesturePath(points, GestureType.LONG_PRESS, start_time)
        self.gesture_history.append(path)
        return path

    def swipe(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration_ms: Optional[float] = None
    ) -> GesturePath:
        """Simulate a swipe gesture."""
        duration = duration_ms or self.config.duration_ms
        original_duration = self.config.duration_ms
        self.config.duration_ms = duration

        path_points = self._generate_path((start_x, start_y), (end_x, end_y))
        self.config.duration_ms = original_duration

        path = GesturePath(path_points, GestureType.SWIPE, time.time())
        self.gesture_history.append(path)
        return path

    def drag(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration_ms: Optional[float] = None
    ) -> GesturePath:
        """Simulate a drag gesture (slower than swipe)."""
        duration = duration_ms or (self.config.duration_ms * 2)
        original_duration = self.config.duration_ms
        self.config.duration_ms = duration
        self.config.steps = 40

        path_points = self._generate_path((start_x, start_y), (end_x, end_y))

        self.config.duration_ms = original_duration
        self.config.steps = 20

        path = GesturePath(path_points, GestureType.DRAG, time.time())
        self.gesture_history.append(path)
        return path

    def pinch(
        self,
        center_x: float,
        center_y: float,
        scale: float = 1.5
    ) -> GesturePath:
        """Simulate a pinch gesture (zoom out) or spread (zoom in)."""
        offset = 100 * scale
        touch1_start = TouchPoint(center_x - offset, center_y, 1.0, time.time())
        touch1_end = TouchPoint(center_x + offset, center_y, 1.0, time.time() + 0.3)
        touch2_start = TouchPoint(center_x + offset, center_y, 1.0, time.time())
        touch2_end = TouchPoint(center_x - offset, center_y, 1.0, time.time() + 0.3)

        path = GesturePath([touch1_start, touch1_end, touch2_start, touch2_end], GestureType.PINCH, time.time())
        self.gesture_history.append(path)
        return path

    def rotate(
        self,
        center_x: float,
        center_y: float,
        angle_degrees: float = 90
    ) -> GesturePath:
        """Simulate a rotation gesture."""
        radius = 80
        start_angle = 0
        end_angle = math.radians(angle_degrees)

        points = []
        for i in range(self.config.steps + 1):
            t = i / self.config.steps
            angle = start_angle + (end_angle - start_angle) * t
            x = center_x + radius * math.cos(angle)
            y = center_y + radius * math.sin(angle)
            points.append(TouchPoint(x, y, 1.0, time.time() + t * 0.3))

        path = GesturePath(points, GestureType.ROTATE, time.time())
        self.gesture_history.append(path)
        return path

    def multi_touch(
        self,
        touch_points: List[Tuple[float, float]]
    ) -> GesturePath:
        """Simulate multi-touch gesture."""
        points = [TouchPoint(x, y, 1.0, time.time()) for x, y in touch_points]
        path = GesturePath(points, GestureType.MULTI_TOUCH, time.time())
        self.gesture_history.append(path)
        return path

    def execute_custom_path(
        self,
        points: List[Tuple[float, float]],
        gesture_type: GestureType = GestureType.PAN
    ) -> GesturePath:
        """Execute a custom path with given points."""
        touch_points = [TouchPoint(x, y, 1.0, time.time() + i * 0.05) for i, (x, y) in enumerate(points)]
        path = GesturePath(touch_points, gesture_type, time.time())
        self.gesture_history.append(path)
        return path

    def replay_gesture(self, gesture_path: GesturePath) -> GesturePath:
        """Replay a previously recorded gesture."""
        path = GesturePath(
            points=gesture_path.points.copy(),
            gesture_type=gesture_path.gesture_type,
            start_time=time.time()
        )
        self.gesture_history.append(path)
        return path

    def get_gesture_history(self) -> List[Dict[str, Any]]:
        """Get history of executed gestures."""
        return [
            {
                "type": g.gesture_type.value,
                "duration_ms": (g.points[-1].timestamp - g.start_time) * 1000 if len(g.points) > 1 else 0,
                "point_count": len(g.points),
                "start": (g.points[0].x, g.points[0].y) if g.points else (0, 0),
                "end": (g.points[-1].x, g.points[-1].y) if g.points else (0, 0)
            }
            for g in self.gesture_history
        ]

    def clear_history(self) -> None:
        """Clear gesture history."""
        self.gesture_history.clear()


class AutomationTouchGestureAction:
    """
    Touch gesture simulation for mobile automation.

    Example:
        sim = AutomationTouchGestureAction(screen_width=1080, screen_height=1920)
        sim.tap(540, 960)  # Tap center
        sim.swipe(540, 1600, 540, 400)  # Swipe up
        sim.pinch(540, 960, scale=1.5)  # Pinch zoom
    """

    def __init__(self, screen_width: int = 1080, screen_height: int = 1920):
        self.simulator = TouchGestureSimulator(screen_width, screen_height)

    def tap(self, x: float, y: float) -> Dict[str, Any]:
        path = self.simulator.tap(x, y)
        return {"type": "tap", "x": x, "y": y}

    def double_tap(self, x: float, y: float) -> Dict[str, Any]:
        path = self.simulator.double_tap(x, y)
        return {"type": "double_tap", "x": x, "y": y}

    def long_press(self, x: float, y: float, duration_ms: float = 500) -> Dict[str, Any]:
        path = self.simulator.long_press(x, y, duration_ms)
        return {"type": "long_press", "x": x, "y": y, "duration_ms": duration_ms}

    def swipe(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration_ms: float = 200
    ) -> Dict[str, Any]:
        path = self.simulator.swipe(start_x, start_y, end_x, end_y, duration_ms)
        return {"type": "swipe", "start": (start_x, start_y), "end": (end_x, end_y), "duration_ms": duration_ms}

    def drag(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration_ms: float = 400
    ) -> Dict[str, Any]:
        path = self.simulator.drag(start_x, start_y, end_x, end_y, duration_ms)
        return {"type": "drag", "start": (start_x, start_y), "end": (end_x, end_y), "duration_ms": duration_ms}

    def pinch(self, x: float, y: float, scale: float = 1.5) -> Dict[str, Any]:
        path = self.simulator.pinch(x, y, scale)
        return {"type": "pinch", "center": (x, y), "scale": scale}

    def rotate(self, x: float, y: float, angle_degrees: float = 90) -> Dict[str, Any]:
        path = self.simulator.rotate(x, y, angle_degrees)
        return {"type": "rotate", "center": (x, y), "angle_degrees": angle_degrees}

    def get_history(self) -> List[Dict[str, Any]]:
        return self.simulator.get_gesture_history()

    def clear_history(self) -> None:
        self.simulator.clear_history()
