"""
Gesture builder utilities for constructing complex multi-touch gestures.

Provides a fluent API for building gestures with points, durations,
pressure, and touch properties.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TouchPoint:
    """Single point in a touch gesture."""
    x: float
    y: float
    pressure: float = 1.0
    timestamp: float = 0.0
    finger_id: int = 0


@dataclass
class GestureSegment:
    """A segment of a gesture from one point to another."""
    from_point: TouchPoint
    to_point: TouchPoint
    duration_ms: float = 100.0
    easing: str = "linear"


@dataclass
class Gesture:
    """Complete gesture composed of multiple segments."""
    name: str = ""
    segments: list[GestureSegment] = field(default_factory=list)
    finger_count: int = 1
    metadata: dict = field(default_factory=dict)

    def add_segment(
        self,
        x1: float, y1: float,
        x2: float, y2: float,
        duration_ms: float = 100.0,
        pressure: float = 1.0,
        easing: str = "linear",
    ) -> Gesture:
        """Add a linear segment to the gesture."""
        from_pt = TouchPoint(x1, y1, pressure=pressure)
        to_pt = TouchPoint(x2, y2, pressure=pressure)
        segment = GestureSegment(from_pt, to_pt, duration_ms, easing)
        self.segments.append(segment)
        return self

    def total_duration_ms(self) -> float:
        return sum(s.duration_ms for s in self.segments)

    def get_all_points(self) -> list[TouchPoint]:
        """Get flat list of all touch points in order."""
        points = []
        for seg in self.segments:
            points.append(seg.from_point)
        if self.segments:
            points.append(self.segments[-1].to_point)
        return points


class GestureBuilder:
    """Fluent builder for creating gestures."""

    def __init__(self, name: str = ""):
        self._gesture = Gesture(name=name)

    def with_name(self, name: str) -> GestureBuilder:
        self._gesture.name = name
        return self

    def with_finger_count(self, count: int) -> GestureBuilder:
        self._gesture.finger_count = count
        return self

    def add_line(
        self, x1: float, y1: float, x2: float, y2: float,
        duration_ms: float = 100.0, pressure: float = 1.0
    ) -> GestureBuilder:
        """Add a straight line segment."""
        self._gesture.add_segment(x1, y1, x2, y2, duration_ms, pressure)
        return self

    def add_arc(
        self,
        cx: float, cy: float, radius: float,
        start_angle: float, end_angle: float,
        duration_ms: float = 200.0, steps: int = 20
    ) -> GestureBuilder:
        """Add an arc segment (approximated with line segments)."""
        import math
        angle_range = end_angle - start_angle
        angle_step = angle_range / steps

        prev_x = cx + radius * math.cos(start_angle)
        prev_y = cy + radius * math.sin(start_angle)

        for i in range(1, steps + 1):
            angle = start_angle + angle_step * i
            x = cx + radius * math.cos(angle)
            y = cy + radius * math.sin(angle)
            seg_duration = duration_ms / steps
            self._gesture.add_segment(prev_x, prev_y, x, y, seg_duration)
            prev_x, prev_y = x, y

        return self

    def add_circle(
        self, cx: float, cy: float, radius: float,
        duration_ms: float = 300.0, clockwise: bool = True
    ) -> GestureBuilder:
        """Add a circular gesture."""
        import math
        start_angle = 0.0
        end_angle = 2 * math.pi if clockwise else -2 * math.pi
        return self.add_arc(cx, cy, radius, start_angle, end_angle, duration_ms)

    def add_zigzag(
        self,
        x: float, y: float,
        amplitude: float, cycles: int,
        duration_ms: float = 300.0
    ) -> GestureBuilder:
        """Add a zigzag pattern."""
        step_ms = duration_ms / (cycles * 4)
        direction = 1
        for i in range(cycles * 2):
            nx = x + amplitude * direction
            self._gesture.add_segment(x, y, nx, y, step_ms)
            x = nx
            direction *= -1
            if i < cycles * 2 - 1:
                ny = y + amplitude if direction == 1 else y - amplitude
                self._gesture.add_segment(x, y, x, ny, step_ms)
                y = ny
        return self

    def set_metadata(self, key: str, value: any) -> GestureBuilder:
        self._gesture.metadata[key] = value
        return self

    def build(self) -> Gesture:
        return self._gesture

    def reset(self) -> GestureBuilder:
        self._gesture = Gesture()
        return self


# Common gesture presets
def build_pinch(factor: float, cx: float, cy: float, size: float) -> Gesture:
    """Build a pinch gesture (zoom in/out)."""
    half = size / 2
    builder = GestureBuilder("pinch").with_finger_count(2)
    if factor > 1:  # Zoom in
        builder.add_line(cx - half, cy, cx - half * factor, cy, 150)
        builder.add_line(cx + half, cy, cx + half * factor, cy, 150)
    else:  # Zoom out
        builder.add_line(cx - half, cy, cx + half * factor, cy, 150)
        builder.add_line(cx + half, cy, cx - half * factor, cy, 150)
    return builder.build()


def build_swipe(direction: str, x: float, y: float, distance: float, duration_ms: float = 200.0) -> Gesture:
    """Build a swipe gesture in the given direction (up/down/left/right)."""
    dirs = {"up": (0, -distance), "down": (0, distance), "left": (-distance, 0), "right": (distance, 0)}
    dx, dy = dirs.get(direction, (0, 0))
    return GestureBuilder("swipe").add_line(x, y, x + dx, y + dy, duration_ms).build()


__all__ = ["GestureBuilder", "Gesture", "GestureSegment", "TouchPoint", "build_pinch", "build_swipe"]
