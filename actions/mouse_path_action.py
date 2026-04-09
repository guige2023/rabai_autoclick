"""
Mouse Path Utilities Action Module

Provides mouse path generation and manipulation utilities for UI automation
workflows. Supports bezier curves, waypoints, gestures, and path smoothing.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import Quartz


@dataclass
class Point:
    """Represents a 2D point."""
    x: float
    y: float

    def distance_to(self, other: "Point") -> float:
        """Calculate distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def __add__(self, other: "Point") -> "Point":
        return Point(self.x + other.x, self.y + other.y)

    def __sub__(self, other: "Point") -> "Point":
        return Point(self.x - other.x, self.y - other.y)

    def __mul__(self, scalar: float) -> "Point":
        return Point(self.x * scalar, self.y * scalar)

    def __repr__(self) -> str:
        return f"Point({self.x:.1f}, {self.y:.1f})"


@dataclass
class MouseEvent:
    """Represents a mouse event."""
    x: float
    y: float
    event_type: str = "move"
    button: int = 0
    timestamp: float = 0.0


class BezierCurve:
    """Bezier curve for smooth path generation."""

    @staticmethod
    def cubic(
        p0: Point,
        p1: Point,
        p2: Point,
        p3: Point,
        segments: int = 20,
    ) -> list[Point]:
        """Generate cubic bezier curve points."""
        points = []
        for i in range(segments + 1):
            t = i / segments
            x = (
                p0.x * (1 - t) ** 3
                + 3 * p1.x * (1 - t) ** 2 * t
                + 3 * p2.x * (1 - t) * t**2
                + p3.x * t**3
            )
            y = (
                p0.y * (1 - t) ** 3
                + 3 * p1.y * (1 - t) ** 2 * t
                + 3 * p2.y * (1 - t) * t**2
                + p3.y * t**3
            )
            points.append(Point(x, y))
        return points

    @staticmethod
    def quadratic(
        p0: Point,
        p1: Point,
        p2: Point,
        segments: int = 20,
    ) -> list[Point]:
        """Generate quadratic bezier curve points."""
        points = []
        for i in range(segments + 1):
            t = i / segments
            x = (1 - t) ** 2 * p0.x + 2 * (1 - t) * t * p1.x + t**2 * p2.x
            y = (1 - t) ** 2 * p0.y + 2 * (1 - t) * t * p1.y + t**2 * p2.y
            points.append(Point(x, y))
        return points


class PathSmoother:
    """Smooths raw mouse paths."""

    @staticmethod
    def smooth(path: list[Point], factor: float = 0.25) -> list[Point]:
        """Apply Chaikin's corner-cutting algorithm."""
        if len(path) < 3:
            return path

        smoothed = [path[0]]

        for i in range(len(path) - 1):
            p0 = path[i]
            p1 = path[i + 1]

            q = Point(
                p0.x + factor * (p1.x - p0.x),
                p0.y + factor * (p1.y - p0.y),
            )
            r = Point(
                p1.x - factor * (p1.x - p0.x),
                p1.y - factor * (p1.y - p0.y),
            )

            smoothed.append(q)
            smoothed.append(r)

        smoothed.append(path[-1])
        return smoothed

    @staticmethod
    def interpolate(
        path: list[Point],
        points_per_segment: int = 10,
    ) -> list[Point]:
        """Interpolate points along path."""
        if len(path) < 2:
            return path

        result = [path[0]]

        for i in range(len(path) - 1):
            p0 = path[i]
            p1 = path[i + 1]

            for j in range(1, points_per_segment + 1):
                t = j / points_per_segment
                x = p0.x + t * (p1.x - p0.x)
                y = p0.y + t * (p1.y - p0.y)
                result.append(Point(x, y))

        return result


class MousePathGenerator:
    """
    Generates mouse paths for automation.

    Example:
        >>> gen = MousePathGenerator()
        >>> path = gen.linear_path(Point(100, 100), Point(500, 500))
    """

    def __init__(self, screen_bounds: Optional[dict] = None) -> None:
        self.screen_bounds = screen_bounds or {}

    def linear_path(
        self,
        start: Point,
        end: Point,
        steps: int = 20,
    ) -> list[Point]:
        """Generate linear path between points."""
        path = []
        for i in range(steps + 1):
            t = i / steps
            x = start.x + t * (end.x - start.x)
            y = start.y + t * (end.y - start.y)
            path.append(Point(x, y))
        return path

    def curved_path(
        self,
        start: Point,
        end: Point,
        control_offset: float = 100,
        segments: int = 30,
    ) -> list[Point]:
        """Generate curved bezier path."""
        mid_x = (start.x + end.x) / 2
        mid_y = (start.y + end.y) / 2

        dx = end.x - start.x
        dy = end.y - start.y
        angle = math.atan2(dy, dx) + math.pi / 2

        cp = Point(
            mid_x + control_offset * math.cos(angle),
            mid_y + control_offset * math.sin(angle),
        )

        return BezierCurve.quadratic(start, cp, end, segments)

    def ease_path(
        self,
        start: Point,
        end: Point,
        ease_type: str = "ease_in_out",
        steps: int = 20,
    ) -> list[Point]:
        """Generate eased path."""
        path = []
        for i in range(steps + 1):
            t = i / steps
            t = self._ease(t, ease_type)
            x = start.x + t * (end.x - start.x)
            y = start.y + t * (end.y - start.y)
            path.append(Point(x, y))
        return path

    @staticmethod
    def _ease(t: float, ease_type: str) -> float:
        """Apply easing function."""
        if ease_type == "ease_in":
            return t * t
        if ease_type == "ease_out":
            return t * (2 - t)
        if ease_type == "ease_in_out":
            return t * t * (3 - 2 * t)
        if ease_type == "ease_in_cubic":
            return t ** 3
        if ease_type == "ease_out_cubic":
            return 1 - (1 - t) ** 3
        if ease_type == "ease_in_out_cubic":
            return t ** 3 if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2
        if ease_type == "linear":
            return t
        return t

    def circle_path(
        self,
        center: Point,
        radius: float,
        start_angle: float = 0,
        end_angle: float = 2 * math.pi,
        steps: int = 36,
    ) -> list[Point]:
        """Generate circular path."""
        path = []
        for i in range(steps + 1):
            t = i / steps
            angle = start_angle + t * (end_angle - start_angle)
            x = center.x + radius * math.cos(angle)
            y = center.y + radius * math.sin(angle)
            path.append(Point(x, y))
        return path

    def zigzag_path(
        self,
        start: Point,
        end: Point,
        amplitude: float = 50,
        periods: int = 5,
    ) -> list[Point]:
        """Generate zigzag path."""
        path = []
        for i in range(periods * 2 + 1):
            t = i / (periods * 2)
            x = start.x + t * (end.x - start.x)
            y = start.y + t * (end.y - start.y)

            if i % 2 == 1:
                dx = end.x - start.x
                dy = end.y - start.y
                angle = math.atan2(dy, dx) + math.pi / 2
                x += amplitude * math.cos(angle)
                y += amplitude * math.sin(angle)

            path.append(Point(x, y))
        return path

    def bezier_path(
        self,
        points: list[Point],
        segments_per_curve: int = 20,
    ) -> list[Point]:
        """Generate smooth bezier path through waypoints."""
        if len(points) < 2:
            return points
        if len(points) == 2:
            return self.linear_path(points[0], points[1], segments_per_curve)

        result = []
        for i in range(len(points) - 1):
            p0 = points[max(0, i - 1)]
            p1 = points[i]
            p2 = points[i + 1]
            p3 = points[min(len(points) - 1, i + 2)]

            curve = BezierCurve.cubic(p1, p2, p2, p3, segments_per_curve)
            result.extend(curve[:-1])

        result.append(points[-1])
        return result


class MousePathExecutor:
    """Executes mouse paths with timing."""

    def __init__(self, generator: Optional[MousePathGenerator] = None) -> None:
        self.generator = generator or MousePathGenerator()

    def execute_linear(
        self,
        start: Point,
        end: Point,
        duration: float = 0.5,
    ) -> list[MouseEvent]:
        """Execute linear mouse movement."""
        steps = max(1, int(duration * 60))
        path = self.generator.linear_path(start, end, steps)
        return self._create_events(path, duration)

    def execute_curved(
        self,
        start: Point,
        end: Point,
        duration: float = 0.5,
        control_offset: float = 100,
    ) -> list[MouseEvent]:
        """Execute curved mouse movement."""
        path = self.generator.curved_path(start, end, control_offset, len(path))
        return self._create_events(path, duration)

    def execute_path(
        self,
        path: list[Point],
        duration: float = 0.5,
    ) -> list[MouseEvent]:
        """Execute predefined path."""
        return self._create_events(path, duration)

    def _create_events(self, path: list[Point], duration: float) -> list[MouseEvent]:
        """Create mouse events from path."""
        if not path:
            return []

        events = []
        interval = duration / len(path)

        for i, point in enumerate(path):
            event = MouseEvent(
                x=point.x,
                y=point.y,
                event_type="move",
                timestamp=i * interval,
            )
            events.append(event)

        return events


class MouseController:
    """
    Low-level mouse control using Quartz.

    Example:
        >>> controller = MouseController()
        >>> controller.move_to(100, 100)
        >>> controller.click(200, 200)
    """

    def __init__(self) -> None:
        self._event_source = Quartz.CGEventSourceCreate(Quartz.kCGEventSourceStateCombinedSessionState)

    def move_to(self, x: float, y: float) -> None:
        """Move mouse to position."""
        point = Quartz.CGPoint(x, y)
        event = Quartz.CGEventCreateMouseEvent(
            None,
            Quartz.kCGEventMouseMoved,
            point,
            Quartz.kCGMouseButtonLeft,
        )
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)

    def click(
        self,
        x: float,
        y: float,
        button: int = Quartz.kCGMouseButtonLeft,
        double: bool = False,
    ) -> None:
        """Click at position."""
        point = Quartz.CGPoint(x, y)

        down_event = Quartz.CGEventCreateMouseEvent(
            None,
            Quartz.kCGEventLeftMouseDown,
            point,
            button,
        )
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, down_event)

        up_event = Quartz.CGEventCreateMouseEvent(
            None,
            Quartz.kCGEventLeftMouseUp,
            point,
            button,
        )
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, up_event)

        if double:
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, down_event)
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, up_event)

    def drag(self, start: tuple, end: tuple) -> None:
        """Drag from start to end."""
        start_point = Quartz.CGPoint(*start)
        end_point = Quartz.CGPoint(*end)

        down_event = Quartz.CGEventCreateMouseEvent(
            None,
            Quartz.kCGEventLeftMouseDown,
            start_point,
            Quartz.kCGMouseButtonLeft,
        )
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, down_event)

        for i in range(1, 11):
            t = i / 10
            x = start[0] + t * (end[0] - start[0])
            y = start[1] + t * (end[1] - start[1])
            point = Quartz.CGPoint(x, y)

            drag_event = Quartz.CGEventCreateMouseEvent(
                None,
                Quartz.kCGEventLeftMouseDragged,
                point,
                Quartz.kCGMouseButtonLeft,
            )
            Quartz.CGEventPost(Quartz.kCGHIDEventTap, drag_event)

        up_event = Quartz.CGEventCreateMouseEvent(
            None,
            Quartz.kCGEventLeftMouseUp,
            end_point,
            Quartz.kCGMouseButtonLeft,
        )
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, up_event)

    def scroll(self, x: float, y: float, delta_x: int, delta_y: int) -> None:
        """Scroll at position."""
        point = Quartz.CGPoint(x, y)

        event = Quartz.CGEventCreateScrollWheelEvent(
            None,
            Quartz.kCGScrollEventUnitLine,
            2,
            delta_y,
            delta_x,
        )
        Quartz.CGEventPost(Quartz.kCGHIDEventTap, event)
