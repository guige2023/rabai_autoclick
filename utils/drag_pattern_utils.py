"""
Drag pattern utilities for common drag gesture patterns.

Provides pre-built drag patterns for common interactions like
scroll, drag-drop, slider, and edge swipes.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable


@dataclass
class DragPattern:
    """A complete drag pattern."""
    name: str
    points: list[tuple[float, float]]
    durations_ms: list[float]
    finger_count: int = 1

    def total_duration_ms(self) -> float:
        return sum(self.durations_ms)


def linear_drag(
    x1: float, y1: float,
    x2: float, y2: float,
    duration_ms: float = 200.0,
    steps: int = 10
) -> DragPattern:
    """Create a straight-line drag pattern."""
    points = []
    durations = []
    for i in range(steps + 1):
        t = i / steps
        px = x1 + (x2 - x1) * t
        py = y1 + (y2 - y1) * t
        points.append((px, py))
    step_ms = duration_ms / steps
    durations = [step_ms] * steps
    return DragPattern("linear", points, durations)


def curved_drag(
    x1: float, y1: float,
    x2: float, y2: float,
    cx: float, cy: float,
    duration_ms: float = 300.0,
    steps: int = 20
) -> DragPattern:
    """Create a bezier-curve drag pattern."""
    points = []
    for i in range(steps + 1):
        t = i / steps
        # Quadratic bezier: (1-t)^2*P0 + 2(1-t)t*P1 + t^2*P2
        px = (1-t)**2 * x1 + 2*(1-t)*t * cx + t**2 * x2
        py = (1-t)**2 * y1 + 2*(1-t)*t * cy + t**2 * y2
        points.append((px, py))
    step_ms = duration_ms / steps
    durations = [step_ms] * steps
    return DragPattern("bezier", points, durations)


def arc_drag(
    x1: float, y1: float,
    x2: float, y2: float,
    curvature: float = 0.5,
    duration_ms: float = 250.0,
    steps: int = 20
) -> DragPattern:
    """Create an arc drag pattern."""
    mx = (x1 + x2) / 2
    my = (y1 + y2) / 2
    dx = x2 - x1
    dy = y2 - y1
    # Perpendicular offset for curvature
    cx = mx - dy * curvature
    cy = my + dx * curvature
    return curved_drag(x1, y1, x2, y2, cx, cy, duration_ms, steps)


def scroll_drag(
    start_x: float, start_y: float,
    distance: float,
    direction: str = "down",
    duration_ms: float = 300.0,
    steps: int = 15,
    easing: str = "ease_out"
) -> DragPattern:
    """Create a scroll-like drag with deceleration."""
    dir_map = {"up": (0, -1), "down": (0, 1), "left": (-1, 0), "right": (1, 0)}
    dx, dy = dir_map.get(direction, (0, 1))

    end_x = start_x + dx * distance
    end_y = start_y + dy * distance

    points = []
    for i in range(steps + 1):
        t = i / steps
        if easing == "ease_out":
            t = 1 - (1 - t) ** 2
        px = start_x + (end_x - start_x) * t
        py = start_y + (end_y - start_y) * t
        points.append((px, py))

    durations = [duration_ms / steps] * steps
    return DragPattern(f"scroll_{direction}", points, durations)


def edge_swipe(
    screen_width: float, screen_height: float,
    edge: str = "left",
    distance: float = 300.0,
    duration_ms: float = 200.0,
    steps: int = 15
) -> DragPattern:
    """Create an edge swipe gesture from screen edge."""
    margin = 50
    if edge == "left":
        x1, y1 = margin, screen_height / 2
        x2, y2 = x1 + distance, y1
    elif edge == "right":
        x1, y1 = screen_width - margin, screen_height / 2
        x2, y2 = x1 - distance, y1
    elif edge == "top":
        x1, y1 = screen_width / 2, margin
        x2, y2 = x1, y1 + distance
    elif edge == "bottom":
        x1, y1 = screen_width / 2, screen_height - margin
        x2, y2 = x1, y1 - distance
    else:
        x1, y1, x2, y2 = 0, 0, 0, 0

    return linear_drag(x1, y1, x2, y2, duration_ms, steps)


def drag_drop(
    drag_x: float, drag_y: float,
    drop_x: float, drop_y: float,
    drag_duration_ms: float = 100.0,
    hold_duration_ms: float = 100.0,
    drop_duration_ms: float = 150.0,
    steps: int = 10
) -> DragPattern:
    """Create a drag-and-drop pattern with hold time at end."""
    drag_points = []
    for i in range(steps + 1):
        t = i / steps
        px = drag_x + (drop_x - drag_x) * t
        py = drag_y + (drop_y - drag_y) * t
        drag_points.append((px, py))

    step_ms = drag_duration_ms / steps
    durations = [step_ms] * steps
    durations.append(hold_duration_ms)  # Hold at drop point
    durations.append(step_ms)  # Release

    return DragPattern("drag_drop", drag_points, durations)


def slider_pattern(
    track_x: float, track_y: float,
    track_width: float,
    initial_value: float = 0.0,
    target_value: float = 1.0,
    duration_ms: float = 300.0,
    steps: int = 20
) -> DragPattern:
    """Create a slider drag pattern."""
    x1 = track_x + initial_value * track_width
    x2 = track_x + target_value * track_width

    points = []
    for i in range(steps + 1):
        t = i / steps
        # Ease in-out
        t = t * 2 - 1 if t > 0.5 else 4 * t * t
        px = x1 + (x2 - x1) * t
        points.append((px, track_y))

    step_ms = duration_ms / steps
    durations = [step_ms] * steps
    return DragPattern("slider", points, durations)


__all__ = [
    "DragPattern", "linear_drag", "curved_drag", "arc_drag",
    "scroll_drag", "edge_swipe", "drag_drop", "slider_pattern"
]
