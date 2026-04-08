"""Drag and drop utilities for advanced mouse gesture automation.

Provides high-level drag-and-drop operations including
cross-application drags, multi-step drags, and
gesture-based drag detection.

Example:
    >>> from utils.drag_drop_utils import drag_and_drop, smooth_drag
    >>> drag_and_drop((100, 100), (500, 300))
    >>> smooth_drag((100, 100), (500, 300), duration=0.5)
"""

from __future__ import annotations

import time
from typing import Callable, Optional

__all__ = [
    "drag_and_drop",
    "smooth_drag",
    "drag_by_offset",
    "DropError",
]


class DropError(Exception):
    """Raised when a drag operation fails."""
    pass


def _get_input_sim():
    try:
        from utils.input_simulation_utils import (
            drag as _drag,
            click,
        )
        return _drag, click
    except ImportError:
        return None, None


def drag_and_drop(
    start: tuple[float, float],
    end: tuple[float, float],
    duration: float = 0.3,
    button: str = "left",
) -> bool:
    """Perform a basic drag from start to end position.

    Args:
        start: Starting (x, y) coordinates.
        end: Ending (x, y) coordinates.
        duration: Drag duration in seconds.
        button: Mouse button to use ('left' or 'right').

    Returns:
        True if successful.
    """
    _drag, _ = _get_input_sim()
    if _drag is None:
        return False

    try:
        _drag(start[0], start[1], end[0], end[1], duration=duration)
        return True
    except Exception:
        return False


def smooth_drag(
    start: tuple[float, float],
    end: tuple[float, float],
    duration: float = 0.5,
    steps: int = 30,
    easing: Optional[Callable[[float], float]] = None,
) -> bool:
    """Perform a smooth drag with interpolation.

    Args:
        start: Starting (x, y) coordinates.
        end: Ending (x, y) coordinates.
        duration: Total drag duration in seconds.
        steps: Number of intermediate positions.
        easing: Easing function (default: quadratic ease-in-out).

    Returns:
        True if successful.
    """
    _drag, _ = _get_input_sim()
    if _drag is None:
        return False

    if easing is None:
        def ease_in_out(t):
            if t < 0.5:
                return 2 * t * t
            return -1 + (4 - 2 * t) * t
        easing = ease_in_out

    x0, y0 = start
    x1, y1 = end

    interval = duration / steps

    # Start the drag
    try:
        from utils.input_simulation_utils import (
            click,
            CGEventCreateMouseEvent,
            CGEventPost,
            kCGHIDEventTap,
            kCGEventLeftMouseDown,
            kCGEventLeftMouseDragged,
            kCGEventLeftMouseUp,
            kCGMouseButtonLeft,
            _cg_point,
            _post_event,
        )
    except ImportError:
        return False

    # Mouse down
    down = CGEventCreateMouseEvent(None, kCGEventLeftMouseDown, _cg_point(x0, y0), kCGMouseButtonLeft)
    _post_event(down)

    # Smooth drag
    for i in range(1, steps + 1):
        t = i / steps
        e_t = easing(t)
        cx = x0 + (x1 - x0) * e_t
        cy = y0 + (y1 - y0) * e_t

        drag_event = CGEventCreateMouseEvent(None, kCGEventLeftMouseDragged, _cg_point(cx, cy), kCGMouseButtonLeft)
        _post_event(drag_event)
        time.sleep(interval)

    # Mouse up
    up = CGEventCreateMouseEvent(None, kCGEventLeftMouseUp, _cg_point(x1, y1), kCGMouseButtonLeft)
    _post_event(up)

    return True


def drag_by_offset(
    start: tuple[float, float],
    dx: float,
    dy: float,
    duration: float = 0.3,
) -> bool:
    """Perform a drag by specifying an offset from the start position.

    Args:
        start: Starting (x, y) coordinates.
        dx: Horizontal delta.
        dy: Vertical delta.
        duration: Drag duration in seconds.

    Returns:
        True if successful.
    """
    end = (start[0] + dx, start[1] + dy)
    return drag_and_drop(start, end, duration=duration)


def multi_step_drag(
    points: list[tuple[float, float]],
    duration_per_step: float = 0.3,
) -> bool:
    """Perform a multi-segment drag through a series of points.

    Args:
        points: List of (x, y) waypoints.
        duration_per_step: Duration for each segment.

    Returns:
        True if successful.
    """
    if len(points) < 2:
        raise DropError("Need at least 2 points for multi-step drag")

    _drag, _ = _get_input_sim()
    if _drag is None:
        return False

    for i in range(len(points) - 1):
        if not drag_and_drop(points[i], points[i + 1], duration=duration_per_step):
            return False

    return True
