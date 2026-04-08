"""Animation and transition utilities for smooth UI automation.

Provides easing functions, smooth animation helpers for cursor
movement and scrolling, and visual transition effects for
making automation actions feel more natural.

Example:
    >>> from utils.animation_utils import ease_in_out, smooth_move, Easing
    >>> path = smooth_move((0, 0), (100, 100), steps=20, easing=Easing.QUAD_IN_OUT)
    >>> for x, y in path:
    ...     move_mouse(x, y)
    ...     sleep(0.01)
"""

from __future__ import annotations

import time
from enum import Enum
from typing import Callable, Literal, Sequence

__all__ = [
    "Easing",
    "ease",
    "ease_linear",
    "ease_in",
    "ease_out",
    "ease_in_out",
    "smooth_move",
    "smooth_scroll",
    "Animate",
    "AnimationPath",
]


class Easing(Enum):
    """Common easing function types."""

    LINEAR = "linear"
    QUAD_IN = "quad_in"
    QUAD_OUT = "quad_out"
    QUAD_IN_OUT = "quad_in_out"
    CUBIC_IN = "cubic_in"
    CUBIC_OUT = "cubic_out"
    CUBIC_IN_OUT = "cubic_in_out"
    SINE_IN = "sine_in"
    SINE_OUT = "sine_out"
    SINE_IN_OUT = "sine_in_out"
    BOUNCE_OUT = "bounce_out"
    ELASTIC_IN = "elastic_in"


# Easing function registry
_EASING_FUNCTIONS: dict[str, Callable[[float], float]] = {}


def _register_easing(name: str, fn: Callable[[float], float]) -> None:
    _EASING_FUNCTIONS[name] = fn


def _quad_in(t: float) -> float:
    return t * t


def _quad_out(t: float) -> float:
    return t * (2 - t)


def _quad_in_out(t: float) -> float:
    if t < 0.5:
        return 2 * t * t
    return -1 + (4 - 2 * t) * t


def _cubic_in(t: float) -> float:
    return t * t * t


def _cubic_out(t: float) -> float:
    t -= 1
    return t * t * t + 1


def _cubic_in_out(t: float) -> float:
    if t < 0.5:
        return 4 * t * t * t
    t = 2 * t - 2
    return (t * t * t + 2) / 2


def _sine_in(t: float) -> float:
    return 1 - __import__("math").cos(t * __import__("math").pi / 2)


def _sine_out(t: float) -> float:
    return __import__("math").sin(t * __import__("math").pi / 2)


def _sine_in_out(t: float) -> float:
    return -(__import__("math").cos(__import__("math").pi * t) - 1) / 2


def _bounce_out(t: float) -> float:
    import math

    if t < 1 / 2.75:
        return 7.5625 * t * t
    elif t < 2 / 2.75:
        t -= 1.5 / 2.75
        return 7.5625 * t * t + 0.75
    elif t < 2.5 / 2.75:
        t -= 2.25 / 2.75
        return 7.5625 * t * t + 0.9375
    else:
        t -= 2.625 / 2.75
        return 7.5625 * t * t + 0.984375


def _elastic_in(t: float) -> float:
    import math

    if t == 0 or t == 1:
        return t
    return -2 ** (10 * t - 10) * math.sin((t * 10 - 10.75) * (2 * math.pi) / 3)


_register_easing("linear", lambda t: t)
_register_easing("quad_in", _quad_in)
_register_easing("quad_out", _quad_out)
_register_easing("quad_in_out", _quad_in_out)
_register_easing("cubic_in", _cubic_in)
_register_easing("cubic_out", _cubic_out)
_register_easing("cubic_in_out", _cubic_in_out)
_register_easing("sine_in", _sine_in)
_register_easing("sine_out", _sine_out)
_register_easing("sine_in_out", _sine_in_out)
_register_easing("bounce_out", _bounce_out)
_register_easing("elastic_in", _elastic_in)


def ease(t: float, easing: str | Easing = Easing.LINEAR) -> float:
    """Apply an easing function to a normalized time value.

    Args:
        t: Normalized time in [0.0, 1.0].
        easing: Easing name or Easing enum value.

    Returns:
        Eased value in [0.0, 1.0].
    """
    if isinstance(easing, Easing):
        easing = easing.value
    fn = _EASING_FUNCTIONS.get(easing, lambda t: t)
    return fn(max(0.0, min(1.0, t)))


def ease_linear(t: float) -> float:
    """Linear easing (identity function)."""
    return max(0.0, min(1.0, t))


def ease_in(t: float) -> float:
    """Quad-in easing."""
    return _quad_in(t)


def ease_out(t: float) -> float:
    """Quad-out easing."""
    return _quad_out(t)


def ease_in_out(t: float) -> float:
    """Quad-in-out easing."""
    return _quad_in_out(t)


def smooth_move(
    start: tuple[float, float],
    end: tuple[float, float],
    steps: int = 20,
    easing: Easing | str = Easing.QUAD_IN_OUT,
) -> Sequence[tuple[float, float]]:
    """Generate a smooth path between two points.

    Args:
        start: Starting (x, y) coordinates.
        end: Ending (x, y) coordinates.
        steps: Number of intermediate points to generate.
        easing: Easing function to use.

    Returns:
        List of (x, y) coordinate tuples forming the path.
    """
    if steps < 2:
        return [start, end]

    x0, y0 = start
    x1, y1 = end
    path: list[tuple[float, float]] = []

    for i in range(steps):
        t = i / (steps - 1)
        e_t = ease(t, easing)
        x = x0 + (x1 - x0) * e_t
        y = y0 + (y1 - y0) * e_t
        path.append((x, y))

    return path


def smooth_scroll(
    start_y: float,
    end_y: float,
    steps: int = 10,
    easing: Easing | str = Easing.QUAD_IN_OUT,
) -> Sequence[float]:
    """Generate smooth scroll Y positions.

    Args:
        start_y: Starting Y position.
        end_y: Ending Y position.
        steps: Number of intermediate values.
        easing: Easing function to use.

    Returns:
        List of Y coordinate values.
    """
    if steps < 2:
        return [start_y, end_y]

    positions: list[float] = []
    for i in range(steps):
        t = i / (steps - 1)
        e_t = ease(t, easing)
        positions.append(start_y + (end_y - start_y) * e_t)
    return positions


class AnimationPath:
    """A 2D animation path with interpolated coordinates.

    Example:
        >>> path = AnimationPath()
        >>> path.add_point(0, (0, 0), easing=Easing.CUBIC_OUT)
        >>> path.add_point(0.5, (100, 50))
        >>> path.add_point(1.0, (200, 0))
        >>> for t, (x, y) in path:
        ...     move_mouse(x, y)
    """

    def __init__(self):
        self._points: list[tuple[float, tuple[float, float], str]] = []

    def add_point(
        self,
        t: float,
        coords: tuple[float, float],
        easing: Easing | str = Easing.LINEAR,
    ) -> "AnimationPath":
        """Add a point to the path.

        Args:
            t: Normalized time (0.0 to 1.0).
            coords: (x, y) coordinates at this time.
            easing: Easing type for the segment leading to this point.
        """
        if not 0.0 <= t <= 1.0:
            raise ValueError(f"t must be in [0, 1], got {t}")
        self._points.append((t, coords, easing.value if isinstance(easing, Easing) else easing))
        self._points.sort(key=lambda p: p[0])
        return self

    def interpolate(self, t: float) -> tuple[float, float]:
        """Get interpolated coordinates at time t.

        Args:
            t: Normalized time in [0.0, 1.0].

        Returns:
            (x, y) coordinate at time t.
        """
        if not self._points:
            return (0.0, 0.0)
        if t <= 0.0:
            return self._points[0][1]
        if t >= 1.0:
            return self._points[-1][1]

        # Find the two points surrounding t
        prev_point: tuple[float, tuple[float, float], str] | None = None
        next_point: tuple[float, tuple[float, float], str] | None = None

        for i, point in enumerate(self._points):
            if point[0] <= t:
                prev_point = point
            if point[0] >= t and next_point is None:
                next_point = point
                break

        if prev_point is None:
            prev_point = next_point
        if next_point is None:
            next_point = prev_point

        if prev_point == next_point:
            return prev_point[1]

        # Normalize t within the segment
        t0, p0, _ = prev_point
        t1, p1, easing_name = next_point
        segment_t = (t - t0) / (t1 - t0)
        e_t = ease(segment_t, easing_name)

        x = p0[0] + (p1[0] - p0[0]) * e_t
        y = p0[1] + (p1[1] - p0[1]) * e_t
        return (x, y)

    def __iter__(self):
        return _AnimationPathIterator(self)


class _AnimationPathIterator:
    """Iterator over an AnimationPath with N evenly-spaced steps."""

    def __init__(self, path: AnimationPath, steps: int = 50):
        self.path = path
        self.steps = steps
        self._i = 0

    def __iter__(self):
        return self

    def __next__(self) -> tuple[float, tuple[float, float]]:
        if self._i > self.steps:
            raise StopIteration
        t = self._i / self.steps
        self._i += 1
        return (t, self.path.interpolate(t))


class Animate:
    """Context manager for animated transitions.

    Example:
        >>> with Animate((100, 100), (500, 500), steps=30) as pos:
        ...     move_mouse(*pos)
    """

    def __init__(
        self,
        start: tuple[float, float],
        end: tuple[float, float],
        steps: int = 20,
        duration: float = 0.3,
        easing: Easing | str = Easing.QUAD_IN_OUT,
    ):
        self.start = start
        self.end = end
        self.steps = steps
        self.duration = duration
        self.easing = easing
        self._path = smooth_move(start, end, steps, easing)
        self._i = 0

    def __enter__(self) -> "Animate":
        return self

    def __exit__(self, *args):
        pass

    def __iter__(self):
        self._i = 0
        return self

    def __next__(self) -> tuple[float, float]:
        if self._i >= self.steps:
            raise StopIteration
        pos = self._path[self._i]
        self._i += 1
        time.sleep(self.duration / self.steps)
        return pos
