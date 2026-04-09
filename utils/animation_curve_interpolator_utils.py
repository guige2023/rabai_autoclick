"""
Animation curve interpolator utilities.

This module provides utilities for interpolating values along
animation curves using various interpolation modes.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Callable, Optional, Dict, Any
from dataclasses import dataclass, field


# Type aliases
Interpolator = Callable[[float], float]
Point2D = Tuple[float, float]
Point3D = Tuple[float, float, float]


@dataclass
class BezierControlPoints:
    """Bezier curve control points."""
    p0: Point2D = (0.0, 0.0)
    p1: Point2D = (0.0, 0.0)
    p2: Point2D = (1.0, 1.0)
    p3: Point2D = (1.0, 1.0)


@dataclass
class KeyFrame:
    """Animation keyframe."""
    time: float  # 0.0 to 1.0
    value: float
    easing: str = "linear"


@dataclass
class CurveInterpolationResult:
    """Result of curve interpolation."""
    value: float
    derivative: float
    metadata: Dict[str, Any] = field(default_factory=dict)


def lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation between a and b."""
    return a + (b - a) * t


def inv_lerp(a: float, b: float, v: float) -> float:
    """Inverse linear interpolation - find t given v."""
    if abs(b - a) < 1e-10:
        return 0.0
    return (v - a) / (b - a)


def smoothstep(t: float) -> float:
    """Smooth step interpolation (smooth easing)."""
    return t * t * (3.0 - 2.0 * t)


def smootherstep(t: float) -> float:
    """Smootherstep (6t^5 - 15t^4 + 10t^3)."""
    return t * t * t * (t * (t * 6.0 - 15.0) + 10.0)


def ease_in_quad(t: float) -> float:
    """Quadratic ease in."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Quadratic ease out."""
    return 1.0 - (1.0 - t) * (1.0 - t)


def ease_in_cubic(t: float) -> float:
    """Cubic ease in."""
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Cubic ease out."""
    return 1.0 - (1.0 - t) ** 3


def cubic_bezier(t: float, p0: float, p1: float, p2: float, p3: float) -> float:
    """
    Evaluate cubic bezier at parameter t.

    Args:
        t: Parameter (0-1).
        p0: Start value.
        p1: Control point 1.
        p2: Control point 2.
        p3: End value.

    Returns:
        Interpolated value.
    """
    u = 1.0 - t
    return u * u * u * p0 + 3.0 * u * u * t * p1 + 3.0 * u * t * t * p2 + t * t * t * p3


def cubic_bezier_derivative(t: float, p0: float, p1: float, p2: float, p3: float) -> float:
    """
    Evaluate derivative of cubic bezier at parameter t.

    Args:
        t: Parameter (0-1).
        p0: Start value.
        p1: Control point 1.
        p2: Control point 2.
        p3: End value.

    Returns:
        Derivative value.
    """
    u = 1.0 - t
    return 3.0 * u * u * (p1 - p0) + 6.0 * u * t * (p2 - p1) + 3.0 * t * t * (p3 - p2)


def interpolate_bezier(
    t: float,
    start: float,
    end: float,
    control1: float,
    control2: float,
) -> CurveInterpolationResult:
    """
    Interpolate using a cubic bezier curve.

    Args:
        t: Parameter (0-1).
        start: Start value.
        end: End value.
        control1: First control point.
        control2: Second control point.

    Returns:
        CurveInterpolationResult with value and derivative.
    """
    value = cubic_bezier(t, start, control1, control2, end)
    derivative = cubic_bezier_derivative(t, start, control1, control2, end)
    return CurveInterpolationResult(value=value, derivative=derivative)


def interpolate_keyframes(keyframes: List[KeyFrame], t: float) -> float:
    """
    Interpolate value from keyframes at time t.

    Args:
        keyframes: List of keyframes (must be sorted by time).
        t: Time parameter (0-1).

    Returns:
        Interpolated value.
    """
    if not keyframes:
        return 0.0
    if len(keyframes) == 1:
        return keyframes[0].value

    # Clamp t
    t = max(0.0, min(1.0, t))

    # Find surrounding keyframes
    prev: Optional[KeyFrame] = None
    next_kf: Optional[KeyFrame] = None
    for kf in keyframes:
        if kf.time <= t:
            prev = kf
        if kf.time >= t and next_kf is None:
            next_kf = kf

    if prev is None:
        return keyframes[0].value
    if next_kf is None:
        return keyframes[-1].value
    if prev.time == next_kf.time:
        return prev.value

    # Normalize time between keyframes
    local_t = (t - prev.time) / (next_kf.time - prev.time)

    # Apply easing
    easing_func = get_easing_function(prev.easing)
    eased_t = easing_func(local_t)

    return lerp(prev.value, next_kf.value, eased_t)


def get_easing_function(name: str) -> Interpolator:
    """
    Get easing function by name.

    Args:
        name: Easing function name.

    Returns:
        Easing interpolator function.
    """
    easings: Dict[str, Interpolator] = {
        "linear": lambda t: t,
        "smoothstep": smoothstep,
        "smootherstep": smootherstep,
        "ease-in-quad": ease_in_quad,
        "ease-out-quad": ease_out_quad,
        "ease-in-cubic": ease_in_cubic,
        "ease-out-cubic": ease_out_cubic,
    }
    return easings.get(name, lambda t: t)


def bounce(t: float) -> float:
    """Bounce easing function."""
    if t < 0.5:
        return ease_out_cubic(t * 2.0)
    else:
        return ease_in_cubic(2.0 * t - 1.0) * 0.5 + 0.5


def elastic(t: float) -> float:
    """Elastic easing function."""
    if t == 0.0:
        return 0.0
    if t == 1.0:
        return 1.0
    return math.sin(t * math.pi * 2.0) * (1.0 - t) + t


def spring(t: float, damping: float = 0.5) -> float:
    """Spring easing with damping."""
    return 1.0 - math.exp(-t * 5.0) * math.cos(t * math.pi * 4.0 * damping)
