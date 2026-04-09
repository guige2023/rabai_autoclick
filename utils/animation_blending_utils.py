"""Animation blending utilities for smooth UI transitions.

This module provides utilities for blending between animation states,
interpolating keyframes, and creating smooth transitions in UI automation.
"""

from __future__ import annotations

from typing import Callable, Sequence
from dataclasses import dataclass
from enum import Enum
import math


class BlendMode(Enum):
    """Animation blend modes."""
    LINEAR = "linear"
    EASE_IN = "ease_in"
    EASE_OUT = "ease_out"
    EASE_IN_OUT = "ease_in_out"
    CUBIC = "cubic"
    BOUNCE = "bounce"
    ELASTIC = "elastic"


@dataclass
class KeyFrame:
    """Animation keyframe definition.

    Attributes:
        time: Normalized time position (0.0 to 1.0).
        value: Value at this keyframe.
        blend_mode: How to blend into this keyframe.
    """
    time: float
    value: float
    blend_mode: BlendMode = BlendMode.LINEAR


@dataclass
class AnimationState:
    """Complete animation state.

    Attributes:
        position: Current X/Y position.
        scale: Current scale factors.
        opacity: Current opacity (0.0 to 1.0).
        rotation: Current rotation in degrees.
    """
    position: tuple[float, float] = (0.0, 0.0)
    scale: tuple[float, float] = (1.0, 1.0)
    opacity: float = 1.0
    rotation: float = 0.0


def ease_in(t: float) -> float:
    """Ease-in cubic function.

    Args:
        t: Normalized time (0.0 to 1.0).

    Returns:
        Eased value.
    """
    return t * t * t


def ease_out(t: float) -> float:
    """Ease-out cubic function.

    Args:
        t: Normalized time (0.0 to 1.0).

    Returns:
        Eased value.
    """
    return 1.0 - math.pow(1.0 - t, 3)


def ease_in_out(t: float) -> float:
    """Ease-in-out cubic function.

    Args:
        t: Normalized time (0.0 to 1.0).

    Returns:
        Eased value.
    """
    if t < 0.5:
        return 4.0 * t * t * t
    return 1.0 - math.pow(-2.0 * t + 2.0, 3) / 2.0


def bounce(t: float) -> float:
    """Bounce easing function.

    Args:
        t: Normalized time (0.0 to 1.0).

    Returns:
        Bounced value.
    """
    if t < 0.5:
        return 0.5 * (1.0 - math.pow(1.0 - 2.0 * t, 2))
    else:
        t2 = 2.0 * t - 1.0
        return 0.5 * (1.0 + math.pow(1.0 - 2.0 * t2, 2))


def elastic(t: float) -> float:
    """Elastic easing function.

    Args:
        t: Normalized time (0.0 to 1.0).

    Returns:
        Elastic eased value.
    """
    if t == 0.0 or t == 1.0:
        return t
    p = 0.3
    s = p / 4.0
    return math.pow(2.0, -10.0 * t) * math.sin((t - s) * (2.0 * math.pi) / p) + 1.0


def get_blend_function(mode: BlendMode) -> Callable[[float], float]:
    """Get blend function for a given blend mode.

    Args:
        mode: The blend mode.

    Returns:
        Blend function that takes normalized time and returns eased value.
    """
    blend_map: dict[BlendMode, Callable[[float], float]] = {
        BlendMode.LINEAR: lambda t: t,
        BlendMode.EASE_IN: ease_in,
        BlendMode.EASE_OUT: ease_out,
        BlendMode.EASE_IN_OUT: ease_in_out,
        BlendMode.CUBIC: ease_in_out,
        BlendMode.BOUNCE: bounce,
        BlendMode.ELASTIC: elastic,
    }
    return blend_map.get(mode, lambda t: t)


def lerp(start: float, end: float, t: float) -> float:
    """Linear interpolation between two values.

    Args:
        start: Start value.
        end: End value.
        t: Interpolation factor (0.0 to 1.0).

    Returns:
        Interpolated value.
    """
    return start + (end - start) * t


def lerp_color(c1: tuple[int, int, int], c2: tuple[int, int, int], t: float) -> tuple[int, int, int]:
    """Linear interpolation between two RGB colors.

    Args:
        c1: Start color as (R, G, B).
        c2: End color as (R, G, B).
        t: Interpolation factor (0.0 to 1.0).

    Returns:
        Interpolated color as (R, G, B).
    """
    return (
        int(lerp(c1[0], c2[0], t)),
        int(lerp(c1[1], c2[1], t)),
        int(lerp(c1[2], c2[2], t)),
    )


def interpolate_keyframes(keyframes: Sequence[KeyFrame], time: float) -> float:
    """Interpolate value from a sequence of keyframes.

    Args:
        keyframes: Sequence of keyframes (must have at least 2).
        time: Normalized time position.

    Returns:
        Interpolated value at given time.
    """
    if not keyframes:
        return 0.0

    if len(keyframes) == 1:
        return keyframes[0].value

    sorted_frames = sorted(keyframes, key=lambda k: k.time)

    if time <= sorted_frames[0].time:
        return sorted_frames[0].value

    if time >= sorted_frames[-1].time:
        return sorted_frames[-1].value

    for i in range(len(sorted_frames) - 1):
        k1 = sorted_frames[i]
        k2 = sorted_frames[i + 1]

        if k1.time <= time <= k2.time:
            local_t = (time - k1.time) / (k2.time - k1.time)
            blend_fn = get_blend_function(k2.blend_mode)
            blended_t = blend_fn(local_t)
            return lerp(k1.value, k2.value, blended_t)

    return sorted_frames[-1].value


def blend_states(state1: AnimationState, state2: AnimationState, t: float, mode: BlendMode = BlendMode.EASE_IN_OUT) -> AnimationState:
    """Blend between two animation states.

    Args:
        state1: Start state.
        state2: End state.
        t: Blend factor (0.0 to 1.0).
        mode: Blend mode to use.

    Returns:
        Blended animation state.
    """
    blend_fn = get_blend_function(mode)
    blended_t = blend_fn(t)

    return AnimationState(
        position=(
            lerp(state1.position[0], state2.position[0], blended_t),
            lerp(state1.position[1], state2.position[1], blended_t),
        ),
        scale=(
            lerp(state1.scale[0], state2.scale[0], blended_t),
            lerp(state1.scale[1], state2.scale[1], blended_t),
        ),
        opacity=lerp(state1.opacity, state2.opacity, blended_t),
        rotation=lerp(state1.rotation, state2.rotation, blended_t),
    )


def create_path_blend(path1: Sequence[tuple[float, float]], path2: Sequence[tuple[float, float]], t: float, mode: BlendMode = BlendMode.LINEAR) -> list[tuple[float, float]]:
    """Blend between two paths (sequences of points).

    Args:
        path1: Start path as sequence of (x, y) points.
        path2: End path as sequence of (x, y) points.
        t: Blend factor (0.0 to 1.0).
        mode: Blend mode to use.

    Returns:
        Blended path with same number of points as shorter path.
    """
    blend_fn = get_blend_function(mode)
    blended_t = blend_fn(t)

    min_len = min(len(path1), len(path2))
    result: list[tuple[float, float]] = []

    for i in range(min_len):
        p1 = path1[i]
        p2 = path2[i]
        result.append((
            lerp(p1[0], p2[0], blended_t),
            lerp(p1[1], p2[1], blended_t),
        ))

    return result


def compute_arc_control_points(p1: tuple[float, float], p2: tuple[float, float], curvature: float = 0.5) -> tuple[tuple[float, float], tuple[float, float]]:
    """Compute cubic bezier control points for an arc between two points.

    Args:
        p1: Start point (x, y).
        p2: End point (x, y).
        curvature: Curve intensity (0.0 to 1.0).

    Returns:
        Tuple of (control1, control2) points.
    """
    mid_x = (p1[0] + p2[0]) / 2.0
    mid_y = (p1[1] + p2[1]) / 2.0

    dx = p2[0] - p1[0]
    dy = p2[1] - p1[1]

    perp_x = -dy * curvature
    perp_y = dx * curvature

    ctrl1 = (mid_x + perp_x, mid_y + perp_y)
    ctrl2 = (mid_x - perp_x, mid_y - perp_y)

    return (ctrl1, ctrl2)


def smooth_path(path: Sequence[tuple[float, float]], iterations: int = 1) -> list[tuple[float, float]]:
    """Apply Chaikin's smoothing algorithm to a path.

    Args:
        path: Original path as sequence of (x, y) points.
        iterations: Number of smoothing iterations.

    Returns:
        Smoothed path with more points.
    """
    if len(path) < 3:
        return list(path)

    result = list(path)

    for _ in range(iterations):
        new_path: list[tuple[float, float]] = [result[0]]

        for i in range(len(result) - 1):
            p1 = result[i]
            p2 = result[i + 1]

            q = (p1[0] * 0.75 + p2[0] * 0.25, p1[1] * 0.75 + p2[1] * 0.25)
            r = (p1[0] * 0.25 + p2[0] * 0.75, p1[1] * 0.25 + p2[1] * 0.75)

            new_path.append(q)
            new_path.append(r)

        new_path.append(result[-1])
        result = new_path

    return result
