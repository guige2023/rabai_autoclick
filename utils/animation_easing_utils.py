"""Animation easing utilities.

This module provides easing functions for smooth animations
and transitions in UI automation.
"""

from __future__ import annotations

from typing import Callable
import math


# Easing function type
EasingFunc = Callable[[float], float]


def linear(t: float) -> float:
    """Linear easing (no easing).

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return t


def ease_in_quad(t: float) -> float:
    """Ease-in quadratic.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return t * t


def ease_out_quad(t: float) -> float:
    """Ease-out quadratic.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return t * (2 - t)


def ease_in_out_quad(t: float) -> float:
    """Ease-in-out quadratic.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return 2 * t * t if t < 0.5 else -1 + (4 - 2 * t) * t


def ease_in_cubic(t: float) -> float:
    """Ease-in cubic.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Ease-out cubic.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return 1 - pow(1 - t, 3)


def ease_in_out_cubic(t: float) -> float:
    """Ease-in-out cubic.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return 4 * t * t * t if t < 0.5 else 1 - pow(-2 * t + 2, 3) / 2


def ease_in_quart(t: float) -> float:
    """Ease-in quart.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return t * t * t * t


def ease_out_quart(t: float) -> float:
    """Ease-out quart.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return 1 - pow(1 - t, 4)


def ease_in_out_quart(t: float) -> float:
    """Ease-in-out quart.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return 8 * t * t * t * t if t < 0.5 else 1 - pow(-2 * t + 2, 4) / 2


def ease_in_sine(t: float) -> float:
    """Ease-in sine.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return 1 - math.cos(t * math.pi / 2)


def ease_out_sine(t: float) -> float:
    """Ease-out sine.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return math.sin(t * math.pi / 2)


def ease_in_out_sine(t: float) -> float:
    """Ease-in-out sine.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return -(math.cos(math.pi * t) - 1) / 2


def ease_in_expo(t: float) -> float:
    """Ease-in exponential.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return 0 if t == 0 else pow(2, 10 * t - 10)


def ease_out_expo(t: float) -> float:
    """Ease-out exponential.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    return 1 if t == 1 else 1 - pow(2, -10 * t)


def ease_in_out_expo(t: float) -> float:
    """Ease-in-out exponential.

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    if t == 0:
        return 0
    if t == 1:
        return 1
    return (pow(2, 20 * t - 10) / 2) if t < 0.5 else (2 - pow(2, -20 * t + 10)) / 2


def ease_in_back(t: float) -> float:
    """Ease-in back (with overshoot).

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    c1 = 1.70158
    c3 = c1 + 1
    return c3 * t * t * t - c1 * t * t


def ease_out_back(t: float) -> float:
    """Ease-out back (with overshoot).

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    c1 = 1.70158
    c3 = c1 + 1
    return 1 + c3 * pow(t - 1, 3) + c1 * pow(t - 1, 2)


def ease_in_out_back(t: float) -> float:
    """Ease-in-out back (with overshoot).

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    c1 = 1.70158
    c2 = c1 * 1.525
    return (pow(2 * t, 2) * ((c2 + 1) * 2 * t - c2)) / 2 if t < 0.5 else (pow(2 * t - 2, 2) * ((c2 + 1) * (t * 2 - 2) + c2) + 2) / 2


def ease_out_elastic(t: float) -> float:
    """Ease-out elastic (with bounce).

    Args:
        t: Progress 0.0-1.0.

    Returns:
        Eased value.
    """
    c4 = (2 * math.pi) / 3
    return 0 if t == 0 else 1 if t == 1 else pow(2, -10 * t) * math.sin((t * 10 - 0.75) * c4) + 1


EASING_FUNCTIONS: dict[str, EasingFunc] = {
    "linear": linear,
    "ease_in_quad": ease_in_quad,
    "ease_out_quad": ease_out_quad,
    "ease_in_out_quad": ease_in_out_quad,
    "ease_in_cubic": ease_in_cubic,
    "ease_out_cubic": ease_out_cubic,
    "ease_in_out_cubic": ease_in_out_cubic,
    "ease_in_quart": ease_in_quart,
    "ease_out_quart": ease_out_quart,
    "ease_in_out_quart": ease_in_out_quart,
    "ease_in_sine": ease_in_sine,
    "ease_out_sine": ease_out_sine,
    "ease_in_out_sine": ease_in_out_sine,
    "ease_in_expo": ease_in_expo,
    "ease_out_expo": ease_out_expo,
    "ease_in_out_expo": ease_in_out_expo,
    "ease_in_back": ease_in_back,
    "ease_out_back": ease_out_back,
    "ease_in_out_back": ease_in_out_back,
    "ease_out_elastic": ease_out_elastic,
}


def lerp(start: float, end: float, t: float) -> float:
    """Linear interpolation between two values.

    Args:
        start: Start value.
        end: End value.
        t: Progress 0.0-1.0.

    Returns:
        Interpolated value.
    """
    return start + (end - start) * t


def interpolate_values(
    start: tuple[float, ...],
    end: tuple[float, ...],
    t: float,
    easing: EasingFunc = linear,
) -> tuple[float, ...]:
    """Interpolate between two coordinate tuples.

    Args:
        start: Start coordinates.
        end: End coordinates.
        t: Progress 0.0-1.0.
        easing: Easing function.

    Returns:
        Interpolated coordinates.
    """
    et = easing(t)
    return tuple(lerp(s, e, et) for s, e in zip(start, end))


__all__ = [
    "EasingFunc",
    "linear",
    "ease_in_quad",
    "ease_out_quad",
    "ease_in_out_quad",
    "ease_in_cubic",
    "ease_out_cubic",
    "ease_in_out_cubic",
    "ease_in_quart",
    "ease_out_quart",
    "ease_in_out_quart",
    "ease_in_sine",
    "ease_out_sine",
    "ease_in_out_sine",
    "ease_in_expo",
    "ease_out_expo",
    "ease_in_out_expo",
    "ease_in_back",
    "ease_out_back",
    "ease_in_out_back",
    "ease_out_elastic",
    "EASING_FUNCTIONS",
    "lerp",
    "interpolate_values",
]
