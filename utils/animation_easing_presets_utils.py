"""
Animation easing presets utilities.

This module provides predefined easing functions and curves
for common animation patterns.
"""

from __future__ import annotations

import math
from typing import Callable, Dict, Tuple


# Type aliases
EasingFunc = Callable[[float], float]


# ---- Standard Easing Functions ----

def ease_in_sine(t: float) -> float:
    """Sinusoidal ease-in."""
    return 1.0 - math.cos(t * math.pi / 2.0)


def ease_out_sine(t: float) -> float:
    """Sinusoidal ease-out."""
    return math.sin(t * math.pi / 2.0)


def ease_in_out_sine(t: float) -> float:
    """Sinusoidal ease-in-out."""
    return -(math.cos(math.pi * t) - 1.0) / 2.0


def ease_in_quad(t: float) -> float:
    """Quadratic ease-in."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Quadratic ease-out."""
    return 1.0 - (1.0 - t) * (1.0 - t)


def ease_in_out_quad(t: float) -> float:
    """Quadratic ease-in-out."""
    return 2.0 * t * t if t < 0.5 else 1.0 - (-2.0 * t + 2.0) ** 2 / 2.0


def ease_in_cubic(t: float) -> float:
    """Cubic ease-in."""
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Cubic ease-out."""
    return 1.0 - (1.0 - t) ** 3


def ease_in_out_cubic(t: float) -> float:
    """Cubic ease-in-out."""
    return 4.0 * t * t * t if t < 0.5 else 1.0 - (-2.0 * t + 2.0) ** 3 / 2.0


def ease_in_quart(t: float) -> float:
    """Quartic ease-in."""
    return t ** 4


def ease_out_quart(t: float) -> float:
    """Quartic ease-out."""
    return 1.0 - (1.0 - t) ** 4


def ease_in_out_quart(t: float) -> float:
    """Quartic ease-in-out."""
    return 8.0 * t ** 4 if t < 0.5 else 1.0 - (-2.0 * t + 2.0) ** 4 / 2.0


def ease_in_quint(t: float) -> float:
    """Quintic ease-in."""
    return t ** 5


def ease_out_quint(t: float) -> float:
    """Quintic ease-out."""
    return 1.0 - (1.0 - t) ** 5


def ease_in_out_quint(t: float) -> float:
    """Quintic ease-in-out."""
    return 16.0 * t ** 5 if t < 0.5 else 1.0 - (-2.0 * t + 2.0) ** 5 / 2.0


def ease_in_expo(t: float) -> float:
    """Exponential ease-in."""
    return 0.0 if t == 0.0 else 2.0 ** (10.0 * t - 10.0)


def ease_out_expo(t: float) -> float:
    """Exponential ease-out."""
    return 1.0 if t >= 1.0 else 1.0 - 2.0 ** (-10.0 * t)


def ease_in_out_expo(t: float) -> float:
    """Exponential ease-in-out."""
    if t == 0.0:
        return 0.0
    if t >= 1.0:
        return 1.0
    return 16.0 * t ** 5 if t < 0.5 else 1.0 - (-2.0 * t + 2.0) ** 5 / 2.0


def ease_in_circ(t: float) -> float:
    """Circular ease-in."""
    return 1.0 - math.sqrt(1.0 - t ** 2)


def ease_out_circ(t: float) -> float:
    """Circular ease-out."""
    return math.sqrt(1.0 - (t - 1.0) ** 2)


def ease_in_out_circ(t: float) -> float:
    """Circular ease-in-out."""
    return (1.0 - math.sqrt(1.0 - (2.0 * t) ** 2)) / 2.0 if t < 0.5 else (math.sqrt(1.0 - (-2.0 * t + 2.0) ** 2) + 1.0) / 2.0


def ease_in_back(t: float) -> float:
    """Back ease-in with overshoot."""
    c1 = 1.70158
    c3 = c1 + 1.0
    return c3 * t ** 3 - c1 * t ** 2


def ease_out_back(t: float) -> float:
    """Back ease-out with overshoot."""
    c1 = 1.70158
    c3 = c1 + 1.0
    return 1.0 + c3 * (t - 1.0) ** 3 + c1 * (t - 1.0) ** 2


def ease_in_out_back(t: float) -> float:
    """Back ease-in-out with overshoot."""
    c1 = 1.70158
    c2 = c1 * 1.525
    if t < 0.5:
        return ((2.0 * t) ** 2 * ((c2 + 1.0) * 2.0 * t - c2)) / 2.0
    return ((2.0 * t - 2.0) ** 2 * ((c2 + 1.0) * (t * 2.0 - 2.0) + c2) + 2.0) / 2.0


def ease_in_elastic(t: float) -> float:
    """Elastic ease-in with bounce."""
    if t == 0.0:
        return 0.0
    if t >= 1.0:
        return 1.0
    return -(2.0 ** (10.0 * t - 10.0)) * math.sin((t * 10.0 - 10.75) * (2.0 * math.pi) / 3.0)


def ease_out_elastic(t: float) -> float:
    """Elastic ease-out with bounce."""
    if t == 0.0:
        return 0.0
    if t >= 1.0:
        return 1.0
    return (2.0 ** (-10.0 * t + 10.0)) * math.sin((t * 10.0 - 0.75) * (2.0 * math.pi) / 3.0) + 1.0


def ease_in_out_elastic(t: float) -> float:
    """Elastic ease-in-out with bounce."""
    if t == 0.0:
        return 0.0
    if t >= 1.0:
        return 1.0
    if t < 0.5:
        return -((2.0 ** (20.0 * t - 10.0)) * math.sin((20.0 * t - 11.125) * (2.0 * math.pi) / 4.5)) / 2.0
    return (2.0 ** (-20.0 * t + 10.0)) * math.sin((20.0 * t - 11.125) * (2.0 * math.pi) / 4.5) / 2.0 + 1.0


def ease_in_bounce(t: float) -> float:
    """Bounce ease-in."""
    return 1.0 - ease_out_bounce(1.0 - t)


def ease_out_bounce(t: float) -> float:
    """Bounce ease-out."""
    n1, d1 = 7.5625, 2.75
    if t < 1.0 / d1:
        return n1 * t * t
    elif t < 2.0 / d1:
        t -= 1.5 / d1
        return n1 * t * t + 0.75
    elif t < 2.5 / d1:
        t -= 2.25 / d1
        return n1 * t * t + 0.9375
    else:
        t -= 2.625 / d1
        return n1 * t * t + 0.984375


def ease_in_out_bounce(t: float) -> float:
    """Bounce ease-in-out."""
    if t < 0.5:
        return (1.0 - ease_out_bounce(1.0 - 2.0 * t)) / 2.0
    return (1.0 + ease_out_bounce(2.0 * t - 1.0)) / 2.0


# ---- Easing Preset Registry ----

EASING_PRESETS: Dict[str, EasingFunc] = {
    "linear": lambda t: t,
    "ease-in-sine": ease_in_sine,
    "ease-out-sine": ease_out_sine,
    "ease-in-out-sine": ease_in_out_sine,
    "ease-in-quad": ease_in_quad,
    "ease-out-quad": ease_out_quad,
    "ease-in-out-quad": ease_in_out_quad,
    "ease-in-cubic": ease_in_cubic,
    "ease-out-cubic": ease_out_cubic,
    "ease-in-out-cubic": ease_in_out_cubic,
    "ease-in-quart": ease_in_quart,
    "ease-out-quart": ease_out_quart,
    "ease-in-out-quart": ease_in_out_quart,
    "ease-in-quint": ease_in_quint,
    "ease-out-quint": ease_out_quint,
    "ease-in-out-quint": ease_in_out_quint,
    "ease-in-expo": ease_in_expo,
    "ease-out-expo": ease_out_expo,
    "ease-in-out-expo": ease_in_out_expo,
    "ease-in-circ": ease_in_circ,
    "ease-out-circ": ease_out_circ,
    "ease-in-out-circ": ease_in_out_circ,
    "ease-in-back": ease_in_back,
    "ease-out-back": ease_out_back,
    "ease-in-out-back": ease_in_out_back,
    "ease-in-elastic": ease_in_elastic,
    "ease-out-elastic": ease_out_elastic,
    "ease-in-out-elastic": ease_in_out_elastic,
    "ease-in-bounce": ease_in_bounce,
    "ease-out-bounce": ease_out_bounce,
    "ease-in-out-bounce": ease_in_out_bounce,
}


def get_easing(name: str) -> EasingFunc:
    """
    Get an easing function by name.

    Args:
        name: Name of the easing preset.

    Returns:
        Easing function.
    """
    return EASING_PRESETS.get(name, lambda t: t)


def list_presets() -> Tuple[str, ...]:
    """Get list of all available easing preset names."""
    return tuple(sorted(EASING_PRESETS.keys()))


def compose_easings(funcs: list[EasingFunc], t: float) -> float:
    """
    Compose multiple easing functions.

    Args:
        funcs: List of easing functions to compose.
        t: Input time value (0-1).

    Returns:
        Composed easing value.
    """
    if not funcs:
        return t
    result = t
    for f in funcs:
        result = f(result)
    return result
