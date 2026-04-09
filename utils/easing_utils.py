"""Easing function utilities for RabAI AutoClick.

Provides:
- Standard easing functions
- Bounce and elastic effects
- Custom easing curves
- Easing composition
"""

from typing import Callable, Dict, Tuple
import math


def linear(t: float) -> float:
    """Linear easing (no easing)."""
    return t


def ease_in_quad(t: float) -> float:
    """Quadratic ease in."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Quadratic ease out."""
    return t * (2 - t)


def ease_in_out_quad(t: float) -> float:
    """Quadratic ease in-out."""
    return 2 * t * t if t < 0.5 else -1 + (4 - 2 * t) * t


def ease_in_cubic(t: float) -> float:
    """Cubic ease in."""
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Cubic ease out."""
    return (t - 1) ** 3 + 1


def ease_in_out_cubic(t: float) -> float:
    """Cubic ease in-out."""
    return 4 * t * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2


def ease_in_quart(t: float) -> float:
    """Quartic ease in."""
    return t ** 4


def ease_out_quart(t: float) -> float:
    """Quartic ease out."""
    return 1 - (1 - t) ** 4


def ease_in_out_quart(t: float) -> float:
    """Quartic ease in-out."""
    return 8 * t ** 4 if t < 0.5 else 1 - (-2 * t + 2) ** 4 / 2


def ease_in_quint(t: float) -> float:
    """Quintic ease in."""
    return t ** 5


def ease_out_quint(t: float) -> float:
    """Quintic ease out."""
    return 1 - (1 - t) ** 5


def ease_in_out_quint(t: float) -> float:
    """Quintic ease in-out."""
    return 16 * t ** 5 if t < 0.5 else 1 + (-2 * t + 2) ** 5 / 2


def ease_in_sine(t: float) -> float:
    """Sine ease in."""
    return 1 - math.cos(t * math.pi / 2)


def ease_out_sine(t: float) -> float:
    """Sine ease out."""
    return math.sin(t * math.pi / 2)


def ease_in_out_sine(t: float) -> float:
    """Sine ease in-out."""
    return -(math.cos(math.pi * t) - 1) / 2


def ease_in_expo(t: float) -> float:
    """Exponential ease in."""
    return 2 ** (10 * t - 10) if t > 0 else 0


def ease_out_expo(t: float) -> float:
    """Exponential ease out."""
    return 1 - 2 ** (-10 * t) if t < 1 else 1


def ease_in_out_expo(t: float) -> float:
    """Exponential ease in-out."""
    if t == 0:
        return 0.0
    if t == 1:
        return 1.0
    if t < 0.5:
        return 2 ** (20 * t - 10) / 2
    return (2 - 2 ** (-20 * t + 10)) / 2


def ease_in_circ(t: float) -> float:
    """Circular ease in."""
    return 1 - math.sqrt(1 - t * t)


def ease_out_circ(t: float) -> float:
    """Circular ease out."""
    return math.sqrt(1 - (1 - t) * (1 - t))


def ease_in_out_circ(t: float) -> float:
    """Circular ease in-out."""
    if t < 0.5:
        return (1 - math.sqrt(1 - (2 * t) ** 2)) / 2
    return (math.sqrt(1 - (-2 * t + 2) ** 2) + 1) / 2


def ease_in_back(t: float, c1: float = 1.70158) -> float:
    """Back ease in (overshoots)."""
    return (c1 + 1) * t ** 3 - c1 * t ** 2


def ease_out_back(t: float, c1: float = 1.70158) -> float:
    """Back ease out (overshoots)."""
    return 1 + (c1 + 1) * (t - 1) ** 3 + c1 * (t - 1) ** 2


def ease_in_out_back(t: float) -> float:
    """Back ease in-out."""
    c1 = 1.70158
    c2 = c1 * 1.525
    if t < 0.5:
        return ((2 * t) ** 2 * ((c2 + 1) * 2 * t - c2)) / 2
    return ((2 * t - 2) ** 2 * ((c2 + 1) * (t * 2 - 2) + c2) + 2) / 2


def ease_in_elastic(t: float) -> float:
    """Elastic ease in."""
    if t == 0 or t == 1:
        return t
    p = 0.3
    return -(2 ** (10 * t - 10)) * math.sin((t * 10 - 10.75) * (2 * math.pi) / p)


def ease_out_elastic(t: float) -> float:
    """Elastic ease out."""
    if t == 0 or t == 1:
        return t
    p = 0.3
    return 2 ** (-10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / p) + 1


def ease_in_out_elastic(t: float) -> float:
    """Elastic ease in-out."""
    if t == 0 or t == 1:
        return t
    p = 0.3 * 1.45
    if t < 0.5:
        return -(2 ** (20 * t - 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / p)) / 2
    return (2 ** (-20 * t + 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / p)) / 2 + 1


def ease_out_bounce(t: float) -> float:
    """Bounce ease out."""
    n1 = 7.5625
    d1 = 2.75
    if t < 1 / d1:
        return n1 * t * t
    elif t < 2 / d1:
        t -= 1.5 / d1
        return n1 * t * t + 0.75
    elif t < 2.5 / d1:
        t -= 2.25 / d1
        return n1 * t * t + 0.9375
    else:
        t -= 2.625 / d1
        return n1 * t * t + 0.984375


def ease_in_bounce(t: float) -> float:
    """Bounce ease in."""
    return 1 - ease_out_bounce(1 - t)


def ease_in_out_bounce(t: float) -> float:
    """Bounce ease in-out."""
    if t < 0.5:
        return (1 - ease_out_bounce(1 - 2 * t)) / 2
    return (1 + ease_out_bounce(2 * t - 1)) / 2


EASING_MAP: Dict[str, Callable[[float], float]] = {
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
    "ease_in_quint": ease_in_quint,
    "ease_out_quint": ease_out_quint,
    "ease_in_out_quint": ease_in_out_quint,
    "ease_in_sine": ease_in_sine,
    "ease_out_sine": ease_out_sine,
    "ease_in_out_sine": ease_in_out_sine,
    "ease_in_expo": ease_in_expo,
    "ease_out_expo": ease_out_expo,
    "ease_in_out_expo": ease_in_out_expo,
    "ease_in_circ": ease_in_circ,
    "ease_out_circ": ease_out_circ,
    "ease_in_out_circ": ease_in_out_circ,
    "ease_in_back": ease_in_back,
    "ease_out_back": ease_out_back,
    "ease_in_out_back": ease_in_out_back,
    "ease_in_elastic": ease_in_elastic,
    "ease_out_elastic": ease_out_elastic,
    "ease_in_out_elastic": ease_in_out_elastic,
    "ease_in_bounce": ease_in_bounce,
    "ease_out_bounce": ease_out_bounce,
    "ease_in_out_bounce": ease_in_out_bounce,
}


def get_easing(name: str) -> Callable[[float], float]:
    """Get easing function by name."""
    return EASING_MAP.get(name, linear)


def compose_easing(
    easing1: Callable[[float], float],
    easing2: Callable[[float], float],
    t: float,
) -> float:
    """Compose two easing functions."""
    return easing2(easing1(t))


def reverse_easing(
    easing: Callable[[float], float],
    t: float,
) -> float:
    """Reverse an easing function."""
    return 1.0 - easing(1.0 - t)


def ease_with_offset(
    easing: Callable[[float], float],
    t: float,
    offset: float,
    duration: float,
) -> float:
    """Apply easing with offset for delayed start."""
    if t < offset:
        return 0.0
    adjusted = (t - offset) / duration
    return easing(max(0.0, min(1.0, adjusted)))


def ease_with_hold(
    easing: Callable[[float], float],
    t: float,
    duration: float,
    hold_start: float = 0.0,
    hold_end: float = 0.0,
) -> float:
    """Apply easing with hold periods at start/end."""
    if t < hold_start:
        return 0.0
    if t > duration - hold_end:
        return 1.0
    adjusted = (t - hold_start) / (duration - hold_start - hold_end)
    return easing(max(0.0, min(1.0, adjusted)))


def ease_to(
    current: float,
    target: float,
    t: float,
    easing: Callable[[float], float] = ease_out_quad,
) -> float:
    """Interpolate from current to target using easing.

    Args:
        current: Start value.
        target: End value.
        t: Easing parameter [0, 1].
        easing: Easing function.

    Returns:
        Interpolated value.
    """
    et = easing(max(0.0, min(1.0, t)))
    return current + (target - current) * et
