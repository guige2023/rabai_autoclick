"""Animation easing utilities for smooth transitions.

This module provides easing functions for animations and transitions.
"""

from typing import Callable, List, Tuple


# Easing functions take a normalized time t (0.0 to 1.0) and return a float.
EasingFunc = Callable[[float], float]


def linear(t: float) -> float:
    """Linear easing, no acceleration.

    Args:
        t: Normalized time from 0.0 to 1.0.

    Returns:
        The same value t.
    """
    return t


def ease_in_quad(t: float) -> float:
    """Quadratic ease-in."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Quadratic ease-out."""
    return t * (2 - t)


def ease_in_out_quad(t: float) -> float:
    """Quadratic ease-in-out."""
    if t < 0.5:
        return 2 * t * t
    return -1 + (4 - 2 * t) * t


def ease_in_cubic(t: float) -> float:
    """Cubic ease-in."""
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Cubic ease-out."""
    return (t - 1) ** 3 + 1


def ease_in_out_cubic(t: float) -> float:
    """Cubic ease-in-out."""
    if t < 0.5:
        return 4 * t * t * t
    return (t - 1) ** 3 * 4 + 1


def ease_in_quart(t: float) -> float:
    """Quartic ease-in."""
    return t ** 4


def ease_out_quart(t: float) -> float:
    """Quartic ease-out."""
    return 1 - (t - 1) ** 4


def ease_in_out_quart(t: float) -> float:
    """Quartic ease-in-out."""
    if t < 0.5:
        return 8 * t ** 4
    return 1 - 8 * (t - 1) ** 4


def ease_in_quint(t: float) -> float:
    """Quintic ease-in."""
    return t ** 5


def ease_out_quint(t: float) -> float:
    """Quintic ease-out."""
    return 1 + (t - 1) ** 5


def ease_in_out_quint(t: float) -> float:
    """Quintic ease-in-out."""
    if t < 0.5:
        return 16 * t ** 5
    return 1 + 16 * (t - 1) ** 5


def ease_in_sine(t: float) -> float:
    """Sine ease-in."""
    return 1 - __cos(t * __HALF_PI)


def ease_out_sine(t: float) -> float:
    """Sine ease-out."""
    return __sin(t * __HALF_PI)


def ease_in_out_sine(t: float) -> float:
    """Sine ease-in-out."""
    return -0.5 * (__cos(__PI * t) - 1)


def ease_in_expo(t: float) -> float:
    """Exponential ease-in."""
    if t == 0:
        return 0.0
    return 2 ** (10 * (t - 1))


def ease_out_expo(t: float) -> float:
    """Exponential ease-out."""
    if t == 1:
        return 1.0
    return 1 - 2 ** (-10 * t)


def ease_in_out_expo(t: float) -> float:
    """Exponential ease-in-out."""
    if t == 0:
        return 0.0
    if t == 1:
        return 1.0
    if t < 0.5:
        return 2 ** (20 * t - 11) * 0.5
    return (2 - 2 ** (-20 * t + 11)) * 0.5


def ease_in_back(t: float) -> float:
    """Back ease-in with slight overshoot."""
    return 2.70158 * t * t * t - 1.70158 * t * t


def ease_out_back(t: float) -> float:
    """Back ease-out with slight overshoot."""
    return 1 + 2.70158 * (t - 1) ** 3 + 1.70158 * (t - 1) ** 2


def ease_in_out_back(t: float) -> float:
    """Back ease-in-out."""
    c1 = 1.70158
    c2 = c1 * 1.525
    if t < 0.5:
        return ((2 * t) ** 2 * ((c2 + 1) * 2 * t - c2)) * 0.5
    return ((2 * t - 2) ** 2 * ((c2 + 1) * (t * 2 - 2) + c2) + 2) * 0.5


def ease_in_elastic(t: float) -> float:
    """Elastic ease-in."""
    if t == 0:
        return 0.0
    if t == 1:
        return 1.0
    return -(2 ** (10 * t - 10)) * __sin((t * 10 - 10.75) * __TWO_PI_OVER_THREE)


def ease_out_elastic(t: float) -> float:
    """Elastic ease-out."""
    if t == 0:
        return 0.0
    if t == 1:
        return 1.0
    return 2 ** (-10 * t) * __sin((t * 10 - 0.75) * __TWO_PI_OVER_THREE) + 1


def ease_in_out_elastic(t: float) -> float:
    """Elastic ease-in-out."""
    if t == 0:
        return 0.0
    if t == 1:
        return 1.0
    if t < 0.5:
        return -(2 ** (20 * t - 11) * __sin((20 * t - 11.625) * __TWO_PI_OVER_THREE)) * 0.5
    return (2 ** (-20 * t + 11) * __sin((20 * t - 11.625) * __TWO_PI_OVER_THREE)) * 0.5 + 1


def ease_in_bounce(t: float) -> float:
    """Bounce ease-in."""
    return 1 - ease_out_bounce(1 - t)


def ease_out_bounce(t: float) -> float:
    """Bounce ease-out."""
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


def ease_in_out_bounce(t: float) -> float:
    """Bounce ease-in-out."""
    if t < 0.5:
        return (1 - ease_out_bounce(1 - 2 * t)) * 0.5
    return (1 + ease_out_bounce(2 * t - 1)) * 0.5


# Pre-built named easing presets
PRESETS: List[Tuple[str, EasingFunc]] = [
    ("linear", linear),
    ("easeInQuad", ease_in_quad),
    ("easeOutQuad", ease_out_quad),
    ("easeInOutQuad", ease_in_out_quad),
    ("easeInCubic", ease_in_cubic),
    ("easeOutCubic", ease_out_cubic),
    ("easeInOutCubic", ease_in_out_cubic),
    ("easeInQuart", ease_in_quart),
    ("easeOutQuart", ease_out_quart),
    ("easeInOutQuart", ease_in_out_quart),
    ("easeInQuint", ease_in_quint),
    ("easeOutQuint", ease_out_quint),
    ("easeInOutQuint", ease_in_out_quint),
    ("easeInSine", ease_in_sine),
    ("easeOutSine", ease_out_sine),
    ("easeInOutSine", ease_in_out_sine),
    ("easeInExpo", ease_in_expo),
    ("easeOutExpo", ease_out_expo),
    ("easeInOutExpo", ease_in_out_expo),
    ("easeInBack", ease_in_back),
    ("easeOutBack", ease_out_back),
    ("easeInOutBack", ease_in_out_back),
    ("easeInElastic", ease_in_elastic),
    ("easeOutElastic", ease_out_elastic),
    ("easeInOutElastic", ease_in_out_elastic),
    ("easeInBounce", ease_in_bounce),
    ("easeOutBounce", ease_out_bounce),
    ("easeInOutBounce", ease_in_out_bounce),
]


# Constants
__HALF_PI = 1.5707963267948966
__PI = 3.141592653589793
__TWO_PI_OVER_THREE = 2.0943951023931953


def __sin(x: float) -> float:
    import math
    return math.sin(x)


def __cos(x: float) -> float:
    import math
    return math.cos(x)


def get_easing(name: str) -> EasingFunc:
    """Get easing function by name.

    Args:
        name: Easing function name (e.g., "easeOutQuad").

    Returns:
        The easing function.

    Raises:
        ValueError: If name is not recognized.
    """
    for n, func in PRESETS:
        if n == name:
            return func
    raise ValueError(f"Unknown easing: {name!r}")
