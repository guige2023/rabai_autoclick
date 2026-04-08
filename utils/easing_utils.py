"""Easing functions for smooth animations and transitions.

This module provides common easing functions used in animations,
transitions, and interpolated movements. Easing functions control
the rate of change to create natural-looking motion.
"""

from __future__ import annotations

from typing import Callable


# Type alias for easing functions
EasingFunc = Callable[[float], float]


def linear(t: float) -> float:
    """Linear easing - constant speed throughout.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Progress value from 0.0 to 1.0.
    """
    return t


def ease_in_quad(t: float) -> float:
    """Quadratic ease-in - starts slow, ends fast.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return t * t


def ease_out_quad(t: float) -> float:
    """Quadratic ease-out - starts fast, ends slow.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return t * (2 - t)


def ease_in_out_quad(t: float) -> float:
    """Quadratic ease-in-out - slow start and end.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    if t < 0.5:
        return 2 * t * t
    return -1 + (4 - 2 * t) * t


def ease_in_cubic(t: float) -> float:
    """Cubic ease-in.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Cubic ease-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return 1 - pow(1 - t, 3)


def ease_in_out_cubic(t: float) -> float:
    """Cubic ease-in-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    if t < 0.5:
        return 4 * t * t * t
    return 1 - pow(-2 * t + 2, 3) / 2


def ease_in_quart(t: float) -> float:
    """Quartic ease-in.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return t * t * t * t


def ease_out_quart(t: float) -> float:
    """Quartic ease-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return 1 - pow(1 - t, 4)


def ease_in_out_quart(t: float) -> float:
    """Quartic ease-in-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    if t < 0.5:
        return 8 * t * t * t * t
    return 1 - pow(-2 * t + 2, 4) / 2


def ease_in_sine(t: float) -> float:
    """Sine ease-in.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return 1 - __import__("math").cos(t * __import__("math").pi / 2)


def ease_out_sine(t: float) -> float:
    """Sine ease-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return __import__("math").sin(t * __import__("math").pi / 2)


def ease_in_out_sine(t: float) -> float:
    """Sine ease-in-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return -(__import__("math").cos(__import__("math").pi * t) - 1) / 2


def ease_in_expo(t: float) -> float:
    """Exponential ease-in.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return 0 if t == 0 else pow(2, 10 * t - 10)


def ease_out_expo(t: float) -> float:
    """Exponential ease-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return 1 if t >= 1 else 1 - pow(2, -10 * t)


def ease_in_out_expo(t: float) -> float:
    """Exponential ease-in-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    if t == 0:
        return 0
    if t == 1:
        return 1
    if t < 0.5:
        return pow(2, 20 * t - 10) / 2
    return (2 - pow(2, -20 * t + 10)) / 2


def ease_in_elastic(t: float) -> float:
    """Elastic ease-in - with elastic bounce.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    import math
    if t == 0:
        return 0
    if t == 1:
        return 1
    return -pow(2, 10 * t - 10) * math.sin((t * 10 - 10.75) * (2 * math.pi) / 3)


def ease_out_elastic(t: float) -> float:
    """Elastic ease-out - with elastic bounce.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    import math
    if t == 0:
        return 0
    if t == 1:
        return 1
    return pow(2, -10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / 3) + 1


def ease_in_out_elastic(t: float) -> float:
    """Elastic ease-in-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    import math
    if t == 0:
        return 0
    if t == 1:
        return 1
    if t < 0.5:
        return -(pow(2, 20 * t - 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2
    return (pow(2, -20 * t + 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2 + 1


def ease_in_back(t: float) -> float:
    """Back ease-in - with overshoot.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    import math
    c1 = 1.70158
    c3 = c1 + 1
    return c3 * t * t * t - c1 * t * t


def ease_out_back(t: float) -> float:
    """Back ease-out - with overshoot.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    import math
    c1 = 1.70158
    c3 = c1 + 1
    return 1 + c3 * pow(t - 1, 3) + c1 * pow(t - 1, 2)


def ease_in_out_back(t: float) -> float:
    """Back ease-in-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    import math
    c1 = 1.70158
    c2 = c1 * 1.525
    if t < 0.5:
        return (pow(2 * t, 2) * ((c2 + 1) * 2 * t - c2)) / 2
    return (pow(2 * t - 2, 2) * ((c2 + 1) * (t * 2 - 2) + c2) + 2) / 2


def ease_in_bounce(t: float) -> float:
    """Bounce ease-in.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    return 1 - ease_out_bounce(1 - t)


def ease_out_bounce(t: float) -> float:
    """Bounce ease-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    import math
    n1 = 7.5625
    d1 = 2.75
    if t < 1 / d1:
        return n1 * t * t
    if t < 2 / d1:
        t -= 1.5 / d1
        return n1 * t * t + 0.75
    if t < 2.5 / d1:
        t -= 2.25 / d1
        return n1 * t * t + 0.9375
    t -= 2.625 / d1
    return n1 * t * t + 0.984375


def ease_in_out_bounce(t: float) -> float:
    """Bounce ease-in-out.
    
    Args:
        t: Progress value from 0.0 to 1.0.
    
    Returns:
        Eased progress value.
    """
    if t < 0.5:
        return (1 - ease_out_bounce(1 - 2 * t)) / 2
    return (1 + ease_out_bounce(2 * t - 1)) / 2


# Registry of named easing functions
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
    "ease_in_elastic": ease_in_elastic,
    "ease_out_elastic": ease_out_elastic,
    "ease_in_out_elastic": ease_in_out_elastic,
    "ease_in_back": ease_in_back,
    "ease_out_back": ease_out_back,
    "ease_in_out_back": ease_in_out_back,
    "ease_in_bounce": ease_in_bounce,
    "ease_out_bounce": ease_out_bounce,
    "ease_in_out_bounce": ease_in_out_bounce,
}


def get_easing(name: str) -> EasingFunc:
    """Get an easing function by name.
    
    Args:
        name: Name of the easing function.
    
    Returns:
        The easing function.
    
    Raises:
        KeyError: If name is not a known easing function.
    """
    if name not in EASING_FUNCTIONS:
        raise KeyError(f"Unknown easing function: {name}. Available: {list(EASING_FUNCTIONS.keys())}")
    return EASING_FUNCTIONS[name]


def interpolate(
    start: float,
    end: float,
    t: float,
    easing: EasingFunc | str = linear,
) -> float:
    """Interpolate between two values using an easing function.
    
    Args:
        start: Starting value.
        end: Ending value.
        t: Progress from 0.0 to 1.0.
        easing: Easing function or name.
    
    Returns:
        Interpolated value between start and end.
    """
    if isinstance(easing, str):
        easing = get_easing(easing)
    return start + (end - start) * easing(t)
