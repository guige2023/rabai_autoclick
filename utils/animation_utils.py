"""
Animation Easing Utilities for UI Automation

Provides easing functions and animation utilities for
smooth, natural-looking UI animations in automation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable


# Easing function type
EasingFunc = Callable[[float], float]


def ease_linear(t: float) -> float:
    """Linear easing (no acceleration)."""
    return t


def ease_in_quad(t: float) -> float:
    """Quadratic ease-in."""
    return t ** 2


def ease_out_quad(t: float) -> float:
    """Quadratic ease-out."""
    return 1 - (1 - t) ** 2


def ease_in_out_quad(t: float) -> float:
    """Quadratic ease-in-out."""
    if t < 0.5:
        return 2 * t ** 2
    return 1 - (-2 * t + 2) ** 2 / 2


def ease_in_cubic(t: float) -> float:
    """Cubic ease-in."""
    return t ** 3


def ease_out_cubic(t: float) -> float:
    """Cubic ease-out."""
    return 1 - (1 - t) ** 3


def ease_in_out_cubic(t: float) -> float:
    """Cubic ease-in-out."""
    if t < 0.5:
        return 4 * t ** 3
    return 1 - (-2 * t + 2) ** 3 / 2


def ease_in_quart(t: float) -> float:
    """Quartic ease-in."""
    return t ** 4


def ease_out_quart(t: float) -> float:
    """Quartic ease-out."""
    return 1 - (1 - t) ** 4


def ease_in_out_quart(t: float) -> float:
    """Quartic ease-in-out."""
    if t < 0.5:
        return 8 * t ** 4
    return 1 - (-2 * t + 2) ** 4 / 2


def ease_in_sine(t: float) -> float:
    """Sine ease-in."""
    return 1 - math.cos(t * math.pi / 2)


def ease_out_sine(t: float) -> float:
    """Sine ease-out."""
    return math.sin(t * math.pi / 2)


def ease_in_out_sine(t: float) -> float:
    """Sine ease-in-out."""
    return -(math.cos(math.pi * t) - 1) / 2


def ease_in_expo(t: float) -> float:
    """Exponential ease-in."""
    return 2 ** (10 * t - 10) if t > 0 else 0


def ease_out_expo(t: float) -> float:
    """Exponential ease-out."""
    return 1 if t >= 1 else 2 ** (10 * t - 10)


def ease_in_out_expo(t: float) -> float:
    """Exponential ease-in-out."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    if t < 0.5:
        return 2 ** (20 * t - 10) / 2
    return (2 - 2 ** (-20 * t + 10)) / 2


def ease_in_back(t: float) -> float:
    """Back ease-in (overshoots before settling)."""
    c1 = 1.70158
    return (c1 + 1) * t ** 3 - c1 * t ** 2


def ease_out_back(t: float) -> float:
    """Back ease-out (overshoots slightly at end)."""
    c1 = 1.70158
    c3 = c1 + 1
    return 1 + c3 * (t - 1) ** 3 + c1 * (t - 1) ** 2


def ease_in_out_back(t: float) -> float:
    """Back ease-in-out."""
    c1 = 1.70158
    c2 = c1 * 1.525
    if t < 0.5:
        return ((2 * t) ** 2 * ((c2 + 1) * 2 * t - c2)) / 2
    return ((2 * t - 2) ** 2 * ((c2 + 1) * (t * 2 - 2) + c2) + 2) / 2


def ease_in_elastic(t: float) -> float:
    """Elastic ease-in (spring-like at start)."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    return -(2 ** (10 * t - 10)) * math.sin((t * 10 - 10.75) * ((2 * math.pi) / 3))


def ease_out_elastic(t: float) -> float:
    """Elastic ease-out (spring-like at end)."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    return 2 ** (-10 * t) * math.sin((t * 10 - 0.75) * ((2 * math.pi) / 3)) + 1


def ease_in_out_elastic(t: float) -> float:
    """Elastic ease-in-out."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    if t < 0.5:
        return -(2 ** (20 * t - 10) * math.sin((20 * t - 11.125) * ((2 * math.pi) / 4.5))) / 2
    return (2 ** (-20 * t + 10) * math.sin((20 * t - 11.125) * ((2 * math.pi) / 4.5))) / 2 + 1


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
        return (1 - ease_out_bounce(1 - 2 * t)) / 2
    return (1 + ease_out_bounce(2 * t - 1)) / 2


# Registry of all easing functions
EASING_FUNCTIONS: dict[str, EasingFunc] = {
    "linear": ease_linear,
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
    "ease_in_elastic": ease_in_elastic,
    "ease_out_elastic": ease_out_elastic,
    "ease_in_out_elastic": ease_in_out_elastic,
    "ease_in_bounce": ease_in_bounce,
    "ease_out_bounce": ease_out_bounce,
    "ease_in_out_bounce": ease_in_out_bounce,
}


@dataclass
class AnimationKeyframe:
    """Keyframe for animation."""
    time: float  # 0.0 to 1.0
    value: float
    easing: str = "linear"


@dataclass
class Animation:
    """Animation definition with keyframes."""
    name: str
    duration: float  # seconds
    keyframes: list[AnimationKeyframe]
    loop: bool = False


class EasingAnimator:
    """
    Animator that applies easing functions to interpolate values.

    Provides methods to calculate animated values at any point
    in time based on keyframes and easing functions.
    """

    def __init__(self) -> None:
        self._easing_funcs: dict[str, EasingFunc] = dict(EASING_FUNCTIONS)

    def register_easing(self, name: str, func: EasingFunc) -> None:
        """Register a custom easing function."""
        self._easing_funcs[name] = func

    def get_easing(self, name: str) -> EasingFunc:
        """Get an easing function by name."""
        return self._easing_funcs.get(name, ease_linear)

    def interpolate(
        self,
        start: float,
        end: float,
        progress: float,
        easing: str = "linear",
    ) -> float:
        """
        Interpolate between two values with easing.

        Args:
            start: Starting value
            end: Ending value
            progress: Progress from 0.0 to 1.0
            easing: Easing function name

        Returns:
            Interpolated value
        """
        eased_progress = self.get_easing(easing)(progress)
        return start + (end - start) * eased_progress

    def animate(
        self,
        animation: Animation,
        time_offset: float,
    ) -> float:
        """
        Calculate animated value at a specific time.

        Args:
            animation: Animation definition
            time_offset: Time in seconds from animation start

        Returns:
            Animated value at the given time
        """
        if not animation.keyframes:
            return 0.0

        # Calculate normalized time
        t = (time_offset % animation.duration) / animation.duration

        # Find surrounding keyframes
        prev_kf: AnimationKeyframe | None = None
        next_kf: AnimationKeyframe | None = None

        for kf in animation.keyframes:
            if kf.time <= t:
                prev_kf = kf
            if kf.time >= t and next_kf is None:
                next_kf = kf

        # Handle edge cases
        if prev_kf is None:
            return animation.keyframes[0].value
        if next_kf is None or prev_kf is next_kf:
            return prev_kf.value

        # Interpolate between keyframes
        range_time = next_kf.time - prev_kf.time
        if range_time <= 0:
            return prev_kf.value

        local_t = (t - prev_kf.time) / range_time
        easing = self.get_easing(prev_kf.easing)
        eased_t = easing(local_t)

        return prev_kf.value + (next_kf.value - prev_kf.value) * eased_t


def spring_animation(
    current: float,
    target: float,
    velocity: float,
    mass: float = 1.0,
    stiffness: float = 100.0,
    damping: float = 10.0,
    dt: float = 0.016,
) -> tuple[float, float]:
    """
    Calculate spring animation step.

    Args:
        current: Current value
        target: Target value
        velocity: Current velocity
        mass: Spring mass
        stiffness: Spring stiffness
        damping: Damping coefficient
        dt: Time step

    Returns:
        Tuple of (new_value, new_velocity)
    """
    spring_force = -stiffness * (current - target)
    damping_force = -damping * velocity
    acceleration = (spring_force + damping_force) / mass

    new_velocity = velocity + acceleration * dt
    new_value = current + new_velocity * dt

    return new_value, new_velocity


def is_animation_complete(
    current: float,
    target: float,
    velocity: float,
    threshold: float = 0.001,
) -> bool:
    """Check if a spring animation is complete."""
    return abs(current - target) < threshold and abs(velocity) < threshold
