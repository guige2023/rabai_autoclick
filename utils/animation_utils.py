"""
Animation easing functions and interpolation utilities.

This module provides common animation curves, interpolation methods,
and utilities for smooth UI animations.
"""

from __future__ import annotations

import math
from typing import Callable, Tuple, List, Protocol
from dataclasses import dataclass


# Type aliases
EasingFunc = Callable[[float], float]
KeyFrame = Tuple[float, float]


class EasingProtocol(Protocol):
    """Protocol for easing functions."""
    def __call__(self, t: float) -> float:
        ...


@dataclass
class KeyFrameAnimation:
    """
    Keyframe-based animation definition.

    Attributes:
        keyframes: List of (time, value) tuples, time in 0.0-1.0 range.
        easing: Easing function applied between keyframes.
        loop: Whether animation should loop.
    """
    keyframes: List[KeyFrame]
    easing: EasingFunc = field(default=lambda t: t)
    loop: bool = False

    def get_value(self, t: float) -> float:
        """Get interpolated value at normalized time t."""
        if not self.keyframes:
            return 0.0

        if self.loop:
            t = t % 1.0
        else:
            t = max(0.0, min(1.0, t))

        # Find surrounding keyframes
        prev: KeyFrame = self.keyframes[0]
        next_kf: KeyFrame = prev

        for kf in self.keyframes:
            if kf[0] <= t:
                prev = kf
            if kf[0] >= t and next_kf == prev:
                next_kf = kf

        if prev == next_kf:
            return prev[1]

        # Interpolate between keyframes
        span = next_kf[0] - prev[0]
        if span <= 0:
            return prev[1]

        local_t = (t - prev[0]) / span
        eased_t = self.easing(local_t)
        return prev[1] + (next_kf[1] - prev[1]) * eased_t


# Built-in easing functions
def linear(t: float) -> float:
    """Linear easing, no acceleration."""
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
    return 1 - ((-2 * t + 2) ** 3) / 2


def ease_in_quart(t: float) -> float:
    """Quartic ease-in."""
    return t ** 4


def ease_out_quart(t: float) -> float:
    """Quartic ease-out."""
    return 1 - ((1 - t) ** 4)


def ease_in_out_quart(t: float) -> float:
    """Quartic ease-in-out."""
    if t < 0.5:
        return 8 * t ** 4
    return 1 - ((-2 * t + 2) ** 4) / 2


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
    return 0 if t == 0 else 2 ** (10 * t - 10)


def ease_out_expo(t: float) -> float:
    """Exponential ease-out."""
    return 1 if t == 1 else 1 - 2 ** (-10 * t)


def ease_in_out_expo(t: float) -> float:
    """Exponential ease-in-out."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    if t < 0.5:
        return 2 ** (20 * t - 10) / 2
    return (2 - 2 ** (-20 * t + 10)) / 2


def ease_in_back(t: float, c1: float = 1.70158) -> float:
    """Back ease-in with overshoot."""
    return (c1 + 1) * t ** 3 - c1 * t ** 2


def ease_out_back(t: float, c1: float = 1.70158) -> float:
    """Back ease-out with overshoot."""
    return 1 + (c1 + 1) * (t - 1) ** 3 + c1 * (t - 1) ** 2


def ease_in_out_back(t: float) -> float:
    """Back ease-in-out with overshoot."""
    c1 = 1.70158
    c2 = c1 * 1.525
    if t < 0.5:
        return ((2 * t) ** 2 * ((c2 + 1) * 2 * t - c2)) / 2
    return ((2 * t - 2) ** 2 * ((c2 + 1) * (t * 2 - 2) + c2) + 2) / 2


def ease_in_elastic(t: float) -> float:
    """Elastic ease-in with oscillation."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    return -2 ** (10 * t - 10) * math.sin((t * 10 - 10.75) * (2 * math.pi) / 3)


def ease_out_elastic(t: float) -> float:
    """Elastic ease-out with oscillation."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    return 2 ** (-10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / 3) + 1


def ease_in_out_elastic(t: float) -> float:
    """Elastic ease-in-out with oscillation."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    if t < 0.5:
        return -(2 ** (20 * t - 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2
    return (2 ** (-20 * t + 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2 + 1


def ease_in_bounce(t: float) -> float:
    """Bounce ease-in."""
    return 1 - ease_out_bounce(1 - t)


def ease_out_bounce(t: float) -> float:
    """Bounce ease-out with multiple bounces."""
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


# Easing function registry
EASING_FUNCTIONS: dict[str, EasingFunc] = {
    "linear": linear,
    "ease-in-quad": ease_in_quad,
    "ease-out-quad": ease_out_quad,
    "ease-in-out-quad": ease_in_out_quad,
    "ease-in-cubic": ease_in_cubic,
    "ease-out-cubic": ease_out_cubic,
    "ease-in-out-cubic": ease_in_out_cubic,
    "ease-in-quart": ease_in_quart,
    "ease-out-quart": ease_out_quart,
    "ease-in-out-quart": ease_in_out_quart,
    "ease-in-sine": ease_in_sine,
    "ease-out-sine": ease_out_sine,
    "ease-in-out-sine": ease_in_out_sine,
    "ease-in-expo": ease_in_expo,
    "ease-out-expo": ease_out_expo,
    "ease-in-out-expo": ease_in_out_expo,
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
    """Get easing function by name."""
    return EASING_FUNCTIONS.get(name, linear)


def lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation between two values."""
    return a + (b - a) * t


def inverse_lerp(a: float, b: float, v: float) -> float:
    """Inverse linear interpolation - find t given v."""
    if a == b:
        return 0.0
    return (v - a) / (b - a)


def remap(
    value: float,
    in_min: float,
    in_max: float,
    out_min: float,
    out_max: float,
) -> float:
    """Remap value from one range to another."""
    t = inverse_lerp(in_min, in_max, value)
    return lerp(out_min, out_max, t)


def damp(
    current: float,
    target: float,
    smoothing: float,
    delta_time: float,
) -> float:
    """
    Apply exponential smoothing to approach target.

    Args:
        current: Current value.
        target: Target value to approach.
        smoothing: Smoothing factor (higher = slower).
        delta_time: Time elapsed since last update.
    """
    if smoothing <= 0:
        return target
    factor = math.exp(-smoothing * delta_time)
    return current + (target - current) * (1 - factor)
