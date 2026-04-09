"""Interpolation and easing function utilities.

Provides various interpolation methods for smooth transitions,
animation timing, and value remapping.
"""

from __future__ import annotations

from typing import Callable, Sequence, Tuple, Optional
from enum import Enum, auto
import math


class InterpolationType(Enum):
    """Supported interpolation methods."""
    LINEAR = auto()
    EASE_IN = auto()
    EASE_OUT = auto()
    EASE_IN_OUT = auto()
    STEP = auto()
    SMOOTHSTEP = auto()
    SIGMOID = auto()
    BOUNCE = auto()
    ELASTIC = auto()
    SPRING = auto()
    CUBIC = auto()
    QUINTIC = auto()
    CIRCULAR = auto()
    EXPONENTIAL = auto()


# Easing functions
def ease_in(t: float, power: float = 2.0) -> float:
    """Ease-in: starts slow, ends fast (inverse of ease-out)."""
    return math.pow(max(0.0, min(1.0, t)), power)


def ease_out(t: float, power: float = 2.0) -> float:
    """Ease-out: starts fast, ends slow."""
    return 1.0 - math.pow(max(0.0, min(1.0, 1.0 - t)), power)


def ease_in_out(t: float) -> float:
    """Ease-in-out: slow start, fast middle, slow end."""
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        return 2.0 * t * t
    return 1.0 - math.pow(-2.0 * t + 2.0, 2.0) / 2.0


def step(t: float, steps: int = 1) -> float:
    """Step function: discretizes the value into steps."""
    return math.floor(t * (steps + 1)) / steps


def smoothstep(t: float) -> float:
    """Smoothstep: smooth Hermite interpolation (0 at 0, 1 at 1)."""
    t = max(0.0, min(1.0, t))
    return t * t * (3.0 - 2.0 * t)


def sigmoid(t: float, center: float = 0.5, steepness: float = 10.0) -> float:
    """Sigmoid/s-curve interpolation."""
    t = max(0.0, min(1.0, t))
    return 1.0 / (1.0 + math.exp(-steepness * (t - center)))


def bounce(t: float, bounces: int = 3) -> float:
    """Bounce easing: like a bouncing ball."""
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        return (1.0 - math.cos(t * math.pi * bounces * 2) *
                (1.0 - t * 2))
    return 1.0 + math.sin((t - 0.5) * math.pi * bounces * 2) * (t * 2 - 1)


def elastic(t: float, oscillations: float = 4.0) -> float:
    """Elastic easing: like a spring snapping into place."""
    t = max(0.0, min(1.0, t))
    return (
        math.pow(2.0, -10.0 * t)
        * math.sin((t - oscillations / 20.0) * (2.0 * math.pi) / oscillations)
        + 1.0
    )


def spring(t: float, stiffness: float = 100.0, damping: float = 10.0) -> float:
    """Spring physics-based interpolation."""
    t = max(0.0, min(1.0, t))
    return 1.0 - math.exp(-damping * t) * math.cos(stiffness * t)


def cubic_ease(t: float) -> float:
    """Cubic ease-in-out."""
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        return 4.0 * t * t * t
    return 1.0 - math.pow(-2.0 * t + 2.0, 3.0) / 2.0


def quintic_ease(t: float) -> float:
    """Quintic ease-in-out (smoother than cubic)."""
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        return 16.0 * t * t * t * t * t
    return 1.0 - math.pow(-2.0 * t + 2.0, 5.0) / 2.0


def circular_ease(t: float) -> float:
    """Circular ease-in-out."""
    t = max(0.0, min(1.0, t))
    if t < 0.5:
        return (1.0 - math.sqrt(1.0 - math.pow(2.0 * t, 2.0))) / 2.0
    return (math.sqrt(1.0 - math.pow(-2.0 * t + 2.0, 2.0)) + 1.0) / 2.0


def exponential_ease(t: float, base: float = 2.0) -> float:
    """Exponential ease-in-out."""
    t = max(0.0, min(1.0, t))
    if t == 0.0:
        return 0.0
    if t == 1.0:
        return 1.0
    if t < 0.5:
        return math.pow(base, 10.0 * (2.0 * t - 1.0)) / 2.0
    return (2.0 - math.pow(base, 10.0 * (-2.0 * t + 1.0))) / 2.0


class Interpolator:
    """Configurable interpolator with multiple easing methods.

    Example:
        interp = Interpolator(InterpolationType.EASE_IN_OUT)
        value = interp.interpolate(0.5)  # Returns 0.5 with eased timing
    """

    def __init__(
        self,
        interp_type: InterpolationType = InterpolationType.LINEAR,
        custom_fn: Optional[Callable[[float], float]] = None,
    ) -> None:
        self.interp_type = interp_type
        self.custom_fn = custom_fn

    def interpolate(self, t: float) -> float:
        """Apply interpolation to normalized input [0, 1]."""
        if self.custom_fn is not None:
            return self.custom_fn(t)
        return self._interpolate(t)

    def _interpolate(self, t: float) -> float:
        """Apply the configured interpolation method."""
        t = max(0.0, min(1.0, t))
        if self.interp_type == InterpolationType.LINEAR:
            return t
        elif self.interp_type == InterpolationType.EASE_IN:
            return ease_in(t)
        elif self.interp_type == InterpolationType.EASE_OUT:
            return ease_out(t)
        elif self.interp_type == InterpolationType.EASE_IN_OUT:
            return ease_in_out(t)
        elif self.interp_type == InterpolationType.STEP:
            return step(t)
        elif self.interp_type == InterpolationType.SMOOTHSTEP:
            return smoothstep(t)
        elif self.interp_type == InterpolationType.SIGMOID:
            return sigmoid(t)
        elif self.interp_type == InterpolationType.BOUNCE:
            return bounce(t)
        elif self.interp_type == InterpolationType.ELASTIC:
            return elastic(t)
        elif self.interp_type == InterpolationType.SPRING:
            return spring(t)
        elif self.interp_type == InterpolationType.CUBIC:
            return cubic_ease(t)
        elif self.interp_type == InterpolationType.QUINTIC:
            return quintic_ease(t)
        elif self.interp_type == InterpolationType.CIRCULAR:
            return circular_ease(t)
        elif self.interp_type == InterpolationType.EXPONENTIAL:
            return exponential_ease(t)
        return t

    def remap(
        self, t: float, in_min: float, in_max: float, out_min: float, out_max: float
    ) -> float:
        """Remap input from [in_min, in_max] to [out_min, out_max] with easing."""
        t_norm = (t - in_min) / (in_max - in_min)
        t_eased = self.interpolate(t_norm)
        return out_min + t_eased * (out_max - out_min)


def lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation between two values."""
    return a + (b - a) * max(0.0, min(1.0, t))


def lerp_points(
    p1: Tuple[float, float], p2: Tuple[float, float], t: float
) -> Tuple[float, float]:
    """Linear interpolation between two 2D points."""
    return (lerp(p1[0], p2[0], t), lerp(p1[1], p2[1], t))


def lerp_sequence(
    values: Sequence[float], t: float
) -> float:
    """Interpolate along a sequence of values.

    Example:
        lerp_sequence([0, 50, 100], 0.5)  # Returns 50
    """
    if len(values) < 2:
        raise ValueError("Sequence must have at least 2 values")
    t = max(0.0, min(1.0, t))
    idx = t * (len(values) - 1)
    lower = int(math.floor(idx))
    upper = min(lower + 1, len(values) - 1)
    frac = idx - lower
    return lerp(values[lower], values[upper], frac)


def catmull_rom(
    p0: float, p1: float, p2: float, p3: float, t: float
) -> float:
    """Catmull-Rom spline interpolation between points."""
    t = max(0.0, min(1.0, t))
    t2 = t * t
    t3 = t2 * t
    return 0.5 * (
        (2.0 * p1)
        + (-p0 + p2) * t
        + (2.0 * p0 - 5.0 * p1 + 4.0 * p2 - p3) * t2
        + (-p0 + 3.0 * p1 - 3.0 * p2 + p3) * t3
    )


def bezier_cubic(
    p0: float, p1: float, p2: float, p3: float, t: float
) -> float:
    """Cubic Bezier curve interpolation."""
    t = max(0.0, min(1.0, t))
    t2 = t * t
    t3 = t2 * t
    mt = 1.0 - t
    mt2 = mt * mt
    mt3 = mt2 * mt
    return mt3 * p0 + 3.0 * mt2 * t * p1 + 3.0 * mt * t2 * p2 + t3 * p3
