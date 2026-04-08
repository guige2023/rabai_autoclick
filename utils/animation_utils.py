"""
Animation easing and interpolation utilities.

Provides utilities for smooth animations including
various easing functions, interpolation, and animation curves.
"""

from __future__ import annotations

import math
from typing import Callable, List, Tuple, Optional, Dict, Any
from dataclasses import dataclass
from enum import Enum


class EasingType(Enum):
    """Types of easing functions."""
    LINEAR = "linear"
    QUAD_IN = "quad_in"
    QUAD_OUT = "quad_out"
    QUAD_IN_OUT = "quad_in_out"
    CUBIC_IN = "cubic_in"
    CUBIC_OUT = "cubic_out"
    CUBIC_IN_OUT = "cubic_in_out"
    QUART_IN = "quart_in"
    QUART_OUT = "quart_out"
    QUART_IN_OUT = "quart_in_out"
    QUINT_IN = "quint_in"
    QUINT_OUT = "quint_out"
    QUINT_IN_OUT = "quint_in_out"
    SINE_IN = "sine_in"
    SINE_OUT = "sine_out"
    SINE_IN_OUT = "sine_in_out"
    EXP_IN = "exp_in"
    EXP_OUT = "exp_out"
    EXP_IN_OUT = "exp_in_out"
    CIRC_IN = "circ_in"
    CIRC_OUT = "circ_out"
    CIRC_IN_OUT = "circ_in_out"
    ELASTIC_IN = "elastic_in"
    ELASTIC_OUT = "elastic_out"
    ELASTIC_IN_OUT = "elastic_in_out"
    BOUNCE_IN = "bounce_in"
    BOUNCE_OUT = "bounce_out"
    BOUNCE_IN_OUT = "bounce_in_out"
    BACK_IN = "back_in"
    BACK_OUT = "back_out"
    BACK_IN_OUT = "back_in_out"


# Linear easing
def linear(t: float) -> float:
    """Linear easing (no easing)."""
    return t


# Quadratic easing
def quad_in(t: float) -> float:
    """Quadratic ease in."""
    return t * t


def quad_out(t: float) -> float:
    """Quadratic ease out."""
    return t * (2 - t)


def quad_in_out(t: float) -> float:
    """Quadratic ease in-out."""
    return 2 * t * t if t < 0.5 else -1 + (4 - 2 * t) * t


# Cubic easing
def cubic_in(t: float) -> float:
    """Cubic ease in."""
    return t * t * t


def cubic_out(t: float) -> float:
    """Cubic ease out."""
    return (t - 1) ** 3 + 1


def cubic_in_out(t: float) -> float:
    """Cubic ease in-out."""
    return 4 * t * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2


# Quartic easing
def quart_in(t: float) -> float:
    """Quartic ease in."""
    return t * t * t * t


def quart_out(t: float) -> float:
    """Quartic ease out."""
    return 1 - (t - 1) ** 4


def quart_in_out(t: float) -> float:
    """Quartic ease in-out."""
    return 8 * t * t * t * t if t < 0.5 else 1 - 8 * (t - 1) ** 4


# Quintic easing
def quint_in(t: float) -> float:
    """Quintic ease in."""
    return t * t * t * t * t


def quint_out(t: float) -> float:
    """Quintic ease out."""
    return 1 + (t - 1) ** 5


def quint_in_out(t: float) -> float:
    """Quintic ease in-out."""
    return 16 * t * t * t * t * t if t < 0.5 else 1 + 16 * (t - 1) ** 5


# Sine easing
def sine_in(t: float) -> float:
    """Sine ease in."""
    return 1 - math.cos(t * math.pi / 2)


def sine_out(t: float) -> float:
    """Sine ease out."""
    return math.sin(t * math.pi / 2)


def sine_in_out(t: float) -> float:
    """Sine ease in-out."""
    return -(math.cos(math.pi * t) - 1) / 2


# Exponential easing
def exp_in(t: float) -> float:
    """Exponential ease in."""
    return 2 ** (10 * (t - 1))


def exp_out(t: float) -> float:
    """Exponential ease out."""
    return 1 - 2 ** (-10 * t)


def exp_in_out(t: float) -> float:
    """Exponential ease in-out."""
    if t < 0.5:
        return 2 ** (20 * t - 10) / 2
    return (2 - 2 ** (-20 * t + 10)) / 2


# Circular easing
def circ_in(t: float) -> float:
    """Circular ease in."""
    return 1 - math.sqrt(1 - t ** 2)


def circ_out(t: float) -> float:
    """Circular ease out."""
    return math.sqrt(1 - (t - 1) ** 2)


def circ_in_out(t: float) -> float:
    """Circular ease in-out."""
    if t < 0.5:
        return (1 - math.sqrt(1 - (2 * t) ** 2)) / 2
    return (math.sqrt(1 - (-2 * t + 2) ** 2) + 1) / 2


# Elastic easing
def elastic_in(t: float) -> float:
    """Elastic ease in."""
    if t == 0 or t == 1:
        return t
    c = 2 * math.pi / 3
    return -(2 ** (10 * t - 10)) * math.sin((t * 10 - 10.75) * c)


def elastic_out(t: float) -> float:
    """Elastic ease out."""
    if t == 0 or t == 1:
        return t
    c = 2 * math.pi / 3
    return 2 ** (-10 * t) * math.sin((t * 10 - 0.75) * c) + 1


def elastic_in_out(t: float) -> float:
    """Elastic ease in-out."""
    if t == 0 or t == 1:
        return t
    c = 2 * math.pi / 4.5
    if t < 0.5:
        return -(2 ** (20 * t - 10) * math.sin((20 * t - 11.125) * c)) / 2
    return (2 ** (-20 * t + 10) * math.sin((20 * t - 11.125) * c)) / 2 + 1


# Bounce easing
def bounce_out(t: float) -> float:
    """Bounce ease out."""
    n = 7.5625
    d = 2.75
    if t < 1 / d:
        return n * t * t
    elif t < 2 / d:
        t -= 1.5 / d
        return n * t * t + 0.75
    elif t < 2.5 / d:
        t -= 2.25 / d
        return n * t * t + 0.9375
    else:
        t -= 2.625 / d
        return n * t * t + 0.984375


def bounce_in(t: float) -> float:
    """Bounce ease in."""
    return 1 - bounce_out(1 - t)


def bounce_in_out(t: float) -> float:
    """Bounce ease in-out."""
    if t < 0.5:
        return (1 - bounce_out(1 - 2 * t)) / 2
    return (1 + bounce_out(2 * t - 1)) / 2


# Back easing
def back_in(t: float) -> float:
    """Back ease in."""
    c = 1.70158
    return c * t * t * t - (c - 1) * t * t


def back_out(t: float) -> float:
    """Back ease out."""
    c = 1.70158
    return 1 + c * ((t - 1) ** 3) + (c - 1) * ((t - 1) ** 2)


def back_in_out(t: float) -> float:
    """Back ease in-out."""
    c = 1.70158 * 1.525
    if t < 0.5:
        return ((2 * t) ** 2 * ((c + 1) * 2 * t - c)) / 2
    return ((2 * t - 2) ** 2 * ((c + 1) * (t * 2 - 2) + c) + 2) / 2


# Easing function registry
EASING_FUNCTIONS: Dict[EasingType, Callable[[float], float]] = {
    EasingType.LINEAR: linear,
    EasingType.QUAD_IN: quad_in,
    EasingType.QUAD_OUT: quad_out,
    EasingType.QUAD_IN_OUT: quad_in_out,
    EasingType.CUBIC_IN: cubic_in,
    EasingType.CUBIC_OUT: cubic_out,
    EasingType.CUBIC_IN_OUT: cubic_in_out,
    EasingType.QUART_IN: quart_in,
    EasingType.QUART_OUT: quart_out,
    EasingType.QUART_IN_OUT: quart_in_out,
    EasingType.QUINT_IN: quint_in,
    EasingType.QUINT_OUT: quint_out,
    EasingType.QUINT_IN_OUT: quint_in_out,
    EasingType.SINE_IN: sine_in,
    EasingType.SINE_OUT: sine_out,
    EasingType.SINE_IN_OUT: sine_in_out,
    EasingType.EXP_IN: exp_in,
    EasingType.EXP_OUT: exp_out,
    EasingType.EXP_IN_OUT: exp_in_out,
    EasingType.CIRC_IN: circ_in,
    EasingType.CIRC_OUT: circ_out,
    EasingType.CIRC_IN_OUT: circ_in_out,
    EasingType.ELASTIC_IN: elastic_in,
    EasingType.ELASTIC_OUT: elastic_out,
    EasingType.ELASTIC_IN_OUT: elastic_in_out,
    EasingType.BOUNCE_IN: bounce_in,
    EasingType.BOUNCE_OUT: bounce_out,
    EasingType.BOUNCE_IN_OUT: bounce_in_out,
    EasingType.BACK_IN: back_in,
    EasingType.BACK_OUT: back_out,
    EasingType.BACK_IN_OUT: back_in_out,
}


def get_easing_function(easing: EasingType) -> Callable[[float], float]:
    """Get easing function by type.
    
    Args:
        easing: Easing type
        
    Returns:
        Easing function
    """
    return EASING_FUNCTIONS.get(easing, linear)


def interpolate(
    start: float,
    end: float,
    t: float,
    easing: EasingType = EasingType.LINEAR
) -> float:
    """Interpolate between two values with easing.
    
    Args:
        start: Start value
        end: End value
        t: Progress (0-1)
        easing: Easing type
        
    Returns:
        Interpolated value
    """
    ease_func = get_easing_function(easing)
    eased_t = ease_func(max(0, min(1, t)))
    return start + (end - start) * eased_t


def interpolate_point(
    start_x: float,
    start_y: float,
    end_x: float,
    end_y: float,
    t: float,
    easing: EasingType = EasingType.LINEAR
) -> Tuple[float, float]:
    """Interpolate between two points with easing.
    
    Args:
        start_x: Start X
        start_y: Start Y
        end_x: End X
        end_y: End Y
        t: Progress (0-1)
        easing: Easing type
        
    Returns:
        Tuple of (x, y)
    """
    return (
        interpolate(start_x, end_x, t, easing),
        interpolate(start_y, end_y, t, easing)
    )


@dataclass
class AnimationKeyframe:
    """An animation keyframe."""
    time: float  # 0-1
    value: float
    easing: EasingType = EasingType.LINEAR


class AnimationCurve:
    """An animation curve with keyframes."""
    
    def __init__(self, keyframes: Optional[List[AnimationKeyframe]] = None):
        """Initialize animation curve.
        
        Args:
            keyframes: List of keyframes
        """
        self.keyframes = keyframes or []
        self.keyframes.sort(key=lambda k: k.time)
    
    def add_keyframe(
        self,
        time: float,
        value: float,
        easing: EasingType = EasingType.LINEAR
    ) -> "AnimationCurve":
        """Add a keyframe.
        
        Args:
            time: Time (0-1)
            value: Value at time
            easing: Easing to next keyframe
            
        Returns:
            Self for chaining
        """
        self.keyframes.append(AnimationKeyframe(time, value, easing))
        self.keyframes.sort(key=lambda k: k.time)
        return self
    
    def get_value(self, t: float) -> float:
        """Get interpolated value at time.
        
        Args:
            t: Time (0-1)
            
        Returns:
            Interpolated value
        """
        if not self.keyframes:
            return 0.0
        
        if t <= self.keyframes[0].time:
            return self.keyframes[0].value
        
        if t >= self.keyframes[-1].time:
            return self.keyframes[-1].value
        
        # Find surrounding keyframes
        for i in range(len(self.keyframes) - 1):
            k1 = self.keyframes[i]
            k2 = self.keyframes[i + 1]
            
            if k1.time <= t <= k2.time:
                local_t = (t - k1.time) / (k2.time - k1.time)
                eased_t = get_easing_function(k1.easing)(local_t)
                return k1.value + (k2.value - k1.value) * eased_t
        
        return self.keyframes[-1].value


@dataclass
class AnimationFrame:
    """A single animation frame."""
    time: float
    x: Optional[float] = None
    y: Optional[float] = None
    width: Optional[float] = None
    height: Optional[float] = None
    alpha: Optional[float] = None
    rotation: Optional[float] = None
    scale: Optional[float] = None


class Animator:
    """Utility for creating animations."""
    
    def __init__(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration: float,
        easing: EasingType = EasingType.QUAD_OUT
    ):
        """Initialize animator.
        
        Args:
            start_x: Start X
            start_y: Start Y
            end_x: End X
            end_y: End Y
            duration: Duration in seconds
            easing: Easing type
        """
        self.start_x = start_x
        self.start_y = start_y
        self.end_x = end_x
        self.end_y = end_y
        self.duration = duration
        self.easing = easing
        self._start_time: Optional[float] = None
    
    def start(self) -> None:
        """Start the animation."""
        import time
        self._start_time = time.time()
    
    def get_current(self) -> Tuple[float, float]:
        """Get current position.
        
        Returns:
            Tuple of (x, y)
        """
        import time
        
        if self._start_time is None:
            return (self.start_x, self.start_y)
        
        elapsed = time.time() - self._start_time
        t = min(1.0, elapsed / self.duration)
        
        x = interpolate(self.start_x, self.end_x, t, self.easing)
        y = interpolate(self.start_y, self.end_y, t, self.easing)
        
        return (x, y)
    
    @property
    def is_complete(self) -> bool:
        """Check if animation is complete."""
        import time
        
        if self._start_time is None:
            return False
        
        return time.time() - self._start_time >= self.duration


def create_ease_in_curve(easing: EasingType) -> AnimationCurve:
    """Create a curve with ease-in for everything going in.
    
    Args:
        easing: Base easing type
        
    Returns:
        AnimationCurve
    """
    curve = AnimationCurve()
    curve.add_keyframe(0, 0, EasingType.LINEAR)
    curve.add_keyframe(1, 1, easing)
    return curve


def create_ease_out_curve() -> AnimationCurve:
    """Create a standard ease-out curve."""
    return create_ease_in_curve(EasingType.QUAD_OUT)


def create_ease_in_out_curve() -> AnimationCurve:
    """Create a standard ease-in-out curve."""
    curve = AnimationCurve()
    curve.add_keyframe(0, 0, EasingType.LINEAR)
    curve.add_keyframe(0.5, 0.5, EasingType.QUAD_IN)
    curve.add_keyframe(1, 1, EasingType.LINEAR)
    return curve
