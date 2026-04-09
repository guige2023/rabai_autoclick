"""
UI animation and easing function utilities.

Provides common easing functions, animation helpers,
and interpolation utilities for smooth UI animations.

Author: Auto-generated
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Sequence


class EasingType(Enum):
    """Standard easing types."""
    LINEAR = auto()
    EASE_IN = auto()
    EASE_OUT = auto()
    EASE_IN_OUT = auto()
    EASE_IN_QUAD = auto()
    EASE_OUT_QUAD = auto()
    EASE_IN_OUT_QUAD = auto()
    EASE_IN_CUBIC = auto()
    EASE_OUT_CUBIC = auto()
    EASE_IN_OUT_CUBIC = auto()
    EASE_IN_ELASTIC = auto()
    EASE_OUT_ELASTIC = auto()
    EASE_IN_OUT_ELASTIC = auto()
    EASE_IN_BOUNCE = auto()
    EASE_OUT_BOUNCE = auto()
    EASE_IN_OUT_BOUNCE = auto()


# Easing functions
def linear(t: float) -> float:
    """Linear easing (no easing)."""
    return t


def ease_in_quad(t: float) -> float:
    """Ease in quadratic."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Ease out quadratic."""
    return t * (2 - t)


def ease_in_out_quad(t: float) -> float:
    """Ease in-out quadratic."""
    return 2 * t * t if t < 0.5 else -1 + (4 - 2 * t) * t


def ease_in_cubic(t: float) -> float:
    """Ease in cubic."""
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Ease out cubic."""
    return 1 - pow(1 - t, 3)


def ease_in_out_cubic(t: float) -> float:
    """Ease in-out cubic."""
    return 4 * t * t * t if t < 0.5 else 1 - pow(-2 * t + 2, 3) / 2


def ease_in_elastic(t: float) -> float:
    """Ease in elastic."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    return -math.pow(2, 10 * t - 10) * math.sin((t * 10 - 10.75) * (2 * math.pi) / 3)


def ease_out_elastic(t: float) -> float:
    """Ease out elastic."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    return math.pow(2, -10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / 3) + 1


def ease_in_out_elastic(t: float) -> float:
    """Ease in-out elastic."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    if t < 0.5:
        return -(math.pow(2, 20 * t - 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2
    return (math.pow(2, -20 * t + 10) * math.sin((20 * t - 11.125) * (2 * math.pi) / 4.5)) / 2 + 1


def ease_in_bounce(t: float) -> float:
    """Ease in bounce."""
    return 1 - _bounce_out(1 - t)


def ease_out_bounce(t: float) -> float:
    """Ease out bounce."""
    return _bounce_out(t)


def _bounce_out(t: float) -> float:
    """Helper for bounce easing."""
    if t < 1 / 2.75:
        return 7.5625 * t * t
    elif t < 2 / 2.75:
        t -= 1.5 / 2.75
        return 7.5625 * t * t + 0.75
    elif t < 2.5 / 2.75:
        t -= 2.25 / 2.75
        return 7.5625 * t * t + 0.9375
    else:
        t -= 2.625 / 2.75
        return 7.5625 * t * t + 0.984375


def ease_in_out_bounce(t: float) -> float:
    """Ease in-out bounce."""
    if t < 0.5:
        return (1 - _bounce_out(1 - 2 * t)) / 2
    return (1 + _bounce_out(2 * t - 1)) / 2


EASING_FUNCTIONS: dict[EasingType, Callable[[float], float]] = {
    EasingType.LINEAR: linear,
    EasingType.EASE_IN: ease_in_quad,
    EasingType.EASE_OUT: ease_out_quad,
    EasingType.EASE_IN_OUT: ease_in_out_quad,
    EasingType.EASE_IN_QUAD: ease_in_quad,
    EasingType.EASE_OUT_QUAD: ease_out_quad,
    EasingType.EASE_IN_OUT_QUAD: ease_in_out_quad,
    EasingType.EASE_IN_CUBIC: ease_in_cubic,
    EasingType.EASE_OUT_CUBIC: ease_out_cubic,
    EasingType.EASE_IN_OUT_CUBIC: ease_in_out_cubic,
    EasingType.EASE_IN_ELASTIC: ease_in_elastic,
    EasingType.EASE_OUT_ELASTIC: ease_out_elastic,
    EasingType.EASE_IN_OUT_ELASTIC: ease_in_out_elastic,
    EasingType.EASE_IN_BOUNCE: ease_in_bounce,
    EasingType.EASE_OUT_BOUNCE: ease_out_bounce,
    EasingType.EASE_IN_OUT_BOUNCE: ease_in_out_bounce,
}


@dataclass
class KeyFrame:
    """A keyframe for animation."""
    time: float  # 0.0 to 1.0
    value: float
    easing: EasingType = EasingType.LINEAR


@dataclass
class AnimationValue:
    """An animated value with interpolation."""
    start_value: float
    end_value: float
    duration_ms: float
    easing: EasingType = EasingType.LINEAR
    start_time_ms: float = 0
    
    def get_value_at(self, elapsed_ms: float) -> float:
        """Get interpolated value at elapsed time."""
        if elapsed_ms <= 0:
            return self.start_value
        if elapsed_ms >= self.duration_ms:
            return self.end_value
        
        t = elapsed_ms / self.duration_ms
        easing_fn = EASING_FUNCTIONS.get(self.easing, linear)
        eased_t = easing_fn(t)
        
        return self.start_value + (self.end_value - self.start_value) * eased_t
    
    @property
    def progress(self) -> float:
        """Get progress from 0.0 to 1.0 (must be set externally)."""
        return 0.0


class Animator:
    """
    Animator for smooth value transitions.
    
    Example:
        animator = Animator()
        anim = animator.animate(0, 100, 1000, EasingType.EASE_OUT_QUAD)
        # In game loop:
        value = anim.get_value_at(elapsed_ms)
    """
    
    def __init__(self):
        self._animations: list[AnimationValue] = []
    
    def animate(
        self,
        start: float,
        end: float,
        duration_ms: float,
        easing: EasingType = EasingType.EASE_OUT_QUAD,
    ) -> AnimationValue:
        """
        Create an animation from start to end.
        
        Args:
            start: Start value
            end: End value
            duration_ms: Duration in milliseconds
            easing: Easing type
            
        Returns:
            AnimationValue object
        """
        anim = AnimationValue(
            start_value=start,
            end_value=end,
            duration_ms=duration_ms,
            easing=easing,
        )
        self._animations.append(anim)
        return anim
    
    def animate_xy(
        self,
        start_x: float,
        start_y: float,
        end_x: float,
        end_y: float,
        duration_ms: float,
        easing: EasingType = EasingType.EASE_OUT_QUAD,
    ) -> tuple[AnimationValue, AnimationValue]:
        """Animate x and y values together."""
        anim_x = self.animate(start_x, end_x, duration_ms, easing)
        anim_y = self.animate(start_y, end_y, duration_ms, easing)
        return (anim_x, anim_y)
    
    def clear_finished(self, current_time_ms: float) -> None:
        """Remove finished animations."""
        self._animations = [
            a for a in self._animations
            if current_time_ms - a.start_time_ms < a.duration_ms
        ]


def interpolate(
    start: float,
    end: float,
    t: float,
    easing: EasingType = EasingType.LINEAR,
) -> float:
    """
    Interpolate between two values.
    
    Args:
        start: Start value
        end: End value
        t: Interpolation factor (0.0 to 1.0)
        easing: Easing type
        
    Returns:
        Interpolated value
    """
    t = max(0.0, min(1.0, t))
    easing_fn = EASING_FUNCTIONS.get(easing, linear)
    eased_t = easing_fn(t)
    return start + (end - start) * eased_t


def interpolate_points(
    points: Sequence[tuple[float, float]],
    t: float,
) -> tuple[float, float]:
    """
    Interpolate between multiple points.
    
    Args:
        points: List of (x, y) points
        t: Overall interpolation factor (0.0 to 1.0)
        
    Returns:
        Interpolated (x, y)
    """
    if len(points) < 2:
        return points[0] if points else (0, 0)
    
    n = len(points) - 1
    segment = min(int(t * n), n - 1)
    local_t = (t * n) - segment
    
    p1 = points[segment]
    p2 = points[segment + 1]
    
    x = interpolate(p1[0], p2[0], local_t)
    y = interpolate(p1[1], p2[1], local_t)
    
    return (x, y)


def lerp_color(
    color1: tuple[int, int, int],
    color2: tuple[int, int, int],
    t: float,
) -> tuple[int, int, int]:
    """Linear interpolate between two RGB colors."""
    t = max(0.0, min(1.0, t))
    return (
        int(color1[0] + (color2[0] - color1[0]) * t),
        int(color1[1] + (color2[1] - color1[1]) * t),
        int(color1[2] + (color2[2] - color1[2]) * t),
    )


@dataclass
class Tween:
    """A tween animation for values."""
    from_value: float
    to_value: float
    duration: float
    elapsed: float = 0
    easing: EasingType = EasingType.LINEAR
    
    @property
    def value(self) -> float:
        return interpolate(self.from_value, self.to_value, self.elapsed / self.duration, self.easing)
    
    @property
    def is_done(self) -> bool:
        return self.elapsed >= self.duration
    
    def update(self, dt: float) -> float:
        """Update tween and return current value."""
        self.elapsed = min(self.elapsed + dt, self.duration)
        return self.value
