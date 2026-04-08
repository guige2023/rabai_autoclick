"""
Animation Timing Utilities.

Utilities for timing and easing functions used in UI animations,
including curves, durations, and interpolation helpers.

Usage:
    from utils.animation_utils import EasingFunctions, Animator

    ease = EasingFunctions.ease_in_out
    value = ease(0.5)  # Get eased value at t=0.5
"""

from __future__ import annotations

from typing import Callable, Optional, Dict, Any, TYPE_CHECKING
from dataclasses import dataclass
import math

if TYPE_CHECKING:
    pass


class EasingFunctions:
    """
    Collection of standard easing functions.

    All functions take a single parameter t (0.0 to 1.0) and
    return the eased value (also typically 0.0 to 1.0).

    Example:
        eased_t = EasingFunctions.ease_in_out(0.5)
    """

    @staticmethod
    def linear(t: float) -> float:
        """Linear easing (no easing)."""
        return t

    @staticmethod
    def ease_in(t: float) -> float:
        """Ease in (starts slow, ends fast)."""
        return t * t

    @staticmethod
    def ease_out(t: float) -> float:
        """Ease out (starts fast, ends slow)."""
        return t * (2 - t)

    @staticmethod
    def ease_in_out(t: float) -> float:
        """Ease in-out (slow start and end)."""
        if t < 0.5:
            return 2 * t * t
        return -1 + (4 - 2 * t) * t

    @staticmethod
    def ease_in_cubic(t: float) -> float:
        """Cubic ease in."""
        return t * t * t

    @staticmethod
    def ease_out_cubic(t: float) -> float:
        """Cubic ease out."""
        return (t - 1) ** 3 + 1

    @staticmethod
    def ease_in_out_cubic(t: float) -> float:
        """Cubic ease in-out."""
        if t < 0.5:
            return 4 * t * t * t
        return (t - 1) * (2 * t - 2) ** 2 + 1

    @staticmethod
    def ease_in_elastic(t: float) -> float:
        """Elastic ease in (with overshoot)."""
        if t == 0 or t == 1:
            return t
        p = 0.3
        s = p / 4
        return -(2 ** (10 * (t - 1))) * math.sin((t - 1 - s) * (2 * math.pi) / p)

    @staticmethod
    def ease_out_elastic(t: float) -> float:
        """Elastic ease out (with overshoot)."""
        if t == 0 or t == 1:
            return t
        p = 0.3
        s = p / 4
        return (2 ** (-10 * t)) * math.sin((t - s) * (2 * math.pi) / p) + 1

    @staticmethod
    def ease_in_back(t: float) -> float:
        """Ease in with backtracking."""
        c1 = 1.70158
        c3 = c1 + 1
        return c3 * t * t * t - c1 * t * t

    @staticmethod
    def ease_out_back(t: float) -> float:
        """Ease out with backtracking."""
        c1 = 1.70158
        c3 = c1 + 1
        return 1 + c3 * ((t - 1) ** 3) + c1 * ((t - 1) ** 2)

    @staticmethod
    def ease_in_out_back(t: float) -> float:
        """Ease in-out with backtracking."""
        c1 = 1.70158
        c2 = c1 * 1.525
        if t < 0.5:
            return ((2 * t) ** 2 * ((c2 + 1) * 2 * t - c2)) / 2
        return ((2 * t - 2) ** 2 * ((c2 + 1) * (t * 2 - 2) + c2) + 2) / 2

    @staticmethod
    def bounce_out(t: float) -> float:
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


@dataclass
class AnimationFrame:
    """A single frame in an animation."""
    time: float
    value: float


class Animator:
    """
    Animation controller for interpolating values over time.

    Example:
        animator = Animator(duration=1.0, easing=EasingFunctions.ease_in_out)
        animator.add_keyframe(0.0, 0.0)
        animator.add_keyframe(1.0, 100.0)

        for frame in animator:
            obj.x = frame.value
    """

    def __init__(
        self,
        duration: float = 1.0,
        easing: Callable[[float], float] = None,
    ) -> None:
        """
        Initialize the animator.

        Args:
            duration: Total animation duration in seconds.
            easing: Easing function to use.
        """
        self._duration = duration
        self._easing = easing or EasingFunctions.linear
        self._keyframes: Dict[float, float] = {}

    def add_keyframe(
        self,
        time: float,
        value: float,
    ) -> "Animator":
        """
        Add a keyframe.

        Args:
            time: Time (0.0 to 1.0).
            value: Value at this time.

        Returns:
            Self for chaining.
        """
        self._keyframes[time] = value
        return self

    def get_value(
        self,
        time: float,
    ) -> float:
        """
        Get the interpolated value at a given time.

        Args:
            time: Time (0.0 to 1.0).

        Returns:
            Interpolated value.
        """
        if not self._keyframes:
            return 0.0

        if time <= 0.0:
            return list(self._keyframes.values())[0]

        if time >= 1.0:
            return list(self._keyframes.values())[-1]

        sorted_times = sorted(self._keyframes.keys())

        before_time = 0.0
        after_time = 1.0
        for t in sorted_times:
            if t <= time:
                before_time = t
            if t >= time and after_time > t:
                after_time = t

        before_val = self._keyframes[before_time]
        after_val = self._keyframes[after_time]

        if before_time == after_time:
            return before_val

        t = (time - before_time) / (after_time - before_time)
        t = max(0.0, min(1.0, t))
        eased_t = self._easing(t)

        return before_val + (after_val - before_val) * eased_t

    def __iter__(self) -> "Animator":
        """Iterate over animation frames."""
        return self

    def __next__(
        self,
        fps: int = 60,
    ) -> AnimationFrame:
        """Return next frame in animation."""
        pass


def interpolate(
    start: float,
    end: float,
    t: float,
    easing: Optional[Callable[[float], float]] = None,
) -> float:
    """
    Interpolate between two values with optional easing.

    Args:
        start: Start value.
        end: End value.
        t: Time parameter (0.0 to 1.0).
        easing: Optional easing function.

    Returns:
        Interpolated value.
    """
    t = max(0.0, min(1.0, t))
    if easing:
        t = easing(t)
    return start + (end - start) * t
