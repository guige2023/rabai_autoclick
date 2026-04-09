"""Animation Curve Builder Utilities.

Builds animation curves from keyframes and easing specifications.

Example:
    >>> from animation_curve_builder_utils import AnimationCurveBuilder
    >>> builder = AnimationCurveBuilder()
    >>> curve = builder.build_keyframe_curve([(0, 0), (0.5, 0.5), (1, 1)])
    >>> value = curve.get_value_at(0.75)
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, List, Tuple


@dataclass
class Keyframe:
    """An animation keyframe."""
    time: float
    value: float
    easing: str = "ease_in_out"


EasingFunc = Callable[[float], float]


class AnimationCurveBuilder:
    """Builds animation curves from keyframes."""

    EASING_FUNCTIONS: dict[str, EasingFunc] = {
        "linear": lambda t: t,
        "ease_in": lambda t: t * t,
        "ease_out": lambda t: t * (2 - t),
        "ease_in_out": lambda t: 0.5 * (1 - math.cos(math.pi * t)),
        "ease_in_cubic": lambda t: t * t * t,
        "ease_out_cubic": lambda t: (t - 1) ** 3 + 1,
        "ease_in_out_cubic": lambda t: (
            4 * t * t * t if t < 0.5 else (t - 1) * (2 * t - 2) ** 2 + 1
        ),
        "elastic": lambda t: (
            math.sin(13 * math.pi / 2 * t) * 2 ** (-10 * t) + 1 if t > 0
            else 0
        ),
        "bounce": lambda t: (
            (1 - (2.75 * (1 - t) ** 2 - (1 - t) ** 3) * (1 - t)) if t < 0.5
            else 1 - (2.75 * (t - 1) ** 2 - (t - 1) ** 3) * (1 - t)
        ),
    }

    def build_keyframe_curve(
        self, keyframes: List[Tuple[float, float]], easing: str = "ease_in_out"
    ) -> AnimationCurve:
        """Build a curve from keyframe tuples.

        Args:
            keyframes: List of (time, value) tuples.
            easing: Default easing name.

        Returns:
            AnimationCurve instance.
        """
        kfs = [Keyframe(time=k[0], value=k[1], easing=easing) for k in keyframes]
        kfs.sort(key=lambda k: k.time)
        return AnimationCurve(kfs, self.EASING_FUNCTIONS)

    def build_linear_curve(self, start: float, end: float) -> AnimationCurve:
        """Build a linear curve between two values.

        Args:
            start: Start value.
            end: End value.

        Returns:
            AnimationCurve instance.
        """
        return self.build_keyframe_curve([(0.0, start), (1.0, end)], "linear")

    def build_step_curve(self, start: float, end: float, steps: int = 1) -> AnimationCurve:
        """Build a stepped curve.

        Args:
            start: Start value.
            end: End value.
            steps: Number of steps.

        Returns:
            AnimationCurve instance.
        """
        keyframes = [(0.0, start)]
        for i in range(1, steps + 1):
            t = i / steps
            keyframes.append((t, end if i == steps else start))
        return AnimationCurve(
            [Keyframe(time=k[0], value=k[1], easing="linear") for k in keyframes],
            self.EASING_FUNCTIONS,
        )


class AnimationCurve:
    """An animation curve defined by keyframes."""

    def __init__(self, keyframes: List[Keyframe], easing_funcs: dict[str, EasingFunc]):
        """Initialize curve.

        Args:
            keyframes: Sorted list of keyframes.
            easing_funcs: Dict of easing function names to functions.
        """
        self.keyframes = keyframes
        self.easing_funcs = easing_funcs

    def get_value_at(self, time: float) -> float:
        """Get interpolated value at time.

        Args:
            time: Time value (0.0 to 1.0).

        Returns:
            Interpolated value.
        """
        if not self.keyframes:
            return 0.0
        if len(self.keyframes) == 1:
            return self.keyframes[0].value

        if time <= self.keyframes[0].time:
            return self.keyframes[0].value
        if time >= self.keyframes[-1].time:
            return self.keyframes[-1].value

        for i in range(len(self.keyframes) - 1):
            k0, k1 = self.keyframes[i], self.keyframes[i + 1]
            if k0.time <= time <= k1.time:
                t = (time - k0.time) / (k1.time - k0.time)
                easing = self.easing_funcs.get(k0.easing, self.easing_funcs["linear"])
                eased_t = easing(t)
                return k0.value + (k1.value - k0.value) * eased_t

        return self.keyframes[-1].value
