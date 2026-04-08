"""
Animation frame utilities for UI animation timing and interpolation.

Provides frame-based animation helpers including easing functions,
frame timing, and animation curve evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class AnimationFrame:
    """Single frame of an animation."""
    timestamp_ms: float
    value: float
    easing: str = "linear"


@dataclass
class AnimationCurve:
    """Animation curve with keyframes."""
    keyframes: list[AnimationFrame]
    duration_ms: float

    def evaluate(self, timestamp_ms: float) -> float:
        """Evaluate the curve at a given timestamp."""
        if not self.keyframes:
            return 0.0
        if timestamp_ms <= self.keyframes[0].timestamp_ms:
            return self.keyframes[0].value
        if timestamp_ms >= self.keyframes[-1].timestamp_ms:
            return self.keyframes[-1].value

        # Find surrounding keyframes
        for i in range(len(self.keyframes) - 1):
            kf1 = self.keyframes[i]
            kf2 = self.keyframes[i + 1]
            if kf1.timestamp_ms <= timestamp_ms <= kf2.timestamp_ms:
                t = (timestamp_ms - kf1.timestamp_ms) / (kf2.timestamp_ms - kf1.timestamp_ms)
                eased_t = self._apply_easing(t, kf2.easing)
                return kf1.value + (kf2.value - kf1.value) * eased_t

        return self.keyframes[-1].value

    def _apply_easing(self, t: float, easing: str) -> float:
        """Apply easing function to normalized time [0, 1]."""
        if easing == "linear":
            return t
        elif easing == "ease_in":
            return t * t
        elif easing == "ease_out":
            return 1 - (1 - t) * (1 - t)
        elif easing == "ease_in_out":
            return 2 * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 2 / 2
        elif easing == "ease_in_cubic":
            return t * t * t
        elif easing == "ease_out_cubic":
            return 1 - (1 - t) ** 3
        elif easing == "ease_in_out_cubic":
            return 4 * t * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 3 / 2
        elif easing == "ease_in_elastic":
            return self._elastic_ease_in(t)
        elif easing == "ease_out_elastic":
            return self._elastic_ease_out(t)
        elif easing == "ease_in_bounce":
            return 1 - self._bounce_out(1 - t)
        elif easing == "ease_out_bounce":
            return self._bounce_out(t)
        elif easing == "spring":
            import math
            return 1 - math.cos(t * math.pi * 4) * math.exp(-t * 6)
        return t

    def _elastic_ease_in(self, t: float) -> float:
        import math
        return 2 ** (10 * (t - 1)) * math.sin((t * 10 - 0.75) * (2 * math.pi) / 3)

    def _elastic_ease_out(self, t: float) -> float:
        import math
        return 2 ** (-10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / 3) + 1

    def _bounce_out(self, t: float) -> float:
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


class AnimationTimeline:
    """Timeline for managing multiple animation curves."""

    def __init__(self):
        self.curves: list[AnimationCurve] = []
        self._start_time_ms: float = 0.0
        self._is_running: bool = False

    def add_curve(self, curve: AnimationCurve) -> None:
        self.curves.append(curve)

    def create_linear(
        self,
        start_value: float,
        end_value: float,
        duration_ms: float,
        easing: str = "ease_in_out",
    ) -> AnimationCurve:
        """Create a simple two-keyframe linear animation."""
        curve = AnimationCurve(
            keyframes=[
                AnimationFrame(0.0, start_value, easing),
                AnimationFrame(duration_ms, end_value, easing),
            ],
            duration_ms=duration_ms,
        )
        self.add_curve(curve)
        return curve

    def create_sequence(self, animations: list[tuple[float, float, float]]) -> AnimationCurve:
        """Create a sequence of animations.

        Args:
            animations: List of (start_value, end_value, duration_ms)
        """
        keyframes = []
        t = 0.0
        for i, (start_v, end_v, dur_ms) in enumerate(animations):
            easing = "ease_in_out" if i < len(animations) - 1 else "linear"
            keyframes.append(AnimationFrame(t, start_v, easing))
            t += dur_ms
            keyframes.append(AnimationFrame(t, end_v, easing))

        total_duration = keyframes[-1].timestamp_ms if keyframes else 0
        curve = AnimationCurve(keyframes=keyframes, duration_ms=total_duration)
        self.add_curve(curve)
        return curve

    def evaluate_all(self, timestamp_ms: float) -> list[tuple[float, float]]:
        """Evaluate all curves at a timestamp. Returns (curve_idx, value) pairs."""
        return [(i, curve.evaluate(timestamp_ms)) for i, curve in enumerate(self.curves)]

    def total_duration_ms(self) -> float:
        """Get the total duration of the longest curve."""
        if not self.curves:
            return 0.0
        return max(c.duration_ms for c in self.curves)


# Preset easing functions
def ease_in(t: float) -> float:
    return t * t

def ease_out(t: float) -> float:
    return 1 - (1 - t) * (1 - t)

def ease_in_out(t: float) -> float:
    return 2 * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 2 / 2


__all__ = [
    "AnimationFrame", "AnimationCurve", "AnimationTimeline",
    "ease_in", "ease_out", "ease_in_out",
]
