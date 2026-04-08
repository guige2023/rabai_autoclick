"""
Opacity animation utilities for fade-in/fade-out animations.

Provides opacity animation with easing functions,
supporting fade transitions for UI elements.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


@dataclass
class OpacityKeyframe:
    """A keyframe in an opacity animation."""
    time_ms: float
    opacity: float
    easing: str = "linear"


class OpacityAnimator:
    """Animates element opacity over time."""

    def __init__(self):
        self._keyframes: list[OpacityKeyframe] = []
        self._duration_ms: float = 0.0

    def fade_in(self, duration_ms: float = 300.0, easing: str = "ease_out") -> OpacityAnimator:
        """Create a fade-in animation."""
        self._keyframes = [
            OpacityKeyframe(time_ms=0.0, opacity=0.0, easing=easing),
            OpacityKeyframe(time_ms=duration_ms, opacity=1.0, easing="linear"),
        ]
        self._duration_ms = duration_ms
        return self

    def fade_out(self, duration_ms: float = 300.0, easing: str = "ease_in") -> OpacityAnimator:
        """Create a fade-out animation."""
        self._keyframes = [
            OpacityKeyframe(time_ms=0.0, opacity=1.0, easing=easing),
            OpacityKeyframe(time_ms=duration_ms, opacity=0.0, easing="linear"),
        ]
        self._duration_ms = duration_ms
        return self

    def to_opacity(
        self,
        target_opacity: float,
        duration_ms: float = 300.0,
        from_opacity: float = 1.0,
        easing: str = "ease_in_out",
    ) -> OpacityAnimator:
        """Create an animation to a specific opacity."""
        self._keyframes = [
            OpacityKeyframe(time_ms=0.0, opacity=from_opacity, easing=easing),
            OpacityKeyframe(time_ms=duration_ms, opacity=target_opacity, easing="linear"),
        ]
        self._duration_ms = duration_ms
        return self

    def pulse(
        self,
        low_opacity: float = 0.0,
        high_opacity: float = 1.0,
        duration_ms: float = 500.0,
    ) -> OpacityAnimator:
        """Create a pulse (fade out and back in)."""
        half = duration_ms / 2
        self._keyframes = [
            OpacityKeyframe(time_ms=0.0, opacity=high_opacity, easing="ease_out"),
            OpacityKeyframe(time_ms=half, opacity=low_opacity, easing="ease_in"),
            OpacityKeyframe(time_ms=duration_ms, opacity=high_opacity, easing="ease_out"),
        ]
        self._duration_ms = duration_ms
        return self

    def blink(
        self,
        on_ms: float = 100.0,
        off_ms: float = 100.0,
        repeat: int = 3,
    ) -> OpacityAnimator:
        """Create a blink animation."""
        self._keyframes = [OpacityKeyframe(time_ms=0.0, opacity=1.0)]
        t = 0.0
        for i in range(repeat):
            t += off_ms
            self._keyframes.append(OpacityKeyframe(time_ms=t, opacity=0.0))
            t += on_ms
            self._keyframes.append(OpacityKeyframe(time_ms=t, opacity=1.0))

        self._keyframes.sort(key=lambda k: k.time_ms)
        self._duration_ms = t
        return self

    def evaluate(self, time_ms: float) -> float:
        """Evaluate opacity at a given time."""
        if not self._keyframes:
            return 1.0

        if time_ms <= 0:
            return self._keyframes[0].opacity
        if time_ms >= self._duration_ms:
            return self._keyframes[-1].opacity

        # Find surrounding keyframes
        for i in range(len(self._keyframes) - 1):
            kf1 = self._keyframes[i]
            kf2 = self._keyframes[i + 1]
            if kf1.time_ms <= time_ms <= kf2.time_ms:
                t = (time_ms - kf1.time_ms) / (kf2.time_ms - kf1.time_ms)
                eased_t = self._apply_easing(t, kf1.easing)
                return kf1.opacity + (kf2.opacity - kf1.opacity) * eased_t

        return self._keyframes[-1].opacity

    def _apply_easing(self, t: float, easing: str) -> float:
        """Apply easing to normalized time."""
        if easing == "linear":
            return t
        elif easing == "ease_in":
            return t * t
        elif easing == "ease_out":
            return 1 - (1 - t) * (1 - t)
        elif easing == "ease_in_out":
            return 2 * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 2 / 2
        return t

    @property
    def duration_ms(self) -> float:
        return self._duration_ms


# Utility functions
def fade_in(duration_ms: float = 300.0, easing: str = "ease_out") -> OpacityAnimator:
    """Quick fade-in animator."""
    return OpacityAnimator().fade_in(duration_ms, easing)


def fade_out(duration_ms: float = 300.0, easing: str = "ease_in") -> OpacityAnimator:
    """Quick fade-out animator."""
    return OpacityAnimator().fade_out(duration_ms, easing)


__all__ = ["OpacityAnimator", "OpacityKeyframe", "fade_in", "fade_out"]
