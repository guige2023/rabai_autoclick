"""Animation timeline utilities for UI automation.

Provides timeline-based animation control for smooth UI transitions
in automation workflows.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class EasingType(Enum):
    """Easing function types."""
    LINEAR = auto()
    EASE_IN = auto()
    EASE_OUT = auto()
    EASE_IN_OUT = auto()
    SPRING = auto()
    BOUNCE = auto()


class KeyframeValue:
    """A value at a keyframe position (0.0-1.0)."""
    def __init__(self, time: float, value: float) -> None:
        self.time = time
        self.value = value


@dataclass
class Keyframe:
    """A keyframe in an animation.

    Attributes:
        time: Position in the timeline (0.0-1.0).
        value: The value at this keyframe.
        easing: Easing type to use from previous keyframe.
    """
    time: float
    value: float
    easing: EasingType = EasingType.EASE_OUT


@dataclass
class Animation:
    """An animation definition.

    Attributes:
        name: Animation name.
        duration: Total duration in seconds.
        keyframes: List of keyframes.
        repeat: Number of repetitions (0 = infinite).
        yoyo: Whether to reverse on repeat.
    """
    name: str
    duration: float
    keyframes: list[Keyframe] = field(default_factory=list)
    repeat: int = 0
    yoyo: bool = False
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def add_keyframe(
        self,
        time: float,
        value: float,
        easing: EasingType = EasingType.EASE_OUT,
    ) -> None:
        """Add a keyframe to this animation."""
        self.keyframes.append(Keyframe(time=time, value=value, easing=easing))
        self.keyframes.sort(key=lambda k: k.time)

    def get_value(self, progress: float) -> float:
        """Get interpolated value at progress (0.0-1.0)."""
        if not self.keyframes:
            return 0.0
        if progress <= self.keyframes[0].time:
            return self.keyframes[0].value
        if progress >= self.keyframes[-1].time:
            return self.keyframes[-1].value

        for i in range(len(self.keyframes) - 1):
            k1 = self.keyframes[i]
            k2 = self.keyframes[i + 1]
            if k1.time <= progress <= k2.time:
                t = (progress - k1.time) / (k2.time - k1.time)
                t = self._apply_easing(t, k2.easing)
                return k1.value + (k2.value - k1.value) * t
        return 0.0

    def _apply_easing(self, t: float, easing: EasingType) -> float:
        """Apply easing function to normalized time."""
        if easing == EasingType.LINEAR:
            return t
        if easing == EasingType.EASE_IN:
            return t * t
        if easing == EasingType.EASE_OUT:
            return 1 - (1 - t) * (1 - t)
        if easing == EasingType.EASE_IN_OUT:
            return 2 * t * t if t < 0.5 else 1 - ((-2 * t + 2) ** 2) / 2
        return t


class AnimationTimeline:
    """Timeline for controlling animations."""

    def __init__(self) -> None:
        """Initialize empty timeline."""
        self._animations: dict[str, Animation] = {}
        self._active: list[tuple[str, float]] = []

    def add(self, animation: Animation) -> str:
        """Add an animation to the timeline."""
        self._animations[animation.id] = animation
        return animation.id

    def play(self, animation_id: str) -> None:
        """Start playing an animation."""
        if animation_id not in self._active:
            self._active.append((animation_id, time.time()))

    def stop(self, animation_id: str) -> bool:
        """Stop an animation. Returns True if was playing."""
        for i, (aid, _) in enumerate(self._active):
            if aid == animation_id:
                self._active.pop(i)
                return True
        return False

    def get_current_value(self, animation_id: str) -> Optional[float]:
        """Get current interpolated value for an animation."""
        for aid, start_time in self._active:
            if aid == animation_id:
                anim = self._animations.get(aid)
                if anim:
                    elapsed = time.time() - start_time
                    progress = min(elapsed / anim.duration, 1.0)
                    return anim.get_value(progress)
        return None

    @property
    def active_count(self) -> int:
        """Return number of active animations."""
        return len(self._active)


def create_scale_animation(
    name: str,
    from_scale: float,
    to_scale: float,
    duration: float = 0.3,
    easing: EasingType = EasingType.EASE_OUT,
) -> Animation:
    """Create a scale animation."""
    anim = Animation(name=name, duration=duration)
    anim.add_keyframe(0.0, from_scale, easing)
    anim.add_keyframe(1.0, to_scale, easing)
    return anim


def create_fade_animation(
    name: str,
    from_alpha: float,
    to_alpha: float,
    duration: float = 0.3,
) -> Animation:
    """Create a fade animation."""
    return create_scale_animation(name, from_alpha, to_alpha, duration)
