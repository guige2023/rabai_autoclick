"""Animation easing action for UI automation.

Provides easing functions for smooth animations:
- Linear, ease-in, ease-out, ease-in-out
- Cubic, quadratic, quartic, quintic
- Elastic and bounce effects
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from enum import Enum, auto
from typing import Callable, Protocol


class EasingType(Enum):
    """Built-in easing types."""
    LINEAR = auto()
    EASE_IN = auto()
    EASE_OUT = auto()
    EASE_IN_OUT = auto()
    EASE_IN_CUBIC = auto()
    EASE_OUT_CUBIC = auto()
    EASE_IN_OUT_CUBIC = auto()
    EASE_IN_QUAD = auto()
    EASE_OUT_QUAD = auto()
    EASE_IN_OUT_QUAD = auto()
    EASE_IN_QUART = auto()
    EASE_OUT_QUART = auto()
    EASE_IN_OUT_QUART = auto()
    EASE_IN_QUINT = auto()
    EASE_OUT_QUINT = auto()
    EASE_IN_OUT_QUINT = auto()
    EASE_IN_ELASTIC = auto()
    EASE_OUT_ELASTIC = auto()
    EASE_IN_OUT_ELASTIC = auto()
    EASE_IN_BOUNCE = auto()
    EASE_OUT_BOUNCE = auto()
    EASE_IN_OUT_BOUNCE = auto()
    EASE_IN_BACK = auto()
    EASE_OUT_BACK = auto()
    EASE_IN_OUT_BACK = auto()
    EASE_IN_CIRCULAR = auto()
    EASE_OUT_CIRCULAR = auto()
    EASE_IN_OUT_CIRCULAR = auto()


# Type alias for easing functions
EasingFunction = Callable[[float], float]


def linear(t: float) -> float:
    """Linear easing (no acceleration)."""
    return t


def ease_in(t: float) -> float:
    """Ease-in (accelerating from zero velocity)."""
    return t * t


def ease_out(t: float) -> float:
    """Ease-out (decelerating to zero velocity)."""
    return t * (2 - t)


def ease_in_out(t: float) -> float:
    """Ease-in-out (accelerate + decelerate)."""
    return 3 * t * t - 2 * t * t * t


def ease_in_cubic(t: float) -> float:
    """Cubic ease-in."""
    return t * t * t


def ease_out_cubic(t: float) -> float:
    """Cubic ease-out."""
    return (t - 1) ** 3 + 1


def ease_in_out_cubic(t: float) -> float:
    """Cubic ease-in-out."""
    return 4 * t * t * t if t < 0.5 else (t - 1) ** 3 * 4 + 1


def ease_in_quad(t: float) -> float:
    """Quadratic ease-in."""
    return t * t


def ease_out_quad(t: float) -> float:
    """Quadratic ease-out."""
    return -t * (t - 2)


def ease_in_out_quad(t: float) -> float:
    """Quadratic ease-in-out."""
    return 2 * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 2 / 2


def ease_in_quart(t: float) -> float:
    """Quartic ease-in."""
    return t * t * t * t


def ease_out_quart(t: float) -> float:
    """Quartic ease-out."""
    return 1 - (t - 1) ** 4


def ease_in_out_quart(t: float) -> float:
    """Quartic ease-in-out."""
    return 8 * t * t * t * t if t < 0.5 else 1 - (-2 * t + 2) ** 4 / 2


def ease_in_quint(t: float) -> float:
    """Quintic ease-in."""
    return t * t * t * t * t


def ease_out_quint(t: float) -> float:
    """Quintic ease-out."""
    return (t - 1) ** 5 + 1


def ease_in_out_quint(t: float) -> float:
    """Quintic ease-in-out."""
    return 16 * t * t * t * t * t if t < 0.5 else 1 + (2 * t - 2) ** 5 / 2


def ease_in_elastic(t: float) -> float:
    """Elastic ease-in (exponential decay)."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    return -2 ** (10 * t - 10) * math.sin((t * 10 - 10.75) * (2 * math.pi) / 3)


def ease_out_elastic(t: float) -> float:
    """Elastic ease-out (exponential decay with bounce)."""
    if t == 0:
        return 0
    if t == 1:
        return 1
    return 2 ** (-10 * t) * math.sin((t * 10 - 0.75) * (2 * math.pi) / 3) + 1


def ease_in_out_elastic(t: float) -> float:
    """Elastic ease-in-out."""
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
    return (1 - ease_out_bounce(1 - 2 * t)) / 2 if t < 0.5 else (1 + ease_out_bounce(2 * t - 1)) / 2


def ease_in_back(t: float) -> float:
    """Back ease-in (overshoots target)."""
    c1 = 1.70158
    c3 = c1 + 1
    return c3 * t * t * t - c1 * t * t


def ease_out_back(t: float) -> float:
    """Back ease-out (overshoots then returns)."""
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


def ease_in_circular(t: float) -> float:
    """Circular ease-in."""
    return 1 - math.sqrt(1 - t * t)


def ease_out_circular(t: float) -> float:
    """Circular ease-out."""
    return math.sqrt(1 - (t - 1) ** 2)


def ease_in_out_circular(t: float) -> float:
    """Circular ease-in-out."""
    return (1 - math.sqrt(1 - (2 * t) ** 2)) / 2 if t < 0.5 else (math.sqrt(1 - (-2 * t + 2) ** 2) + 1) / 2


# Mapping from EasingType to function
_EASING_MAP: dict[EasingType, EasingFunction] = {
    EasingType.LINEAR: linear,
    EasingType.EASE_IN: ease_in,
    EasingType.EASE_OUT: ease_out,
    EasingType.EASE_IN_OUT: ease_in_out,
    EasingType.EASE_IN_CUBIC: ease_in_cubic,
    EasingType.EASE_OUT_CUBIC: ease_out_cubic,
    EasingType.EASE_IN_OUT_CUBIC: ease_in_out_cubic,
    EasingType.EASE_IN_QUAD: ease_in_quad,
    EasingType.EASE_OUT_QUAD: ease_out_quad,
    EasingType.EASE_IN_OUT_QUAD: ease_in_out_quad,
    EasingType.EASE_IN_QUART: ease_in_quart,
    EasingType.EASE_OUT_QUART: ease_out_quart,
    EasingType.EASE_IN_OUT_QUART: ease_in_out_quart,
    EasingType.EASE_IN_QUINT: ease_in_quint,
    EasingType.EASE_OUT_QUINT: ease_out_quint,
    EasingType.EASE_IN_OUT_QUINT: ease_in_out_quint,
    EasingType.EASE_IN_ELASTIC: ease_in_elastic,
    EasingType.EASE_OUT_ELASTIC: ease_out_elastic,
    EasingType.EASE_IN_OUT_ELASTIC: ease_in_out_elastic,
    EasingType.EASE_IN_BOUNCE: ease_in_bounce,
    EasingType.EASE_OUT_BOUNCE: ease_out_bounce,
    EasingType.EASE_IN_OUT_BOUNCE: ease_in_out_bounce,
    EasingType.EASE_IN_BACK: ease_in_back,
    EasingType.EASE_OUT_BACK: ease_out_back,
    EasingType.EASE_IN_OUT_BACK: ease_in_out_back,
    EasingType.EASE_IN_CIRCULAR: ease_in_circular,
    EasingType.EASE_OUT_CIRCULAR: ease_out_circular,
    EasingType.EASE_IN_OUT_CIRCULAR: ease_in_out_circular,
}


@dataclass
class Keyframe:
    """Animation keyframe."""
    time: float  # 0.0 to 1.0
    value: float
    easing: EasingType = EasingType.LINEAR

    def __lt__(self, other: Keyframe) -> bool:
        return self.time < other.time


@dataclass
class AnimationState:
    """Current animation state."""
    progress: float = 0.0  # 0.0 to 1.0
    value: float = 0.0
    running: bool = False
    completed: bool = False


class EasingController:
    """Controller for animation easing.

    Provides:
    - Easing function application
    - Keyframe interpolation
    - Animation state tracking
    - Custom easing composition
    """

    def __init__(self):
        self._custom_easings: dict[str, EasingFunction] = {}

    def get_easing(self, easing_type: EasingType) -> EasingFunction:
        """Get easing function by type."""
        return _EASING_MAP.get(easing_type, linear)

    def register_easing(self, name: str, func: EasingFunction) -> None:
        """Register custom easing function.

        Args:
            name: Unique name for the easing
            func: Easing function (input: 0-1, output: 0-1)
        """
        if not callable(func):
            raise EasingError("Easing must be a callable function")
        if not (0.0 <= func(0.0) <= 1.0 and 0.0 <= func(1.0) <= 1.0):
            raise EasingError("Easing function must map [0,1] to [0,1]")
        self._custom_easings[name] = func

    def apply(self, t: float, easing: EasingType | str | EasingFunction) -> float:
        """Apply easing to normalized time.

        Args:
            t: Normalized time (0.0 to 1.0)
            easing: Easing type, name, or function

        Returns:
            Eased value (0.0 to 1.0)
        """
        t = max(0.0, min(1.0, t))

        if isinstance(easing, EasingType):
            return self.get_easing(easing)(t)
        elif isinstance(easing, str):
            if easing in _EASING_MAP:
                return _EASING_MAP[EasingType(easing)](t)
            if easing in self._custom_easings:
                return self._custom_easings[easing](t)
            raise EasingError(f"Unknown easing: {easing}")
        elif callable(easing):
            return easing(t)
        else:
            raise EasingError(f"Invalid easing type: {type(easing)}")

    def interpolate_keyframes(
        self,
        keyframes: list[Keyframe],
        t: float,
    ) -> float:
        """Interpolate value from keyframes.

        Args:
            keyframes: List of keyframes (must be sorted by time)
            t: Normalized time (0.0 to 1.0)

        Returns:
            Interpolated value
        """
        if not keyframes:
            return 0.0
        if len(keyframes) == 1:
            return keyframes[0].value

        keyframes = sorted(keyframes)
        kf0 = keyframes[0]
        kf1 = keyframes[-1]

        # Before first keyframe
        if t <= kf0.time:
            return kf0.value

        # After last keyframe
        if t >= kf1.time:
            return kf1.value

        # Find surrounding keyframes
        for i in range(len(keyframes) - 1):
            if keyframes[i].time <= t <= keyframes[i + 1].time:
                kf0 = keyframes[i]
                kf1 = keyframes[i + 1]
                break

        # Normalize time between keyframes
        span = kf1.time - kf0.time
        if span == 0:
            return kf0.value

        local_t = (t - kf0.time) / span
        eased_t = self.apply(local_t, kf1.easing)

        # Interpolate value
        return kf0.value + (kf1.value - kf0.value) * eased_t

    def compose(
        self,
        easings: list[EasingType | str | EasingFunction],
    ) -> EasingFunction:
        """Compose multiple easings into one.

        Each easing is applied to a portion of the timeline.
        Useful for sequencing easings.

        Args:
            easings: List of easings to compose

        Returns:
            Composed easing function
        """
        def composed(t: float) -> float:
            if not easings:
                return t
            n = len(easings)
            segment = int(t * n)
            if segment >= n:
                segment = n - 1
            local_t = (t * n) - segment
            return self.apply(local_t, easings[segment])
        return composed

    def reverse(self, easing: EasingType | str | EasingFunction) -> EasingFunction:
        """Reverse an easing function.

        Args:
            easing: Easing to reverse

        Returns:
            Reversed easing function
        """
        def reversed_easing(t: float) -> float:
            return 1.0 - self.apply(1.0 - t, easing)
        return reversed_easing

    def mirror(self, easing: EasingType | str | EasingFunction) -> EasingFunction:
        """Mirror easing (forward then reverse).

        Creates an easing that goes 0->1 then 1->0.
        Useful for pulsing animations.

        Args:
            easing: Base easing to mirror

        Returns:
            Mirrored easing function
        """
        def mirrored(t: float) -> float:
            if t <= 0.5:
                return self.apply(t * 2, easing) / 2
            else:
                return (1.0 + self.apply(2 - t * 2, easing)) / 2
        return mirrored


class EasingError(Exception):
    """Easing operation error."""
    pass


def create_easing_controller() -> EasingController:
    """Create easing controller."""
    return EasingController()


def get_all_easing_types() -> list[EasingType]:
    """Get all available easing types."""
    return list(EasingType)
