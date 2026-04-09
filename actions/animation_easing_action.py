"""
Animation Easing Action Module

Provides easing functions and animation curves
for smooth UI transitions and effects.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class EasingType(Enum):
    """Easing function types."""

    LINEAR = "linear"
    QUAD_IN = "quad_in"
    QUAD_OUT = "quad_out"
    QUAD_IN_OUT = "quad_in_out"
    CUBIC_IN = "cubic_in"
    CUBIC_OUT = "cubic_out"
    CUBIC_IN_OUT = "cubic_in_out"
    BOUNCE_OUT = "bounce_out"
    ELASTIC_OUT = "elastic_out"
    SINE_IN_OUT = "sine_in_out"
    EXPO_OUT = "expo_out"


@dataclass
class EasingFunction:
    """Easing function with configurable parameters."""

    type: EasingType
    duration: float = 1.0
    amplitude: float = 1.0
    period: float = 0.3


class EasingCalculator:
    """
    Calculates easing values for animations.

    Provides standard easing functions and supports
    custom easing curves.
    """

    @staticmethod
    def calculate(t: float, easing_type: EasingType) -> float:
        """
        Calculate eased value.

        Args:
            t: Normalized time (0.0 to 1.0)
            easing_type: Type of easing

        Returns:
            Eased value (0.0 to 1.0)
        """
        t = max(0.0, min(1.0, t))

        if easing_type == EasingType.LINEAR:
            return t

        elif easing_type == EasingType.QUAD_IN:
            return t * t

        elif easing_type == EasingType.QUAD_OUT:
            return 1 - (1 - t) * (1 - t)

        elif easing_type == EasingType.QUAD_IN_OUT:
            if t < 0.5:
                return 2 * t * t
            return 1 - ((-2 * t + 2) ** 2) / 2

        elif easing_type == EasingType.CUBIC_IN:
            return t * t * t

        elif easing_type == EasingType.CUBIC_OUT:
            return 1 - (1 - t) ** 3

        elif easing_type == EasingType.CUBIC_IN_OUT:
            if t < 0.5:
                return 4 * t * t * t
            return 1 - ((-2 * t + 2) ** 3) / 2

        elif easing_type == EasingType.BOUNCE_OUT:
            n1, p = 7.5625, 0.75
            if t < p:
                return n1 * t * t
            elif t < 0.875:
                t -= 0.875 / p
                return n1 * t * t + 0.984375
            else:
                t -= 0.9375 / p
                return n1 * t * t + 0.984375

        elif easing_type == EasingType.ELASTIC_OUT:
            if t == 0 or t == 1:
                return t
            return (2 ** (-10 * t)) * math.sin((t - 0.075) * (2 * math.pi) / 0.3) + 1

        elif easing_type == EasingType.SINE_IN_OUT:
            return -(math.cos(math.pi * t) - 1) / 2

        elif easing_type == EasingType.EXPO_OUT:
            return 1 if t >= 1 else 1 - (2 ** (-10 * t))

        return t

    @staticmethod
    def interpolate(
        start: float,
        end: float,
        t: float,
        easing_type: EasingType = EasingType.QUAD_OUT,
    ) -> float:
        """
        Interpolate between two values with easing.

        Args:
            start: Start value
            end: End value
            t: Normalized time
            easing_type: Easing type

        Returns:
            Interpolated value
        """
        eased_t = EasingCalculator.calculate(t, easing_type)
        return start + (end - start) * eased_t

    @staticmethod
    def generate_curve(
        easing_type: EasingType,
        steps: int = 20,
    ) -> List[float]:
        """
        Generate easing curve points.

        Args:
            easing_type: Type of easing
            steps: Number of points

        Returns:
            List of eased values
        """
        return [
            EasingCalculator.calculate(i / (steps - 1), easing_type)
            for i in range(steps)
        ]


def create_easing_calculator() -> EasingCalculator:
    """Factory function."""
    return EasingCalculator()
