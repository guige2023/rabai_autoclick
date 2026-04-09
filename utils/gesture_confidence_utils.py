"""
Gesture confidence scoring utilities.

This module provides confidence scoring for gesture recognition,
including multi-signal fusion and threshold-based decision making.
"""

from __future__ import annotations

import math
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum, auto


class GestureType(Enum):
    """Recognized gesture types."""
    UNKNOWN = auto()
    TAP = auto()
    DOUBLE_TAP = auto()
    SWIPE_LEFT = auto()
    SWIPE_RIGHT = auto()
    SWIPE_UP = auto()
    SWIPE_DOWN = auto()
    PINCH = auto()
    PINCH_OPEN = auto()
    PINCH_CLOSE = auto()
    ROTATE = auto()
    DRAG = auto()
    LONG_PRESS = auto()


@dataclass
class GestureSignal:
    """A single signal contributing to gesture confidence."""
    name: str
    confidence: float
    weight: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GestureScore:
    """Scored result of gesture classification."""
    gesture_type: GestureType
    confidence: float
    signals: List[GestureSignal] = field(default_factory=list)
    duration_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def is_confident(self, threshold: float = 0.7) -> bool:
        """Return True if confidence exceeds threshold."""
        return self.confidence >= threshold


@dataclass
class ConfidenceConfig:
    """Configuration for gesture confidence scoring."""
    default_threshold: float = 0.7
    use_signal_weighting: bool = True
    normalize_weights: bool = True


def fuse_signals(signals: List[GestureSignal], config: Optional[ConfidenceConfig] = None) -> float:
    """
    Fuse multiple gesture signals into a single confidence score.

    Args:
        signals: List of GestureSignal contributions.
        config: Scoring configuration.

    Returns:
        Fused confidence score between 0 and 1.
    """
    if config is None:
        config = ConfidenceConfig()
    if not signals:
        return 0.0

    if config.use_signal_weighting:
        total_weight = sum(s.weight for s in signals)
        if total_weight <= 0:
            return 0.0
        weighted_sum = sum(s.confidence * s.weight for s in signals)
        return min(1.0, max(0.0, weighted_sum / total_weight))
    else:
        # Simple average
        return sum(s.confidence for s in signals) / len(signals)


def score_tap_gesture(
    point_count: int,
    duration_ms: float,
    displacement_px: float,
    config: Optional[ConfidenceConfig] = None,
) -> GestureScore:
    """
    Score a gesture as a tap.

    Args:
        point_count: Number of touch points.
        duration_ms: Gesture duration.
        displacement_px: Total movement during gesture.
        config: Scoring configuration.

    Returns:
        GestureScore for tap classification.
    """
    if config is None:
        config = ConfidenceConfig()

    signals: List[GestureSignal] = []

    # Point count signal
    if point_count == 1:
        signals.append(GestureSignal(name="single_point", confidence=1.0, weight=0.5))
    elif point_count == 2:
        signals.append(GestureSignal(name="two_point", confidence=0.6, weight=0.3))
    else:
        signals.append(GestureSignal(name="multi_point", confidence=0.2, weight=0.1))

    # Duration signal
    if duration_ms < 150:
        signals.append(GestureSignal(name="short_duration", confidence=1.0, weight=0.4))
    elif duration_ms < 300:
        signals.append(GestureSignal(name="medium_duration", confidence=0.7, weight=0.3))
    else:
        signals.append(GestureSignal(name="long_duration", confidence=0.3, weight=0.2))

    # Displacement signal
    if displacement_px < 10:
        signals.append(GestureSignal(name="minimal_movement", confidence=1.0, weight=0.5))
    elif displacement_px < 25:
        signals.append(GestureSignal(name="slight_movement", confidence=0.7, weight=0.4))
    else:
        signals.append(GestureSignal(name="significant_movement", confidence=0.3, weight=0.3))

    confidence = fuse_signals(signals, config)
    return GestureScore(
        gesture_type=GestureType.TAP,
        confidence=confidence,
        signals=signals,
        duration_ms=duration_ms,
    )


def score_swipe_gesture(
    start_point: Tuple[float, float],
    end_point: Tuple[float, float],
    duration_ms: float,
    points: List[Tuple[float, float]],
    config: Optional[ConfidenceConfig] = None,
) -> GestureScore:
    """
    Score a gesture as a swipe and determine direction.

    Args:
        start_point: Starting touch coordinates.
        end_point: Ending touch coordinates.
        duration_ms: Gesture duration.
        points: Full trajectory points.
        config: Scoring configuration.

    Returns:
        GestureScore with swipe direction classified.
    """
    if config is None:
        config = ConfidenceConfig()

    dx = end_point[0] - start_point[0]
    dy = end_point[1] - start_point[1]
    distance = math.sqrt(dx * dx + dy * dy)

    # Determine direction
    if abs(dx) > abs(dy):
        direction = GestureType.SWIPE_RIGHT if dx > 0 else GestureType.SWIPE_LEFT
        direction_strength = abs(dx) / max(abs(dx) + abs(dy), 1.0)
    else:
        direction = GestureType.SWIPE_DOWN if dy > 0 else GestureType.SWIPE_UP
        direction_strength = abs(dy) / max(abs(dx) + abs(dy), 1.0)

    signals: List[GestureSignal] = []

    # Distance signal
    if distance > 80:
        signals.append(GestureSignal(name="good_distance", confidence=1.0, weight=0.6))
    elif distance > 40:
        signals.append(GestureSignal(name="medium_distance", confidence=0.7, weight=0.5))
    else:
        signals.append(GestureSignal(name="short_distance", confidence=0.4, weight=0.4))

    # Speed signal
    speed = distance / max(duration_ms / 1000.0, 0.001)
    if 200 < speed < 3000:
        signals.append(GestureSignal(name="natural_speed", confidence=1.0, weight=0.4))
    elif 100 < speed < 5000:
        signals.append(GestureSignal(name="acceptable_speed", confidence=0.7, weight=0.3))
    else:
        signals.append(GestureSignal(name="unusual_speed", confidence=0.4, weight=0.2))

    # Straightness signal
    path_length = sum(
        math.sqrt((points[i][0] - points[i - 1][0]) ** 2 + (points[i][1] - points[i - 1][1]) ** 2)
        for i in range(1, len(points))
    ) if len(points) > 1 else 0.0
    straightness = distance / max(path_length, 1.0)
    signals.append(GestureSignal(name="straightness", confidence=straightness, weight=0.5))

    # Direction signal
    signals.append(GestureSignal(name="direction", confidence=direction_strength, weight=0.3))

    confidence = fuse_signals(signals, config)
    return GestureScore(
        gesture_type=direction,
        confidence=confidence,
        signals=signals,
        duration_ms=duration_ms,
        metadata={"dx": dx, "dy": dy, "distance": distance, "speed": speed},
    )


def score_long_press_gesture(
    duration_ms: float,
    displacement_px: float,
    config: Optional[ConfidenceConfig] = None,
) -> GestureScore:
    """
    Score a gesture as a long press.

    Args:
        duration_ms: How long the touch was held.
        displacement_px: Movement during the hold.
        config: Scoring configuration.

    Returns:
        GestureScore for long press classification.
    """
    if config is None:
        config = ConfidenceConfig()

    signals: List[GestureSignal] = []

    # Duration signal (most important for long press)
    if duration_ms >= 500:
        signals.append(GestureSignal(name="sufficient_duration", confidence=1.0, weight=0.8))
    elif duration_ms >= 300:
        signals.append(GestureSignal(name="borderline_duration", confidence=0.6, weight=0.6))
    else:
        signals.append(GestureSignal(name="insufficient_duration", confidence=0.2, weight=0.5))

    # Stationary signal
    if displacement_px < 5:
        signals.append(GestureSignal(name="stationary", confidence=1.0, weight=0.6))
    elif displacement_px < 15:
        signals.append(GestureSignal(name="mostly_stationary", confidence=0.6, weight=0.4))
    else:
        signals.append(GestureSignal(name="moved_during_press", confidence=0.1, weight=0.3))

    confidence = fuse_signals(signals, config)
    return GestureScore(
        gesture_type=GestureType.LONG_PRESS,
        confidence=confidence,
        signals=signals,
        duration_ms=duration_ms,
    )


def select_best_gesture(scores: List[GestureScore]) -> GestureScore:
    """
    Select the highest-confidence gesture from a list of candidates.

    Args:
        scores: List of scored gesture candidates.

    Returns:
        GestureScore with highest confidence.
    """
    if not scores:
        return GestureScore(gesture_type=GestureType.UNKNOWN, confidence=0.0)
    return max(scores, key=lambda s: s.confidence)
