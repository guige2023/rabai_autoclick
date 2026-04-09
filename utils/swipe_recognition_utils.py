"""
Swipe recognition utilities.

This module provides utilities for recognizing and classifying swipe gestures
including direction, speed, and geometric characteristics.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Dict, Any
from dataclasses import dataclass, field
from enum import Enum, auto


class SwipeDirection(Enum):
    """Swipe direction classification."""
    LEFT = auto()
    RIGHT = auto()
    UP = auto()
    DOWN = auto()
    DIAGONAL = auto()
    UNKNOWN = auto()


@dataclass
class SwipeCharacteristics:
    """Characteristics of a recognized swipe."""
    direction: SwipeDirection
    angle_degrees: float
    distance_px: float
    duration_ms: float
    avg_speed_pxs: float
    peak_speed_pxs: float
    confidence: float
    is_straight: bool
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SwipeRecognitionConfig:
    """Configuration for swipe recognition."""
    min_distance_px: float = 30.0
    max_duration_ms: float = 1000.0
    min_speed_pxs: float = 50.0
    max_deviation_degrees: float = 30.0
    straightness_threshold: float = 0.85


def compute_swipe_angle(start: Tuple[float, float], end: Tuple[float, float]) -> float:
    """
    Compute swipe angle in degrees from start to end.

    Args:
        start: Starting coordinates (x, y).
        end: Ending coordinates (x, y).

    Returns:
        Angle in degrees (0 = right, 90 = up, -90 = down, 180/-180 = left).
    """
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    return math.degrees(math.atan2(-dy, dx))  # Negative dy because screen y is inverted


def compute_swipe_distance(start: Tuple[float, float], end: Tuple[float, float]) -> float:
    """Compute Euclidean distance between start and end."""
    dx = end[0] - start[0]
    dy = end[1] - start[1]
    return math.sqrt(dx * dx + dy * dy)


def classify_direction(angle_degrees: float) -> SwipeDirection:
    """
    Classify swipe direction from angle.

    Args:
        angle_degrees: Swipe angle in degrees.

    Returns:
        SwipeDirection enum value.
    """
    # Normalize to 0-360
    angle = angle_degrees % 360
    if angle > 337.5 or angle <= 22.5:
        return SwipeDirection.RIGHT
    elif 22.5 < angle <= 67.5:
        return SwipeDirection.DOWN  # Screen coords flip y
    elif 67.5 < angle <= 112.5:
        return SwipeDirection.DOWN
    elif 112.5 < angle <= 157.5:
        return SwipeDirection.DOWN
    elif 157.5 < angle <= 202.5:
        return SwipeDirection.LEFT
    elif 202.5 < angle <= 247.5:
        return SwipeDirection.UP
    elif 247.5 < angle <= 292.5:
        return SwipeDirection.UP
    elif 292.5 < angle <= 337.5:
        return SwipeDirection.UP
    return SwipeDirection.UNKNOWN


def compute_path_length(points: List[Tuple[float, float]]) -> float:
    """Compute total path length of a trajectory."""
    if len(points) < 2:
        return 0.0
    return sum(
        math.sqrt((points[i][0] - points[i - 1][0]) ** 2 + (points[i][1] - points[i - 1][1]) ** 2)
        for i in range(1, len(points))
    )


def compute_straightness(start: Tuple[float, float], end: Tuple[float, float], path_length: float) -> float:
    """
    Compute straightness ratio (1 = perfectly straight).

    Args:
        start: Start point.
        end: End point.
        path_length: Total path length.

    Returns:
        Straightness ratio between 0 and 1.
    """
    displacement = compute_swipe_distance(start, end)
    if path_length < 1e-10:
        return 0.0
    return displacement / path_length


def recognize_swipe(
    points: List[Tuple[float, float]],
    timestamps_ms: List[float],
    config: Optional[SwipeRecognitionConfig] = None,
) -> Optional[SwipeCharacteristics]:
    """
    Recognize a swipe gesture from trajectory points.

    Args:
        points: Trajectory points (x, y).
        timestamps_ms: Timestamp for each point in milliseconds.
        config: Recognition configuration.

    Returns:
        SwipeCharacteristics if recognized, None otherwise.
    """
    if config is None:
        config = SwipeRecognitionConfig()

    if len(points) < 2 or len(points) != len(timestamps_ms):
        return None

    start = points[0]
    end = points[-1]
    duration_ms = timestamps_ms[-1] - timestamps_ms[0]

    distance = compute_swipe_distance(start, end)
    angle = compute_swipe_angle(start, end)
    path_length = compute_path_length(points)
    straightness = compute_straightness(start, end, path_length)

    # Speed calculations
    dt_s = duration_ms / 1000.0
    avg_speed = distance / dt_s if dt_s > 0 else 0.0

    # Peak speed
    speeds: List[float] = []
    for i in range(1, len(points)):
        d = math.sqrt((points[i][0] - points[i - 1][0]) ** 2 + (points[i][1] - points[i - 1][1]) ** 2)
        dt = (timestamps_ms[i] - timestamps_ms[i - 1]) / 1000.0
        if dt > 0:
            speeds.append(d / dt)
    peak_speed = max(speeds) if speeds else 0.0

    # Check if it's a valid swipe
    if distance < config.min_distance_px:
        return None
    if duration_ms > config.max_duration_ms:
        return None
    if avg_speed < config.min_speed_pxs:
        return None

    direction = classify_direction(angle)
    is_straight = straightness >= config.straightness_threshold

    # Compute confidence
    dist_score = min(1.0, distance / 100.0)
    speed_score = min(1.0, avg_speed / 500.0)
    straight_score = straightness
    confidence = (dist_score + speed_score + straight_score) / 3.0

    return SwipeCharacteristics(
        direction=direction,
        angle_degrees=angle,
        distance_px=distance,
        duration_ms=duration_ms,
        avg_speed_pxs=avg_speed,
        peak_speed_pxs=peak_speed,
        confidence=confidence,
        is_straight=is_straight,
        metadata={
            "path_length": path_length,
            "straightness": straightness,
            "point_count": len(points),
        },
    )
