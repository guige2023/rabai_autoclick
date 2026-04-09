"""
Swipe Detection Utilities for UI Automation.

This module provides utilities for detecting and analyzing
swipe gestures in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


class SwipeDirection(Enum):
    """Cardinal directions for swipe gestures."""
    UP = "up"
    DOWN = "down"
    LEFT = "left"
    RIGHT = "right"
    DIAGONAL_UP_LEFT = "diagonal_up_left"
    DIAGONAL_UP_RIGHT = "diagonal_up_right"
    DIAGONAL_DOWN_LEFT = "diagonal_down_left"
    DIAGONAL_DOWN_RIGHT = "diagonal_down_right"
    UNKNOWN = "unknown"


@dataclass
class SwipeEvent:
    """Represents a detected swipe gesture."""
    direction: SwipeDirection
    start_x: float
    start_y: float
    end_x: float
    end_y: float
    distance: float
    duration: float
    velocity: float
    timestamp: float
    confidence: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SwipeConfig:
    """Configuration for swipe detection."""
    min_distance: float = 50.0
    max_distance: float = 3000.0
    max_duration: float = 2000.0
    min_velocity: float = 100.0
    diagonal_threshold: float = 0.25
    direction_smoothing: int = 3


class SwipeDetector:
    """Detects swipe gestures from touch/mouse input."""

    def __init__(self, config: Optional[SwipeConfig] = None) -> None:
        self._config = config or SwipeConfig()
        self._points: List[Tuple[float, float, float]] = []
        self._in_progress: bool = False

    def set_config(self, config: SwipeConfig) -> None:
        """Update the swipe detection configuration."""
        self._config = config

    def begin_swipe(self, x: float, y: float) -> None:
        """Begin tracking a potential swipe gesture."""
        self._points.clear()
        self._points.append((x, y, time.time()))
        self._in_progress = True

    def add_point(self, x: float, y: float) -> None:
        """Add a point to the current swipe tracking."""
        if not self._in_progress:
            return
        self._points.append((x, y, time.time()))

    def end_swipe(self) -> Optional[SwipeEvent]:
        """End tracking and return detected swipe, if any."""
        if not self._in_progress or len(self._points) < 2:
            self._in_progress = False
            return None

        self._in_progress = False

        start_x, start_y, start_time = self._points[0]
        end_x, end_y, end_time = self._points[-1]

        dx = end_x - start_x
        dy = end_y - start_y
        distance = math.sqrt(dx * dx + dy * dy)
        duration = (end_time - start_time) * 1000.0
        velocity = distance / duration if duration > 0 else 0.0

        if distance < self._config.min_distance:
            return None

        if distance > self._config.max_distance:
            return None

        if duration > self._config.max_duration:
            return None

        direction = self._determine_direction(dx, dy, distance)
        confidence = self._calculate_confidence(distance, velocity, duration)

        return SwipeEvent(
            direction=direction,
            start_x=start_x,
            start_y=start_y,
            end_x=end_x,
            end_y=end_y,
            distance=distance,
            duration=duration,
            velocity=velocity,
            timestamp=start_time,
            confidence=confidence,
        )

    def cancel_swipe(self) -> None:
        """Cancel the current swipe tracking."""
        self._in_progress = False
        self._points.clear()

    def _determine_direction(
        self,
        dx: float,
        dy: float,
        distance: float,
    ) -> SwipeDirection:
        """Determine the swipe direction from deltas."""
        if distance < 1.0:
            return SwipeDirection.UNKNOWN

        norm_dx = dx / distance
        norm_dy = dy / distance

        threshold = self._config.diagonal_threshold

        if norm_dy < -threshold and abs(norm_dx) < threshold:
            return SwipeDirection.UP
        if norm_dy > threshold and abs(norm_dx) < threshold:
            return SwipeDirection.DOWN
        if norm_dx < -threshold and abs(norm_dy) < threshold:
            return SwipeDirection.LEFT
        if norm_dx > threshold and abs(norm_dy) < threshold:
            return SwipeDirection.RIGHT

        if norm_dx < -threshold and norm_dy < -threshold:
            return SwipeDirection.DIAGONAL_UP_LEFT
        if norm_dx > threshold and norm_dy < -threshold:
            return SwipeDirection.DIAGONAL_UP_RIGHT
        if norm_dx < -threshold and norm_dy > threshold:
            return SwipeDirection.DIAGONAL_DOWN_LEFT
        if norm_dx > threshold and norm_dy > threshold:
            return SwipeDirection.DIAGONAL_DOWN_RIGHT

        return SwipeDirection.UNKNOWN

    def _calculate_confidence(
        self,
        distance: float,
        velocity: float,
        duration: float,
    ) -> float:
        """Calculate confidence score for the detected swipe."""
        distance_score = min(distance / self._config.max_distance, 1.0)
        velocity_score = min(velocity / self._config.min_velocity, 1.0) if velocity > 0 else 0.0
        duration_score = max(0.0, 1.0 - duration / self._config.max_duration)

        return (distance_score * 0.4 + velocity_score * 0.4 + duration_score * 0.2)

    def is_in_progress(self) -> bool:
        """Check if a swipe is currently being tracked."""
        return self._in_progress

    def get_points(self) -> List[Tuple[float, float, float]]:
        """Get all tracked points for the current swipe."""
        return list(self._points)


def detect_swipe_from_points(
    points: List[Tuple[float, float, float]],
    config: Optional[SwipeConfig] = None,
) -> Optional[SwipeEvent]:
    """Detect a swipe from a list of (x, y, timestamp) points."""
    if len(points) < 2:
        return None

    detector = SwipeDetector(config)
    detector.begin_swipe(points[0][0], points[0][1])
    for pt in points[1:]:
        detector.add_point(pt[0], pt[1])
    return detector.end_swipe()
