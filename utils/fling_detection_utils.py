"""
Fling Detection Utilities for UI Automation.

This module provides utilities for detecting fling gestures
in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


class FlingDirection(Enum):
    """Direction of a fling gesture."""
    UP = "up"
    DOWN = "down"
    LEFT = "left"
    RIGHT = "right"
    UNKNOWN = "unknown"


@dataclass
class FlingEvent:
    """Represents a detected fling gesture."""
    direction: FlingDirection
    velocity_x: float
    velocity_y: float
    speed: float
    start_x: float
    start_y: float
    end_x: float
    end_y: float
    duration: float
    timestamp: float
    confidence: float = 1.0


@dataclass
class FlingConfig:
    """Configuration for fling detection."""
    min_velocity: float = 500.0
    max_duration_ms: float = 300.0
    min_distance: float = 30.0
    direction_threshold: float = 0.7
    velocity_smoothing: int = 3


class FlingDetector:
    """Detects fling gestures from touch/mouse input."""

    def __init__(self, config: Optional[FlingConfig] = None) -> None:
        self._config = config or FlingConfig()
        self._points: List[Tuple[float, float, float]] = []
        self._is_tracking: bool = False
        self._start_x: float = 0.0
        self._start_y: float = 0.0
        self._start_time: float = 0.0

    def begin_fling(self, x: float, y: float) -> None:
        """Begin tracking a potential fling gesture."""
        self._points.clear()
        self._points.append((x, y, time.time()))
        self._start_x = x
        self._start_y = y
        self._start_time = time.time()
        self._is_tracking = True

    def add_point(self, x: float, y: float) -> None:
        """Add a point to the current fling tracking."""
        if not self._is_tracking:
            return
        self._points.append((x, y, time.time()))

    def end_fling(self) -> Optional[FlingEvent]:
        """End tracking and return detected fling, if any."""
        if not self._is_tracking:
            return None

        self._is_tracking = False

        if len(self._points) < 2:
            return None

        start_x, start_y, start_time = self._points[0]
        end_x, end_y, end_time = self._points[-1]

        duration_ms = (end_time - start_time) * 1000.0

        if duration_ms > self._config.max_duration_ms:
            return None

        dx = end_x - start_x
        dy = end_y - start_y
        distance = math.sqrt(dx * dx + dy * dy)

        if distance < self._config.min_distance:
            return None

        velocity_x = dx / duration_ms * 1000.0 if duration_ms > 0 else 0.0
        velocity_y = dy / duration_ms * 1000.0 if duration_ms > 0 else 0.0
        speed = distance / duration_ms * 1000.0 if duration_ms > 0 else 0.0

        if speed < self._config.min_velocity:
            return None

        direction = self._determine_direction(dx, dy, distance)
        confidence = self._calculate_confidence(speed, duration_ms, distance)

        return FlingEvent(
            direction=direction,
            velocity_x=velocity_x,
            velocity_y=velocity_y,
            speed=speed,
            start_x=start_x,
            start_y=start_y,
            end_x=end_x,
            end_y=end_y,
            duration=duration_ms,
            timestamp=start_time,
            confidence=confidence,
        )

    def cancel(self) -> None:
        """Cancel the current fling tracking."""
        self._is_tracking = False
        self._points.clear()

    def _determine_direction(
        self,
        dx: float,
        dy: float,
        distance: float,
    ) -> FlingDirection:
        """Determine the fling direction."""
        if distance < 1.0:
            return FlingDirection.UNKNOWN

        norm_dx = dx / distance
        norm_dy = dy / distance

        if abs(norm_dx) > abs(norm_dy):
            if norm_dx > self._config.direction_threshold:
                return FlingDirection.RIGHT
            if norm_dx < -self._config.direction_threshold:
                return FlingDirection.LEFT
        else:
            if norm_dy > self._config.direction_threshold:
                return FlingDirection.DOWN
            if norm_dy < -self._config.direction_threshold:
                return FlingDirection.UP

        return FlingDirection.UNKNOWN

    def _calculate_confidence(
        self,
        speed: float,
        duration: float,
        distance: float,
    ) -> float:
        """Calculate confidence score for the fling."""
        speed_score = min(speed / (self._config.min_velocity * 2), 1.0)
        duration_score = max(0.0, 1.0 - duration / self._config.max_duration_ms)
        distance_score = min(distance / (self._config.min_distance * 3), 1.0)

        return (speed_score * 0.5 + duration_score * 0.25 + distance_score * 0.25)

    def get_raw_velocity(self) -> Tuple[float, float]:
        """Calculate raw velocity from recent points without smoothing."""
        if len(self._points) < 2:
            return (0.0, 0.0)

        recent = self._points[-self._config.velocity_smoothing:]
        if len(recent) < 2:
            recent = self._points

        p0 = recent[0]
        p1 = recent[-1]
        dt = p1[2] - p0[2]

        if dt <= 0:
            return (0.0, 0.0)

        vx = (p1[0] - p0[0]) / dt
        vy = (p1[1] - p0[1]) / dt

        return (vx, vy)

    def is_tracking(self) -> bool:
        """Check if a fling is currently being tracked."""
        return self._is_tracking

    def get_points(self) -> List[Tuple[float, float, float]]:
        """Get all tracked points."""
        return list(self._points)
