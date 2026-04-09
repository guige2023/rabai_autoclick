"""
Rotation Detection Utilities for UI Automation.

This module provides utilities for detecting and analyzing
two-finger rotation gestures in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


@dataclass
class RotationEvent:
    """Represents a rotation gesture event."""
    angle_delta: float
    total_angle: float
    center_x: float
    center_y: float
    velocity: float
    timestamp: float
    confidence: float = 1.0


@dataclass
class RotationConfig:
    """Configuration for rotation detection."""
    min_rotation_delta: float = 5.0
    smoothing_factor: float = 0.2
    velocity_window_ms: float = 150.0
    confidence_threshold: float = 0.5


@dataclass
class TouchPoint:
    """Represents a single touch point."""
    id: int
    x: float
    y: float
    timestamp: float


class RotationDetector:
    """Detects two-finger rotation gestures."""

    def __init__(self, config: Optional[RotationConfig] = None) -> None:
        self._config = config or RotationConfig()
        self._active_touches: Dict[int, TouchPoint] = {}
        self._initial_angle: Optional[float] = None
        self._current_angle: float = 0.0
        self._total_rotation: float = 0.0
        self._angle_history: List[float] = []
        self._timestamps: List[float] = []

    def add_touch(self, touch_id: int, x: float, y: float) -> None:
        """Add or update a touch point."""
        self._active_touches[touch_id] = TouchPoint(
            id=touch_id,
            x=x,
            y=y,
            timestamp=time.time(),
        )
        self._update_rotation()

    def remove_touch(self, touch_id: int) -> Optional[RotationEvent]:
        """Remove a touch point and return rotation event if detected."""
        self._active_touches.pop(touch_id, None)

        if len(self._active_touches) < 2 and self._initial_angle is not None:
            return self._create_rotation_event()

        self._initial_angle = None
        return None

    def _update_rotation(self) -> None:
        """Update rotation based on active touches."""
        if len(self._active_touches) < 2:
            return

        points = list(self._active_touches.values())
        angle = self._calculate_angle(points[0], points[1])

        if self._initial_angle is None:
            self._initial_angle = angle
            self._current_angle = angle
        else:
            delta = self._normalize_angle(angle - self._current_angle)
            self._current_angle = self._normalize_angle(self._current_angle + delta)
            self._total_rotation += delta

        self._angle_history.append(self._total_rotation)
        self._timestamps.append(time.time())

    def _calculate_angle(self, p0: TouchPoint, p1: TouchPoint) -> float:
        """Calculate the angle of the line between two touch points."""
        return math.atan2(p1.y - p0.y, p1.x - p0.x)

    def _normalize_angle(self, angle: float) -> float:
        """Normalize angle to [-pi, pi] range."""
        while angle > math.pi:
            angle -= 2 * math.pi
        while angle < -math.pi:
            angle += 2 * math.pi
        return angle

    def get_center(self) -> Tuple[float, float]:
        """Get the center point between the two active touches."""
        if len(self._active_touches) < 2:
            return (0.0, 0.0)

        points = list(self._active_touches.values())
        return ((points[0].x + points[1].x) / 2, (points[0].y + points[1].y) / 2)

    def get_total_rotation(self) -> float:
        """Get the total rotation in degrees."""
        return math.degrees(self._total_rotation)

    def get_velocity(self) -> float:
        """Calculate the angular velocity in degrees per second."""
        if len(self._angle_history) < 2:
            return 0.0

        now = time.time()
        cutoff = now - self._config.velocity_window_ms / 1000.0

        relevant: List[Tuple[float, float]] = []
        for i, ts in enumerate(self._timestamps):
            if ts >= cutoff:
                relevant.append((ts, self._angle_history[i]))

        if len(relevant) < 2:
            return 0.0

        first = relevant[0]
        last = relevant[-1]
        dt = last[0] - first[0]

        if dt <= 0:
            return 0.0

        delta_deg = math.degrees(abs(last[1] - first[1]))
        return delta_deg / dt

    def _create_rotation_event(self) -> Optional[RotationEvent]:
        """Create a rotation event from the final state."""
        total_deg = abs(self.get_total_rotation())

        if total_deg < self._config.min_rotation_delta:
            return None

        center_x, center_y = self.get_center()

        return RotationEvent(
            angle_delta=math.degrees(self._total_rotation),
            total_angle=total_deg,
            center_x=center_x,
            center_y=center_y,
            velocity=self.get_velocity(),
            timestamp=time.time(),
            confidence=self._calculate_confidence(),
        )

    def _calculate_confidence(self) -> float:
        """Calculate confidence score for the rotation."""
        base_confidence = min(self.get_total_rotation() / 90.0, 1.0)
        velocity_confidence = min(self.get_velocity() / 180.0, 1.0)
        return (base_confidence * 0.6 + velocity_confidence * 0.4)

    def reset(self) -> None:
        """Reset all state."""
        self._active_touches.clear()
        self._initial_angle = None
        self._current_angle = 0.0
        self._total_rotation = 0.0
        self._angle_history.clear()
        self._timestamps.clear()

    def has_active_touches(self) -> bool:
        """Check if there are active touches being tracked."""
        return len(self._active_touches) >= 2


def create_rotation_event(
    angle_delta: float,
    center_x: float,
    center_y: float,
    **kwargs: Any,
) -> RotationEvent:
    """Create a rotation event with the specified parameters."""
    return RotationEvent(
        angle_delta=angle_delta,
        total_angle=kwargs.get("total_angle", abs(angle_delta)),
        center_x=center_x,
        center_y=center_y,
        velocity=kwargs.get("velocity", 0.0),
        timestamp=kwargs.get("timestamp", time.time()),
        confidence=kwargs.get("confidence", 1.0),
    )
