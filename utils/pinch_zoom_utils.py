"""
Pinch-Zoom Detection Utilities for UI Automation.

This module provides utilities for detecting and analyzing
pinch and zoom gestures in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


class ZoomGesture(Enum):
    """Types of zoom gestures."""
    PINCH_IN = "pinch_in"
    PINCH_OUT = "pinch_out"
    NONE = "none"


@dataclass
class ZoomEvent:
    """Represents a zoom/pinch gesture event."""
    gesture: ZoomGesture
    scale: float
    velocity: float
    center_x: float
    center_y: float
    timestamp: float
    confidence: float = 1.0


@dataclass
class PinchZoomConfig:
    """Configuration for pinch-zoom detection."""
    min_scale_change: float = 0.05
    min_pinch_distance: float = 30.0
    smoothing_factor: float = 0.3
    velocity_window_ms: float = 100.0


@dataclass
class TouchPoint:
    """Represents a single touch point."""
    id: int
    x: float
    y: float
    timestamp: float


class PinchZoomDetector:
    """Detects pinch and zoom gestures from two-finger touch input."""

    def __init__(self, config: Optional[PinchZoomConfig] = None) -> None:
        self._config = config or PinchZoomConfig()
        self._active_touches: Dict[int, TouchPoint] = {}
        self._initial_distance: Optional[float] = None
        self._current_scale: float = 1.0
        self._scale_history: List[float] = []
        self._timestamps: List[float] = []

    def add_touch(self, touch_id: int, x: float, y: float) -> None:
        """Add or update a touch point."""
        self._active_touches[touch_id] = TouchPoint(
            id=touch_id,
            x=x,
            y=y,
            timestamp=time.time(),
        )
        self._update_scale()

    def remove_touch(self, touch_id: int) -> Optional[ZoomEvent]:
        """Remove a touch point and return zoom event if detected."""
        self._active_touches.pop(touch_id, None)

        if len(self._active_touches) < 2 and self._initial_distance is not None:
            return self._create_zoom_event()

        self._initial_distance = None
        return None

    def _update_scale(self) -> None:
        """Update the current scale based on active touches."""
        if len(self._active_touches) < 2:
            return

        points = list(self._active_touches.values())
        p0, p1 = points[0], points[1]
        distance = self._calculate_distance(p0, p1)

        if self._initial_distance is None:
            self._initial_distance = distance
            self._current_scale = 1.0
        else:
            raw_scale = distance / self._initial_distance
            smoothed = self._current_scale * (1 - self._config.smoothing_factor) + raw_scale * self._config.smoothing_factor
            self._current_scale = smoothed

        self._scale_history.append(self._current_scale)
        self._timestamps.append(time.time())

    def _calculate_distance(self, p0: TouchPoint, p1: TouchPoint) -> float:
        """Calculate the distance between two touch points."""
        dx = p1.x - p0.x
        dy = p1.y - p0.y
        return math.sqrt(dx * dx + dy * dy)

    def get_center(self) -> Tuple[float, float]:
        """Get the center point between the two active touches."""
        if len(self._active_touches) < 2:
            return (0.0, 0.0)

        points = list(self._active_touches.values())
        return ((points[0].x + points[1].x) / 2, (points[0].y + points[1].y) / 2)

    def get_scale(self) -> float:
        """Get the current pinch scale."""
        return self._current_scale

    def get_gesture(self) -> ZoomGesture:
        """Determine the current zoom gesture type."""
        if len(self._active_touches) < 2:
            return ZoomGesture.NONE

        scale_delta = abs(self._current_scale - 1.0)
        if scale_delta < self._config.min_scale_change:
            return ZoomGesture.NONE

        if self._current_scale > 1.0:
            return ZoomGesture.PINCH_OUT
        else:
            return ZoomGesture.PINCH_IN

    def get_velocity(self) -> float:
        """Calculate the velocity of the zoom gesture."""
        if len(self._scale_history) < 2:
            return 0.0

        now = time.time()
        cutoff = now - self._config.velocity_window_ms / 1000.0

        relevant: List[Tuple[float, float]] = []
        for i, ts in enumerate(self._timestamps):
            if ts >= cutoff:
                relevant.append((ts, self._scale_history[i]))

        if len(relevant) < 2:
            return 0.0

        first = relevant[0]
        last = relevant[-1]
        dt = last[0] - first[0]

        if dt <= 0:
            return 0.0

        return abs(last[1] - first[1]) / dt

    def _create_zoom_event(self) -> Optional[ZoomEvent]:
        """Create a zoom event from the final state."""
        gesture = self.get_gesture()
        if gesture == ZoomGesture.NONE:
            return None

        center_x, center_y = self.get_center()
        scale_delta = abs(self._current_scale - 1.0)

        if scale_delta < self._config.min_scale_change:
            return None

        return ZoomEvent(
            gesture=gesture,
            scale=self._current_scale,
            velocity=self.get_velocity(),
            center_x=center_x,
            center_y=center_y,
            timestamp=time.time(),
        )

    def reset(self) -> None:
        """Reset all state."""
        self._active_touches.clear()
        self._initial_distance = None
        self._current_scale = 1.0
        self._scale_history.clear()
        self._timestamps.clear()

    def has_active_touches(self) -> bool:
        """Check if there are active touches being tracked."""
        return len(self._active_touches) >= 2


def create_zoom_event(
    gesture: ZoomGesture,
    scale: float,
    center_x: float,
    center_y: float,
    **kwargs: Any,
) -> ZoomEvent:
    """Create a zoom event with the specified parameters."""
    return ZoomEvent(
        gesture=gesture,
        scale=scale,
        velocity=kwargs.get("velocity", 0.0),
        center_x=center_x,
        center_y=center_y,
        timestamp=kwargs.get("timestamp", time.time()),
        confidence=kwargs.get("confidence", 1.0),
    )
