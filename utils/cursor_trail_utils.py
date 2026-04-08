"""
Cursor Trail Utilities

Provides utilities for cursor trail visualization
and tracking in UI automation.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import time


@dataclass
class TrailPoint:
    """Represents a point in the cursor trail."""
    x: int
    y: int
    timestamp: float
    pressure: float = 1.0


class CursorTrail:
    """
    Manages cursor trail for visualization.
    
    Records cursor positions and provides
    trail data for rendering effects.
    """

    def __init__(self, max_points: int = 50) -> None:
        self._points: list[TrailPoint] = []
        self._max_points = max_points
        self._is_recording = False

    def start_recording(self) -> None:
        """Start recording cursor positions."""
        self._is_recording = True

    def stop_recording(self) -> None:
        """Stop recording cursor positions."""
        self._is_recording = False

    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._is_recording

    def record(self, x: int, y: int, pressure: float = 1.0) -> None:
        """Record a cursor position."""
        if not self._is_recording:
            return
        point = TrailPoint(
            x=x,
            y=y,
            timestamp=time.time(),
            pressure=pressure,
        )
        self._points.append(point)
        if len(self._points) > self._max_points:
            self._points.pop(0)

    def get_trail(self) -> list[TrailPoint]:
        """Get the recorded trail."""
        return list(self._points)

    def get_smoothed_trail(self, factor: float = 0.3) -> list[TrailPoint]:
        """Get smoothed trail using moving average."""
        if len(self._points) < 3:
            return list(self._points)

        smoothed = [self._points[0]]
        for i in range(1, len(self._points) - 1):
            prev = smoothed[-1]
            curr = self._points[i]
            smoothed.append(TrailPoint(
                x=int(prev.x * (1 - factor) + curr.x * factor),
                y=int(prev.y * (1 - factor) + curr.y * factor),
                timestamp=curr.timestamp,
                pressure=curr.pressure,
            ))
        smoothed.append(self._points[-1])
        return smoothed

    def clear(self) -> None:
        """Clear the trail."""
        self._points.clear()

    def get_velocity(self) -> float:
        """Calculate average velocity of recent points."""
        if len(self._points) < 2:
            return 0.0
        recent = self._points[-5:]
        total_dist = 0.0
        total_time = 0.0
        for i in range(1, len(recent)):
            dx = recent[i].x - recent[i - 1].x
            dy = recent[i].y - recent[i - 1].y
            dist = (dx * dx + dy * dy) ** 0.5
            dt = recent[i].timestamp - recent[i - 1].timestamp
            total_dist += dist
            total_time += dt
        if total_time > 0:
            return total_dist / total_time
        return 0.0
