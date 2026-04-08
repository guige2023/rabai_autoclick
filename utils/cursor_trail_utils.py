"""
Cursor trail utilities for cursor trail effects.

Provides cursor trail simulation and tracking for
visual feedback and teaching mode features.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TrailPoint:
    """A point in the cursor trail."""
    x: float
    y: float
    timestamp_ms: float
    alpha: float = 1.0
    size: float = 1.0


@dataclass
class CursorTrail:
    """A cursor trail with fading points."""
    points: list[TrailPoint] = field(default_factory=list)
    max_points: int = 50
    fade_duration_ms: float = 300.0

    def add_point(self, x: float, y: float, timestamp_ms: float) -> None:
        """Add a point to the trail."""
        self.points.append(TrailPoint(x=x, y=y, timestamp_ms=timestamp_ms, alpha=1.0, size=1.0))
        if len(self.points) > self.max_points:
            self.points.pop(0)
        self._update_alphas(timestamp_ms)

    def _update_alphas(self, current_time_ms: float) -> None:
        """Update alpha values based on age."""
        for point in self.points:
            age = current_time_ms - point.timestamp_ms
            if age < self.fade_duration_ms:
                point.alpha = 1.0 - (age / self.fade_duration_ms)
            else:
                point.alpha = 0.0

    def get_visible_points(self, min_alpha: float = 0.05) -> list[TrailPoint]:
        """Get points that are still visible."""
        return [p for p in self.points if p.alpha >= min_alpha]

    def clear(self) -> None:
        self.points.clear()


class CursorTrailManager:
    """Manages multiple cursor trails for teaching mode."""

    def __init__(
        self,
        max_points: int = 50,
        fade_duration_ms: float = 300.0,
        auto_start: bool = False,
    ):
        self.max_points = max_points
        self.fade_duration_ms = fade_duration_ms
        self._trails: dict[str, CursorTrail] = {}
        self._is_recording: bool = auto_start

    def start_recording(self) -> None:
        """Start recording cursor trails."""
        self._is_recording = True

    def stop_recording(self) -> None:
        """Stop recording cursor trails."""
        self._is_recording = False

    def is_recording(self) -> bool:
        return self._is_recording

    def record_point(self, cursor_id: str, x: float, y: float) -> None:
        """Record a cursor position."""
        if not self._is_recording:
            return

        if cursor_id not in self._trails:
            self._trails[cursor_id] = CursorTrail(
                max_points=self.max_points,
                fade_duration_ms=self.fade_duration_ms,
            )

        timestamp_ms = time.time() * 1000
        self._trails[cursor_id].add_point(x, y, timestamp_ms)

    def get_trail(self, cursor_id: str) -> Optional[CursorTrail]:
        """Get trail for a cursor."""
        return self._trails.get(cursor_id)

    def get_all_visible_points(self, cursor_id: str, min_alpha: float = 0.05) -> list[TrailPoint]:
        """Get all visible points for a cursor."""
        trail = self._trails.get(cursor_id)
        if not trail:
            return []
        return trail.get_visible_points(min_alpha)

    def update(self) -> None:
        """Update trail states (call periodically)."""
        timestamp_ms = time.time() * 1000
        for trail in self._trails.values():
            trail._update_alphas(timestamp_ms)

    def clear_trail(self, cursor_id: str) -> None:
        """Clear trail for a cursor."""
        if cursor_id in self._trails:
            self._trails[cursor_id].clear()

    def clear_all(self) -> None:
        """Clear all trails."""
        for trail in self._trails.values():
            trail.clear()

    def prune_empty(self) -> None:
        """Remove empty trails to save memory."""
        self._trails = {k: v for k, v in self._trails.items() if len(v.points) > 0}


__all__ = ["CursorTrailManager", "CursorTrail", "TrailPoint"]
