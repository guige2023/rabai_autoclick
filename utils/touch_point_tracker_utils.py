"""
Touch point tracker utilities for real-time touch point tracking.

Provides touch point tracking with interpolation,
velocity computation, and trajectory smoothing.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TrackedPoint:
    """A tracked touch point with velocity."""
    x: float
    y: float
    timestamp_ms: float
    velocity_x: float = 0.0
    velocity_y: float = 0.0
    speed: float = 0.0
    finger_id: int = 0


class TouchPointTracker:
    """Real-time touch point tracker with smoothing."""

    def __init__(
        self,
        smoothing_factor: float = 0.3,
        min_movement_threshold: float = 1.0,
    ):
        self.smoothing_factor = smoothing_factor
        self.min_movement_threshold = min_movement_threshold
        self._active_tracks: dict[int, list[TrackedPoint]] = {}
        self._raw_points: dict[int, list[TrackedPoint]] = {}

    def update(
        self,
        finger_id: int,
        x: float,
        y: float,
        timestamp_ms: float,
    ) -> TrackedPoint:
        """Update tracking for a finger and return the smoothed point."""
        if finger_id not in self._active_tracks:
            self._active_tracks[finger_id] = []
            self._raw_points[finger_id] = []

        raw_point = TrackedPoint(x=x, y=y, timestamp_ms=timestamp_ms, finger_id=finger_id)
        self._raw_points[finger_id].append(raw_point)

        # Get last smoothed point
        track = self._active_tracks[finger_id]
        if track:
            last = track[-1]
            # Compute velocity
            dt = timestamp_ms - last.timestamp_ms
            if dt > 0:
                raw_point.velocity_x = (x - last.x) / dt * 1000  # pixels/sec
                raw_point.velocity_y = (y - last.y) / dt * 1000
                raw_point.speed = math.hypot(raw_point.velocity_x, raw_point.velocity_y)

            # Apply smoothing
            smoothed_x = last.x + (x - last.x) * self.smoothing_factor
            smoothed_y = last.y + (y - last.y) * self.smoothing_factor
            raw_point.x = smoothed_x
            raw_point.y = smoothed_y
        else:
            # First point
            raw_point.x = x
            raw_point.y = y

        self._active_tracks[finger_id].append(raw_point)

        # Keep only recent points
        if len(self._active_tracks[finger_id]) > 100:
            self._active_tracks[finger_id] = self._active_tracks[finger_id][-100:]

        return raw_point

    def end_track(self, finger_id: int) -> Optional[TrackedPoint]:
        """End tracking for a finger and return the final point."""
        track = self._active_tracks.pop(finger_id, None)
        if track:
            return track[-1] if track else None
        return None

    def get_current(self, finger_id: int) -> Optional[TrackedPoint]:
        """Get the current (most recent) tracked point for a finger."""
        track = self._active_tracks.get(finger_id)
        if track:
            return track[-1]
        return None

    def get_trajectory(self, finger_id: int, max_points: int = 50) -> list[TrackedPoint]:
        """Get the trajectory (history) for a finger."""
        track = self._active_tracks.get(finger_id, [])
        if len(track) <= max_points:
            return track[:]
        return track[-max_points:]

    def get_all_current(self) -> dict[int, TrackedPoint]:
        """Get current points for all active fingers."""
        return {
            fid: track[-1]
            for fid, track in self._active_tracks.items()
            if track
        }

    def interpolate(
        self,
        finger_id: int,
        target_time_ms: float,
    ) -> Optional[TrackedPoint]:
        """Interpolate position at a target time."""
        track = self._active_tracks.get(finger_id)
        if not track or len(track) < 2:
            return self.get_current(finger_id)

        # Find surrounding points
        for i in range(len(track) - 1):
            p1 = track[i]
            p2 = track[i + 1]
            if p1.timestamp_ms <= target_time_ms <= p2.timestamp_ms:
                t = (target_time_ms - p1.timestamp_ms) / (p2.timestamp_ms - p1.timestamp_ms)
                x = p1.x + (p2.x - p1.x) * t
                y = p1.y + (p2.y - p1.y) * t
                vx = p1.velocity_x + (p2.velocity_x - p1.velocity_x) * t
                vy = p1.velocity_y + (p2.velocity_y - p1.velocity_y) * t
                return TrackedPoint(
                    x=x, y=y, timestamp_ms=target_time_ms,
                    velocity_x=vx, velocity_y=vy,
                    speed=math.hypot(vx, vy),
                    finger_id=finger_id,
                )

        return track[-1]

    def clear(self) -> None:
        """Clear all tracks."""
        self._active_tracks.clear()
        self._raw_points.clear()


__all__ = ["TouchPointTracker", "TrackedPoint"]
