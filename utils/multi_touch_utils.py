"""
Multi-touch utilities for complex touch gesture handling.

Provides multi-touch gesture recognition, finger tracking,
and touch point correlation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TouchTrack:
    """Tracks a single finger's touch path."""
    finger_id: int
    points: list[tuple[float, float, float]] = field(default_factory=list)  # x, y, timestamp
    is_active: bool = True

    def add_point(self, x: float, y: float, timestamp: float) -> None:
        self.points.append((x, y, timestamp))

    def start_point(self) -> Optional[tuple[float, float, float]]:
        return self.points[0] if self.points else None

    def end_point(self) -> Optional[tuple[float, float, float]]:
        return self.points[-1] if self.points else None

    def total_distance(self) -> float:
        """Compute total distance traveled."""
        if len(self.points) < 2:
            return 0.0
        total = 0.0
        for i in range(1, len(self.points)):
            dx = self.points[i][0] - self.points[i-1][0]
            dy = self.points[i][1] - self.points[i-1][1]
            total += (dx*dx + dy*dy) ** 0.5
        return total

    def velocity(self) -> float:
        """Compute average velocity."""
        if len(self.points) < 2:
            return 0.0
        total_dist = self.total_distance()
        total_time = self.points[-1][2] - self.points[0][2]
        if total_time <= 0:
            return 0.0
        return total_dist / total_time


@dataclass
class MultiTouchGesture:
    """A recognized multi-touch gesture."""
    gesture_type: str
    finger_count: int
    tracks: list[TouchTrack]
    duration_ms: float
    metadata: dict = field(default_factory=dict)


class MultiTouchEngine:
    """Engine for multi-touch gesture recognition and tracking."""

    def __init__(self):
        self._tracks: dict[int, TouchTrack] = {}

    def begin_touch(self, finger_id: int, x: float, y: float, timestamp: float) -> TouchTrack:
        """Start tracking a new touch."""
        track = TouchTrack(finger_id=finger_id)
        track.add_point(x, y, timestamp)
        self._tracks[finger_id] = track
        return track

    def update_touch(self, finger_id: int, x: float, y: float, timestamp: float) -> None:
        """Update an existing touch point."""
        if finger_id in self._tracks:
            self._tracks[finger_id].add_point(x, y, timestamp)

    def end_touch(self, finger_id: int) -> Optional[TouchTrack]:
        """End tracking a touch and return the track."""
        track = self._tracks.pop(finger_id, None)
        if track:
            track.is_active = False
        return track

    def recognize_gesture(self) -> Optional[MultiTouchGesture]:
        """Recognize the current gesture from active tracks."""
        active_tracks = [t for t in self._tracks.values() if t.is_active]
        if len(active_tracks) < 2:
            return None

        all_tracks = list(self._tracks.values())
        if not all_tracks:
            return None

        total_duration = 0.0
        for track in all_tracks:
            if len(track.points) >= 2:
                duration = track.points[-1][2] - track.points[0][2]
                total_duration = max(total_duration, duration)

        gesture_type = self._classify_gesture(active_tracks)
        return MultiTouchGesture(
            gesture_type=gesture_type,
            finger_count=len(active_tracks),
            tracks=all_tracks,
            duration_ms=total_duration,
        )

    def _classify_gesture(self, tracks: list[TouchTrack]) -> str:
        """Classify the gesture type from tracks."""
        if len(tracks) == 2:
            return self._classify_two_finger(tracks)
        elif len(tracks) == 3:
            return "three_finger_tap"
        else:
            return f"{len(tracks)}_finger_tap"

    def _classify_two_finger(self, tracks: list[TouchTrack]) -> str:
        """Classify a two-finger gesture."""
        t1, t2 = tracks[0], tracks[1]

        # Compute distance change between fingers
        if len(t1.points) < 2 or len(t2.points) < 2:
            return "two_finger_tap"

        start_dist = self._distance(t1.points[0], t2.points[0])
        end_dist = self._distance(t1.points[-1], t2.points[-1])

        if end_dist > start_dist * 1.5:
            return "pinch_open"
        elif end_dist < start_dist * 0.67:
            return "pinch_close"
        elif abs(t1.velocity() - t2.velocity()) < 50:
            return "two_finger_scroll"
        else:
            return "two_finger_drag"

    def _distance(self, p1: tuple[float, float, float], p2: tuple[float, float, float]) -> float:
        return ((p1[0] - p2[0])**2 + (p1[1] - p2[1])**2) ** 0.5

    def clear(self) -> None:
        self._tracks.clear()


def recognize_from_touch_list(
    touches: list[tuple[int, float, float, float]]
) -> str:
    """Quick recognition from a list of (finger_id, x, y, timestamp) tuples.

    Args:
        touches: List of touch events in chronological order

    Returns:
        Gesture type string
    """
    engine = MultiTouchEngine()
    last_finger_id = -1

    for finger_id, x, y, ts in touches:
        if finger_id != last_finger_id:
            engine.begin_touch(finger_id, x, y, ts)
            last_finger_id = finger_id
        else:
            engine.update_touch(finger_id, x, y, ts)

    gesture = engine.recognize_gesture()
    return gesture.gesture_type if gesture else "unknown"


__all__ = ["MultiTouchEngine", "MultiTouchGesture", "TouchTrack", "recognize_from_touch_list"]
