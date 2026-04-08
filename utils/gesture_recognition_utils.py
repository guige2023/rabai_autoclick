"""
Gesture recognition utilities for recognizing complex touch gestures.

Provides gesture recognition based on touch point sequences,
supporting common gestures like swipe, tap, long-press, and custom patterns.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class RecognizedGesture:
    """Result of gesture recognition."""
    gesture_type: str
    confidence: float
    start_x: float
    start_y: float
    end_x: float
    end_y: float
    duration_ms: float
    finger_count: int
    metadata: dict


class GestureRecognizer:
    """Recognizes gestures from touch point sequences."""

    def __init__(
        self,
        swipe_threshold: float = 50.0,
        tap_max_duration: float = 300.0,
        tap_max_distance: float = 20.0,
        long_press_min_duration: float = 500.0,
    ):
        self.swipe_threshold = swipe_threshold
        self.tap_max_duration = tap_max_duration
        self.tap_max_distance = tap_max_distance
        self.long_press_min_duration = long_press_min_duration

    def recognize(
        self,
        points: list[tuple[float, float, float]],  # (x, y, timestamp_ms)
        finger_count: int = 1,
    ) -> RecognizedGesture:
        """Recognize a gesture from touch points.

        Args:
            points: List of (x, y, timestamp_ms) tuples
            finger_count: Number of fingers used

        Returns:
            RecognizedGesture with type and confidence
        """
        if len(points) < 2:
            return RecognizedGesture(
                gesture_type="unknown",
                confidence=0.0,
                start_x=0, start_y=0, end_x=0, end_y=0,
                duration_ms=0.0,
                finger_count=finger_count,
            )

        start_x, start_y, start_ts = points[0]
        end_x, end_y, end_ts = points[-1]
        duration_ms = end_ts - start_ts

        dx = end_x - start_x
        dy = end_y - start_y
        distance = math.hypot(dx, dy)
        direction = self._get_direction(dx, dy)

        # Classify gesture
        gesture_type, confidence = self._classify(
            distance=distance,
            duration_ms=duration_ms,
            dx=dx,
            dy=dy,
            points_count=len(points),
        )

        return RecognizedGesture(
            gesture_type=gesture_type,
            confidence=confidence,
            start_x=start_x,
            start_y=start_y,
            end_x=end_x,
            end_y=end_y,
            duration_ms=duration_ms,
            finger_count=finger_count,
            metadata={"direction": direction, "distance": distance, "dx": dx, "dy": dy},
        )

    def _classify(
        self,
        distance: float,
        duration_ms: float,
        dx: float,
        dy: float,
        points_count: int,
    ) -> tuple[str, float]:
        """Classify the gesture type."""
        # Tap
        if duration_ms <= self.tap_max_duration and distance <= self.tap_max_distance:
            if points_count <= 3:
                return "tap", 0.95

        # Long press
        if duration_ms >= self.long_press_min_duration and distance <= self.tap_max_distance:
            return "long_press", 0.9

        # Swipe
        if distance >= self.swipe_threshold:
            return f"swipe_{self._get_direction(dx, dy)}", 0.85

        # Drag
        if distance > self.tap_max_distance:
            return "drag", 0.8

        return "unknown", 0.0

    def _get_direction(self, dx: float, dy: float) -> str:
        """Get direction from delta."""
        angle = math.degrees(math.atan2(dy, dx))
        if -45 <= angle < 45:
            return "right"
        elif 45 <= angle < 135:
            return "down"
        elif -135 <= angle < -45:
            return "up"
        else:
            return "left"

    def recognize_multi_finger(
        self,
        finger_tracks: list[list[tuple[float, float, float]]],
    ) -> RecognizedGesture:
        """Recognize gesture from multiple finger tracks.

        Args:
            finger_tracks: List of tracks, each track is [(x, y, timestamp_ms), ...]
        """
        finger_count = len(finger_tracks)

        if finger_count == 1:
            return self.recognize(finger_tracks[0], finger_count=1)

        # Two finger gestures
        if finger_count == 2:
            track1 = finger_tracks[0]
            track2 = finger_tracks[1]

            if len(track1) >= 2 and len(track2) >= 2:
                # Compute distance change between fingers
                d1_start = math.hypot(track1[0][0] - track2[0][0], track1[0][1] - track2[0][1])
                d1_end = math.hypot(track1[-1][0] - track2[-1][0], track1[-1][1] - track2[-1][1])

                if d1_end > d1_start * 1.5:
                    return RecognizedGesture(
                        gesture_type="pinch_open",
                        confidence=0.85,
                        start_x=track1[0][0], start_y=track1[0][1],
                        end_x=track1[-1][0], end_y=track1[-1][1],
                        duration_ms=track1[-1][2] - track1[0][2],
                        finger_count=2,
                        metadata={},
                    )
                elif d1_end < d1_start * 0.67:
                    return RecognizedGesture(
                        gesture_type="pinch_close",
                        confidence=0.85,
                        start_x=track1[0][0], start_y=track1[0][1],
                        end_x=track1[-1][0], end_y=track1[-1][1],
                        duration_ms=track1[-1][2] - track1[0][2],
                        finger_count=2,
                        metadata={},
                    )

        return RecognizedGesture(
            gesture_type=f"{finger_count}_finger",
            confidence=0.5,
            start_x=0, start_y=0, end_x=0, end_y=0,
            duration_ms=0.0,
            finger_count=finger_count,
            metadata={},
        )


__all__ = ["GestureRecognizer", "RecognizedGesture"]
