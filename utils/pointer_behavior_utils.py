"""
Pointer behavior utilities for analyzing pointer movement patterns.

Provides pointer behavior analysis including velocity patterns,
gesture recognition, and movement classification.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PointerSample:
    """A single pointer movement sample."""
    x: float
    y: float
    timestamp_ms: float
    button_state: int = 0  # 0=up, 1=down


@dataclass
class MovementSegment:
    """A segment of pointer movement."""
    start_x: float
    start_y: float
    end_x: float
    end_y: float
    duration_ms: float
    distance: float
    velocity: float
    direction: str  # "up", "down", "left", "right", "diagonal"


@dataclass
class BehaviorProfile:
    """Classified behavior profile of a pointer session."""
    session_id: str
    total_distance: float
    total_duration_ms: float
    average_velocity: float
    peak_velocity: float
    dominant_direction: str
    is_drag: bool
    is_click: bool
    click_count: int
    segments: list[MovementSegment] = field(default_factory=list)


class PointerBehaviorAnalyzer:
    """Analyzes pointer movement patterns and classifies behavior."""

    def __init__(self, session_id: str = "default"):
        self.session_id = session_id
        self._samples: list[PointerSample] = []
        self._click_count = 0
        self._button_down_time: Optional[float] = None

    def add_sample(self, x: float, y: float, timestamp_ms: float, button_state: int = 0) -> None:
        """Add a pointer sample."""
        self._samples.append(PointerSample(x=x, y=y, timestamp_ms=timestamp_ms, button_state=button_state))

        if button_state == 1 and self._button_down_time is None:
            self._button_down_time = timestamp_ms
        elif button_state == 0 and self._button_down_time is not None:
            self._button_down_time = None
            self._click_count += 1

    def analyze(self) -> BehaviorProfile:
        """Analyze the pointer session and return a behavior profile."""
        if len(self._samples) < 2:
            return BehaviorProfile(
                session_id=self.session_id,
                total_distance=0.0,
                total_duration_ms=0.0,
                average_velocity=0.0,
                peak_velocity=0.0,
                dominant_direction="none",
                is_drag=False,
                is_click=False,
                click_count=self._click_count,
            )

        # Compute segments
        segments = self._compute_segments()
        total_distance = sum(s.distance for s in segments)
        total_duration = sum(s.duration_ms for s in segments)
        peak_velocity = max(s.velocity for s in segments) if segments else 0.0
        avg_velocity = total_distance / total_duration if total_duration > 0 else 0.0

        # Direction
        direction_counts: dict[str, int] = {}
        for seg in segments:
            direction_counts[seg.direction] = direction_counts.get(seg.direction, 0) + 1
        dominant_direction = max(direction_counts, key=direction_counts.get) if direction_counts else "none"

        # Detect drag
        is_drag = self._click_count == 0 and len(self._samples) > 3

        # Detect click
        is_click = self._click_count > 0 and total_distance < 20

        return BehaviorProfile(
            session_id=self.session_id,
            total_distance=total_distance,
            total_duration_ms=total_duration,
            average_velocity=avg_velocity,
            peak_velocity=peak_velocity,
            dominant_direction=dominant_direction,
            is_drag=is_drag,
            is_click=is_click,
            click_count=self._click_count,
            segments=segments,
        )

    def _compute_segments(self) -> list[MovementSegment]:
        """Compute movement segments from samples."""
        segments = []

        for i in range(1, len(self._samples)):
            p1 = self._samples[i - 1]
            p2 = self._samples[i]

            dx = p2.x - p1.x
            dy = p2.y - p1.y
            distance = math.hypot(dx, dy)
            duration = p2.timestamp_ms - p1.timestamp_ms
            velocity = distance / duration if duration > 0 else 0.0
            direction = self._classify_direction(dx, dy)

            segments.append(MovementSegment(
                start_x=p1.x,
                start_y=p1.y,
                end_x=p2.x,
                end_y=p2.y,
                duration_ms=duration,
                distance=distance,
                velocity=velocity,
                direction=direction,
            ))

        return segments

    def _classify_direction(self, dx: float, dy: float) -> str:
        """Classify movement direction."""
        threshold = 2.0
        if abs(dx) < threshold and abs(dy) < threshold:
            return "stationary"

        angle = math.degrees(math.atan2(dy, dx))
        if -22.5 <= angle < 22.5:
            return "right"
        elif 22.5 <= angle < 67.5:
            return "down-right"
        elif 67.5 <= angle < 112.5:
            return "down"
        elif 112.5 <= angle < 157.5:
            return "down-left"
        elif angle >= 157.5 or angle < -157.5:
            return "left"
        elif -157.5 <= angle < -112.5:
            return "up-left"
        elif -112.5 <= angle < -67.5:
            return "up"
        elif -67.5 <= angle < -22.5:
            return "up-right"
        return "diagonal"

    def clear(self) -> None:
        """Clear all samples."""
        self._samples.clear()
        self._click_count = 0
        self._button_down_time = None


__all__ = ["PointerBehaviorAnalyzer", "PointerSample", "MovementSegment", "BehaviorProfile"]
