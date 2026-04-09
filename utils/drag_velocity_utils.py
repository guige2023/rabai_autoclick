"""
Drag Velocity Utilities for UI Automation.

This module provides utilities for calculating and analyzing
velocity patterns during drag operations in UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple


@dataclass
class VelocityPoint:
    """A point with velocity information."""
    x: float
    y: float
    vx: float
    vy: float
    speed: float
    timestamp: float


@dataclass
class DragPhase:
    """Represents a phase in a drag operation."""
    name: str
    start_time: float
    end_time: Optional[float] = None
    start_x: float = 0.0
    start_y: float = 0.0
    end_x: float = 0.0
    end_y: float = 0.0
    avg_speed: float = 0.0


@dataclass
class DragAnalysis:
    """Complete analysis of a drag operation."""
    total_distance: float
    total_duration: float
    avg_speed: float
    max_speed: float
    min_speed: float
    phases: List[DragPhase] = field(default_factory=list)
    direction_changes: int = 0


class DragVelocityCalculator:
    """Calculates velocity metrics for drag operations."""

    def __init__(self) -> None:
        self._points: List[VelocityPoint] = []
        self._phases: List[DragPhase] = []

    def reset(self) -> None:
        """Reset all tracking data."""
        self._points.clear()
        self._phases.clear()

    def add_point(
        self,
        x: float,
        y: float,
        timestamp: Optional[float] = None,
    ) -> None:
        """Add a point to the velocity tracking."""
        if timestamp is None:
            timestamp = time.time()

        vx, vy, speed = 0.0, 0.0, 0.0

        if self._points:
            last = self._points[-1]
            dt = timestamp - last.timestamp
            if dt > 0:
                vx = (x - last.x) / dt
                vy = (y - last.y) / dt
                speed = math.sqrt(vx * vx + vy * vy)

        point = VelocityPoint(x=x, y=y, vx=vx, vy=vy, speed=speed, timestamp=timestamp)
        self._points.append(point)

    def get_points(self) -> List[VelocityPoint]:
        """Get all tracked points."""
        return list(self._points)

    def calculate_analysis(self) -> DragAnalysis:
        """Calculate complete analysis of tracked drag."""
        if not self._points:
            return DragAnalysis(
                total_distance=0.0,
                total_duration=0.0,
                avg_speed=0.0,
                max_speed=0.0,
                min_speed=0.0,
            )

        distances = []
        for i in range(1, len(self._points)):
            p0 = self._points[i - 1]
            p1 = self._points[i]
            d = math.sqrt((p1.x - p0.x) ** 2 + (p1.y - p0.y) ** 2)
            distances.append(d)

        total_distance = sum(distances)
        total_duration = self._points[-1].timestamp - self._points[0].timestamp

        speeds = [p.speed for p in self._points]
        avg_speed = sum(speeds) / len(speeds) if speeds else 0.0
        max_speed = max(speeds) if speeds else 0.0
        min_speed = min(speeds) if speeds else 0.0

        direction_changes = self._count_direction_changes()

        return DragAnalysis(
            total_distance=total_distance,
            total_duration=total_duration,
            avg_speed=avg_speed,
            max_speed=max_speed,
            min_speed=min_speed,
            phases=list(self._phases),
            direction_changes=direction_changes,
        )

    def _count_direction_changes(self) -> int:
        """Count the number of direction changes in the drag."""
        if len(self._points) < 3:
            return 0

        changes = 0
        prev_angle = self._calculate_angle(self._points[0], self._points[1])

        for i in range(2, len(self._points)):
            angle = self._calculate_angle(self._points[i - 1], self._points[i])
            angle_diff = abs(angle - prev_angle)

            if angle_diff > math.pi / 4 and angle_diff < 2 * math.pi - math.pi / 4:
                changes += 1

            prev_angle = angle

        return changes

    def _calculate_angle(self, p0: VelocityPoint, p1: VelocityPoint) -> float:
        """Calculate the angle between two velocity points."""
        return math.atan2(p1.y - p0.y, p1.x - p0.x)

    def start_phase(self, name: str, x: float, y: float) -> None:
        """Mark the start of a drag phase."""
        phase = DragPhase(
            name=name,
            start_time=time.time(),
            start_x=x,
            start_y=y,
        )
        self._phases.append(phase)

    def end_phase(self, x: float, y: float) -> None:
        """Mark the end of the current drag phase."""
        if not self._phases:
            return

        phase = self._phases[-1]
        phase.end_time = time.time()
        phase.end_x = x
        phase.end_y = y

        if phase.end_time > phase.start_time:
            dx = phase.end_x - phase.start_x
            dy = phase.end_y - phase.start_y
            distance = math.sqrt(dx * dx + dy * dy)
            duration = phase.end_time - phase.start_time
            phase.avg_speed = distance / duration if duration > 0 else 0.0

    def get_peak_velocity(self) -> Tuple[float, float, float]:
        """Get the peak velocity as (vx, vy, speed)."""
        if not self._points:
            return (0.0, 0.0, 0.0)

        max_speed_point = max(self._points, key=lambda p: p.speed)
        return (max_speed_point.vx, max_speed_point.vy, max_speed_point.speed)
