"""Pointer trajectory analysis for UI automation.

Analyzes, smooths, and classifies pointer (mouse/touch) trajectories
for gesture recognition and automation replay.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class TrajectoryPoint:
    """A single point in a pointer trajectory.

    Attributes:
        x: X coordinate.
        y: Y coordinate.
        timestamp: Time in seconds.
        pressure: Touch pressure if available (0.0-1.0).
        tilt_x: X tilt angle in degrees.
        tilt_y: Y tilt angle in degrees.
    """
    x: float
    y: float
    timestamp: float = 0.0
    pressure: float = 1.0
    tilt_x: float = 0.0
    tilt_y: float = 0.0

    def distance_to(self, other: TrajectoryPoint) -> float:
        """Euclidean distance to another point."""
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)

    def speed_to(self, other: TrajectoryPoint) -> float:
        """Speed (distance/time) to another point."""
        dt = other.timestamp - self.timestamp
        if dt <= 0:
            return 0.0
        return self.distance_to(other) / dt

    def velocity_vector(self, other: TrajectoryPoint) -> tuple[float, float]:
        """Velocity vector (vx, vy) to another point."""
        dt = other.timestamp - self.timestamp
        if dt <= 0:
            return (0.0, 0.0)
        return (
            (other.x - self.x) / dt,
            (other.y - self.y) / dt,
        )


@dataclass
class TrajectorySegment:
    """A segment between two consecutive trajectory points."""
    start: TrajectoryPoint
    end: TrajectoryPoint

    @property
    def length(self) -> float:
        """Segment length."""
        return self.start.distance_to(self.end)

    @property
    def angle(self) -> float:
        """Segment angle in radians."""
        return math.atan2(
            self.end.y - self.start.y,
            self.end.x - self.start.x,
        )

    @property
    def duration(self) -> float:
        """Time duration."""
        return self.end.timestamp - self.start.timestamp

    @property
    def speed(self) -> float:
        """Average speed over this segment."""
        return self.start.speed_to(self.end)

    @property
    def velocity(self) -> tuple[float, float]:
        """Velocity vector."""
        return self.start.velocity_vector(self.end)


@dataclass
class Trajectory:
    """A complete pointer trajectory.

    Attributes:
        points: Ordered list of trajectory points.
        is_smoothed: Whether this trajectory has been smoothed.
    """
    points: list[TrajectoryPoint] = field(default_factory=list)
    is_smoothed: bool = False

    def add_point(self, point: TrajectoryPoint) -> None:
        """Append a point to the trajectory."""
        self.points.append(point)

    def get_segment(self, index: int) -> Optional[TrajectorySegment]:
        """Get the segment starting at index."""
        if 0 <= index < len(self.points) - 1:
            return TrajectorySegment(self.points[index], self.points[index + 1])
        return None

    @property
    def segments(self) -> list[TrajectorySegment]:
        """All consecutive segments."""
        return [
            TrajectorySegment(self.points[i], self.points[i + 1])
            for i in range(len(self.points) - 1)
        ]

    @property
    def path_length(self) -> float:
        """Total path length."""
        return sum(s.length for s in self.segments)

    @property
    def displacement(self) -> float:
        """Direct distance from start to end."""
        if len(self.points) < 2:
            return 0.0
        return self.points[0].distance_to(self.points[-1])

    @property
    def duration(self) -> float:
        """Total time duration."""
        if len(self.points) < 2:
            return 0.0
        return self.points[-1].timestamp - self.points[0].timestamp

    @property
    def start(self) -> Optional[TrajectoryPoint]:
        """First point."""
        return self.points[0] if self.points else None

    @property
    def end(self) -> Optional[TrajectoryPoint]:
        """Last point."""
        return self.points[-1] if self.points else None

    @property
    def bounding_box(self) -> tuple[float, float, float, float]:
        """(min_x, min_y, max_x, max_y) bounding box."""
        if not self.points:
            return (0, 0, 0, 0)
        xs = [p.x for p in self.points]
        ys = [p.y for p in self.points]
        return (min(xs), min(ys), max(xs), max(ys))

    def resample(self, interval: float) -> Trajectory:
        """Resample trajectory at regular distance intervals.

        Returns a new Trajectory with evenly spaced points.
        """
        if len(self.points) < 2:
            return Trajectory(points=list(self.points))

        new_points: list[TrajectoryPoint] = [self.points[0]]
        accumulated = 0.0
        seg_idx = 0
        seg_progress = 0.0

        while seg_idx < len(self.points) - 1:
            seg = TrajectorySegment(self.points[seg_idx], self.points[seg_idx + 1])
            seg_len = seg.length
            if seg_len == 0:
                seg_idx += 1
                continue

            while seg_progress + (interval - accumulated) <= seg_len:
                t = (interval - accumulated) / seg_len
                new_x = seg.start.x + (seg.end.x - seg.start.x) * t
                new_y = seg.start.y + (seg.end.y - seg.start.y) * t
                dt = (interval - accumulated) / max(seg.speed, 0.001)
                new_t = seg.start.timestamp + dt
                new_pressure = seg.start.pressure + (
                    seg.end.pressure - seg.start.pressure
                ) * t
                new_points.append(
                    TrajectoryPoint(
                        x=new_x, y=new_y, timestamp=new_t, pressure=new_pressure,
                    )
                )
                accumulated = 0.0
                seg_progress = 0.0
                break

            accumulated += seg_len - seg_progress
            seg_progress = seg_len
            seg_idx += 1

        if new_points[-1] != self.points[-1]:
            new_points.append(self.points[-1])

        result = Trajectory(points=new_points, is_smoothed=self.is_smoothed)
        return result

    def smooth_moving_average(self, window: int = 3) -> Trajectory:
        """Smooth trajectory using a moving average filter.

        Args:
            window: Number of points to average (must be odd).
        """
        if len(self.points) <= window:
            return Trajectory(points=list(self.points), is_smoothed=True)

        half = window // 2
        new_points: list[TrajectoryPoint] = []

        for i in range(len(self.points)):
            start_i = max(0, i - half)
            end_i = min(len(self.points), i + half + 1)
            window_points = self.points[start_i:end_i]

            avg_x = sum(p.x for p in window_points) / len(window_points)
            avg_y = sum(p.y for p in window_points) / len(window_points)
            new_points.append(
                TrajectoryPoint(
                    x=avg_x,
                    y=avg_y,
                    timestamp=self.points[i].timestamp,
                    pressure=self.points[i].pressure,
                )
            )

        return Trajectory(points=new_points, is_smoothed=True)

    def smooth_bezier(self, factor: float = 0.2) -> Trajectory:
        """Smooth trajectory using Bezier curve fitting.

        Args:
            factor: Smoothing factor (0.0 = no smoothing, 1.0 = maximum).
        """
        if len(self.points) < 3:
            return self.smooth_moving_average(3)

        new_points: list[TrajectoryPoint] = [self.points[0]]

        for i in range(1, len(self.points) - 1):
            prev_p = self.points[i - 1]
            curr_p = self.points[i]
            next_p = self.points[i + 1]

            smoothed_x = (
                curr_p.x * (1 - factor)
                + (prev_p.x + next_p.x) * factor / 2
            )
            smoothed_y = (
                curr_p.y * (1 - factor)
                + (prev_p.y + next_p.y) * factor / 2
            )
            new_points.append(
                TrajectoryPoint(
                    x=smoothed_x,
                    y=smoothed_y,
                    timestamp=curr_p.timestamp,
                    pressure=curr_p.pressure,
                )
            )

        new_points.append(self.points[-1])
        return Trajectory(points=new_points, is_smoothed=True)

    def classify_motion(self) -> str:
        """Classify the overall motion type.

        Returns: 'stationary', 'linear', 'curved', 'oscillating', or 'complex'.
        """
        if len(self.points) < 3:
            return 'stationary' if self.path_length < 5 else 'linear'

        straightness = self.displacement / max(self.path_length, 0.001)
        if self.path_length < 5:
            return 'stationary'
        if straightness > 0.95:
            return 'linear'

        angles = [s.angle for s in self.segments]
        angle_changes = [
            abs(angles[i] - angles[i - 1])
            for i in range(1, len(angles))
        ]
        total_change = sum(angle_changes)

        if total_change < 0.5:
            return 'linear'
        if total_change < 2.0:
            return 'curved'
        if total_change > 6.0:
            return 'oscillating'
        return 'complex'

    def get_velocity_profile(self) -> list[float]:
        """Return list of speeds at each segment."""
        return [s.speed for s in self.segments]

    def get_acceleration_profile(self) -> list[float]:
        """Return list of acceleration values at each segment."""
        speeds = self.get_velocity_profile()
        if len(speeds) < 2:
            return []
        return [
            speeds[i] - speeds[i - 1]
            for i in range(1, len(speeds))
        ]

    def reverse(self) -> Trajectory:
        """Return a reversed copy of this trajectory."""
        reversed_pts = list(reversed([
            TrajectoryPoint(
                x=p.x, y=p.y,
                timestamp=self.duration - p.timestamp + (self.points[0].timestamp if self.points else 0),
                pressure=p.pressure,
                tilt_x=p.tilt_x,
                tilt_y=p.tilt_y,
            )
            for p in self.points
        ]))
        return Trajectory(points=reversed_pts, is_smoothed=self.is_smoothed)
