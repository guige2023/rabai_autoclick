"""
Drag pattern recognition utilities for gesture analysis.

This module provides utilities for recognizing and classifying
drag patterns based on trajectory analysis.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Optional, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum, auto


class DragPattern(Enum):
    """Recognized drag pattern types."""
    STRAIGHT = auto()
    CURVED = auto()
    ZIGZAG = auto()
    CIRCULAR = auto()
    SPIRAL = auto()
    DIAGONAL = auto()
    L_SHAPE = auto()
    ARC = auto()
    UNKNOWN = auto()


@dataclass
class DragPoint:
    """
    A single point in a drag trajectory.

    Attributes:
        x: X coordinate.
        y: Y coordinate.
        timestamp: Timestamp in seconds.
        pressure: Touch pressure (0.0-1.0), if available.
    """
    x: int
    y: int
    timestamp: float
    pressure: float = 1.0

    def time_delta(self, other: DragPoint) -> float:
        """Get time difference to another point."""
        return abs(other.timestamp - self.timestamp)


@dataclass
class DragTrajectory:
    """
    A drag trajectory consisting of multiple points.

    Attributes:
        points: List of points in the trajectory.
        start_time: Start timestamp.
        end_time: End timestamp.
        start_x: Start x coordinate.
        start_y: Start y coordinate.
    """
    points: List[DragPoint] = field(default_factory=list)
    start_time: float = 0.0
    start_x: int = 0
    start_y: int = 0

    def add_point(self, x: int, y: int, timestamp: float, pressure: float = 1.0) -> None:
        """Add a point to the trajectory."""
        if not self.points:
            self.start_time = timestamp
            self.start_x = x
            self.start_y = y
        self.points.append(DragPoint(x, y, timestamp, pressure))

    @property
    def end_x(self) -> int:
        """End x coordinate."""
        return self.points[-1].x if self.points else self.start_x

    @property
    def end_y(self) -> int:
        """End y coordinate."""
        return self.points[-1].y if self.points else self.start_y

    @property
    def duration(self) -> float:
        """Total duration in seconds."""
        if len(self.points) < 2:
            return 0.0
        return self.points[-1].timestamp - self.start_time

    @property
    def total_distance(self) -> float:
        """Total Euclidean distance traveled."""
        if len(self.points) < 2:
            return 0.0
        total = 0.0
        for i in range(1, len(self.points)):
            dx = self.points[i].x - self.points[i - 1].x
            dy = self.points[i].y - self.points[i - 1].y
            total += math.sqrt(dx * dx + dy * dy)
        return total

    @property
    def displacement(self) -> float:
        """Straight-line distance from start to end."""
        dx = self.end_x - self.start_x
        dy = self.end_y - self.start_y
        return math.sqrt(dx * dx + dy * dy)

    @property
    def straightness_ratio(self) -> float:
        """Ratio of displacement to total distance (1.0 = perfectly straight)."""
        if self.total_distance == 0:
            return 1.0
        return min(1.0, self.displacement / self.total_distance)

    def average_speed(self) -> float:
        """Average speed in pixels per second."""
        if self.duration == 0:
            return 0.0
        return self.total_distance / self.duration

    def speed_at_index(self, index: int) -> float:
        """Speed at a specific point index."""
        if index < 1 or index >= len(self.points):
            return 0.0
        prev = self.points[index - 1]
        curr = self.points[index]
        dist = math.sqrt(
            (curr.x - prev.x) ** 2 + (curr.y - prev.y) ** 2
        )
        dt = curr.time_delta(prev)
        return dist / max(dt, 0.001)

    def total_angle_change(self) -> float:
        """Total cumulative angle change in degrees."""
        if len(self.points) < 3:
            return 0.0
        total = 0.0
        for i in range(1, len(self.points) - 1):
            angle = self._angle_between_points(
                self.points[i - 1], self.points[i], self.points[i + 1]
            )
            total += angle
        return math.degrees(total)

    @staticmethod
    def _angle_between_points(
        p1: DragPoint, p2: DragPoint, p3: DragPoint
    ) -> float:
        """Calculate angle at p2 formed by p1-p2-p3."""
        v1x = p1.x - p2.x
        v1y = p1.y - p2.y
        v2x = p3.x - p2.x
        v2y = p3.y - p2.y
        dot = v1x * v2x + v1y * v2y
        mag1 = math.sqrt(v1x * v1x + v1y * v1y)
        mag2 = math.sqrt(v2x * v2x + v2y * v2y)
        if mag1 == 0 or mag2 == 0:
            return 0.0
        cos_angle = max(-1.0, min(1.0, dot / (mag1 * mag2)))
        return math.acos(cos_angle)

    def bounding_box(self) -> Tuple[int, int, int, int]:
        """Get bounding box of trajectory as (min_x, min_y, max_x, max_y)."""
        if not self.points:
            return (self.start_x, self.start_y, self.start_x, self.start_y)
        xs = [p.x for p in self.points]
        ys = [p.y for p in self.points]
        return (min(xs), min(ys), max(xs), max(ys))


class DragPatternRecognizer:
    """
    Recognizes drag patterns from trajectory data.
    """

    def __init__(
        self,
        straightness_threshold: float = 0.95,
        curve_threshold: float = 0.7,
        zigzag_window: int = 5,
        zigzag_angle_threshold: float = 45.0,
    ):
        """
        Initialize the recognizer.

        Args:
            straightness_threshold: Ratio for straight line classification.
            curve_threshold: Ratio threshold for curved classification.
            zigzag_window: Window size for zigzag detection.
            zigzag_angle_threshold: Angle in degrees for direction change.
        """
        self._straightness_threshold = straightness_threshold
        self._curve_threshold = curve_threshold
        self._zigzag_window = zigzag_window
        self._zigzag_angle_threshold = zigzag_angle_threshold

    def recognize(self, trajectory: DragTrajectory) -> Tuple[DragPattern, float]:
        """
        Recognize the pattern from a trajectory.

        Args:
            trajectory: The drag trajectory to classify.

        Returns:
            Tuple of (pattern, confidence) where confidence is 0.0-1.0.
        """
        if len(trajectory.points) < 3:
            return (DragPattern.UNKNOWN, 0.0)

        straightness = trajectory.straightness_ratio
        # Check for straight line
        if straightness >= self._straightness_threshold:
            return self._classify_straight(trajectory)

        # Check for circular/spiral
        circularity = self._calculate_circularity(trajectory)
        if circularity > 0.8:
            if self._is_spiral(trajectory):
                return (DragPattern.SPIRAL, circularity)
            return (DragPattern.CIRCULAR, circularity)

        # Check for zigzag
        if self._has_zigzag(trajectory):
            return (DragPattern.ZIGZAG, 0.7)

        # Check for arc
        if self._is_arc(trajectory):
            return (DragPattern.ARC, 0.8)

        # Check for L-shape
        if self._is_l_shape(trajectory):
            return (DragPattern.L_SHAPE, 0.8)

        return (DragPattern.CURVED, straightness)

    def _classify_straight(self, trajectory: DragTrajectory) -> Tuple[DragPattern, float]:
        """Classify a straight-line drag."""
        dx = trajectory.end_x - trajectory.start_x
        dy = trajectory.end_y - trajectory.start_y

        if dx == 0 and dy == 0:
            return (DragPattern.STRAIGHT, 1.0)

        angle = math.degrees(math.atan2(dy, dx)) % 360
        # Classify by angle
        if 20 <= angle <= 70:
            return (DragPattern.DIAGONAL, 0.95)
        elif 110 <= angle <= 160:
            return (DragPattern.DIAGONAL, 0.95)
        elif 250 <= angle <= 290:
            return (DragPattern.DIAGONAL, 0.95)
        elif 335 <= angle or angle <= 25:
            return (DragPattern.STRAIGHT, 0.95)
        elif 85 <= angle <= 95:
            return (DragPattern.STRAIGHT, 0.95)
        elif 175 <= angle <= 185:
            return (DragPattern.STRAIGHT, 0.95)

        return (DragPattern.STRAIGHT, trajectory.straightness_ratio)

    def _calculate_circularity(self, trajectory: DragTrajectory) -> float:
        """Calculate how circular the trajectory is (0.0-1.0)."""
        if len(trajectory.points) < 5:
            return 0.0

        # Calculate centroid
        cx = sum(p.x for p in trajectory.points) / len(trajectory.points)
        cy = sum(p.y for p in trajectory.points) / len(trajectory.points)

        # Calculate variance of distances from centroid
        distances = [
            math.sqrt((p.x - cx) ** 2 + (p.y - cy) ** 2)
            for p in trajectory.points
        ]
        mean_dist = sum(distances) / len(distances)
        if mean_dist == 0:
            return 0.0

        variance = sum((d - mean_dist) ** 2 for d in distances) / len(distances)
        std_dev = math.sqrt(variance)

        # Low variance relative to mean = circular
        coefficient = std_dev / mean_dist
        return max(0.0, 1.0 - coefficient)

    def _is_spiral(self, trajectory: DragTrajectory) -> bool:
        """Check if trajectory is a spiral (increasing or decreasing radius)."""
        if len(trajectory.points) < 10:
            return False

        cx = sum(p.x for p in trajectory.points) / len(trajectory.points)
        cy = sum(p.y for p in trajectory.points) / len(trajectory.points)

        first_half = len(trajectory.points) // 2
        first_dist = sum(
            math.sqrt((p.x - cx) ** 2 + (p.y - cy) ** 2)
            for p in trajectory.points[:first_half]
        ) / first_half
        second_dist = sum(
            math.sqrt((p.x - cx) ** 2 + (p.y - cy) ** 2)
            for p in trajectory.points[first_half:]
        ) / (len(trajectory.points) - first_half)

        # Spiral if radius changes significantly
        ratio = max(first_dist, second_dist) / max(min(first_dist, second_dist), 1)
        return ratio > 1.5

    def _has_zigzag(self, trajectory: DragTrajectory) -> bool:
        """Detect zigzag pattern."""
        if len(trajectory.points) < self._zigzag_window * 2:
            return False

        zigzag_count = 0
        for i in range(self._zigzag_window, len(trajectory.points) - self._zigzag_window):
            prev = trajectory.points[i - self._zigzag_window]
            curr = trajectory.points[i]
            next_p = trajectory.points[i + self._zigzag_window]

            angle = self._angle_between_points(prev, curr, next_p)
            if math.degrees(angle) > self._zigzag_angle_threshold:
                zigzag_count += 1

        return zigzag_count >= 3

    def _is_arc(self, trajectory: DragTrajectory) -> bool:
        """Check if trajectory forms an arc (portion of a circle)."""
        if len(trajectory.points) < 5:
            return False
        circularity = self._calculate_circularity(trajectory)
        # Arc has moderate circularity and doesn't close
        start_to_end = math.sqrt(
            (trajectory.end_x - trajectory.start_x) ** 2
            + (trajectory.end_y - trajectory.start_y) ** 2
        )
        return circularity > 0.6 and start_to_end > trajectory.total_distance * 0.3

    def _is_l_shape(self, trajectory: DragTrajectory) -> bool:
        """Check if trajectory is L-shaped."""
        if len(trajectory.points) < 10:
            return False

        # Split trajectory in half
        half = len(trajectory.points) // 2
        first_half = trajectory.points[:half]
        second_half = trajectory.points[half:]

        # Check if each half is relatively straight
        def straightness_of_points(points: List[DragPoint]) -> float:
            if len(points) < 2:
                return 1.0
            total = sum(
                math.sqrt((points[i].x - points[i-1].x)**2 + (points[i].y - points[i-1].y)**2)
                for i in range(1, len(points))
            )
            disp = math.sqrt(
                (points[-1].x - points[0].x)**2 + (points[-1].y - points[0].y)**2
            )
            return disp / max(total, 1)

        s1 = straightness_of_points(first_half)
        s2 = straightness_of_points(second_half)
        return s1 > 0.9 and s2 > 0.9 and abs(s1 - s2) < 0.2
