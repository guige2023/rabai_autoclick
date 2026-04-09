"""Touch trajectory builder utilities for UI automation.

Provides utilities for building, optimizing, and executing
touch trajectories for gesture automation.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple


@dataclass
class TrajectoryPoint:
    """A point in a touch trajectory."""
    x: float
    y: float
    timestamp_ms: float
    pressure: float = 1.0
    duration_ms: float = 0.0


@dataclass
class TrajectorySegment:
    """A segment of a trajectory."""
    start: TrajectoryPoint
    end: TrajectoryPoint
    distance: float
    velocity: float
    acceleration: float


@dataclass
class TouchPhase:
    """Phase of a touch gesture."""
    START = "start"
    MOVE = "move"
    END = "end"
    CANCEL = "cancel"


class TrajectoryBuilder:
    """Builds optimized touch trajectories.
    
    Creates trajectories from waypoints with velocity profiles
    and smooth interpolation.
    """
    
    def __init__(self) -> None:
        """Initialize the trajectory builder."""
        self._waypoints: List[TrajectoryPoint] = []
        self._segments: List[TrajectorySegment] = []
    
    def add_waypoint(
        self,
        x: float,
        y: float,
        timestamp_ms: float = 0.0,
        pressure: float = 1.0
    ) -> None:
        """Add a waypoint to the trajectory.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            timestamp_ms: Timestamp in milliseconds.
            pressure: Touch pressure.
        """
        self._waypoints.append(TrajectoryPoint(
            x=x,
            y=y,
            timestamp_ms=timestamp_ms,
            pressure=pressure
        ))
        self._rebuild_segments()
    
    def add_waypoints(self, points: List[Tuple[float, float]]) -> None:
        """Add multiple waypoints.
        
        Args:
            points: List of (x, y) tuples.
        """
        for x, y in points:
            self.add_waypoint(x, y)
    
    def clear(self) -> None:
        """Clear all waypoints."""
        self._waypoints = []
        self._segments = []
    
    def _rebuild_segments(self) -> None:
        """Rebuild trajectory segments from waypoints."""
        self._segments = []
        
        for i in range(len(self._waypoints) - 1):
            start = self._waypoints[i]
            end = self._waypoints[i + 1]
            
            dx = end.x - start.x
            dy = end.y - start.y
            distance = math.sqrt(dx * dx + dy * dy)
            
            dt = max(end.timestamp_ms - start.timestamp_ms, 0.001)
            velocity = distance / dt * 1000
            
            prev_velocity = 0.0
            if i > 0:
                prev_velocity = self._segments[i - 1].velocity
            
            acceleration = (velocity - prev_velocity) / dt * 1000
            
            self._segments.append(TrajectorySegment(
                start=start,
                end=end,
                distance=distance,
                velocity=velocity,
                acceleration=acceleration
            ))
    
    def get_point_at_distance(self, distance: float) -> Optional[TrajectoryPoint]:
        """Get a point at a specific distance along the trajectory.
        
        Args:
            distance: Distance along trajectory.
            
        Returns:
            Trajectory point or None.
        """
        if not self._segments:
            return None
        
        accumulated = 0.0
        for segment in self._segments:
            if accumulated + segment.distance >= distance:
                segment_progress = (
                    (distance - accumulated) / segment.distance
                    if segment.distance > 0 else 0.0
                )
                
                x = segment.start.x + (
                    segment.end.x - segment.start.x
                ) * segment_progress
                y = segment.start.y + (
                    segment.end.y - segment.start.y
                ) * segment_progress
                
                dt = segment.end.timestamp_ms - segment.start.timestamp_ms
                timestamp = segment.start.timestamp_ms + dt * segment_progress
                
                pressure = segment.start.pressure + (
                    segment.end.pressure - segment.start.pressure
                ) * segment_progress
                
                return TrajectoryPoint(
                    x=x,
                    y=y,
                    timestamp_ms=timestamp,
                    pressure=pressure
                )
            
            accumulated += segment.distance
        
        return self._waypoints[-1] if self._waypoints else None
    
    def get_point_at_time(self, timestamp_ms: float) -> Optional[TrajectoryPoint]:
        """Get a point at a specific time along the trajectory.
        
        Args:
            timestamp_ms: Time in milliseconds.
            
        Returns:
            Trajectory point or None.
        """
        if not self._waypoints:
            return None
        
        if timestamp_ms <= self._waypoints[0].timestamp_ms:
            return self._waypoints[0]
        
        if timestamp_ms >= self._waypoints[-1].timestamp_ms:
            return self._waypoints[-1]
        
        for i in range(len(self._waypoints) - 1):
            start = self._waypoints[i]
            end = self._waypoints[i + 1]
            
            if start.timestamp_ms <= timestamp_ms <= end.timestamp_ms:
                progress = (
                    (timestamp_ms - start.timestamp_ms) /
                    (end.timestamp_ms - start.timestamp_ms)
                )
                
                x = start.x + (end.x - start.x) * progress
                y = start.y + (end.y - start.y) * progress
                pressure = start.pressure + (
                    end.pressure - start.pressure
                ) * progress
                
                return TrajectoryPoint(
                    x=x,
                    y=y,
                    timestamp_ms=timestamp_ms,
                    pressure=pressure
                )
        
        return None
    
    def get_total_distance(self) -> float:
        """Get total distance of the trajectory.
        
        Returns:
            Total distance.
        """
        return sum(s.distance for s in self._segments)
    
    def get_total_duration_ms(self) -> float:
        """Get total duration of the trajectory.
        
        Returns:
            Total duration in milliseconds.
        """
        if not self._waypoints:
            return 0.0
        return self._waypoints[-1].timestamp_ms - self._waypoints[0].timestamp_ms
    
    def get_waypoints(self) -> List[TrajectoryPoint]:
        """Get all waypoints.
        
        Returns:
            List of waypoints.
        """
        return list(self._waypoints)
    
    def get_segments(self) -> List[TrajectorySegment]:
        """Get all segments.
        
        Returns:
            List of segments.
        """
        return list(self._segments)


class VelocityProfileBuilder:
    """Builds velocity profiles for trajectories.
    
    Creates velocity profiles that follow trapezoidal or
    S-curve patterns for natural-feeling gestures.
    """
    
    @staticmethod
    def trapezoidal_profile(
        distance: float,
        max_velocity: float,
        acceleration: float,
        start_time_ms: float = 0.0
    ) -> List[TrajectoryPoint]:
        """Create a trapezoidal velocity profile.
        
        Args:
            distance: Total distance.
            max_velocity: Maximum velocity.
            acceleration: Acceleration rate.
            start_time_ms: Start time in milliseconds.
            
        Returns:
            List of trajectory points.
        """
        accel_time = max_velocity / acceleration
        accel_dist = 0.5 * acceleration * accel_time * accel_time
            
        if accel_dist * 2 > distance:
            accel_dist = distance / 2
            acceleration = 2 * accel_dist
            accel_time = math.sqrt(2 * accel_dist / acceleration)
        
        decel_dist = accel_dist
        cruise_dist = distance - accel_dist - decel_dist
        cruise_time = cruise_dist / max_velocity if max_velocity > 0 else 0
        
        points = []
        current_time = start_time_ms
        current_distance = 0.0
        
        t = 0.0
        while t <= accel_time:
            d = 0.5 * acceleration * t * t
            points.append(TrajectoryPoint(
                x=d,
                y=0,
                timestamp_ms=current_time + t * 1000,
                duration_ms=accel_time * 1000
            ))
            t += 0.01
        
        current_time += accel_time * 1000
        current_distance += accel_dist
        
        if cruise_time > 0:
            t = 0.0
            while t <= cruise_time:
                d = current_distance + max_velocity * t
                points.append(TrajectoryPoint(
                    x=d,
                    y=0,
                    timestamp_ms=current_time + t * 1000
                ))
                t += 0.01
            current_time += cruise_time * 1000
            current_distance += cruise_dist
        
        decel_time = max_velocity / acceleration
        t = 0.0
        while t <= decel_time:
            d = current_distance + (
                max_velocity * t - 0.5 * acceleration * t * t
            )
            points.append(TrajectoryPoint(
                x=d,
                y=0,
                timestamp_ms=current_time + t * 1000
            ))
            t += 0.01
        
        return points
    
    @staticmethod
    def s_curve_profile(
        distance: float,
        max_velocity: float,
        jerk: float,
        start_time_ms: float = 0.0
    ) -> List[TrajectoryPoint]:
        """Create an S-curve velocity profile.
        
        Args:
            distance: Total distance.
            max_velocity: Maximum velocity.
            jerk: Jerk (rate of change of acceleration).
            start_time_ms: Start time in milliseconds.
            
        Returns:
            List of trajectory points.
        """
        accel_time = max_velocity / jerk
        accel_dist = jerk * accel_time * accel_time * accel_time / 6
        
        if accel_dist * 2 > distance:
            scale = math.pow(distance / 2 / accel_dist, 1/3)
            accel_time *= scale
            jerk *= scale
        
        points = []
        t = 0.0
        dt = 0.005
        
        while t <= accel_time:
            a = jerk * t
            v = 0.5 * jerk * t * t
            d = jerk * t * t * t / 6
            points.append(TrajectoryPoint(
                x=d,
                y=0,
                timestamp_ms=start_time_ms + t * 1000
            ))
            t += dt
        
        cruise_time = (distance - 2 * accel_dist) / max_velocity
        if cruise_time > 0:
            t = 0.0
            while t <= cruise_time:
                d = accel_dist + max_velocity * t
                points.append(TrajectoryPoint(
                    x=d,
                    y=0,
                    timestamp_ms=start_time_ms + (accel_time + t) * 1000
                ))
                t += dt
        
        return points


class TrajectoryOptimizer:
    """Optimizes touch trajectories for execution.
    
    Reduces point count while maintaining path accuracy,
    useful for efficient gesture playback.
    """
    
    def __init__(
        self,
        tolerance: float = 2.0,
        angle_threshold_deg: float = 15.0
    ) -> None:
        """Initialize the trajectory optimizer.
        
        Args:
            tolerance: Distance tolerance for point removal.
            angle_threshold_deg: Angle threshold for simplification.
        """
        self.tolerance = tolerance
        self.angle_threshold_deg = angle_threshold_deg
    
    def simplify(self, points: List[TrajectoryPoint]) -> List[TrajectoryPoint]:
        """Simplify a trajectory using the Ramer-Douglas-Peucker algorithm.
        
        Args:
            points: Points to simplify.
            
        Returns:
            Simplified points.
        """
        if len(points) < 3:
            return list(points)
        
        return self._rdp_simplify(points, 0, len(points) - 1)
    
    def _rdp_simplify(
        self,
        points: List[TrajectoryPoint],
        start: int,
        end: int
    ) -> List[TrajectoryPoint]:
        """Recursive RDP simplification.
        
        Args:
            points: All points.
            start: Start index.
            end: End index.
            
        Returns:
            Simplified points.
        """
        if end <= start + 1:
            return [points[start]]
        
        max_distance = 0.0
        max_index = start
        
        start_point = points[start]
        end_point = points[end]
        
        for i in range(start + 1, end):
            distance = self._perpendicular_distance(
                points[i], start_point, end_point
            )
            if distance > max_distance:
                max_distance = distance
                max_index = i
        
        if max_distance > self.tolerance:
            left = self._rdp_simplify(points, start, max_index)
            right = self._rdp_simplify(points, max_index, end)
            return left + right[1:]
        else:
            return [points[start], points[end]]
    
    @staticmethod
    def _perpendicular_distance(
        point: TrajectoryPoint,
        line_start: TrajectoryPoint,
        line_end: TrajectoryPoint
    ) -> float:
        """Calculate perpendicular distance from point to line.
        
        Args:
            point: Point to measure.
            line_start: Line start point.
            line_end: Line end point.
            
        Returns:
            Perpendicular distance.
        """
        dx = line_end.x - line_start.x
        dy = line_end.y - line_start.y
        
        if dx == 0 and dy == 0:
            return math.sqrt(
                (point.x - line_start.x) ** 2 +
                (point.y - line_start.y) ** 2
            )
        
        t = ((point.x - line_start.x) * dx + 
             (point.y - line_start.y) * dy) / (dx * dx + dy * dy)
        
        nearest_x = line_start.x + t * dx
        nearest_y = line_start.y + t * dy
        
        return math.sqrt(
            (point.x - nearest_x) ** 2 +
            (point.y - nearest_y) ** 2
        )
    
    def resample(
        self,
        points: List[TrajectoryPoint],
        target_count: int
    ) -> List[TrajectoryPoint]:
        """Resample trajectory to a target point count.
        
        Args:
            points: Points to resample.
            target_count: Desired number of points.
            
        Returns:
            Resampled points.
        """
        if len(points) < 2:
            return list(points)
        
        if target_count <= 2:
            return [points[0], points[-1]]
        
        total_distance = 0.0
        distances = [0.0]
        
        for i in range(1, len(points)):
            dx = points[i].x - points[i-1].x
            dy = points[i].y - points[i-1].y
            total_distance += math.sqrt(dx * dx + dy * dy)
            distances.append(total_distance)
        
        if total_distance == 0:
            return list(points)
        
        step = total_distance / (target_count - 1)
        
        result = [points[0]]
        current_distance = step
        
        for i in range(1, len(points)):
            while distances[i] >= current_distance:
                progress = (current_distance - distances[i-1]) / (
                    distances[i] - distances[i-1]
                ) if distances[i] != distances[i-1] else 0
                
                x = points[i-1].x + (
                    points[i].x - points[i-1].x
                ) * progress
                y = points[i-1].y + (
                    points[i].y - points[i-1].y
                ) * progress
                
                timestamp = points[i-1].timestamp_ms + (
                    points[i].timestamp_ms - points[i-1].timestamp_ms
                ) * progress
                
                result.append(TrajectoryPoint(
                    x=x,
                    y=y,
                    timestamp_ms=timestamp
                ))
                
                current_distance += step
        
        while len(result) < target_count:
            result.append(points[-1])
        
        return result[:target_count]
