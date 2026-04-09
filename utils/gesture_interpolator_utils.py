"""Gesture interpolation utilities for UI automation.

Provides utilities for interpolating between gesture points,
creating smooth gesture paths, and generating gesture sequences.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple


@dataclass
class GesturePoint:
    """A single point in a gesture sequence."""
    x: float
    y: float
    pressure: float = 1.0
    timestamp_ms: float = 0.0
    tilt_x: float = 0.0
    tilt_y: float = 0.0


@dataclass
class GestureSegment:
    """A segment between two gesture points."""
    start: GesturePoint
    end: GesturePoint
    distance: float
    duration_ms: float


class GestureInterpolator:
    """Interpolates between gesture points for smooth gesture execution.
    
    Supports linear, bezier, and arc interpolation for creating
    natural-feeling gesture paths.
    """
    
    def __init__(self) -> None:
        """Initialize the gesture interpolator."""
        self._points: List[GesturePoint] = []
        self._segments: List[GestureSegment] = []
        self._total_distance = 0.0
        self._total_duration_ms = 0.0
    
    def add_point(self, point: GesturePoint) -> None:
        """Add a point to the gesture.
        
        Args:
            point: Gesture point to add.
        """
        self._points.append(point)
        self._recalculate_segments()
    
    def add_points(self, points: List[GesturePoint]) -> None:
        """Add multiple points to the gesture.
        
        Args:
            points: List of gesture points to add.
        """
        self._points.extend(points)
        self._recalculate_segments()
    
    def clear(self) -> None:
        """Clear all points from the gesture."""
        self._points = []
        self._segments = []
        self._total_distance = 0.0
        self._total_duration_ms = 0.0
    
    def _recalculate_segments(self) -> None:
        """Recalculate all segments between points."""
        self._segments = []
        self._total_distance = 0.0
        self._total_duration_ms = 0.0
        
        for i in range(len(self._points) - 1):
            start = self._points[i]
            end = self._points[i + 1]
            
            dx = end.x - start.x
            dy = end.y - start.y
            distance = math.sqrt(dx * dx + dy * dy)
            
            duration_ms = end.timestamp_ms - start.timestamp_ms
            
            self._segments.append(GestureSegment(
                start=start,
                end=end,
                distance=distance,
                duration_ms=duration_ms
            ))
            
            self._total_distance += distance
            self._total_duration_ms += duration_ms
    
    def get_point_at_progress(self, progress: float) -> Optional[GesturePoint]:
        """Get an interpolated point at a given progress.
        
        Args:
            progress: Progress value from 0.0 to 1.0.
            
        Returns:
            Interpolated gesture point.
        """
        if not self._segments:
            return self._points[0] if self._points else None
        
        progress = max(0.0, min(1.0, progress))
        
        target_distance = self._total_distance * progress
        accumulated_distance = 0.0
        
        for segment in self._segments:
            if accumulated_distance + segment.distance >= target_distance:
                segment_progress = (
                    (target_distance - accumulated_distance) / segment.distance
                    if segment.distance > 0 else 0.0
                )
                
                x = segment.start.x + (segment.end.x - segment.start.x) * segment_progress
                y = segment.start.y + (segment.end.y - segment.start.y) * segment_progress
                
                duration_diff = segment.end.timestamp_ms - segment.start.timestamp_ms
                timestamp = segment.start.timestamp_ms + duration_diff * segment_progress
                
                pressure = segment.start.pressure + (
                    segment.end.pressure - segment.start.pressure
                ) * segment_progress
                
                return GesturePoint(
                    x=x,
                    y=y,
                    pressure=pressure,
                    timestamp_ms=timestamp,
                    tilt_x=segment.start.tilt_x,
                    tilt_y=segment.start.tilt_y
                )
            
            accumulated_distance += segment.distance
        
        return self._points[-1] if self._points else None
    
    def get_points_at_interval(self, interval_ms: float) -> List[GesturePoint]:
        """Get points at a fixed time interval.
        
        Args:
            interval_ms: Time interval in milliseconds.
            
        Returns:
            List of interpolated points.
        """
        if self._total_duration_ms <= 0:
            return list(self._points)
        
        points = []
        current_time = 0.0
        
        while current_time <= self._total_duration_ms:
            progress = current_time / self._total_duration_ms
            point = self.get_point_at_progress(progress)
            if point:
                points.append(point)
            current_time += interval_ms
        
        if self._points:
            last = self._points[-1]
            if not points or points[-1].timestamp_ms != last.timestamp_ms:
                points.append(last)
        
        return points
    
    def get_total_distance(self) -> float:
        """Get the total distance of the gesture.
        
        Returns:
            Total distance in coordinate units.
        """
        return self._total_distance
    
    def get_total_duration_ms(self) -> float:
        """Get the total duration of the gesture.
        
        Returns:
            Total duration in milliseconds.
        """
        return self._total_duration_ms


class BezierGestureInterpolator(GestureInterpolator):
    """Gesture interpolator using cubic bezier curves.
    
    Creates smoother paths than linear interpolation by using
    bezier curves between control points.
    """
    
    def __init__(
        self,
        control_point_offset: float = 0.3
    ) -> None:
        """Initialize the bezier interpolator.
        
        Args:
            control_point_offset: Offset for control points as fraction of distance.
        """
        super().__init__()
        self.control_point_offset = control_point_offset
    
    def get_point_at_progress(self, progress: float) -> Optional[GesturePoint]:
        """Get a bezier-interpolated point at given progress.
        
        Args:
            progress: Progress value from 0.0 to 1.0.
            
        Returns:
            Interpolated gesture point.
        """
        if len(self._points) < 2:
            return self._points[0] if self._points else None
        
        progress = max(0.0, min(1.0, progress))
        
        segments = len(self._points) - 1
        segment_progress = progress * segments
        segment_index = min(int(segment_progress), segments - 1)
        t = segment_progress - segment_index
        
        p0 = self._points[max(0, segment_index - 1)]
        p1 = self._points[segment_index]
        p2 = self._points[min(segment_index + 1, len(self._points) - 1)]
        p3 = self._points[min(segment_index + 2, len(self._points) - 1)]
        
        offset = self.control_point_offset
        
        cp1x = p1.x + (p2.x - p0.x) * offset
        cp1y = p1.y + (p2.y - p0.y) * offset
        cp2x = p2.x - (p3.x - p1.x) * offset
        cp2y = p2.y - (p3.y - p1.y) * offset
        
        x = self._cubic_bezier(t, p1.x, cp1x, cp2x, p2.x)
        y = self._cubic_bezier(t, p1.y, cp1y, cp2y, p2.y)
        
        pressure = self._cubic_bezier(t, p1.pressure, p1.pressure, p2.pressure, p2.pressure)
        
        duration = p2.timestamp_ms - p1.timestamp_ms
        timestamp = p1.timestamp_ms + duration * t
        
        return GesturePoint(
            x=x,
            y=y,
            pressure=pressure,
            timestamp_ms=timestamp,
            tilt_x=p1.tilt_x,
            tilt_y=p1.tilt_y
        )
    
    @staticmethod
    def _cubic_bezier(t: float, p0: float, p1: float, p2: float, p3: float) -> float:
        """Calculate cubic bezier value.
        
        Args:
            t: Parameter value (0 to 1).
            p0: Start point.
            p1: First control point.
            p2: Second control point.
            p3: End point.
            
        Returns:
            Interpolated value.
        """
        u = 1 - t
        return u*u*u*p0 + 3*u*u*t*p1 + 3*u*t*t*p2 + t*t*t*p3


class ArcGestureInterpolator(GestureInterpolator):
    """Gesture interpolator using arc paths.
    
    Creates curved paths using circular arc segments between points.
    """
    
    def get_point_at_progress(self, progress: float) -> Optional[GesturePoint]:
        """Get an arc-interpolated point at given progress.
        
        Args:
            progress: Progress value from 0.0 to 1.0.
            
        Returns:
            Interpolated gesture point.
        """
        if len(self._points) < 2:
            return self._points[0] if self._points else None
        
        progress = max(0.0, min(1.0, progress))
        
        segments = len(self._points) - 1
        segment_progress = progress * segments
        segment_index = min(int(segment_progress), segments - 1)
        t = segment_progress - segment_index
        
        p1 = self._points[segment_index]
        p2 = self._points[segment_index + 1]
        
        mid_x = (p1.x + p2.x) / 2
        mid_y = (p1.y + p2.y) / 2
        
        dx = p2.x - p1.x
        dy = p2.y - p1.y
        
        radius = math.sqrt(dx * dx + dy * dy) / 2
        
        arc_center_x = mid_x - dy * 0.5
        arc_center_y = mid_y + dx * 0.5
        
        start_angle = math.atan2(p1.y - arc_center_y, p1.x - arc_center_x)
        end_angle = math.atan2(p2.y - arc_center_y, p2.x - arc_center_x)
        
        current_angle = start_angle + (end_angle - start_angle) * t
        
        x = arc_center_x + radius * math.cos(current_angle)
        y = arc_center_y + radius * math.sin(current_angle)
        
        pressure = p1.pressure + (p2.pressure - p1.pressure) * t
        
        duration = p2.timestamp_ms - p1.timestamp_ms
        timestamp = p1.timestamp_ms + duration * t
        
        return GesturePoint(
            x=x,
            y=y,
            pressure=pressure,
            timestamp_ms=timestamp,
            tilt_x=p1.tilt_x,
            tilt_y=p1.tilt_y
        )


def create_gesture_from_points(
    points: List[Tuple[float, float]],
    duration_ms: float = 1000.0,
    interpolation: str = "linear"
) -> GestureInterpolator:
    """Create a gesture interpolator from a list of points.
    
    Args:
        points: List of (x, y) coordinates.
        duration_ms: Total gesture duration in milliseconds.
        interpolation: Type of interpolation ("linear", "bezier", or "arc").
        
    Returns:
        Configured GestureInterpolator.
    """
    if interpolation == "bezier":
        interpolator = BezierGestureInterpolator()
    elif interpolation == "arc":
        interpolator = ArcGestureInterpolator()
    else:
        interpolator = GestureInterpolator()
    
    if len(points) < 2:
        return interpolator
    
    interval_ms = duration_ms / (len(points) - 1)
    
    gesture_points = []
    for i, (x, y) in enumerate(points):
        gesture_points.append(GesturePoint(
            x=x,
            y=y,
            timestamp_ms=i * interval_ms
        ))
    
    interpolator.add_points(gesture_points)
    return interpolator


def smooth_gesture_path(
    points: List[GesturePoint],
    smoothness: float = 0.5
) -> List[GesturePoint]:
    """Smooth a gesture path using Catmull-Rom spline interpolation.
    
    Args:
        points: List of gesture points.
        smoothness: Smoothness factor (0.0 to 1.0).
        
    Returns:
        Smoothed list of gesture points.
    """
    if len(points) < 4:
        return points
    
    num_segments = len(points) - 1
    points_per_segment = max(2, int(10 * smoothness) + 1)
    
    smoothed: List[GesturePoint] = []
    
    for i in range(num_segments):
        p0 = points[max(0, i - 1)]
        p1 = points[i]
        p2 = points[min(i + 1, len(points) - 1)]
        p3 = points[min(i + 2, len(points) - 1)]
        
        for j in range(points_per_segment):
            t = j / points_per_segment if points_per_segment > 1 else 0.0
            
            t2 = t * t
            t3 = t2 * t
            
            x = 0.5 * (
                (2 * p1.x) +
                (-p0.x + p2.x) * t +
                (2*p0.x - 5*p1.x + 4*p2.x - p3.x) * t2 +
                (-p0.x + 3*p1.x - 3*p2.x + p3.x) * t3
            )
            
            y = 0.5 * (
                (2 * p1.y) +
                (-p0.y + p2.y) * t +
                (2*p0.y - 5*p1.y + 4*p2.y - p3.y) * t2 +
                (-p0.y + 3*p1.y - 3*p2.y + p3.y) * t3
            )
            
            pressure = p1.pressure + (p2.pressure - p1.pressure) * t
            timestamp = p1.timestamp_ms + (p2.timestamp_ms - p1.timestamp_ms) * t
            
            smoothed.append(GesturePoint(
                x=x,
                y=y,
                pressure=pressure,
                timestamp_ms=timestamp
            ))
    
    smoothed.append(points[-1])
    return smoothed
