"""
Cursor trail effects and animation utilities for UI automation.

Provides functions for creating cursor trail effects,
visual feedback, and smooth cursor path animations.
"""

from __future__ import annotations

import time
from typing import List, Tuple, Optional, Callable, Deque
from collections import deque
from dataclasses import dataclass, field


Point = Tuple[float, float]


@dataclass
class TrailPoint:
    """A point in the cursor trail with metadata."""
    position: Point
    timestamp: float
    pressure: float = 1.0
    size: float = 1.0
    opacity: float = 1.0
    
    @property
    def age(self) -> float:
        """Get age of this trail point in seconds."""
        return time.time() - self.timestamp


class CursorTrail:
    """Manages cursor trail effect with configurable decay."""
    
    def __init__(
        self,
        max_points: int = 50,
        decay_rate: float = 0.05,
        min_opacity: float = 0.0,
        min_size: float = 0.2,
        fade_enabled: bool = True,
    ) -> None:
        """Initialize cursor trail manager.
        
        Args:
            max_points: Maximum trail points to retain
            decay_rate: Opacity decay per second
            min_opacity: Minimum opacity before removal
            min_size: Minimum point size multiplier
            fade_enabled: Whether to fade out older points
        """
        self.max_points = max_points
        self.decay_rate = decay_rate
        self.min_opacity = min_opacity
        self.min_size = min_size
        self.fade_enabled = fade_enabled
        self._points: Deque[TrailPoint] = deque(maxlen=max_points)
    
    def add_point(
        self,
        position: Point,
        pressure: float = 1.0,
        size: float = 1.0,
    ) -> None:
        """Add a new point to the trail.
        
        Args:
            position: (x, y) coordinates
            pressure: Touch pressure (0.0 to 1.0)
            size: Point size multiplier
        """
        point = TrailPoint(
            position=position,
            timestamp=time.time(),
            pressure=pressure,
            size=size,
            opacity=1.0,
        )
        self._points.append(point)
    
    def update(self, delta_time: float) -> None:
        """Update trail state, decaying old points.
        
        Args:
            delta_time: Time since last update in seconds
        """
        if not self.fade_enabled:
            return
        
        points_to_remove: List[TrailPoint] = []
        
        for point in self._points:
            point.opacity -= self.decay_rate * delta_time
            point.size = max(self.min_size, point.size - 0.02 * delta_time)
            
            if point.opacity <= self.min_opacity:
                points_to_remove.append(point)
        
        for point in points_to_remove:
            self._points.remove(point)
    
    def get_active_points(self) -> List[TrailPoint]:
        """Get all points with remaining opacity.
        
        Returns:
            List of visible trail points
        """
        if not self.fade_enabled:
            return list(self._points)
        
        return [p for p in self._points if p.opacity > self.min_opacity]
    
    def clear(self) -> None:
        """Clear all trail points."""
        self._points.clear()
    
    def get_latest_position(self) -> Optional[Point]:
        """Get most recent trail position.
        
        Returns:
            Latest point position or None if empty
        """
        if not self._points:
            return None
        return self._points[-1].position
    
    @property
    def length(self) -> int:
        """Get number of points in trail."""
        return len(self._points)


class TrailRenderer:
    """Renders cursor trail effects."""
    
    def __init__(
        self,
        trail: CursorTrail,
        color: Tuple[int, int, int] = (100, 200, 255),
        line_width: float = 3.0,
    ) -> None:
        """Initialize trail renderer.
        
        Args:
            trail: CursorTrail instance to render
            color: RGB trail color
            line_width: Base line width
        """
        self.trail = trail
        self.color = color
        self.line_width = line_width
    
    def get_render_data(self) -> List[dict]:
        """Get rendering data for current trail state.
        
        Returns:
            List of segment dicts with color, width, opacity, and points
        """
        points = self.trail.get_active_points()
        if len(points) < 2:
            return []
        
        segments = []
        for i in range(len(points) - 1):
            p1 = points[i]
            p2 = points[i + 1]
            
            segments.append({
                'start': p1.position,
                'end': p2.position,
                'color': self.color,
                'width': self.line_width * p2.size,
                'opacity': p2.opacity,
            })
        
        return segments


def smooth_path(points: List[Point], smoothness: float = 0.2) -> List[Point]:
    """Smooth a path using cardinal spline interpolation.
    
    Args:
        points: List of path points
        smoothness: Smoothness factor (0.0 = straight lines, 1.0 = very smooth)
    
    Returns:
        Smoothed path points
    """
    if len(points) < 3:
        return points.copy()
    
    smoothed = [points[0]]
    
    for i in range(1, len(points) - 1):
        p0 = points[i - 1]
        p1 = points[i]
        p2 = points[i + 1]
        
        smooth_x = p1[0] + smoothness * ((p0[0] + p2[0]) / 2 - p1[0])
        smooth_y = p1[1] + smoothness * ((p0[1] + p2[1]) / 2 - p1[1])
        
        smoothed.append((smooth_x, smooth_y))
    
    smoothed.append(points[-1])
    return smoothed


def catmull_rom_spline(
    p0: Point,
    p1: Point,
    p2: Point,
    p3: Point,
    segments: int = 10,
) -> List[Point]:
    """Generate Catmull-Rom spline through control points.
    
    Args:
        p0: First control point
        p1: Start of spline segment
        p2: End of spline segment
        p3: Last control point
        segments: Number of points to generate between p1 and p2
    
    Returns:
        List of interpolated points
    """
    result = []
    
    for i in range(segments):
        t = i / segments
        t2 = t * t
        t3 = t2 * t
        
        x = 0.5 * (
            (2 * p1[0]) +
            (-p0[0] + p2[0]) * t +
            (2 * p0[0] - 5 * p1[0] + 4 * p2[0] - p3[0]) * t2 +
            (-p0[0] + 3 * p1[0] - 3 * p2[0] + p3[0]) * t3
        )
        
        y = 0.5 * (
            (2 * p1[1]) +
            (-p0[1] + p2[1]) * t +
            (2 * p0[1] - 5 * p1[1] + 4 * p2[1] - p3[1]) * t2 +
            (-p0[1] + 3 * p1[1] - 3 * p2[1] + p3[1]) * t3
        )
        
        result.append((x, y))
    
    return result


def smooth_path_catmull(
    points: List[Point],
    segments: int = 10,
) -> List[Point]:
    """Smooth a path using Catmull-Rom spline.
    
    Args:
        points: List of path points
        segments: Points to generate between each pair
    
    Returns:
        Smoothed path points
    """
    if len(points) < 2:
        return points.copy()
    if len(points) == 2:
        return points.copy()
    
    result = []
    
    for i in range(len(points) - 1):
        p0 = points[max(0, i - 1)]
        p1 = points[i]
        p2 = points[min(len(points) - 1, i + 1)]
        p3 = points[min(len(points) - 1, i + 2)]
        
        if i == 0:
            result.append(p1)
        
        result.extend(catmull_rom_spline(p0, p1, p2, p3, segments))
    
    result.append(points[-1])
    return result


@dataclass
class CursorGesture:
    """Represents a captured cursor gesture."""
    points: List[Point] = field(default_factory=list)
    timestamps: List[float] = field(default_factory=list)
    duration: float = 0.0
    
    def add_point(self, position: Point) -> None:
        """Add a point with current timestamp."""
        now = time.time()
        self.points.append(position)
        self.timestamps.append(now)
        
        if len(self.timestamps) >= 2:
            self.duration = self.timestamps[-1] - self.timestamps[0]
    
    def get_velocity_profile(self) -> List[float]:
        """Calculate velocity at each point.
        
        Returns:
            List of velocities (pixels per second)
        """
        if len(self.points) < 2:
            return []
        
        velocities = []
        for i in range(len(self.points) - 1):
            dx = self.points[i + 1][0] - self.points[i][0]
            dy = self.points[i + 1][1] - self.points[i][1]
            dist = (dx * dx + dy * dy) ** 0.5
            dt = self.timestamps[i + 1] - self.timestamps[i]
            vel = dist / dt if dt > 0 else 0
            velocities.append(vel)
        
        return velocities
    
    def get_bounding_box(self) -> Optional[Tuple[float, float, float, float]]:
        """Get bounding box of gesture.
        
        Returns:
            (min_x, min_y, max_x, max_y) or None if empty
        """
        if not self.points:
            return None
        
        xs = [p[0] for p in self.points]
        ys = [p[1] for p in self.points]
        
        return (min(xs), min(ys), max(xs), max(ys))
    
    def resample(self, target_count: int) -> 'CursorGesture':
        """Resample gesture to have exactly target_count points.
        
        Args:
            target_count: Desired number of points
        
        Returns:
            New CursorGesture with resampled points
        """
        if not self.points or target_count < 2:
            return CursorGesture(points=self.points.copy(), timestamps=self.timestamps.copy())
        
        total_length = sum(
            ((self.points[i + 1][0] - self.points[i][0]) ** 2 +
             (self.points[i + 1][1] - self.points[i][1]) ** 2) ** 0.5
            for i in range(len(self.points) - 1)
        )
        
        interval = total_length / (target_count - 1)
        
        resampled = [self.points[0]]
        remaining = interval
        accumulated = 0.0
        
        for i in range(len(self.points) - 1):
            segment_length = (
                (self.points[i + 1][0] - self.points[i][0]) ** 2 +
                (self.points[i + 1][1] - self.points[i][1]) ** 2
            ) ** 0.5
            
            while remaining <= segment_length:
                ratio = remaining / segment_length
                new_x = self.points[i][0] + ratio * (self.points[i + 1][0] - self.points[i][0])
                new_y = self.points[i][1] + ratio * (self.points[i + 1][1] - self.points[i][1])
                resampled.append((new_x, new_y))
                
                remaining += interval
                accumulated += remaining
            
            remaining -= segment_length
        
        while len(resampled) < target_count:
            resampled.append(self.points[-1])
        
        result = CursorGesture()
        result.points = resampled[:target_count]
        result.timestamps = self.timestamps[:2] if len(self.timestamps) >= 2 else [time.time(), time.time()]
        result.duration = self.duration
        
        return result
