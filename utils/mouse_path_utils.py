"""
Mouse path generation and smoothing utilities.

Provides utilities for generating smooth mouse movement paths,
including Bezier curves, splines, and various trajectory patterns.
"""

from __future__ import annotations

import math
import random
from typing import List, Tuple, Optional, Callable, Iterator
from dataclasses import dataclass


@dataclass
class Point:
    """Represents a 2D point."""
    x: float
    y: float
    
    def __add__(self, other: "Point") -> "Point":
        return Point(self.x + other.x, self.y + other.y)
    
    def __sub__(self, other: "Point") -> "Point":
        return Point(self.x - other.x, self.y - other.y)
    
    def __mul__(self, scalar: float) -> "Point":
        return Point(self.x * scalar, self.y * scalar)
    
    def __rmul__(self, scalar: float) -> "Point":
        return Point(self.x * scalar, self.y * scalar)
    
    def distance_to(self, other: "Point") -> float:
        """Calculate distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)
    
    def lerp(self, other: "Point", t: float) -> "Point":
        """Linear interpolation to another point."""
        return self + (other - self) * t
    
    def angle_to(self, other: "Point") -> float:
        """Calculate angle in radians to another point."""
        dx = other.x - self.x
        dy = other.y - self.y
        return math.atan2(dy, dx)


@dataclass
class PathSegment:
    """Represents a segment of a mouse path."""
    start: Point
    end: Point
    control_points: List[Point] = None
    
    def __post_init__(self):
        if self.control_points is None:
            self.control_points = []
    
    def point_at(self, t: float) -> Point:
        """Get point at parameter t (0-1) along the segment."""
        raise NotImplementedError


@dataclass
class LinearSegment(PathSegment):
    """Linear path segment (straight line)."""
    
    def point_at(self, t: float) -> Point:
        return self.start.lerp(self.end, t)


@dataclass
class QuadraticBezierSegment(PathSegment):
    """Quadratic Bezier curve segment."""
    
    def __post_init__(self):
        super().__post_init__()
        if len(self.control_points) < 1:
            # Default control point
            mid = Point((self.start.x + self.end.x) / 2, (self.start.y + self.end.y) / 2)
            self.control_points = [mid]
    
    def point_at(self, t: float) -> Point:
        cp = self.control_points[0]
        t1 = 1 - t
        x = t1 * t1 * self.start.x + 2 * t1 * t * cp.x + t * t * self.end.x
        y = t1 * t1 * self.start.y + 2 * t1 * t * cp.y + t * t * self.end.y
        return Point(x, y)


@dataclass
class CubicBezierSegment(PathSegment):
    """Cubic Bezier curve segment."""
    
    def __post_init__(self):
        super().__post_init__()
        if len(self.control_points) < 2:
            # Default control points
            dx = self.end.x - self.start.x
            dy = self.end.y - self.start.y
            self.control_points = [
                Point(self.start.x + dx * 0.33, self.start.y + dy * 0.33),
                Point(self.start.x + dx * 0.66, self.start.y + dy * 0.66),
            ]
    
    def point_at(self, t: float) -> Point:
        p0, p1, p2, p3 = self.start, self.control_points[0], self.control_points[1], self.end
        t1 = 1 - t
        x = (t1**3) * p0.x + 3 * (t1**2) * t * p1.x + 3 * t1 * (t**2) * p2.x + (t**3) * p3.x
        y = (t1**3) * p0.y + 3 * (t1**2) * t * p1.y + 3 * t1 * (t**2) * p2.y + (t**3) * p3.y
        return Point(x, y)


class MousePath:
    """Represents a complete mouse movement path."""
    
    def __init__(self):
        """Initialize empty path."""
        self.segments: List[PathSegment] = []
        self._points_cache: Optional[List[Point]] = None
        self._total_length: Optional[float] = None
    
    def add_linear(self, start: Point, end: Point) -> "MousePath":
        """Add a linear segment."""
        self.segments.append(LinearSegment(start=start, end=end))
        self._invalidate_cache()
        return self
    
    def add_quadratic_bezier(
        self,
        start: Point,
        control: Point,
        end: Point
    ) -> "MousePath":
        """Add a quadratic Bezier segment."""
        self.segments.append(
            QuadraticBezierSegment(
                start=start,
                end=end,
                control_points=[control]
            )
        )
        self._invalidate_cache()
        return self
    
    def add_cubic_bezier(
        self,
        start: Point,
        control1: Point,
        control2: Point,
        end: Point
    ) -> "MousePath":
        """Add a cubic Bezier segment."""
        self.segments.append(
            CubicBezierSegment(
                start=start,
                end=end,
                control_points=[control1, control2]
            )
        )
        self._invalidate_cache()
        return self
    
    def _invalidate_cache(self) -> None:
        """Invalidate cached path data."""
        self._points_cache = None
        self._total_length = None
    
    def total_length(self) -> float:
        """Calculate total path length."""
        if self._total_length is None:
            total = 0.0
            for i, seg in enumerate(self.segments):
                for j in range(100):
                    p1 = seg.point_at(j / 100)
                    p2 = seg.point_at((j + 1) / 100)
                    total += p1.distance_to(p2)
            self._total_length = total
        return self._total_length
    
    def point_at_length(self, length: float) -> Point:
        """Get point at a specific distance along the path."""
        if not self.segments:
            return Point(0, 0)
        
        target_length = max(0, min(length, self.total_length()))
        accumulated = 0.0
        
        for seg in self.segments:
            seg_length = 0.0
            for j in range(100):
                p1 = seg.point_at(j / 100)
                p2 = seg.point_at((j + 1) / 100)
                seg_length += p1.distance_to(p2)
            
            if accumulated + seg_length >= target_length:
                # Point is in this segment
                local_length = target_length - accumulated
                t = local_length / seg_length if seg_length > 0 else 0
                return seg.point_at(t)
            
            accumulated += seg_length
        
        return self.segments[-1].end if self.segments else Point(0, 0)
    
    def points(self, num_points: int) -> List[Point]:
        """Sample evenly-spaced points along the path."""
        if self._points_cache is None or len(self._points_cache) != num_points:
            points = []
            total_len = self.total_length()
            for i in range(num_points):
                length = (i / (num_points - 1)) * total_len if num_points > 1 else 0
                points.append(self.point_at_length(length))
            self._points_cache = points
        return self._points_cache
    
    def to_coordinates(self, num_points: int) -> List[Tuple[int, int]]:
        """Convert path to integer coordinates for mouse movement.
        
        Args:
            num_points: Number of points to sample
            
        Returns:
            List of (x, y) integer tuples
        """
        pts = self.points(num_points)
        return [(int(p.x), int(p.y)) for p in pts]


def generate_bezier_path(
    start: Tuple[int, int],
    end: Tuple[int, int],
    control_offset: Optional[Tuple[float, float]] = None,
    curvature: float = 0.5
) -> MousePath:
    """Generate a smooth Bezier path between two points.
    
    Args:
        start: Starting coordinates (x, y)
        end: Ending coordinates (x, y)
        control_offset: Optional offset for control point
        curvature: Curvature factor (0.0-1.0, default 0.5)
        
    Returns:
        MousePath with the generated path
    """
    p_start = Point(*start)
    p_end = Point(*end)
    
    # Calculate control point
    if control_offset:
        cx = (start[0] + end[0]) / 2 + control_offset[0]
        cy = (start[1] + end[1]) / 2 + control_offset[1]
    else:
        mid_x = (start[0] + end[0]) / 2
        mid_y = (start[1] + end[1]) / 2
        
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        
        # Perpendicular offset for curve
        if dx == 0 and dy == 0:
            cx, cy = mid_x, mid_y
        else:
            length = math.sqrt(dx * dx + dy * dy)
            nx, ny = -dy / length, dx / length  # Perpendicular
            offset = length * curvature * 0.5
            cx = mid_x + nx * offset
            cy = mid_y + ny * offset
    
    path = MousePath()
    path.add_quadratic_bezier(p_start, Point(cx, cy), p_end)
    return path


def generate_curved_path(
    start: Tuple[int, int],
    end: Tuple[int, int],
    num_segments: int = 5,
    wave_amplitude: float = 50.0,
    seed: Optional[int] = None
) -> MousePath:
    """Generate a curved path with wave-like motion.
    
    Args:
        start: Starting coordinates
        end: Ending coordinates
        num_segments: Number of curve segments
        wave_amplitude: Amplitude of wave motion
        seed: Optional random seed
        
    Returns:
        MousePath with curved path
    """
    if seed is not None:
        random.seed(seed)
    
    p_start = Point(*start)
    p_end = Point(*end)
    
    # Generate intermediate control points
    points = [p_start]
    for i in range(1, num_segments):
        t = i / num_segments
        x = start[0] + (end[0] - start[0]) * t
        y = start[1] + (end[1] - start[1]) * t
        
        # Add random offset perpendicular to the line
        dx = end[0] - start[0]
        dy = end[1] - start[1]
        length = math.sqrt(dx * dx + dy * dy) or 1
        
        nx, ny = -dy / length, dx / length
        offset = (random.random() - 0.5) * 2 * wave_amplitude
        
        x += nx * offset
        y += ny * offset
        
        points.append(Point(x, y))
    
    points.append(p_end)
    
    # Create smooth path through points using Catmull-Rom spline
    path = MousePath()
    for i in range(len(points) - 1):
        p0 = points[max(0, i - 1)]
        p1 = points[i]
        p2 = points[i + 1]
        p3 = points[min(len(points) - 1, i + 2)]
        
        # Calculate control points for cubic bezier
        cp1 = Point(
            p1.x + (p2.x - p0.x) / 6,
            p1.y + (p2.y - p0.y) / 6
        )
        cp2 = Point(
            p2.x - (p3.x - p1.x) / 6,
            p2.y - (p3.y - p1.y) / 6
        )
        
        path.add_cubic_bezier(p1, cp1, cp2, p2)
    
    return path


def generate_spiral_path(
    center: Tuple[int, int],
    start_radius: float,
    end_radius: float,
    num_turns: float = 2.0,
    start_angle: float = 0.0
) -> MousePath:
    """Generate a spiral path.
    
    Args:
        center: Center coordinates
        start_radius: Starting radius
        end_radius: Ending radius
        num_turns: Number of full rotations
        start_angle: Starting angle in radians
        
    Returns:
        MousePath with spiral path
    """
    path = MousePath()
    num_points = max(50, int(abs(end_radius - start_radius) * 2 + num_turns * 50))
    
    for i in range(num_points):
        t = i / (num_points - 1)
        angle = start_angle + t * num_turns * 2 * math.pi
        radius = start_radius + (end_radius - start_radius) * t
        
        x = center[0] + radius * math.cos(angle)
        y = center[1] + radius * math.sin(angle)
        
        if i == 0:
            current = Point(x, y)
        else:
            path.add_linear(current, Point(x, y))
            current = Point(x, y)
    
    return path


def generate_figure_eight_path(
    center: Tuple[int, int],
    width: float,
    height: float,
    num_points: int = 100
) -> MousePath:
    """Generate a figure-8 (lemniscate) path.
    
    Args:
        center: Center coordinates
        width: Width of the figure-8
        height: Height of the figure-8
        num_points: Number of points to sample
        
    Returns:
        MousePath with figure-8 path
    """
    path = MousePath()
    previous_point = None
    
    for i in range(num_points):
        t = i / num_points
        angle = t * 2 * math.pi
        
        # Lemniscate of Bernoulli (approximation)
        denominator = 1 + math.sin(angle) ** 2
        x = center[0] + (width * math.cos(angle) / denominator)
        y = center[1] + (height * math.sin(angle) * math.cos(angle) / denominator)
        
        point = Point(x, y)
        if previous_point:
            path.add_linear(previous_point, point)
        previous_point = point
    
    return path


def ease_path(
    start: Tuple[int, int],
    end: Tuple[int, int],
    easing: Callable[[float], float] = None,
    num_points: int = 50
) -> List[Tuple[int, int]]:
    """Generate an eased path between two points.
    
    Args:
        start: Starting coordinates
        end: Ending coordinates
        easing: Easing function (default: ease-in-out)
        num_points: Number of points to generate
        
    Returns:
        List of (x, y) coordinates
    """
    if easing is None:
        def easing(t):
            if t < 0.5:
                return 2 * t * t
            return 1 - (-2 * t + 2) ** 2 / 2
    
    points = []
    for i in range(num_points):
        t = i / (num_points - 1)
        eased_t = easing(t)
        x = int(start[0] + (end[0] - start[0]) * eased_t)
        y = int(start[1] + (end[1] - start[1]) * eased_t)
        points.append((x, y))
    
    return points


def smooth_path_with_bspline(
    points: List[Tuple[int, int]],
    num_interpolated: int = 10
) -> List[Tuple[int, int]]:
    """Smooth a path using B-spline interpolation.
    
    Args:
        points: List of waypoints
        num_interpolated: Number of points to interpolate between each pair
        
    Returns:
        Smoothed list of coordinates
    """
    if len(points) < 3:
        return points
    
    result = []
    
    for i in range(len(points) - 1):
        p0 = points[max(0, i - 1)]
        p1 = points[i]
        p2 = points[i + 1]
        p3 = points[min(len(points) - 1, i + 2)]
        
        for j in range(num_interpolated):
            t = j / num_interpolated
            t2 = t * t
            t3 = t2 * t
            
            # B-spline basis functions
            b0 = (-t3 + 2 * t2 - t) / 2
            b1 = (3 * t3 - 5 * t2 + 2) / 2
            b2 = (-3 * t3 + 4 * t2 + t) / 2
            b3 = (t3 - t2) / 2
            
            x = (b0 * p0[0] + b1 * p1[0] + b2 * p2[0] + b3 * p3[0]) / 2
            y = (b0 * p0[1] + b1 * p1[1] + b2 * p2[1] + b3 * p3[1]) / 2
            
            result.append((int(x), int(y)))
    
    # Add the last point
    result.append(points[-1])
    
    return result


def add_human_variance(
    points: List[Tuple[int, int]],
    max_offset: float = 3.0,
    seed: Optional[int] = None
) -> List[Tuple[int, int]]:
    """Add human-like variance to a path.
    
    Args:
        points: Original path points
        max_offset: Maximum pixel offset
        seed: Optional random seed
        
    Returns:
        Points with added variance
    """
    if seed is not None:
        random.seed(seed)
    
    result = []
    prev_direction = None
    
    for i, (x, y) in enumerate(points):
        if i == 0 or i == len(points) - 1:
            result.append((x, y))
            continue
        
        # Calculate direction
        next_x, next_y = points[min(i + 1, len(points) - 1)]
        dx = next_x - x
        dy = next_y - y
        length = math.sqrt(dx * dx + dy * dy) or 1
        
        # Perpendicular offset (more natural)
        nx = -dy / length
        ny = dx / length
        
        # Random offset
        offset = (random.random() - 0.5) * 2 * max_offset
        
        # Vary the speed of movement
        speed_factor = 0.8 + random.random() * 0.4
        
        new_x = int(x + nx * offset)
        new_y = int(y + ny * offset)
        
        result.append((new_x, new_y))
    
    return result
