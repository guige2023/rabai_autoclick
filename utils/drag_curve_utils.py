"""
Drag Curve Utilities

Provides curve interpolation and path generation for drag operations
in UI automation workflows.
"""

from typing import List, Tuple, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import math


class CurveType(Enum):
    """Supported curve types for drag paths."""
    LINEAR = "linear"
    EASE_IN = "ease_in"
    EASE_OUT = "ease_out"
    EASE_IN_OUT = "ease_in_out"
    BEZIER = "bezier"
    CATMULL_ROM = "catmull_rom"
    B_SPLINE = "b_spline"


@dataclass(frozen=True)
class Point:
    """2D point representation."""
    x: float
    y: float
    
    def __add__(self, other: "Point") -> "Point":
        return Point(self.x + other.x, self.y + other.y)
    
    def __sub__(self, other: "Point") -> "Point":
        return Point(self.x - other.x, self.y - other.y)
    
    def __mul__(self, scalar: float) -> "Point":
        return Point(self.x * scalar, self.y * scalar)
    
    def distance_to(self, other: "Point") -> float:
        """Calculate Euclidean distance to another point."""
        dx = self.x - other.x
        dy = self.y - other.y
        return math.sqrt(dx * dx + dy * dy)


@dataclass
class DragSegment:
    """A segment of a drag path with curve information."""
    start: Point
    end: Point
    control_points: List[Point]
    curve_type: CurveType
    
    @property
    def length(self) -> float:
        """Approximate length of the segment."""
        return self.start.distance_to(self.end)


class DragCurveInterpolator:
    """
    Interpolates smooth curves through a series of points for drag paths.
    
    Supports multiple curve types including linear, ease curves, bezier,
    Catmull-Rom splines, and B-splines for natural-looking drag gestures.
    """
    
    def __init__(self, curve_type: CurveType = CurveType.EASE_IN_OUT) -> None:
        """
        Initialize interpolator with curve type.
        
        Args:
            curve_type: Type of curve to generate.
        """
        self._curve_type = curve_type
    
    def interpolate(
        self,
        points: List[Point],
        segments: int = 20
    ) -> List[Point]:
        """
        Interpolate a smooth curve through the given points.
        
        Args:
            points: Control points for the curve.
            segments: Number of interpolation segments per control point pair.
            
        Returns:
            List of interpolated points along the curve.
        """
        if len(points) < 2:
            return points
        
        if self._curve_type == CurveType.LINEAR:
            return self._linear_interpolate(points, segments)
        elif self._curve_type == CurveType.EASE_IN_OUT:
            return self._ease_in_out_interpolate(points, segments)
        elif self._curve_type == CurveType.BEZIER:
            return self._bezier_interpolate(points, segments)
        elif self._curve_type == CurveType.CATMULL_ROM:
            return self._catmull_rom_interpolate(points, segments)
        elif self._curve_type == CurveType.B_SPLINE:
            return self._b_spline_interpolate(points, segments)
        else:
            return self._ease_in_out_interpolate(points, segments)
    
    def _linear_interpolate(
        self,
        points: List[Point],
        segments: int
    ) -> List[Point]:
        """Linear interpolation between points."""
        result: List[Point] = []
        for i in range(len(points) - 1):
            for j in range(segments):
                t = j / segments
                x = points[i].x + (points[i + 1].x - points[i].x) * t
                y = points[i].y + (points[i + 1].y - points[i].y) * t
                result.append(Point(x, y))
        result.append(points[-1])
        return result
    
    def _ease_in_out_interpolate(
        self,
        points: List[Point],
        segments: int
    ) -> List[Point]:
        """Ease-in-out interpolation for smooth acceleration."""
        result: List[Point] = []
        for i in range(len(points) - 1):
            for j in range(segments):
                t = j / segments
                eased = self._ease_in_out_curve(t)
                x = points[i].x + (points[i + 1].x - points[i].x) * eased
                y = points[i].y + (points[i + 1].y - points[i].y) * eased
                result.append(Point(x, y))
        result.append(points[-1])
        return result
    
    def _ease_in_out_curve(self, t: float) -> float:
        """Ease-in-out cubic curve."""
        if t < 0.5:
            return 4 * t * t * t
        return 1 - math.pow(-2 * t + 2, 3) / 2
    
    def _bezier_interpolate(
        self,
        points: List[Point],
        segments: int
    ) -> List[Point]:
        """Cubic bezier interpolation."""
        result: List[Point] = []
        for i in range(len(points) - 1):
            if i == 0:
                p0 = points[0]
            else:
                p0 = Point(
                    (points[i - 1].x + points[i].x) / 2,
                    (points[i - 1].y + points[i].y) / 2
                )
            
            p3 = points[i + 1]
            if i + 2 < len(points):
                p3 = Point(
                    (points[i + 1].x + points[i + 2].x) / 2,
                    (points[i + 1].y + points[i + 2].y) / 2
                )
            
            p1 = Point(
                p0.x + (p3.x - p0.x) / 3,
                p0.y + (p3.y - p0.y) / 3
            )
            p2 = Point(
                p0.x + 2 * (p3.x - p0.x) / 3,
                p0.y + 2 * (p3.y - p0.y) / 3
            )
            
            for j in range(segments):
                t = j / segments
                result.append(self._cubic_bezier(p0, p1, p2, p3, t))
        result.append(points[-1])
        return result
    
    def _cubic_bezier(
        self,
        p0: Point,
        p1: Point,
        p2: Point,
        p3: Point,
        t: float
    ) -> Point:
        """Calculate point on cubic bezier curve."""
        mt = 1 - t
        mt2 = mt * mt
        mt3 = mt2 * mt
        t2 = t * t
        t3 = t2 * t
        return Point(
            mt3 * p0.x + 3 * mt2 * t * p1.x + 3 * mt * t2 * p2.x + t3 * p3.x,
            mt3 * p0.y + 3 * mt2 * t * p1.y + 3 * mt * t2 * p2.y + t3 * p3.y
        )
    
    def _catmull_rom_interpolate(
        self,
        points: List[Point],
        segments: int
    ) -> List[Point]:
        """Catmull-Rom spline interpolation."""
        result: List[Point] = []
        for i in range(len(points) - 1):
            p0 = points[max(0, i - 1)]
            p1 = points[i]
            p2 = points[min(len(points) - 1, i + 1)]
            p3 = points[min(len(points) - 1, i + 2)]
            
            for j in range(segments):
                t = j / segments
                result.append(self._catmull_rom_point(p0, p1, p2, p3, t))
        result.append(points[-1])
        return result
    
    def _catmull_rom_point(
        self,
        p0: Point,
        p1: Point,
        p2: Point,
        p3: Point,
        t: float
    ) -> Point:
        """Calculate point on Catmull-Rom spline."""
        t2 = t * t
        t3 = t2 * t
        return Point(
            0.5 * ((2 * p1.x) + (-p0.x + p2.x) * t +
                   (2 * p0.x - 5 * p1.x + 4 * p2.x - p3.x) * t2 +
                   (-p0.x + 3 * p1.x - 3 * p2.x + p3.x) * t3),
            0.5 * ((2 * p1.y) + (-p0.y + p2.y) * t +
                   (2 * p0.y - 5 * p1.y + 4 * p2.y - p3.y) * t2 +
                   (-p0.y + 3 * p1.y - 3 * p2.y + p3.y) * t3)
        )
    
    def _b_spline_interpolate(
        self,
        points: List[Point],
        segments: int
    ) -> List[Point]:
        """B-spline interpolation."""
        result: List[Point] = []
        for i in range(len(points) - 3):
            p0, p1, p2, p3 = points[i], points[i + 1], points[i + 2], points[i + 3]
            for j in range(segments):
                t = j / segments
                result.append(self._b_spline_point(p0, p1, p2, p3, t))
        if len(points) >= 4:
            result.append(points[-1])
        else:
            result.extend(points)
        return result
    
    def _b_spline_point(
        self,
        p0: Point,
        p1: Point,
        p2: Point,
        p3: Point,
        t: float
    ) -> Point:
        """Calculate point on B-spline."""
        t2 = t * t
        t3 = t2 * t
        b0 = (-t3 + 3 * t2 - 3 * t + 1) / 6
        b1 = (3 * t3 - 6 * t2 + 4) / 6
        b2 = (-3 * t3 + 3 * t2 + 3 * t + 1) / 6
        b3 = t3 / 6
        return Point(
            b0 * p0.x + b1 * p1.x + b2 * p2.x + b3 * p3.x,
            b0 * p0.y + b1 * p1.y + b2 * p2.y + b3 * p3.y
        )


def create_drag_path(
    start: Tuple[float, float],
    end: Tuple[float, float],
    curve_type: CurveType = CurveType.EASE_IN_OUT,
    num_points: int = 20
) -> List[Tuple[float, float]]:
    """
    Create a smooth drag path between two points.
    
    Args:
        start: Starting coordinates (x, y).
        end: Ending coordinates (x, y).
        curve_type: Type of curve to generate.
        num_points: Number of points in the output path.
        
    Returns:
        List of (x, y) coordinate tuples representing the drag path.
    """
    points = [Point(start[0], start[1]), Point(end[0], end[1])]
    interpolator = DragCurveInterpolator(curve_type)
    interpolated = interpolator.interpolate(points, num_points)
    return [(p.x, p.y) for p in interpolated]


def create_curved_drag_path(
    waypoints: List[Tuple[float, float]],
    curve_type: CurveType = CurveType.CATMULL_ROM,
    segments_per_waypoint: int = 15
) -> List[Tuple[float, float]]:
    """
    Create a smooth curved drag path through multiple waypoints.
    
    Args:
        waypoints: List of (x, y) coordinate tuples defining the path.
        curve_type: Type of curve to use for smoothing.
        segments_per_waypoint: Number of interpolated points per waypoint.
        
    Returns:
        List of (x, y) coordinate tuples representing the drag path.
    """
    if len(waypoints) < 2:
        return waypoints
    
    points = [Point(w[0], w[1]) for w in waypoints]
    interpolator = DragCurveInterpolator(curve_type)
    interpolated = interpolator.interpolate(points, segments_per_waypoint)
    return [(p.x, p.y) for p in interpolated]


def calculate_path_length(points: List[Tuple[float, float]]) -> float:
    """
    Calculate total length of a path.
    
    Args:
        points: List of (x, y) coordinate tuples.
        
    Returns:
        Total path length in the same units as coordinates.
    """
    if len(points) < 2:
        return 0.0
    
    total = 0.0
    for i in range(len(points) - 1):
        dx = points[i + 1][0] - points[i][0]
        dy = points[i + 1][1] - points[i][1]
        total += math.sqrt(dx * dx + dy * dy)
    return total


def resample_path(
    points: List[Tuple[float, float]],
    num_points: int
) -> List[Tuple[float, float]]:
    """
    Resample a path to have exactly the specified number of points.
    
    Args:
        points: Original path as list of (x, y) tuples.
        num_points: Desired number of output points.
        
    Returns:
        Resampled path with exactly num_points.
    """
    if len(points) == num_points:
        return points
    
    if len(points) < 2 or num_points < 2:
        return points
    
    total_length = calculate_path_length(points)
    segment_length = total_length / (num_points - 1)
    
    result: List[Tuple[float, float]] = [points[0]]
    accumulated = 0.0
    current_segment = 0
    
    for i in range(1, len(points)):
        seg_len = math.sqrt(
            (points[i][0] - points[i - 1][0]) ** 2 +
            (points[i][1] - points[i - 1][1]) ** 2
        )
        accumulated += seg_len
        
        while accumulated >= segment_length and len(result) < num_points:
            t = (segment_length * len(result) - (accumulated - seg_len)) / seg_len
            x = points[i - 1][0] + (points[i][0] - points[i - 1][0]) * t
            y = points[i - 1][1] + (points[i][1] - points[i - 1][1]) * t
            result.append((x, y))
            accumulated -= segment_length
    
    while len(result) < num_points:
        result.append(points[-1])
    
    return result[:num_points]
