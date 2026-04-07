"""
Geometry utilities for 2D and 3D geometric calculations.

This module provides comprehensive geometric operations including:
- Point and vector operations
- Line and line segment calculations
- Circle and ellipse geometry
- Polygon operations (area, perimeter, containment)
- Distance calculations
- Angle and rotation operations
- Bounding box calculations

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple, Union


@dataclass
class Point:
    """
    A 2D point in Cartesian coordinates.
    
    Attributes:
        x: The x-coordinate.
        y: The y-coordinate.
    
    Example:
        >>> p1 = Point(3, 4)
        >>> p2 = Point(6, 8)
        >>> p1.distance_to(p2)
        5.0
    """
    x: float = 0.0
    y: float = 0.0
    
    def distance_to(self, other: Point) -> float:
        """
        Calculate the Euclidean distance to another point.
        
        Args:
            other: The target point.
            
        Returns:
            The distance between the two points.
        """
        return math.sqrt((self.x - other.x) ** 2 + (self.y - other.y) ** 2)
    
    def midpoint_to(self, other: Point) -> Point:
        """
        Calculate the midpoint between this point and another.
        
        Args:
            other: The target point.
            
        Returns:
            A new Point at the midpoint.
        """
        return Point((self.x + other.x) / 2, (self.y + other.y) / 2)
    
    def translate(self, dx: float, dy: float) -> Point:
        """
        Translate the point by a vector.
        
        Args:
            dx: Change in x.
            dy: Change in y.
            
        Returns:
            A new translated Point.
        """
        return Point(self.x + dx, self.y + dy)
    
    def scale(self, factor: float, origin: Optional[Point] = None) -> Point:
        """
        Scale the point relative to an origin.
        
        Args:
            factor: Scaling factor.
            origin: Origin of scaling. Defaults to (0, 0).
            
        Returns:
            A new scaled Point.
        """
        if origin is None:
            origin = Point(0, 0)
        
        return Point(
            origin.x + (self.x - origin.x) * factor,
            origin.y + (self.y - origin.y) * factor
        )
    
    def rotate(self, angle: float, origin: Optional[Point] = None) -> Point:
        """
        Rotate the point around an origin by an angle in radians.
        
        Args:
            angle: Rotation angle in radians.
            origin: Origin of rotation. Defaults to (0, 0).
            
        Returns:
            A new rotated Point.
        """
        if origin is None:
            origin = Point(0, 0)
        
        cos_a = math.cos(angle)
        sin_a = math.sin(angle)
        
        dx = self.x - origin.x
        dy = self.y - origin.y
        
        return Point(
            origin.x + dx * cos_a - dy * sin_a,
            origin.y + dx * sin_a + dy * cos_a
        )
    
    def dot(self, other: Point) -> float:
        """
        Calculate the dot product with another point (treated as a vector).
        
        Args:
            other: The other point.
            
        Returns:
            The dot product.
        """
        return self.x * other.x + self.y * other.y
    
    def cross(self, other: Point) -> float:
        """
        Calculate the 2D cross product (z-component of 3D cross product).
        
        Args:
            other: The other point.
            
        Returns:
            The cross product (scalar in 2D).
        """
        return self.x * other.y - self.y * other.x
    
    def magnitude(self) -> float:
        """
        Calculate the magnitude (length) of the point as a vector.
        
        Returns:
            The magnitude from origin to this point.
        """
        return math.sqrt(self.x ** 2 + self.y ** 2)
    
    def normalize(self) -> Point:
        """
        Normalize the point as a unit vector from origin.
        
        Returns:
            A new Point representing the unit vector.
        """
        mag = self.magnitude()
        if mag == 0:
            return Point(0, 0)
        return Point(self.x / mag, self.y / mag)
    
    def angle_to(self, other: Point) -> float:
        """
        Calculate the angle from this point to another (in radians).
        
        Args:
            other: The target point.
            
        Returns:
            The angle in radians (-pi to pi).
        """
        return math.atan2(other.y - self.y, other.x - self.x)
    
    def __add__(self, other: Point) -> Point:
        """Add two points (vector addition)."""
        return Point(self.x + other.x, self.y + other.y)
    
    def __sub__(self, other: Point) -> Point:
        """Subtract two points (vector subtraction)."""
        return Point(self.x - other.x, self.y - other.y)
    
    def __mul__(self, scalar: float) -> Point:
        """Multiply point by a scalar."""
        return Point(self.x * scalar, self.y * scalar)
    
    def __repr__(self) -> str:
        return f"Point({self.x:.2f}, {self.y:.2f})"


@dataclass
class Line:
    """
    An infinite line defined by a point and a direction vector.
    
    Attributes:
        point: A point on the line.
        direction: The direction vector of the line (does not need to be normalized).
    
    Example:
        >>> line = Line(Point(0, 0), Point(1, 1))
        >>> line.y_at_x(5)
        5.0
    """
    point: Point = field(default_factory=Point)
    direction: Point = field(default_factory=lambda: Point(1, 0))
    
    def y_at_x(self, x: float) -> Optional[float]:
        """
        Get the y-coordinate of the line at a given x-coordinate.
        
        Args:
            x: The x-coordinate.
            
        Returns:
            The y-coordinate, or None if the line is vertical.
        """
        if abs(self.direction.x) < 1e-10:
            return None
        
        t = (x - self.point.x) / self.direction.x
        return self.point.y + t * self.direction.y
    
    def x_at_y(self, y: float) -> Optional[float]:
        """
        Get the x-coordinate of the line at a given y-coordinate.
        
        Args:
            y: The y-coordinate.
            
        Returns:
            The x-coordinate, or None if the line is horizontal.
        """
        if abs(self.direction.y) < 1e-10:
            return None
        
        t = (y - self.point.y) / self.direction.y
        return self.point.x + t * self.direction.x
    
    def distance_to_point(self, p: Point) -> float:
        """
        Calculate the perpendicular distance from a point to the line.
        
        Args:
            p: The point.
            
        Returns:
            The shortest distance from the point to the line.
        """
        line_vec = self.direction
        point_vec = Point(p.x - self.point.x, p.y - self.point.y)
        
        cross = line_vec.x * point_vec.y - line_vec.y * point_vec.x
        return abs(cross) / line_vec.magnitude()
    
    def project_point(self, p: Point) -> Point:
        """
        Project a point onto the line (find the closest point on the line).
        
        Args:
            p: The point to project.
            
        Returns:
            The point on the line closest to p.
        """
        line_vec = self.direction.normalize()
        point_vec = p - self.point
        
        dot = point_vec.dot(line_vec)
        return self.point + line_vec * dot
    
    def intersection_with(self, other: Line) -> Optional[Point]:
        """
        Find the intersection point with another line.
        
        Args:
            other: The other line.
            
        Returns:
            The intersection point, or None if lines are parallel.
        """
        x1, y1 = self.point.x, self.point.y
        x2, y2 = x1 + self.direction.x, y1 + self.direction.y
        x3, y3 = other.point.x, other.point.y
        x4, y4 = x3 + other.direction.x, y3 + other.direction.y
        
        denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
        
        if abs(denom) < 1e-10:
            return None
        
        t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
        u = -((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3)) / denom
        
        if 0 <= t <= 1 and 0 <= u <= 1:
            return Point(x1 + t * (x2 - x1), y1 + t * (y2 - y1))
        
        return Point(x1 + t * (x2 - x1), y1 + t * (y2 - y1))


@dataclass
class Circle:
    """
    A circle defined by a center point and radius.
    
    Attributes:
        center: The center point of the circle.
        radius: The radius of the circle (must be positive).
    
    Example:
        >>> circle = Circle(Point(0, 0), 5)
        >>> circle.area
        78.53981633974483
    """
    center: Point = field(default_factory=Point)
    radius: float = 1.0
    
    def __post_init__(self) -> None:
        if self.radius <= 0:
            raise ValueError(f"Radius must be positive, got {self.radius}")
    
    @property
    def area(self) -> float:
        """Calculate the area of the circle."""
        return math.pi * self.radius ** 2
    
    @property
    def circumference(self) -> float:
        """Calculate the circumference of the circle."""
        return 2 * math.pi * self.radius
    
    def contains_point(self, p: Point) -> bool:
        """
        Check if a point is inside the circle.
        
        Args:
            p: The point to check.
            
        Returns:
            True if the point is inside or on the circle.
        """
        return self.center.distance_to(p) <= self.radius
    
    def distance_to_point(self, p: Point) -> float:
        """
        Calculate the distance from a point to the circle's boundary.
        
        Args:
            p: The point.
            
        Returns:
            The distance (negative if inside the circle).
        """
        return self.center.distance_to(p) - self.radius
    
    def intersection_with_line(self, line: Line) -> List[Point]:
        """
        Find intersection points between the circle and a line.
        
        Args:
            line: The line to intersect with.
            
        Returns:
            List of intersection points (0, 1, or 2).
        """
        proj = line.project_point(self.center)
        dist = self.center.distance_to(proj)
        
        if dist > self.radius:
            return []
        
        if abs(dist - self.radius) < 1e-10:
            return [proj]
        
        offset = math.sqrt(self.radius ** 2 - dist ** 2)
        direction = line.direction.normalize()
        
        return [
            Point(proj.x - offset * direction.x, proj.y - offset * direction.y),
            Point(proj.x + offset * direction.x, proj.y + offset * direction.y)
        ]


@dataclass
class Rectangle:
    """
    An axis-aligned rectangle defined by its bounds.
    
    Attributes:
        min_x: Minimum x-coordinate (left edge).
        min_y: Minimum y-coordinate (bottom edge).
        max_x: Maximum x-coordinate (right edge).
        max_y: Maximum y-coordinate (top edge).
    
    Example:
        >>> rect = Rectangle(0, 0, 10, 5)
        >>> rect.area
        50
    """
    min_x: float = 0.0
    min_y: float = 0.0
    max_x: float = 0.0
    max_y: float = 0.0
    
    def __post_init__(self) -> None:
        if self.max_x < self.min_x or self.max_y < self.min_y:
            raise ValueError("Invalid rectangle bounds")
    
    @property
    def width(self) -> float:
        """Return the width of the rectangle."""
        return self.max_x - self.min_x
    
    @property
    def height(self) -> float:
        """Return the height of the rectangle."""
        return self.max_y - self.min_y
    
    @property
    def area(self) -> float:
        """Return the area of the rectangle."""
        return self.width * self.height
    
    @property
    def center(self) -> Point:
        """Return the center point of the rectangle."""
        return Point((self.min_x + self.max_x) / 2, (self.min_y + self.max_y) / 2)
    
    def contains_point(self, p: Point) -> bool:
        """Check if a point is inside the rectangle."""
        return self.min_x <= p.x <= self.max_x and self.min_y <= p.y <= self.max_y
    
    def intersects(self, other: Rectangle) -> bool:
        """Check if this rectangle intersects another rectangle."""
        return not (
            self.max_x < other.min_x or other.max_x < self.min_x or
            self.max_y < other.min_y or other.max_y < self.min_y
        )
    
    def intersection(self, other: Rectangle) -> Optional[Rectangle]:
        """Get the intersection rectangle with another rectangle."""
        if not self.intersects(other):
            return None
        
        return Rectangle(
            max(self.min_x, other.min_x),
            max(self.min_y, other.min_y),
            min(self.max_x, other.max_x),
            min(self.max_y, other.max_y)
        )
    
    def union(self, other: Rectangle) -> Rectangle:
        """Get the smallest rectangle that contains both rectangles."""
        return Rectangle(
            min(self.min_x, other.min_x),
            min(self.min_y, other.min_y),
            max(self.max_x, other.max_x),
            max(self.max_y, other.max_y)
        )
    
    def expand(self, amount: float) -> Rectangle:
        """Expand the rectangle by a given amount in all directions."""
        return Rectangle(
            self.min_x - amount,
            self.min_y - amount,
            self.max_x + amount,
            self.max_y + amount
        )


@dataclass
class Polygon:
    """
    A polygon defined by a list of vertices.
    
    Attributes:
        vertices: List of points defining the polygon boundary.
    
    Example:
        >>> poly = Polygon([Point(0,0), Point(4,0), Point(4,3), Point(0,3)])
        >>> poly.area
        12.0
    """
    vertices: List[Point] = field(default_factory=list)
    
    @property
    def area(self) -> float:
        """
        Calculate the area of the polygon using the Shoelace formula.
        
        Returns:
            The signed area (positive for counter-clockwise, negative for clockwise).
        """
        n = len(self.vertices)
        if n < 3:
            return 0.0
        
        area = 0.0
        for i in range(n):
            j = (i + 1) % n
            area += self.vertices[i].x * self.vertices[j].y
            area -= self.vertices[j].x * self.vertices[i].y
        
        return area / 2.0
    
    @property
    def perimeter(self) -> float:
        """Calculate the perimeter of the polygon."""
        n = len(self.vertices)
        if n < 2:
            return 0.0
        
        perimeter = 0.0
        for i in range(n):
            j = (i + 1) % n
            perimeter += self.vertices[i].distance_to(self.vertices[j])
        
        return perimeter
    
    def contains_point(self, p: Point) -> bool:
        """
        Check if a point is inside the polygon using ray casting.
        
        Args:
            p: The point to check.
            
        Returns:
            True if the point is inside the polygon.
        """
        n = len(self.vertices)
        inside = False
        
        for i in range(n):
            j = (i + 1) % n
            
            if ((self.vertices[i].y > p.y) != (self.vertices[j].y > p.y) and
                p.x < (self.vertices[j].x - self.vertices[i].x) * (p.y - self.vertices[i].y) /
                       (self.vertices[j].y - self.vertices[i].y) + self.vertices[i].x):
                inside = not inside
        
        return inside
    
    def centroid(self) -> Point:
        """
        Calculate the centroid (center of mass) of the polygon.
        
        Returns:
            The centroid point.
        """
        n = len(self.vertices)
        if n == 0:
            return Point(0, 0)
        if n == 1:
            return self.vertices[0]
        if n == 2:
            return self.vertices[0].midpoint_to(self.vertices[1])
        
        signed_area = self.area
        cx, cy = 0.0, 0.0
        
        for i in range(n):
            j = (i + 1) % n
            factor = (
                self.vertices[i].x * self.vertices[j].y -
                self.vertices[j].x * self.vertices[i].y
            )
            cx += (self.vertices[i].x + self.vertices[j].x) * factor
            cy += (self.vertices[i].y + self.vertices[j].y) * factor
        
        cx /= (6.0 * signed_area)
        cy /= (6.0 * signed_area)
        
        return Point(cx, cy)


def angle_between_vectors(v1: Point, v2: Point) -> float:
    """
    Calculate the angle between two vectors in radians.
    
    Args:
        v1: First vector (from origin).
        v2: Second vector (from origin).
        
    Returns:
        The angle in radians (0 to pi).
    """
    dot = v1.dot(v2)
    mag_product = v1.magnitude() * v2.magnitude()
    
    if mag_product == 0:
        return 0.0
    
    cos_angle = max(-1.0, min(1.0, dot / mag_product))
    return math.acos(cos_angle)


def rotate_point_around_origin(p: Point, angle: float) -> Point:
    """
    Rotate a point around the origin by an angle in radians.
    
    Args:
        p: The point to rotate.
        angle: Rotation angle in radians.
        
    Returns:
        The rotated point.
    """
    cos_a = math.cos(angle)
    sin_a = math.sin(angle)
    
    return Point(p.x * cos_a - p.y * sin_a, p.x * sin_a + p.y * cos_a)


def line_segment_length(p1: Point, p2: Point) -> float:
    """
    Calculate the length of a line segment between two points.
    
    Args:
        p1: First endpoint.
        p2: Second endpoint.
        
    Returns:
        The length of the segment.
    """
    return p1.distance_to(p2)


def point_on_segment(p1: Point, p2: Point, t: float) -> Point:
    """
    Get a point on a line segment at parameter t.
    
    Args:
        p1: First endpoint.
        p2: Second endpoint.
        t: Parameter from 0 (p1) to 1 (p2).
        
    Returns:
        The point at parameter t.
    """
    return Point(p1.x + t * (p2.x - p1.x), p1.y + t * (p2.y - p1.y))


def closest_point_on_segment(p: Point, seg_start: Point, seg_end: Point) -> Point:
    """
    Find the closest point on a line segment to a given point.
    
    Args:
        p: The point to find the closest point to.
        seg_start: Start of the segment.
        seg_end: End of the segment.
        
    Returns:
        The closest point on the segment.
    """
    dx = seg_end.x - seg_start.x
    dy = seg_end.y - seg_start.y
    
    length_sq = dx * dx + dy * dy
    
    if length_sq < 1e-10:
        return seg_start
    
    t = max(0, min(1, ((p.x - seg_start.x) * dx + (p.y - seg_start.y) * dy) / length_sq))
    
    return Point(seg_start.x + t * dx, seg_start.y + t * dy)


def triangle_area(p1: Point, p2: Point, p3: Point) -> float:
    """
    Calculate the area of a triangle formed by three points.
    
    Uses the cross product method: area = |AB x AC| / 2
    
    Args:
        p1, p2, p3: The three vertices of the triangle.
        
    Returns:
        The area of the triangle.
    """
    v1 = p2 - p1
    v2 = p3 - p1
    
    return abs(v1.cross(v2)) / 2.0


def convex_hull(points: List[Point]) -> List[Point]:
    """
    Compute the convex hull of a set of points using Graham scan.
    
    Args:
        points: List of points.
        
    Returns:
        List of points forming the convex hull in order.
    """
    if len(points) < 3:
        return points.copy()
    
    sorted_points = sorted(set(points), key=lambda p: (p.x, p.y))
    
    def cross(o: Point, a: Point, b: Point) -> float:
        return (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x)
    
    lower = []
    for p in sorted_points:
        while len(lower) >= 2 and cross(lower[-2], lower[-1], p) <= 0:
            lower.pop()
        lower.append(p)
    
    upper = []
    for p in reversed(sorted_points):
        while len(upper) >= 2 and cross(upper[-2], upper[-1], p) <= 0:
            upper.pop()
        upper.append(p)
    
    return lower[:-1] + upper[:-1]


def bounding_box(points: List[Point]) -> Rectangle:
    """
    Calculate the bounding rectangle for a set of points.
    
    Args:
        points: List of points.
        
    Returns:
        A Rectangle that bounds all points.
    """
    if not points:
        return Rectangle(0, 0, 0, 0)
    
    xs = [p.x for p in points]
    ys = [p.y for p in points]
    
    return Rectangle(min(xs), min(ys), max(xs), max(ys))


def distance_point_to_line_segment(p: Point, seg_start: Point, seg_end: Point) -> float:
    """
    Calculate the minimum distance from a point to a line segment.
    
    Args:
        p: The point.
        seg_start: Start of the segment.
        seg_end: End of the segment.
        
    Returns:
        The minimum distance from p to the segment.
    """
    closest = closest_point_on_segment(p, seg_start, seg_end)
    return p.distance_to(closest)


def lerp(a: float, b: float, t: float) -> float:
    """
    Linear interpolation between two values.
    
    Args:
        a: Start value.
        b: End value.
        t: Parameter from 0 to 1.
        
    Returns:
        The interpolated value.
    """
    return a + (b - a) * t


def lerp_point(a: Point, b: Point, t: float) -> Point:
    """
    Linear interpolation between two points.
    
    Args:
        a: Start point.
        b: End point.
        t: Parameter from 0 to 1.
        
    Returns:
        The interpolated point.
    """
    return Point(lerp(a.x, b.x, t), lerp(a.y, b.y, t))


def catmull_rom_spline(p0: Point, p1: Point, p2: Point, p3: Point, t: float) -> Point:
    """
    Calculate a point on a Catmull-Rom spline.
    
    Args:
        p0, p1, p2, p3: Four control points.
        t: Parameter from 0 to 1.
        
    Returns:
        The interpolated point.
    """
    t2 = t * t
    t3 = t2 * t
    
    x = 0.5 * ((2 * p1.x) +
               (-p0.x + p2.x) * t +
               (2 * p0.x - 5 * p1.x + 4 * p2.x - p3.x) * t2 +
               (-p0.x + 3 * p1.x - 3 * p2.x + p3.x) * t3)
    
    y = 0.5 * ((2 * p1.y) +
               (-p0.y + p2.y) * t +
               (2 * p0.y - 5 * p1.y + 4 * p2.y - p3.y) * t2 +
               (-p0.y + 3 * p1.y - 3 * p2.y + p3.y) * t3)
    
    return Point(x, y)
