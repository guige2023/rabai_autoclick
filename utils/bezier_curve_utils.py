"""Bezier curve operations for smooth path generation."""

from typing import List, Tuple, Optional
import numpy as np


Point = Tuple[float, float]


def bezier_point(
    p0: Point,
    p1: Point,
    p2: Point,
    p3: Point,
    t: float
) -> Point:
    """Calculate point on cubic Bezier curve at parameter t.
    
    Args:
        p0, p1, p2, p3: Control points.
        t: Parameter in [0, 1].
    
    Returns:
        (x, y) point on curve.
    """
    t2 = t * t
    t3 = t2 * t
    mt = 1 - t
    mt2 = mt * mt
    mt3 = mt2 * mt
    x = mt3 * p0[0] + 3 * mt2 * t * p1[0] + 3 * mt * t2 * p2[0] + t3 * p3[0]
    y = mt3 * p0[1] + 3 * mt2 * t * p1[1] + 3 * mt * t2 * p2[1] + t3 * p3[1]
    return (x, y)


def quadratic_bezier_point(
    p0: Point,
    p1: Point,
    p2: Point,
    t: float
) -> Point:
    """Calculate point on quadratic Bezier curve.
    
    Args:
        p0, p1, p2: Control points.
        t: Parameter in [0, 1].
    
    Returns:
        (x, y) point on curve.
    """
    mt = 1 - t
    x = mt * mt * p0[0] + 2 * mt * t * p1[0] + t * t * p2[0]
    y = mt * mt * p0[1] + 2 * mt * t * p1[1] + t * t * p2[1]
    return (x, y)


def bezier_curve(
    p0: Point,
    p1: Point,
    p2: Point,
    p3: Point,
    num_points: int = 50
) -> List[Point]:
    """Generate cubic Bezier curve points.
    
    Args:
        p0, p1, p2, p3: Control points.
        num_points: Number of points to generate.
    
    Returns:
        List of (x, y) points on the curve.
    """
    return [bezier_point(p0, p1, p2, p3, t) for t in np.linspace(0, 1, num_points)]


def smooth_path(
    points: List[Point],
    num_interpolated: int = 20
) -> List[Point]:
    """Create smooth curve through points using Catmull-Rom spline.
    
    Args:
        points: List of control points.
        num_interpolated: Points between each control point.
    
    Returns:
        Smooth curve points.
    """
    if len(points) < 2:
        return points
    result = []
    for i in range(len(points) - 1):
        p0 = points[max(0, i - 1)]
        p1 = points[i]
        p2 = points[min(len(points) - 1, i + 1)]
        p3 = points[min(len(points) - 1, i + 2)]
        for t in np.linspace(0, 1, num_interpolated):
            t2 = t * t
            t3 = t2 * t
            x = 0.5 * ((2 * p1[0]) + (-p0[0] + p2[0]) * t +
                       (2 * p0[0] - 5 * p1[0] + 4 * p2[0] - p3[0]) * t2 +
                       (-p0[0] + 3 * p1[0] - 3 * p2[0] + p3[0]) * t3)
            y = 0.5 * ((2 * p1[1]) + (-p0[1] + p2[1]) * t +
                       (2 * p0[1] - 5 * p1[1] + 4 * p2[1] - p3[1]) * t2 +
                       (-p0[1] + 3 * p1[1] - 3 * p2[1] + p3[1]) * t3)
            result.append((x, y))
    result.append(points[-1])
    return result


def bezier_derivative(
    p0: Point,
    p1: Point,
    p2: Point,
    p3: Point,
    t: float
) -> Tuple[float, float]:
    """Calculate first derivative of cubic Bezier at t.
    
    Args:
        p0, p1, p2, p3: Control points.
        t: Parameter in [0, 1].
    
    Returns:
        (dx, dy) tangent vector.
    """
    mt = 1 - t
    mt2 = mt * mt
    t2 = t * t
    dx = 3 * mt2 * (p1[0] - p0[0]) + 6 * mt * t * (p2[0] - p1[0]) + 3 * t2 * (p3[0] - p2[0])
    dy = 3 * mt2 * (p1[1] - p0[1]) + 6 * mt * t * (p2[1] - p1[1]) + 3 * t2 * (p3[1] - p2[1])
    return (dx, dy)


def bezier_length(
    p0: Point,
    p1: Point,
    p2: Point,
    p3: Point,
    num_samples: int = 100
) -> float:
    """Approximate length of cubic Bezier curve.
    
    Args:
        p0, p1, p2, p3: Control points.
        num_samples: Number of samples for approximation.
    
    Returns:
        Approximate curve length.
    """
    points = bezier_curve(p0, p1, p2, p3, num_samples)
    length = 0.0
    for i in range(1, len(points)):
        dx = points[i][0] - points[i-1][0]
        dy = points[i][1] - points[i-1][1]
        length += np.sqrt(dx * dx + dy * dy)
    return length
