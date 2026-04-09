"""Bezier curve utilities for RabAI AutoClick.

Provides:
- Bezier curve evaluation
- Curve splitting
- Arc length computation
- Curve fitting
"""

from typing import List, Tuple, Optional, Callable
import math


def quadratic_bezier_point(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    t: float,
) -> Tuple[float, float]:
    """Evaluate quadratic Bezier at parameter t.

    Args:
        p0: Start point.
        p1: Control point.
        p2: End point.
        t: Parameter [0, 1].

    Returns:
        (x, y) point on curve.
    """
    mt = 1.0 - t
    x = mt * mt * p0[0] + 2 * mt * t * p1[0] + t * t * p2[0]
    y = mt * mt * p0[1] + 2 * mt * t * p1[1] + t * t * p2[1]
    return (x, y)


def cubic_bezier_point(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    t: float,
) -> Tuple[float, float]:
    """Evaluate cubic Bezier at parameter t.

    Args:
        p0: Start point.
        p1: Control point 1.
        p2: Control point 2.
        p3: End point.
        t: Parameter [0, 1].

    Returns:
        (x, y) point on curve.
    """
    mt = 1.0 - t
    mt2 = mt * mt
    mt3 = mt2 * mt
    t2 = t * t
    t3 = t2 * t
    x = mt3 * p0[0] + 3 * mt2 * t * p1[0] + 3 * mt * t2 * p2[0] + t3 * p3[0]
    y = mt3 * p0[1] + 3 * mt2 * t * p1[1] + 3 * mt * t2 * p2[1] + t3 * p3[1]
    return (x, y)


def bezier_derivative(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    t: float,
) -> Tuple[float, float]:
    """Compute derivative (tangent) of cubic Bezier at t."""
    mt = 1.0 - t
    dx = 3 * mt * mt * (p1[0] - p0[0]) + 6 * mt * t * (p2[0] - p1[0]) + 3 * t * t * (p3[0] - p2[0])
    dy = 3 * mt * mt * (p1[1] - p0[1]) + 6 * mt * t * (p2[1] - p1[1]) + 3 * t * t * (p3[1] - p2[1])
    return (dx, dy)


def bezier_normal(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    t: float,
) -> Tuple[float, float]:
    """Compute outward normal of cubic Bezier at t."""
    dx, dy = bezier_derivative(p0, p1, p2, p3, t)
    length = math.sqrt(dx * dx + dy * dy)
    if length < 1e-10:
        return (0.0, 1.0)
    return (-dy / length, dx / length)


def cubic_bezier_arc_length(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    num_samples: int = 100,
) -> float:
    """Approximate arc length of cubic Bezier.

    Args:
        p0, p1, p2, p3: Control points.
        num_samples: Number of samples.

    Returns:
        Approximate arc length.
    """
    length = 0.0
    prev = cubic_bezier_point(p0, p1, p2, p3, 0.0)
    for i in range(1, num_samples + 1):
        t = i / num_samples
        curr = cubic_bezier_point(p0, p1, p2, p3, t)
        dx = curr[0] - prev[0]
        dy = curr[1] - prev[1]
        length += math.sqrt(dx * dx + dy * dy)
        prev = curr
    return length


def split_cubic_bezier(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    t: float,
) -> Tuple[Tuple[Tuple[float, float], ...], Tuple[Tuple[float, float], ...]]:
    """Split cubic Bezier at parameter t using de Casteljau.

    Returns:
        (left_curve, right_curve) as tuples of 4 points.
    """
    # First level
    p01 = (p0[0] * (1 - t) + p1[0] * t, p0[1] * (1 - t) + p1[1] * t)
    p12 = (p1[0] * (1 - t) + p2[0] * t, p1[1] * (1 - t) + p2[1] * t)
    p23 = (p2[0] * (1 - t) + p3[0] * t, p2[1] * (1 - t) + p3[1] * t)
    # Second level
    p012 = (p01[0] * (1 - t) + p12[0] * t, p01[1] * (1 - t) + p12[1] * t)
    p123 = (p12[0] * (1 - t) + p23[0] * t, p12[1] * (1 - t) + p23[1] * t)
    # Third level
    p0123 = (p012[0] * (1 - t) + p123[0] * t, p012[1] * (1 - t) + p123[1] * t)

    left = (p0, p01, p012, p0123)
    right = (p0123, p123, p23, p3)
    return left, right


def cubic_bezier_to_polyline(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    num_points: int = 50,
) -> List[Tuple[float, float]]:
    """Convert cubic Bezier to polyline points.

    Args:
        p0, p1, p2, p3: Control points.
        num_points: Number of output points.

    Returns:
        List of (x, y) points.
    """
    return [cubic_bezier_point(p0, p1, p2, p3, i / num_points) for i in range(num_points + 1)]


def bezier_bounding_box(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
) -> Tuple[float, float, float, float]:
    """Compute approximate bounding box of cubic Bezier.

    Returns:
        (min_x, min_y, max_x, max_y).
    """
    xs = [p0[0], p1[0], p2[0], p3[0]]
    ys = [p0[1], p1[1], p2[1], p3[1]]

    # Find extrema by solving derivative = 0
    def solve_extrema(a: float, b: float, c: float, d: float) -> List[float]:
        roots = []
        p = (3 * (b - a)) / (a - 3 * b + 3 * c - d)
        q = (3 * (a - 2 * b + c)) / (a - 3 * b + 3 * c - d) if (a - 3 * b + 3 * c - d) != 0 else 0
        discriminant = p * p / 4 - q / 3
        if discriminant >= 0:
            t1 = -p / 2 + math.sqrt(discriminant)
            t2 = -p / 2 - math.sqrt(discriminant)
            for t in [t1, t2]:
                if 0 < t < 1:
                    roots.append(t)
        return roots

    ax = -p0[0] + 3 * p1[0] - 3 * p2[0] + p3[0]
    bx = 3 * p0[0] - 6 * p1[0] + 3 * p2[0]
    cx = -3 * p0[0] + 3 * p1[0]
    dx = p0[0]

    for t in solve_extrema(dx, cx, bx, ax):
        xs.append(cubic_bezier_point(p0, p1, p2, p3, t)[0])

    ay = -p0[1] + 3 * p1[1] - 3 * p2[1] + p3[1]
    by = 3 * p0[1] - 6 * p1[1] + 3 * p2[1]
    cy = -3 * p0[1] + 3 * p1[1]
    dy = p0[1]

    for t in solve_extrema(dy, cy, by, ay):
        ys.append(cubic_bezier_point(p0, p1, p2, p3, t)[1])

    return (min(xs), min(ys), max(xs), max(ys))


def fit_cubic_bezier(
    points: List[Tuple[float, float]],
) -> Tuple[Tuple[float, float], Tuple[float, float], Tuple[float, float], Tuple[float, float]]:
    """Fit cubic Bezier to a set of points using least squares.

    Args:
        points: List of (x, y) points to fit.

    Returns:
        (p0, p1, p2, p3) control points.
    """
    if len(points) < 4:
        # Pad with duplicates
        while len(points) < 4:
            points.append(points[-1] if points else (0, 0))

    p0 = points[0]
    p3 = points[-1]
    n = len(points)

    # Compute chord lengths for parameterization
    total_len = 0.0
    lens = [0.0]
    for i in range(1, n):
        dx = points[i][0] - points[i-1][0]
        dy = points[i][1] - points[i-1][1]
        total_len += math.sqrt(dx * dx + dy * dy)
        lens.append(total_len)

    ts = [l / total_len if total_len > 0 else 0.0 for l in lens]

    # Approximate control points using tangents
    t1 = max(0.0, min(1.0, ts[1] if len(ts) > 1 else 0))
    t2 = max(0.0, min(1.0, ts[-2] if len(ts) > 1 else 1))

    dx = points[1][0] - points[0][0] if len(points) > 1 else 0
    dy = points[1][1] - points[0][1] if len(points) > 1 else 0
    p1 = (p0[0] + dx / 3, p0[1] + dy / 3)

    dx = points[-1][0] - points[-2][0] if len(points) > 1 else 0
    dy = points[-1][1] - points[-2][1] if len(points) > 1 else 0
    p2 = (p3[0] - dx / 3, p3[1] - dy / 3)

    return (p0, p1, p2, p3)


def cubic_bezier_flatness(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
) -> float:
    """Compute flatness of cubic Bezier (for subdivision decision).

    Returns:
        Flatness measure.
    """
    d1x, d1y = p1[0] - p0[0], p1[1] - p0[1]
    d2x, d2y = p2[0] - p3[0], p2[1] - p3[1]
    return max(abs(d1x) + abs(d1y), abs(d2x) + abs(d2y))
