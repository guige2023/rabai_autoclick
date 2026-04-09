"""Spline interpolation utilities for RabAI AutoClick.

Provides:
- Cubic spline interpolation
- B-spline curve generation
- Bezier curve utilities
- Catmull-Rom spline
"""

from typing import Callable, List, Optional, Tuple
from dataclasses import dataclass
import math


@dataclass
class Point2D:
    """2D point."""
    x: float
    y: float


def catmull_rom_spline(
    points: List[Tuple[float, float]],
    num_segments: int = 10,
    tension: float = 0.5,
    closed: bool = False,
) -> List[Tuple[float, float]]:
    """Generate Catmull-Rom spline through control points.

    Args:
        points: List of control (x, y) points.
        num_segments: Interpolation segments between each pair.
        tension: Curve tension (0 = loose, 1 = tight).
        closed: Whether to close the curve.

    Returns:
        List of interpolated (x, y) points.
    """
    if len(points) < 2:
        return points
    if len(points) == 2:
        return points[:]

    result: List[Tuple[float, float]] = []
    pts = points[:]

    if closed:
        pts = [points[-1]] + pts + [points[0], points[1]]

    for i in range(1, len(pts) - 2):
        p0, p1, p2, p3 = pts[i - 1], pts[i], pts[i + 1], pts[i + 2]
        alpha = tension

        for t in range(num_segments):
            s = t / num_segments
            s2 = s * s
            s3 = s2 * s

            x = 0.5 * (
                (2 * p1[0]) +
                (-p0[0] + p2[0]) * s +
                (2 * p0[0] - 5 * p1[0] + 4 * p2[0] - p3[0]) * s2 +
                (-p0[0] + 3 * p1[0] - 3 * p2[0] + p3[0]) * s3
            )
            y = 0.5 * (
                (2 * p1[1]) +
                (-p0[1] + p2[1]) * s +
                (2 * p0[1] - 5 * p1[1] + 4 * p2[1] - p3[1]) * s2 +
                (-p0[1] + 3 * p1[1] - 3 * p2[1] + p3[1]) * s3
            )
            result.append((x, y))

    return result


def cubic_bezier_point(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    t: float,
) -> Tuple[float, float]:
    """Compute point on cubic Bezier curve.

    Args:
        p0: Start point.
        p1: Control point 1.
        p2: Control point 2.
        p3: End point.
        t: Parameter [0, 1].

    Returns:
        (x, y) point on curve.
    """
    t2 = t * t
    t3 = t2 * t
    mt = 1.0 - t
    mt2 = mt * mt
    mt3 = mt2 * mt

    x = mt3 * p0[0] + 3 * mt2 * t * p1[0] + 3 * mt * t2 * p2[0] + t3 * p3[0]
    y = mt3 * p0[1] + 3 * mt2 * t * p1[1] + 3 * mt * t2 * p2[1] + t3 * p3[1]
    return (x, y)


def cubic_bezier_curve(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    num_points: int = 50,
) -> List[Tuple[float, float]]:
    """Generate cubic Bezier curve points.

    Args:
        p0: Start point.
        p1: Control point 1.
        p2: Control point 2.
        p3: End point.
        num_points: Number of points to generate.

    Returns:
        List of (x, y) points on curve.
    """
    return [cubic_bezier_point(p0, p1, p2, p3, i / num_points) for i in range(num_points + 1)]


def cubic_spline_coefficients(
    points: List[Tuple[float, float]],
) -> Tuple[List[float], List[float], List[float], List[float]]:
    """Compute cubic spline coefficients for a sequence of points.

    Args:
        points: List of (x, y) points (x must be sorted).

    Returns:
        Tuple of (a, b, c, d) coefficient lists for each segment.
    """
    n = len(points) - 1
    if n < 1:
        return [], [], [], []

    xs = [p[0] for p in points]
    ys = [p[1] for p in points]

    h: List[float] = [xs[i + 1] - xs[i] for i in range(n)]
    alpha: List[float] = [0.0] * (n + 1)
    for i in range(1, n):
        alpha[i] = (3 / h[i]) * (ys[i + 1] - ys[i]) - (3 / h[i - 1]) * (ys[i] - ys[i - 1])

    l: List[float] = [1.0] * (n + 1)
    mu: List[float] = [0.0] * (n + 1)
    z: List[float] = [0.0] * (n + 1)

    for i in range(1, n):
        l[i] = 2 * (xs[i + 1] - xs[i - 1]) - h[i - 1] * mu[i - 1]
        mu[i] = h[i] / l[i] if l[i] != 0 else 0
        z[i] = (alpha[i] - h[i - 1] * z[i - 1]) / l[i] if l[i] != 0 else 0

    b: List[float] = [0.0] * n
    c: List[float] = [0.0] * (n + 1)
    d: List[float] = [0.0] * n

    c[n] = 0.0
    for i in range(n - 1, -1, -1):
        c[i] = z[i] - mu[i] * c[i + 1] if l[i] != 0 else 0
        b[i] = (ys[i + 1] - ys[i]) / h[i] - h[i] * (c[i + 1] + 2 * c[i]) / 3 if h[i] != 0 else 0
        d[i] = (c[i + 1] - c[i]) / (3 * h[i]) if h[i] != 0 else 0

    a = ys[:-1]
    return a, b, c[:n], d


def cubic_spline_interpolate(
    points: List[Tuple[float, float]],
    num_segments: int = 20,
) -> List[Tuple[float, float]]:
    """Interpolate points using cubic spline.

    Args:
        points: Control (x, y) points.
        num_segments: Points to generate between each pair.

    Returns:
        Interpolated (x, y) points.
    """
    if len(points) < 2:
        return points

    sorted_pts = sorted(points, key=lambda p: p[0])
    a, b, c, d = cubic_spline_coefficients(sorted_pts)

    result: List[Tuple[float, float]] = []
    for i in range(len(a)):
        xs = sorted_pts[i][0]
        xe = sorted_pts[i + 1][0]
        dx = (xe - xs) / num_segments
        for j in range(num_segments):
            x = xs + j * dx
            dxj = x - xs
            y = a[i] + b[i] * dxj + c[i] * dxj * dxj + d[i] * dxj * dxj * dxj
            result.append((x, y))
    result.append(sorted_pts[-1])
    return result


def b_spline_basis(i: int, k: int, t: float, knots: List[float]) -> float:
    """Compute B-spline basis function."""
    if k == 0:
        return 1.0 if knots[i] <= t < knots[i + 1] else 0.0
    denom1 = knots[i + k] - knots[i] if knots[i + k] != knots[i] else 0
    denom2 = knots[i + k + 1] - knots[i + 1] if knots[i + k + 1] != knots[i + 1] else 0
    v1 = ((t - knots[i]) / denom1 * b_spline_basis(i, k - 1, t, knots)) if denom1 != 0 else 0.0
    v2 = ((knots[i + k + 1] - t) / denom2 * b_spline_basis(i + 1, k - 1, t, knots)) if denom2 != 0 else 0.0
    return v1 + v2


def b_spline_curve(
    control_points: List[Tuple[float, float]],
    degree: int = 3,
    num_points: int = 100,
) -> List[Tuple[float, float]]:
    """Generate B-spline curve.

    Args:
        control_points: Control polygon points.
        degree: Spline degree.
        num_points: Output points count.

    Returns:
        Curve points.
    """
    n = len(control_points) - 1
    k = min(degree, n)
    m = n + k + 1
    knots = [float(i) / (m - 1) for i in range(m)]

    result: List[Tuple[float, float]] = []
    t_range = knots[k], knots[n + 1]
    for i in range(num_points + 1):
        t = t_range[0] + (t_range[1] - t_range[0]) * i / num_points
        x = 0.0
        y = 0.0
        for j in range(n + 1):
            basis = b_spline_basis(j, k, t, knots)
            x += control_points[j][0] * basis
            y += control_points[j][1] * basis
        result.append((x, y))
    return result


def resample_path(
    points: List[Tuple[float, float]],
    num_points: int,
) -> List[Tuple[float, float]]:
    """Resample a path to a fixed number of evenly-spaced points.

    Args:
        points: Original path.
        num_points: Desired output count.

    Returns:
        Resampled path.
    """
    if len(points) < 2 or num_points < 2:
        return points[:]

    # Compute cumulative arc lengths
    dists = [0.0]
    for i in range(1, len(points)):
        dx = points[i][0] - points[i - 1][0]
        dy = points[i][1] - points[i - 1][1]
        dists.append(dists[-1] + math.sqrt(dx * dx + dy * dy))

    total = dists[-1]
    if total == 0:
        return [points[0]] * num_points

    result: List[Tuple[float, float]] = []
    for i in range(num_points):
        target = total * i / (num_points - 1)
        # Binary search for segment
        lo, hi = 0, len(dists) - 1
        while lo < hi - 1:
            mid = (lo + hi) // 2
            if dists[mid] <= target:
                lo = mid
            else:
                hi = mid
        seg_len = dists[hi] - dists[lo]
        if seg_len > 0:
            frac = (target - dists[lo]) / seg_len
        else:
            frac = 0.0
        x = points[lo][0] + frac * (points[hi][0] - points[lo][0])
        y = points[lo][1] + frac * (points[hi][1] - points[lo][1])
        result.append((x, y))

    return result
