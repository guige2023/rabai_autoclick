"""Path smoothing and waypoint navigation utilities for RabAI AutoClick.

Provides:
- Waypoint path generation
- Path smoothing with various algorithms
- Velocity profiling along paths
- Path following controllers
"""

from typing import List, Tuple, Optional, Callable
import math


def generate_waypoints(
    start: Tuple[float, float],
    end: Tuple[float, float],
    num_waypoints: int = 5,
) -> List[Tuple[float, float]]:
    """Generate straight-line waypoints between start and end.

    Args:
        start: Start point (x, y).
        end: End point (x, y).
        num_waypoints: Number of intermediate waypoints.

    Returns:
        List of waypoints including start and end.
    """
    result = [start]
    for i in range(1, num_waypoints + 1):
        t = i / (num_waypoints + 1)
        result.append((
            start[0] + (end[0] - start[0]) * t,
            start[1] + (end[1] - start[1]) * t,
        ))
    result.append(end)
    return result


def smooth_path_chaikins(
    points: List[Tuple[float, float]],
    iterations: int = 3,
) -> List[Tuple[float, float]]:
    """Smooth path using Chaikin's corner-cutting algorithm.

    Args:
        points: Control points.
        iterations: Number of smoothing passes.

    Returns:
        Smoothed path.
    """
    if len(points) < 3:
        return points[:]

    result = points[:]
    for _ in range(iterations):
        smoothed: List[Tuple[float, float]] = [result[0]]
        for i in range(len(result) - 1):
            p0 = result[i]
            p1 = result[i + 1]
            smoothed.append((
                0.75 * p0[0] + 0.25 * p1[0],
                0.75 * p0[1] + 0.25 * p1[1],
            ))
            smoothed.append((
                0.25 * p0[0] + 0.75 * p1[0],
                0.25 * p0[1] + 0.75 * p1[1],
            ))
        smoothed.append(result[-1])
        result = smoothed

    return result


def smooth_path_four_sided(
    points: List[Tuple[float, float]],
    iterations: int = 2,
) -> List[Tuple[float, float]]:
    """Smooth path using 4-point smoothing.

    Args:
        points: Control points.
        iterations: Smoothing iterations.

    Returns:
        Smoothed path.
    """
    if len(points) < 4:
        return points[:]

    result = points[:]
    for _ in range(iterations):
        smoothed: List[Tuple[float, float]] = [result[0], result[1]]
        for i in range(1, len(result) - 2):
            p0 = result[i - 1]
            p1 = result[i]
            p2 = result[i + 1]
            p3 = result[i + 2]
            smoothed.append((
                (-0.0625 * p0[0] + 0.5625 * p1[0] + 0.5625 * p2[0] - 0.0625 * p3[0]),
                (-0.0625 * p0[1] + 0.5625 * p1[1] + 0.5625 * p2[1] - 0.0625 * p3[1]),
            ))
        smoothed.append(result[-2])
        smoothed.append(result[-1])
        result = smoothed

    return result


def path_total_length(points: List[Tuple[float, float]]) -> float:
    """Compute total path length.

    Args:
        points: Path points.

    Returns:
        Total length.
    """
    if len(points) < 2:
        return 0.0
    total = 0.0
    for i in range(len(points) - 1):
        dx = points[i + 1][0] - points[i][0]
        dy = points[i + 1][1] - points[i][1]
        total += math.sqrt(dx * dx + dy * dy)
    return total


def path_segment_lengths(
    points: List[Tuple[float, float]],
) -> List[float]:
    """Compute length of each path segment.

    Args:
        points: Path points.

    Returns:
        List of segment lengths.
    """
    lengths: List[float] = []
    for i in range(len(points) - 1):
        dx = points[i + 1][0] - points[i][0]
        dy = points[i + 1][1] - points[i][1]
        lengths.append(math.sqrt(dx * dx + dy * dy))
    return lengths


def path_cumulative_lengths(
    points: List[Tuple[float, float]],
) -> List[float]:
    """Compute cumulative distances along path.

    Args:
        points: Path points.

    Returns:
        Cumulative distance at each point.
    """
    cum = [0.0]
    for i in range(len(points) - 1):
        dx = points[i + 1][0] - points[i][0]
        dy = points[i + 1][1] - points[i][1]
        cum.append(cum[-1] + math.sqrt(dx * dx + dy * dy))
    return cum


def position_along_path(
    points: List[Tuple[float, float]],
    distance: float,
) -> Tuple[Tuple[float, float], float]:
    """Get position and heading at given distance along path.

    Args:
        points: Path points.
        distance: Distance to travel.

    Returns:
        (position, heading_angle) at that distance.
    """
    if len(points) < 2:
        return points[0] if points else (0.0, 0.0), 0.0

    cum = path_cumulative_lengths(points)
    total = cum[-1]
    distance = max(0.0, min(distance, total))

    for i in range(len(cum) - 1):
        if cum[i + 1] >= distance:
            segment_dist = distance - cum[i]
            seg_len = cum[i + 1] - cum[i]
            if seg_len < 1e-10:
                return points[i], 0.0
            t = segment_dist / seg_len
            x = points[i][0] + (points[i + 1][0] - points[i][0]) * t
            y = points[i][1] + (points[i + 1][1] - points[i][1]) * t
            angle = math.atan2(
                points[i + 1][1] - points[i][1],
                points[i + 1][0] - points[i][0],
            )
            return (x, y), angle

    return points[-1], 0.0


def generate_circular_waypoints(
    center: Tuple[float, float],
    radius: float,
    num_points: int = 20,
    start_angle: float = 0.0,
    end_angle: float = 2 * math.pi,
) -> List[Tuple[float, float]]:
    """Generate waypoints along a circle arc.

    Args:
        center: Circle center.
        radius: Circle radius.
        num_points: Number of waypoints.
        start_angle: Start angle in radians.
        end_angle: End angle in radians.

    Returns:
        Waypoints on the arc.
    """
    result: List[Tuple[float, float]] = []
    for i in range(num_points + 1):
        t = i / num_points
        angle = start_angle + (end_angle - start_angle) * t
        result.append((
            center[0] + radius * math.cos(angle),
            center[1] + radius * math.sin(angle),
        ))
    return result


def generate_bezier_waypoints(
    p0: Tuple[float, float],
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    num_points: int = 50,
) -> List[Tuple[float, float]]:
    """Generate waypoints along a cubic Bezier curve.

    Args:
        p0: Start point.
        p1: Control point 1.
        p2: Control point 2.
        p3: End point.
        num_points: Number of waypoints.

    Returns:
        Waypoints along the curve.
    """
    result: List[Tuple[float, float]] = []
    for i in range(num_points + 1):
        t = i / num_points
        mt = 1.0 - t
        mt2 = mt * mt
        mt3 = mt2 * mt
        t2 = t * t
        t3 = t2 * t
        x = mt3 * p0[0] + 3 * mt2 * t * p1[0] + 3 * mt * t2 * p2[0] + t3 * p3[0]
        y = mt3 * p0[1] + 3 * mt2 * t * p1[1] + 3 * mt * t2 * p2[1] + t3 * p3[1]
        result.append((x, y))
    return result


def simplify_path(
    points: List[Tuple[float, float]],
    tolerance: float = 1.0,
) -> List[Tuple[float, float]]:
    """Simplify path using Ramer-Douglas-Peucker algorithm.

    Args:
        points: Input path points.
        tolerance: Simplification tolerance.

    Returns:
        Simplified path.
    """
    if len(points) < 3:
        return points[:]

    def perpendicular_dist(
        p: Tuple[float, float],
        a: Tuple[float, float],
        b: Tuple[float, float],
    ) -> float:
        ax, ay = p[0] - a[0], p[1] - a[1]
        bx, by = b[0] - a[0], b[1] - a[1]
        norm_b = math.sqrt(bx * bx + by * by)
        if norm_b < 1e-10:
            return math.sqrt(ax * ax + ay * ay)
        return abs(ax * by - ay * bx) / norm_b

    def rdp(pts: List[Tuple[float, float]], start: int, end: int) -> List[Tuple[float, float]]:
        max_dist = 0.0
        max_idx = 0
        for i in range(start + 1, end):
            d = perpendicular_dist(pts[i], pts[start], pts[end])
            if d > max_dist:
                max_dist = d
                max_idx = i
        if max_dist > tolerance:
            left = rdp(pts, start, max_idx)
            right = rdp(pts, max_idx, end)
            return left + right[1:]
        else:
            return [pts[start], pts[end]]

    result = rdp(points, 0, len(points) - 1)
    return result


def path_curvature(
    points: List[Tuple[float, float]],
) -> List[float]:
    """Compute curvature at each point on path.

    Args:
        points: Path points.

    Returns:
        Curvature values.
    """
    if len(points) < 3:
        return [0.0] * len(points)

    curvatures: List[float] = [0.0]
    for i in range(1, len(points) - 1):
        p0 = points[i - 1]
        p1 = points[i]
        p2 = points[i + 1]

        v1x = p1[0] - p0[0]
        v1y = p1[1] - p0[1]
        v2x = p2[0] - p1[0]
        v2y = p2[1] - p1[1]

        cross = v1x * v2y - v1y * v2x
        dot = v1x * v2x + v1y * v2y
        len1 = math.sqrt(v1x * v1x + v1y * v1y)
        len2 = math.sqrt(v2x * v2x + v2y * v2y)

        if len1 < 1e-10 or len2 < 1e-10:
            curvatures.append(0.0)
        else:
            curvatures.append(abs(cross) / (len1 * len2))

    curvatures.append(0.0)
    return curvatures
