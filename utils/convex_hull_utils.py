"""Convex hull computation utilities for RabAI AutoClick.

Provides:
- Graham scan algorithm
- Jarvis march (gift wrapping)
- Monotone chain algorithm
- Hull area and perimeter calculations
"""

from typing import List, Tuple, Optional
import math


def cross_product(o: Tuple[float, float], a: Tuple[float, float], b: Tuple[float, float]) -> float:
    """Compute cross product of OA x OB.

    Returns:
        > 0 if counter-clockwise, < 0 if clockwise, 0 if collinear.
    """
    return (a[0] - o[0]) * (b[1] - o[1]) - (a[1] - o[1]) * (b[0] - o[0])


def convex_hull_graham(
    points: List[Tuple[float, float]],
) -> List[Tuple[float, float]]:
    """Compute convex hull using Graham scan.

    Args:
        points: Input points.

    Returns:
        Points on convex hull in counter-clockwise order.
    """
    if len(points) < 3:
        return points[:]

    # Find pivot (lowest, then leftmost)
    pivot = min(points, key=lambda p: (p[1], p[0]))

    # Sort by polar angle with respect to pivot
    def angle_atan2(p: Tuple[float, float]) -> float:
        return math.atan2(p[1] - pivot[1], p[0] - pivot[0])

    sorted_pts = sorted(
        [p for p in points if p != pivot],
        key=angle_atan2
    )

    # Filter collinear (keep furthest)
    filtered: List[Tuple[float, float]] = []
    for pt in sorted_pts:
        while (len(filtered) >= 1 and
               abs(cross_product(filtered[-1], pt, pivot)) < 1e-10):
            filtered.pop()
        filtered.append(pt)

    hull = [pivot] + filtered

    # Graham scan
    i = 1
    while i < len(hull) - 1:
        if cross_product(hull[i - 1], hull[i], hull[i + 1]) >= 0:
            hull.pop(i)
            if i > 1:
                i -= 1
        else:
            i += 1

    return hull


def convex_hull_jarvis(
    points: List[Tuple[float, float]],
) -> List[Tuple[float, float]]:
    """Compute convex hull using Jarvis march (gift wrapping).

    Args:
        points: Input points.

    Returns:
        Points on convex hull in counter-clockwise order.
    """
    if len(points) < 3:
        return points[:]

    # Find leftmost point
    leftmost = min(points, key=lambda p: p[0])
    hull: List[Tuple[float, float]] = [leftmost]

    current = leftmost
    while True:
        next_point = points[0]
        for p in points[1:]:
            if next_point == current or cross_product(current, p, next_point) > 0:
                next_point = p
        current = next_point
        if current == leftmost:
            break
        hull.append(current)

    return hull


def convex_hull_monotone_chain(
    points: List[Tuple[float, float]],
) -> List[Tuple[float, float]]:
    """Compute convex hull using monotone chain (Andrew's) algorithm.

    Args:
        points: Input points.

    Returns:
        Points on convex hull in counter-clockwise order.
    """
    if len(points) < 3:
        return points[:]

    sorted_pts = sorted(set(points))

    if len(sorted_pts) <= 1:
        return sorted_pts[:]

    # Build lower hull
    lower: List[Tuple[float, float]] = []
    for p in sorted_pts:
        while (len(lower) >= 2 and
               cross_product(lower[-2], lower[-1], p) <= 0):
            lower.pop()
        lower.append(p)

    # Build upper hull
    upper: List[Tuple[float, float]] = []
    for p in reversed(sorted_pts):
        while (len(upper) >= 2 and
               cross_product(upper[-2], upper[-1], p) <= 0):
            upper.pop()
        upper.append(p)

    # Remove last point of each half (it's repeated)
    lower.pop()
    upper.pop()

    return lower + upper


def convex_hull(
    points: List[Tuple[float, float]],
    algorithm: str = "monotone",
) -> List[Tuple[float, float]]:
    """Compute convex hull of points.

    Args:
        points: Input points.
        algorithm: 'graham', 'jarvis', or 'monotone'.

    Returns:
        Points on convex hull.
    """
    if algorithm == "graham":
        return convex_hull_graham(points)
    elif algorithm == "jarvis":
        return convex_hull_jarvis(points)
    else:
        return convex_hull_monotone_chain(points)


def hull_area(hull: List[Tuple[float, float]]) -> float:
    """Compute area of convex polygon using shoelace formula.

    Args:
        hull: Polygon vertices in order.

    Returns:
        Area.
    """
    if len(hull) < 3:
        return 0.0
    area = 0.0
    n = len(hull)
    for i in range(n):
        j = (i + 1) % n
        area += hull[i][0] * hull[j][1]
        area -= hull[j][0] * hull[i][1]
    return abs(area) / 2.0


def hull_perimeter(hull: List[Tuple[float, float]]) -> float:
    """Compute perimeter of convex polygon.

    Args:
        hull: Polygon vertices in order.

    Returns:
        Perimeter.
    """
    if len(hull) < 2:
        return 0.0
    perimeter = 0.0
    n = len(hull)
    for i in range(n):
        j = (i + 1) % n
        dx = hull[j][0] - hull[i][0]
        dy = hull[j][1] - hull[i][1]
        perimeter += math.sqrt(dx * dx + dy * dy)
    return perimeter


def hull_centroid(hull: List[Tuple[float, float]]) -> Tuple[float, float]:
    """Compute centroid of convex polygon.

    Args:
        hull: Polygon vertices.

    Returns:
        Centroid (cx, cy).
    """
    if len(hull) < 3:
        if not hull:
            return (0.0, 0.0)
        return hull[0]

    cx = sum(p[0] for p in hull) / len(hull)
    cy = sum(p[1] for p in hull) / len(hull)
    return (cx, cy)


def is_point_in_hull(
    point: Tuple[float, float],
    hull: List[Tuple[float, float]],
) -> bool:
    """Check if point is inside convex polygon.

    Args:
        point: (x, y) to check.
        hull: Convex polygon vertices.

    Returns:
        True if point is inside or on boundary.
    """
    if len(hull) < 3:
        return False

    n = len(hull)
    for i in range(n):
        j = (i + 1) % n
        if cross_product(hull[i], hull[j], point) < 0:
            return False
    return True


def hull_diameter(hull: List[Tuple[float, float]]) -> Tuple[Tuple[float, float], Tuple[float, float]]:
    """Find the diameter of convex hull (farthest pair of points).

    Uses rotating calipers method.

    Args:
        hull: Convex polygon vertices.

    Returns:
        Pair of points that are farthest apart.
    """
    if len(hull) < 2:
        return (hull[0] if hull else (0, 0), hull[0] if hull else (0, 0))

    if len(hull) == 2:
        return (hull[0], hull[1])

    def dist_sq(a: Tuple[float, float], b: Tuple[float, float]) -> float:
        dx = a[0] - b[0]
        dy = a[1] - b[1]
        return dx * dx + dy * dy

    n = len(hull)
    antipodal: List[Tuple[int, int]] = []

    p1, p2 = 0, 0
    max_dist = 0.0

    for i in range(n):
        next_i = (i + 1) % n
        while abs(cross_product(hull[next_i], hull[i], hull[(p2 + 1) % n])) > \
              abs(cross_product(hull[next_i], hull[i], hull[p2])):
            p2 = (p2 + 1) % n
        if dist_sq(hull[i], hull[p2]) > max_dist:
            max_dist = dist_sq(hull[i], hull[p2])
            p1 = i
        antipodal.append((i, p2))

    # Find maximum distance among antipodal pairs
    best = (hull[0], hull[0])
    max_d = 0.0
    for i, j in antipodal:
        d = dist_sq(hull[i], hull[j])
        if d > max_d:
            max_d = d
            best = (hull[i], hull[j])

    return best
