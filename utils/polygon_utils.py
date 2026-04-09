"""Polygon utilities for RabAI AutoClick.

Provides:
- Polygon area and perimeter
- Point-in-polygon tests
- Polygon clipping (Sutherland-Hodgman)
- Polygon triangulation
- Convex hull operations
"""

from typing import List, Tuple, Optional, Set
import math


def polygon_area(polygon: List[Tuple[float, float]]) -> float:
    """Compute polygon area using shoelace formula.

    Args:
        polygon: Vertices in order.

    Returns:
        Signed area (positive for CCW).
    """
    n = len(polygon)
    if n < 3:
        return 0.0
    area = 0.0
    for i in range(n):
        j = (i + 1) % n
        area += polygon[i][0] * polygon[j][1]
        area -= polygon[j][0] * polygon[i][1]
    return area / 2.0


def polygon_perimeter(polygon: List[Tuple[float, float]]) -> float:
    """Compute polygon perimeter."""
    n = len(polygon)
    if n < 2:
        return 0.0
    total = 0.0
    for i in range(n):
        j = (i + 1) % n
        dx = polygon[j][0] - polygon[i][0]
        dy = polygon[j][1] - polygon[i][1]
        total += math.sqrt(dx * dx + dy * dy)
    return total


def point_in_polygon(
    point: Tuple[float, float],
    polygon: List[Tuple[float, float]],
) -> bool:
    """Ray casting point-in-polygon test.

    Args:
        point: (x, y) to test.
        polygon: Polygon vertices.

    Returns:
        True if point is inside.
    """
    x, y = point
    n = len(polygon)
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def polygon_centroid(
    polygon: List[Tuple[float, float]],
) -> Tuple[float, float]:
    """Compute polygon centroid.

    Args:
        polygon: Polygon vertices.

    Returns:
        (cx, cy) centroid.
    """
    n = len(polygon)
    if n < 3:
        return polygon[0] if polygon else (0.0, 0.0)
    cx = 0.0
    cy = 0.0
    signed_area = 0.0
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[(i + 1) % n]
        cross = xi * yj - xj * yi
        signed_area += cross
        cx += (xi + xj) * cross
        cy += (yi + yj) * cross
    signed_area *= 0.5
    if abs(signed_area) < 1e-10:
        cx = sum(p[0] for p in polygon) / n
        cy = sum(p[1] for p in polygon) / n
    else:
        cx /= (6 * signed_area)
        cy /= (6 * signed_area)
    return (cx, cy)


def line_segment_intersection(
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    p3: Tuple[float, float],
    p4: Tuple[float, float],
) -> Optional[Tuple[float, float]]:
    """Check if line segments p1-p2 and p3-p4 intersect.

    Returns:
        Intersection point or None.
    """
    x1, y1 = p1
    x2, y2 = p2
    x3, y3 = p3
    x4, y4 = p4

    denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
    if abs(denom) < 1e-10:
        return None

    t = ((x1 - x3) * (y3 - y4) - (y1 - y3) * (x3 - x4)) / denom
    u = -((x1 - x2) * (y1 - y3) - (y1 - y2) * (x1 - x3)) / denom

    if 0 <= t <= 1 and 0 <= u <= 1:
        ix = x1 + t * (x2 - x1)
        iy = y1 + t * (y2 - y1)
        return (ix, iy)
    return None


def clip_polygon_by_polygon(
    subject: List[Tuple[float, float]],
    clip: List[Tuple[float, float]],
) -> List[Tuple[float, float]]:
    """Clip polygon using Sutherland-Hodgman algorithm.

    Args:
        subject: Input polygon.
        clip: Convex clip polygon.

    Returns:
        Clipped polygon.
    """
    def inside(p: Tuple[float, float], edge_start: Tuple[float, float], edge_end: Tuple[float, float]) -> bool:
        return (edge_end[0] - edge_start[0]) * (p[1] - edge_start[1]) - \
               (edge_end[1] - edge_start[1]) * (p[0] - edge_start[0]) >= 0

    def intersection(
        s: Tuple[float, float],
        e: Tuple[float, float],
        edge_start: Tuple[float, float],
        edge_end: Tuple[float, float],
    ) -> Tuple[float, float]:
        denom = (e[0] - s[0]) * (edge_end[1] - edge_start[1]) - \
                (e[1] - s[1]) * (edge_end[0] - edge_start[0])
        if abs(denom) < 1e-10:
            return s
        t = ((s[0] - edge_start[0]) * (edge_end[1] - edge_start[1]) -
             (s[1] - edge_start[1]) * (edge_end[0] - edge_start[0])) / denom
        return (s[0] + t * (e[0] - s[0]), s[1] + t * (e[1] - s[1]))

    output = subject[:]
    n = len(clip)

    for i in range(n):
        if not output:
            return []
        input_list = output[:]
        output.clear()
        edge_start = clip[i]
        edge_end = clip[(i + 1) % n]

        for j in range(len(input_list)):
            current = input_list[j]
            prev = input_list[(j - 1) % len(input_list)]

            if inside(current, edge_start, edge_end):
                if not inside(prev, edge_start, edge_end):
                    output.append(intersection(prev, current, edge_start, edge_end))
                output.append(current)
            elif inside(prev, edge_start, edge_end):
                output.append(intersection(prev, current, edge_start, edge_end))

    return output


def polygon_union(
    polygons: List[List[Tuple[float, float]]],
) -> List[List[Tuple[float, float]]]:
    """Simple union of multiple polygons (greedy approach).

    Args:
        polygons: List of polygons.

    Returns:
        List of merged polygons.
    """
    if len(polygons) <= 1:
        return polygons[:]

    # Greedy merge: repeatedly merge overlapping polygons
    merged: List[List[Tuple[float, float]]] = polygons[:]

    changed = True
    while changed:
        changed = False
        new_merged: List[List[Tuple[float, float]]] = []
        used = set()

        for i, poly_i in enumerate(merged):
            if i in used:
                continue
            current = poly_i
            for j, poly_j in enumerate(merged[i + 1:], i + 1):
                if j in used:
                    continue
                clipped = clip_polygon_by_polygon(current, poly_j)
                if clipped:
                    union = clip_polygon_by_polygon(poly_j, current)
                    union.extend(clipped)
                    current = union
                    used.add(j)
                    changed = True
            new_merged.append(current)

        merged = new_merged

    return merged


def polygon_simplify(
    polygon: List[Tuple[float, float]],
    tolerance: float = 1.0,
) -> List[Tuple[float, float]]:
    """Simplify polygon using Ramer-Douglas-Peucker.

    Args:
        polygon: Input polygon.
        tolerance: Simplification tolerance.

    Returns:
        Simplified polygon.
    """
    if len(polygon) < 4:
        return polygon[:]

    def perp_dist(
        p: Tuple[float, float],
        a: Tuple[float, float],
        b: Tuple[float, float],
    ) -> float:
        ax, ay = p[0] - a[0], p[1] - a[1]
        bx, by = b[0] - a[0], b[1] - a[1]
        len_b = math.sqrt(bx * bx + by * by)
        if len_b < 1e-10:
            return math.sqrt(ax * ax + ay * ay)
        return abs(ax * by - ay * bx) / len_b

    def rdp(pts: List[Tuple[float, float]], start: int, end: int) -> List[Tuple[float, float]]:
        max_d = 0.0
        max_i = 0
        for i in range(start + 1, end):
            d = perp_dist(pts[i], pts[start], pts[end])
            if d > max_d:
                max_d = d
                max_i = i
        if max_d > tolerance:
            left = rdp(pts, start, max_i)
            right = rdp(pts, max_i, end)
            return left[:-1] + right
        return [pts[start], pts[end]]

    result = rdp(polygon, 0, len(polygon) - 1)
    return result


def regular_polygon(
    n: int,
    radius: float,
    center: Tuple[float, float] = (0.0, 0.0),
    rotation: float = 0.0,
) -> List[Tuple[float, float]]:
    """Generate regular polygon vertices.

    Args:
        n: Number of sides.
        radius: Circumradius.
        center: Center point.
        rotation: Rotation angle in radians.

    Returns:
        List of vertices.
    """
    vertices: List[Tuple[float, float]] = []
    for i in range(n):
        angle = 2 * math.pi * i / n + rotation
        vertices.append((
            center[0] + radius * math.cos(angle),
            center[1] + radius * math.sin(angle),
        ))
    return vertices


def star_polygon(
    n: int,
    outer_radius: float,
    inner_radius: float,
    center: Tuple[float, float] = (0.0, 0.0),
    rotation: float = 0.0,
) -> List[Tuple[float, float]]:
    """Generate star polygon vertices.

    Args:
        n: Number of points.
        outer_radius: Outer point radius.
        inner_radius: Inner point radius.
        center: Center point.
        rotation: Rotation angle.

    Returns:
        List of vertices.
    """
    vertices: List[Tuple[float, float]] = []
    for i in range(n * 2):
        angle = math.pi * i / n + rotation
        r = outer_radius if i % 2 == 0 else inner_radius
        vertices.append((
            center[0] + r * math.cos(angle),
            center[1] + r * math.sin(angle),
        ))
    return vertices
