"""Visibility and occlusion utilities for RabAI AutoClick.

Provides:
- Point-to-rectangle visibility checks
- Line-of-sight tests
- Occlusion detection
- Viewshed computation
"""

from typing import List, Tuple, Optional, Set, Callable
import math


def line_intersects_rect(
    p1: Tuple[float, float],
    p2: Tuple[float, float],
    rx: float, ry: float, rw: float, rh: float,
) -> bool:
    """Check if line segment intersects rectangle.

    Args:
        p1: Line start point.
        p2: Line end point.
        rx, ry, rw, rh: Rectangle bounds.

    Returns:
        True if line intersects rectangle.
    """
    rminx, rminy = rx, ry
    rmaxx, rmaxy = rx + rw, ry + rh

    # Check if line segment endpoints are inside
    def inside(x: float, y: float) -> bool:
        return rminx <= x <= rmaxx and rminy <= y <= rmaxy

    if inside(p1[0], p1[1]) or inside(p2[0], p2[1]):
        return True

    # Check intersection with each edge
    def seg_intersects(
        ax: float, ay: float, bx: float, by: float,
        cx: float, cy: float, dx: float, dy: float,
    ) -> bool:
        d1x, d1y = bx - ax, by - ay
        d2x, d2y = dx - cx, dy - cy
        cross = d1x * d2y - d1y * d2x
        if abs(cross) < 1e-10:
            return False
        t = ((cx - ax) * d2y - (cy - ay) * d2x) / cross
        u = ((cx - ax) * d1y - (cy - ay) * d1x) / cross
        return 0 <= t <= 1 and 0 <= u <= 1

    return (
        seg_intersects(p1[0], p1[1], p2[0], p2[1], rminx, rminy, rmaxx, rminy) or
        seg_intersects(p1[0], p1[1], p2[0], p2[1], rmaxx, rminy, rmaxx, rmaxy) or
        seg_intersects(p1[0], p1[1], p2[0], p2[1], rmaxx, rmaxy, rminx, rmaxy) or
        seg_intersects(p1[0], p1[1], p2[0], p2[1], rminx, rmaxy, rminx, rminy)
    )


def point_in_rect(
    px: float, py: float,
    rx: float, ry: float, rw: float, rh: float,
) -> bool:
    """Check if point is inside rectangle."""
    return rx <= px <= rx + rw and ry <= py <= ry + rh


def line_of_sight(
    origin: Tuple[float, float],
    target: Tuple[float, float],
    obstacles: List[Tuple[float, float, float, float]],
) -> bool:
    """Check if there is clear line of sight between two points.

    Args:
        origin: Observer position.
        target: Target position.
        obstacles: List of (x, y, w, h) obstacle rectangles.

    Returns:
        True if line of sight is clear.
    """
    for ox, oy, ow, oh in obstacles:
        if line_intersects_rect(origin, target, ox, oy, ow, oh):
            return False
    return True


def compute_viewshed(
    origin: Tuple[float, float],
    radius: float,
    obstacles: List[Tuple[float, float, float, float]],
    resolution: int = 50,
) -> List[Tuple[float, float]]:
    """Compute viewshed: all visible points within radius.

    Args:
        origin: Observer position.
        radius: View radius.
        obstacles: Obstacle rectangles.
        resolution: Angular resolution (number of rays).

    Returns:
        List of visible (x, y) points.
    """
    visible: Set[Tuple[int, int]] = set()

    for i in range(resolution):
        angle = 2 * math.pi * i / resolution
        for r_frac in [0.2, 0.4, 0.6, 0.8, 1.0]:
            target = (
                origin[0] + radius * r_frac * math.cos(angle),
                origin[1] + radius * r_frac * math.sin(angle),
            )
            blocked = False
            for ox, oy, ow, oh in obstacles:
                if line_intersects_rect(origin, target, ox, oy, ow, oh):
                    blocked = True
                    break
            if not blocked:
                gx = int(target[0])
                gy = int(target[1])
                visible.add((gx, gy))

    return [(float(x), float(y)) for x, y in visible]


def ray_cast(
    origin: Tuple[float, float],
    angle: float,
    obstacles: List[Tuple[float, float, float, float]],
    max_dist: float = 10000.0,
) -> Tuple[float, float, float]:
    """Cast a ray and find closest intersection.

    Args:
        origin: Ray origin.
        angle: Ray angle in radians.
        obstacles: List of obstacle rectangles.
        max_dist: Maximum ray length.

    Returns:
        (hit_x, hit_y, distance). Returns (origin + ray * max_dist, max_dist) if no hit.
    """
    dir_x = math.cos(angle)
    dir_y = math.sin(angle)

    min_dist = max_dist
    hit = (origin[0] + dir_x * max_dist, origin[1] + dir_y * max_dist)

    for ox, oy, ow, oh in obstacles:
        # Ray vs rectangle intersection
        rminx, rminy = ox, oy
        rmaxx, rmaxy = ox + ow, oy + oh

        tmin = (rminx - origin[0]) / dir_x if dir_x != 0 else float("-inf")
        tmax = (rmaxx - origin[0]) / dir_x if dir_x != 0 else float("inf")

        if dir_x < 0:
            tmin, tmax = tmax, tmin

        tymin = (rminy - origin[1]) / dir_y if dir_y != 0 else float("-inf")
        tymax = (rmaxy - origin[1]) / dir_y if dir_y != 0 else float("inf")

        if dir_y < 0:
            tymin, tymax = tymax, tymin

        if tmin > tymax or tymin > tmax:
            continue

        tmin = max(tmin, tymin)
        tmax = min(tmax, tymax)

        if tmin > 0 and tmin < min_dist:
            min_dist = tmin
            hit = (origin[0] + dir_x * tmin, origin[1] + dir_y * tmin)

    return (*hit, min_dist)


def is_visible_from_origin(
    point: Tuple[float, float],
    origin: Tuple[float, float],
    obstacles: List[Tuple[float, float, float, float]],
) -> bool:
    """Check if point is visible from origin.

    Args:
        point: Point to check.
        origin: Observer position.
        obstacles: Obstacle rectangles.

    Returns:
        True if visible.
    """
    return line_of_sight(origin, point, obstacles)


def occluded_rectangles(
    origin: Tuple[float, float],
    targets: List[Tuple[float, float, float, float]],
) -> List[int]:
    """Find indices of rectangles occluded from origin.

    Args:
        origin: Observer position.
        targets: List of (x, y, w, h) rectangles.

    Returns:
        List of occluded rectangle indices.
    """
    occluded: List[int] = []
    for i, (tx, ty, tw, th) in enumerate(targets):
        cx, cy = tx + tw / 2, ty + th / 2
        if not line_of_sight(origin, (cx, cy), targets[:i] + targets[i + 1:]):
            occluded.append(i)
    return occluded


def compute_shadow_polygon(
    light: Tuple[float, float],
    obstacle: Tuple[float, float, float, float],
) -> List[Tuple[float, float]]:
    """Compute shadow polygon cast by obstacle from light source.

    Args:
        light: Light source position.
        obstacle: (x, y, w, h) obstacle rectangle.

    Returns:
        Shadow polygon as list of points.
    """
    ox, oy, ow, oh = obstacle
    corners = [
        (ox, oy),
        (ox + ow, oy),
        (ox + ow, oy + oh),
        (ox, oy + oh),
    ]

    # Compute direction from light to each corner
    dirs = [(c[0] - light[0], c[1] - light[1]) for c in corners]

    # Extend rays to infinity
    far_dist = 10000.0
    far_corners = [
        (light[0] + d[0] * far_dist, light[1] + d[1] * far_dist)
        for d in dirs
    ]

    # Shadow polygon: near corners + far corners (in reverse order)
    shadow = corners + list(reversed(far_corners))
    return shadow
