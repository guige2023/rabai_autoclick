"""Bezier surface utilities for RabAI AutoClick.

Provides:
- Bezier surface evaluation
- Bezier surface rendering as triangle mesh
- Surface normal computation
"""

from typing import List, Tuple, Optional
import math


def binomial(n: int, k: int) -> float:
    """Compute binomial coefficient C(n, k)."""
    if k < 0 or k > n:
        return 0.0
    if k == 0 or k == n:
        return 1.0
    result = 1.0
    for i in range(k):
        result = result * (n - i) / (i + 1)
    return result


def bernstein(n: int, i: int, t: float) -> float:
    """Bernstein polynomial basis."""
    return binomial(n, i) * (t ** i) * ((1 - t) ** (n - i))


def bezier_surface_point(
    control_points: List[List[Tuple[float, float, float]]],
    u: float,
    v: float,
) -> Tuple[float, float, float]:
    """Evaluate point on Bezier surface.

    Args:
        control_points: 2D grid of (x, y, z) control points [row][col].
        u: Surface parameter U [0, 1].
        v: Surface parameter V [0, 1].

    Returns:
        (x, y, z) point on surface.
    """
    n = len(control_points) - 1
    m = len(control_points[0]) - 1

    x = 0.0
    y = 0.0
    z = 0.0

    for i in range(n + 1):
        for j in range(m + 1):
            b_i = bernstein(n, i, u)
            b_j = bernstein(m, j, v)
            b = b_i * b_j
            cp = control_points[i][j]
            x += cp[0] * b
            y += cp[1] * b
            z += cp[2] * b

    return (x, y, z)


def bezier_surface_normal(
    control_points: List[List[Tuple[float, float, float]]],
    u: float,
    v: float,
) -> Tuple[float, float, float]:
    """Compute surface normal at (u, v).

    Args:
        control_points: Control point grid.
        u, v: Surface parameters.

    Returns:
        Normalized (nx, ny, nz).
    """
    n = len(control_points) - 1
    m = len(control_points[0]) - 1
    eps = 1e-4

    # Partial derivatives using central differences
    du = bezier_surface_point(control_points, u + eps, v)
    du_minus = bezier_surface_point(control_points, u - eps, v)
    dv = bezier_surface_point(control_points, u, v + eps)
    dv_minus = bezier_surface_point(control_points, u, v - eps)

    d_u = (du[0] - du_minus[0], du[1] - du_minus[1], du[2] - du_minus[2])
    d_v = (dv[0] - dv_minus[0], dv[1] - dv_minus[1], dv[2] - dv_minus[2])

    # Cross product
    nx = d_u[1] * d_v[2] - d_u[2] * d_v[1]
    ny = d_u[2] * d_v[0] - d_u[0] * d_v[2]
    nz = d_u[0] * d_v[1] - d_u[1] * d_v[0]

    length = math.sqrt(nx * nx + ny * ny + nz * nz)
    if length < 1e-10:
        return (0.0, 0.0, 1.0)
    return (nx / length, ny / length, nz / length)


def bezier_surface_to_mesh(
    control_points: List[List[Tuple[float, float, float]]],
    resolution: int = 20,
) -> List[Tuple[Tuple[float, float, float], Tuple[float, float, float], Tuple[float, float, float]]]:
    """Convert Bezier surface to triangle mesh.

    Args:
        control_points: Control point grid.
        resolution: Grid resolution (u_steps, v_steps).

    Returns:
        List of triangles, each as 3 vertex tuples (x, y, z).
    """
    if resolution < 2:
        resolution = 2

    u_steps = resolution
    v_steps = resolution
    points: List[List[Tuple[float, float, float]]] = []

    for i in range(u_steps + 1):
        row: List[Tuple[float, float, float]] = []
        u = i / u_steps
        for j in range(v_steps + 1):
            v = j / v_steps
            row.append(bezier_surface_point(control_points, u, v))
        points.append(row)

    triangles: List[Tuple[Tuple[float, float, float], Tuple[float, float, float], Tuple[float, float, float]]] = []

    for i in range(u_steps):
        for j in range(v_steps):
            p00 = points[i][j]
            p10 = points[i + 1][j]
            p01 = points[i][j + 1]
            p11 = points[i + 1][j + 1]

            # Two triangles per quad
            triangles.append((p00, p10, p01))
            triangles.append((p10, p11, p01))

    return triangles


def split_bezier_surface_u(
    control_points: List[List[Tuple[float, float, float]]],
    u: float,
) -> Tuple[List[List[Tuple[float, float, float]]], List[List[Tuple[float, float, float]]]]:
    """Split Bezier surface along U direction.

    Args:
        control_points: Control point grid.
        u: Split parameter [0, 1].

    Returns:
        (left_surface, right_surface) control point grids.
    """
    n = len(control_points) - 1
    m = len(control_points[0]) - 1

    # Compute de Casteljau along u for each row
    left: List[List[Tuple[float, float, float]]] = []
    right: List[List[Tuple[float, float, float]]] = []

    for row in control_points:
        left_row, right_row = split_bezier_curve_row(row, u)
        left.append(left_row)
        right.append(right_row)

    return (left, right)


def split_bezier_curve_row(
    row: List[Tuple[float, float, float]],
    t: float,
) -> Tuple[List[Tuple[float, float, float]], List[Tuple[float, float, float]]]:
    """Split a row of control points at parameter t."""
    n = len(row)
    left: List[Tuple[float, float, float]] = [row[0]]
    right: List[Tuple[float, float, float]] = [row[-1]]

    # De Casteljau
    current = row[:]
    for level in range(1, n):
        level_pts: List[Tuple[float, float, float]] = []
        for i in range(n - level):
            pt = (
                (1 - t) * current[i][0] + t * current[i + 1][0],
                (1 - t) * current[i][1] + t * current[i + 1][1],
                (1 - t) * current[i][2] + t * current[i + 1][2],
            )
            level_pts.append(pt)
        left.append(level_pts[0])
        right.insert(0, level_pts[-1])
        current = level_pts

    return (left, right)


def create_plane_surface(
    width: float = 1.0,
    height: float = 1.0,
    u_segments: int = 1,
    v_segments: int = 1,
) -> List[List[Tuple[float, float, float]]]:
    """Create control points for a planar surface.

    Args:
        width: Surface width.
        height: Surface height.
        u_segments: Control points in U direction.
        v_segments: Control points in V direction.

    Returns:
        2D grid of control points.
    """
    control_points: List[List[Tuple[float, float, float]]] = []
    for i in range(u_segments + 1):
        row: List[Tuple[float, float, float]] = []
        u = i / u_segments
        for j in range(v_segments + 1):
            v = j / v_segments
            row.append((u * width, v * height, 0.0))
        control_points.append(row)
    return control_points


def create_cylinder_surface(
    radius: float,
    height: float,
    segments: int = 10,
    height_segments: int = 1,
) -> List[List[Tuple[float, float, float]]]:
    """Create control points for a cylindrical surface.

    Args:
        radius: Cylinder radius.
        height: Cylinder height.
        segments: Circumferential segments.
        height_segments: Height segments.

    Returns:
        Control point grid.
    """
    control_points: List[List[Tuple[float, float, float]]] = []
    for i in range(height_segments + 1):
        row: List[Tuple[float, float, float]] = []
        y = i / height_segments * height
        for j in range(segments + 1):
            angle = 2 * math.pi * j / segments
            x = radius * math.cos(angle)
            z = radius * math.sin(angle)
            row.append((x, y, z))
        control_points.append(row)
    return control_points


def create_sphere_surface(
    radius: float,
    u_segments: int = 10,
    v_segments: int = 10,
) -> List[List[Tuple[float, float, float]]]:
    """Create control points for a spherical surface.

    Args:
        radius: Sphere radius.
        u_segments: Latitude segments.
        v_segments: Longitude segments.

    Returns:
        Control point grid.
    """
    control_points: List[List[Tuple[float, float, float]]] = []
    for i in range(v_segments + 1):
        row: List[Tuple[float, float, float]] = []
        v = i / v_segments
        phi = math.pi * v  # 0 to pi (latitude)
        for j in range(u_segments + 1):
            u = j / u_segments
            theta = 2 * math.pi * u  # 0 to 2pi (longitude)
            x = radius * math.sin(phi) * math.cos(theta)
            y = radius * math.cos(phi)
            z = radius * math.sin(phi) * math.sin(theta)
            row.append((x, y, z))
        control_points.append(row)
    return control_points
