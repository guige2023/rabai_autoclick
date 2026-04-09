"""Mesh processing utilities for RabAI AutoClick.

Provides:
- Triangle mesh operations
- Mesh simplification
- Normal computation
- Mesh smoothing
"""

from typing import List, Tuple, Set, Optional, Dict
import math


Triangle = Tuple[Tuple[float, float, float], Tuple[float, float, float], Tuple[float, float, float]]
Vertex = Tuple[float, float, float]


def triangle_normal(t: Triangle) -> Vertex:
    """Compute normal of triangle."""
    v0, v1, v2 = t
    ax = v1[0] - v0[0]
    ay = v1[1] - v0[1]
    az = v1[2] - v0[2]
    bx = v2[0] - v0[0]
    by = v2[1] - v0[1]
    bz = v2[2] - v0[2]
    nx = ay * bz - az * by
    ny = az * bx - ax * bz
    nz = ax * by - ay * bx
    length = math.sqrt(nx * nx + ny * ny + nz * nz)
    if length < 1e-10:
        return (0.0, 1.0, 0.0)
    return (nx / length, ny / length, nz / length)


def triangle_area(t: Triangle) -> float:
    """Compute area of triangle."""
    v0, v1, v2 = t
    ax = v1[0] - v0[0]
    ay = v1[1] - v0[1]
    az = v1[2] - v0[2]
    bx = v2[0] - v0[0]
    by = v2[1] - v0[1]
    bz = v2[2] - v0[2]
    cx = ay * bz - az * by
    cy = az * bx - ax * bz
    cz = ax * by - ay * bx
    return math.sqrt(cx * cx + cy * cy + cz * cz) / 2


def triangle_centroid(t: Triangle) -> Vertex:
    """Compute centroid of triangle."""
    v0, v1, v2 = t
    return (
        (v0[0] + v1[0] + v2[0]) / 3,
        (v0[1] + v1[1] + v2[1]) / 3,
        (v0[2] + v1[2] + v2[2]) / 3,
    )


def mesh_surface_area(triangles: List[Triangle]) -> float:
    """Compute total surface area of mesh."""
    return sum(triangle_area(t) for t in triangles)


def mesh_centroid(triangles: List[Triangle]) -> Vertex:
    """Compute centroid of mesh (center of bounding box)."""
    if not triangles:
        return (0.0, 0.0, 0.0)
    all_vertices: List[Vertex] = []
    for t in triangles:
        all_vertices.extend([t[0], t[1], t[2]])
    xs = [v[0] for v in all_vertices]
    ys = [v[1] for v in all_vertices]
    zs = [v[2] for v in all_vertices]
    return (
        (min(xs) + max(xs)) / 2,
        (min(ys) + max(ys)) / 2,
        (min(zs) + max(zs)) / 2,
    )


def mesh_bounding_box(
    triangles: List[Triangle],
) -> Tuple[float, float, float, float, float, float]:
    """Get bounding box (min_x, min_y, min_z, max_x, max_y, max_z)."""
    if not triangles:
        return (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
    all_vertices: List[Vertex] = []
    for t in triangles:
        all_vertices.extend([t[0], t[1], t[2]])
    xs = [v[0] for v in all_vertices]
    ys = [v[1] for v in all_vertices]
    zs = [v[2] for v in all_vertices]
    return (min(xs), min(ys), min(zs), max(xs), max(ys), max(zs))


def smooth_mesh_laplacian(
    triangles: List[Triangle],
    iterations: int = 1,
) -> List[Triangle]:
    """Smooth mesh using Laplacian smoothing.

    Args:
        triangles: Input mesh.
        iterations: Number of smoothing passes.

    Returns:
        Smoothed mesh.
    """
    # Build vertex to neighbors map
    vertex_neighbors: Dict[int, Set[int]] = {}
    vertex_map: Dict[Vertex, int] = {}
    vertices: List[Vertex] = []
    indices: List[Tuple[int, int, int]] = []

    def get_vertex_idx(v: Vertex) -> int:
        if v not in vertex_map:
            vertex_map[v] = len(vertices)
            vertices.append(v)
            vertex_neighbors[vertex_map[v]] = set()
        return vertex_map[v]

    for t in triangles:
        i0 = get_vertex_idx(t[0])
        i1 = get_vertex_idx(t[1])
        i2 = get_vertex_idx(t[2])
        indices.append((i0, i1, i2))
        vertex_neighbors[i0].update([i1, i2])
        vertex_neighbors[i1].update([i0, i2])
        vertex_neighbors[i2].update([i0, i1])

    current = vertices[:]
    for _ in range(iterations):
        new_vertices: List[Vertex] = current[:]
        for i, v in enumerate(current):
            neighbors = vertex_neighbors.get(i, set())
            if neighbors:
                nx = sum(current[n][0] for n in neighbors) / len(neighbors)
                ny = sum(current[n][1] for n in neighbors) / len(neighbors)
                nz = sum(current[n][2] for n in neighbors) / len(neighbors)
                new_vertices[i] = (nx, ny, nz)
        current = new_vertices

    return [(current[i0], current[i1], current[i2]) for i0, i1, i2 in indices]


def subdivide_triangles(
    triangles: List[Triangle],
) -> List[Triangle]:
    """Subdivide each triangle into 4 triangles (1-to-4 subdivision).

    Returns:
        List of subdivided triangles.
    """
    result: List[Triangle] = []
    cache: Dict[Tuple[int, int], Vertex] = {}

    def midpoint(v1: Vertex, v2: Vertex) -> Vertex:
        key = (min(id(v1), id(v2)), max(id(v1), id(v2)))
        if key in cache:
            return cache[key]
        mid = ((v1[0] + v2[0]) / 2, (v1[1] + v2[1]) / 2, (v1[2] + v2[2]) / 2)
        cache[key] = mid
        return mid

    for t in triangles:
        v0, v1, v2 = t
        m01 = midpoint(v0, v1)
        m12 = midpoint(v1, v2)
        m20 = midpoint(v2, v0)
        result.append((v0, m01, m20))
        result.append((m01, v1, m12))
        result.append((m20, m12, v2))
        result.append((m01, m12, m20))

    return result


def merge_meshes(
    mesh1: List[Triangle],
    mesh2: List[Triangle],
) -> List[Triangle]:
    """Merge two meshes."""
    return mesh1[:] + mesh2[:]


def translate_mesh(
    triangles: List[Triangle],
    dx: float,
    dy: float,
    dz: float,
) -> List[Triangle]:
    """Translate mesh by (dx, dy, dz)."""
    return [
        (
            (v0[0] + dx, v0[1] + dy, v0[2] + dz),
            (v1[0] + dx, v1[1] + dy, v1[2] + dz),
            (v2[0] + dx, v2[1] + dy, v2[2] + dz),
        )
        for v0, v1, v2 in triangles
    ]


def scale_mesh(
    triangles: List[Triangle],
    sx: float,
    sy: float,
    sz: float,
) -> List[Triangle]:
    """Scale mesh."""
    return [
        (
            (v0[0] * sx, v0[1] * sy, v0[2] * sz),
            (v1[0] * sx, v1[1] * sy, v1[2] * sz),
            (v2[0] * sx, v2[1] * sy, v2[2] * sz),
        )
        for v0, v1, v2 in triangles
    ]


def rotate_mesh_x(
    triangles: List[Triangle],
    angle: float,
) -> List[Triangle]:
    """Rotate mesh around X axis."""
    c = math.cos(angle)
    s = math.sin(angle)
    return [
        (
            (v0[0], v0[1] * c - v0[2] * s, v0[1] * s + v0[2] * c),
            (v1[0], v1[1] * c - v1[2] * s, v1[1] * s + v1[2] * c),
            (v2[0], v2[1] * c - v2[2] * s, v2[1] * s + v2[2] * c),
        )
        for v0, v1, v2 in triangles
    ]


def rotate_mesh_y(
    triangles: List[Triangle],
    angle: float,
) -> List[Triangle]:
    """Rotate mesh around Y axis."""
    c = math.cos(angle)
    s = math.sin(angle)
    return [
        (
            (v0[0] * c + v0[2] * s, v0[1], -v0[0] * s + v0[2] * c),
            (v1[0] * c + v1[2] * s, v1[1], -v1[0] * s + v1[2] * c),
            (v2[0] * c + v2[2] * s, v2[1], -v2[0] * s + v2[2] * c),
        )
        for v0, v1, v2 in triangles
    ]


def ray_hit_triangle(
    origin: Vertex,
    direction: Vertex,
    t_tri: Triangle,
    max_dist: float = 1e10,
) -> Optional[float]:
    """Ray-triangle intersection (Moller-Trumbore).

    Returns:
        Distance to hit, or None.
    """
    EPS = 1e-10
    v0, v1, v2 = t_tri

    e1 = (v1[0] - v0[0], v1[1] - v0[1], v1[2] - v0[2])
    e2 = (v2[0] - v0[0], v2[1] - v0[1], v2[2] - v0[2])

    hx = direction[1] * e2[2] - direction[2] * e2[1]
    hy = direction[2] * e2[0] - direction[0] * e2[2]
    hz = direction[0] * e2[1] - direction[1] * e2[0]
    a = e1[0] * hx + e1[1] * hy + e1[2] * hz

    if abs(a) < EPS:
        return None

    f = 1.0 / a
    sx = origin[0] - v0[0]
    sy = origin[1] - v0[1]
    sz = origin[2] - v0[2]
    u = f * (sx * hx + sy * hy + sz * hz)
    if u < 0.0 or u > 1.0:
        return None

    qx = sy * e1[2] - sz * e1[1]
    qy = sz * e1[0] - sx * e1[2]
    qz = sx * e1[1] - sy * e1[0]
    v = f * (direction[0] * qx + direction[1] * qy + direction[2] * qz)
    if v < 0.0 or u + v > 1.0:
        return None

    t = f * (e2[0] * qx + e2[1] * qy + e2[2] * qz)
    if t > EPS and t < max_dist:
        return t
    return None
