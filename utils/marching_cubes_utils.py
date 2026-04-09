"""Marching cubes algorithm utilities for RabAI AutoClick.

Provides:
- 3D marching cubes for surface extraction
- Grid sampling and interpolation
- Normal computation for surfaces
"""

from typing import Callable, List, Optional, Tuple, Set
import math


# Edge table for marching cubes (simplified 256 entries as dict)
EDGE_TABLE: dict = {
    0: [], 1: [0,3], 2: [0,1], 3: [1,3], 4: [1,2], 5: [0,3,1,2], 6: [0,2], 7: [2,3],
    8: [2,3], 9: [0,2], 10: [0,1,2,3], 11: [1,2], 12: [1,3], 13: [0,1], 14: [0,3], 15: [],
}


def lerp(a: float, b: float, t: float) -> float:
    """Linear interpolation."""
    return a + (b - a) * t


def sample_grid(
    x: int,
    y: int,
    z: int,
    get_value: Callable[[int, int, int], float],
    dims: Tuple[int, int, int],
) -> List[float]:
    """Sample 8 corner values of a grid cell.

    Args:
        x, y, z: Grid cell origin.
        get_value: Function(x, y, z) -> float.
        dims: Grid dimensions (nx, ny, nz).

    Returns:
        List of 8 corner values.
    """
    nx, ny, nz = dims
    corners: List[float] = []
    for cz in range(2):
        for cy in range(2):
            for cx in range(2):
                gx = min(x + cx, nx - 1)
                gy = min(y + cy, ny - 1)
                gz = min(z + cz, nz - 1)
                corners.append(get_value(gx, gy, gz))
    return corners


def marching_cubes(
    get_value: Callable[[int, int, int], float],
    dims: Tuple[int, int, int],
    isovalue: float = 0.0,
) -> List[Tuple[Tuple[float, float, float], ...]]:
    """Extract isosurface using marching cubes algorithm.

    Args:
        get_value: Function(x, y, z) -> scalar field value.
        dims: Grid dimensions (nx, ny, nz).
        isovalue: Surface threshold value.

    Returns:
        List of triangles, each triangle is a tuple of 3 (x, y, z) vertices.
    """
    nx, ny, nz = dims
    triangles: List[Tuple[Tuple[float, float, float], ...]] = []

    for z in range(nz - 1):
        for y in range(ny - 1):
            for x in range(nx - 1):
                values = sample_grid(x, y, z, get_value, dims)
                cube_index = 0
                for i in range(8):
                    if values[i] < isovalue:
                        cube_index |= (1 << i)

                edges = EDGE_TABLE.get(cube_index, [])

                # Compute vertex positions along edges
                edge_verts: List[Optional[Tuple[float, float, float]]] = [None] * 12
                for e in edges:
                    if e == 0:
                        t = (isovalue - values[0]) / (values[1] - values[0]) if values[1] != values[0] else 0.5
                        edge_verts[0] = (lerp(x, x+1, t), float(y), float(z))
                    elif e == 1:
                        t = (isovalue - values[1]) / (values[2] - values[1]) if values[2] != values[1] else 0.5
                        edge_verts[1] = (float(x+1), lerp(y, y+1, t), float(z))
                    elif e == 2:
                        t = (isovalue - values[2]) / (values[3] - values[2]) if values[3] != values[2] else 0.5
                        edge_verts[2] = (lerp(x, x+1, t), float(y+1), float(z))
                    elif e == 3:
                        t = (isovalue - values[3]) / (values[0] - values[3]) if values[0] != values[3] else 0.5
                        edge_verts[3] = (float(x), lerp(y, y+1, t), float(z))
                    elif e == 4:
                        t = (isovalue - values[4]) / (values[5] - values[4]) if values[5] != values[4] else 0.5
                        edge_verts[4] = (lerp(x, x+1, t), float(y), float(z+1))
                    elif e == 5:
                        t = (isovalue - values[5]) / (values[6] - values[5]) if values[6] != values[5] else 0.5
                        edge_verts[5] = (float(x+1), lerp(y, y+1, t), float(z+1))
                    elif e == 6:
                        t = (isovalue - values[6]) / (values[7] - values[6]) if values[7] != values[6] else 0.5
                        edge_verts[6] = (lerp(x, x+1, t), float(y+1), float(z+1))
                    elif e == 7:
                        t = (isovalue - values[7]) / (values[4] - values[7]) if values[4] != values[7] else 0.5
                        edge_verts[7] = (float(x), lerp(y, y+1, t), float(z+1))
                    elif e == 8:
                        t = (isovalue - values[0]) / (values[4] - values[0]) if values[4] != values[0] else 0.5
                        edge_verts[8] = (float(x), float(y), lerp(z, z+1, t))
                    elif e == 9:
                        t = (isovalue - values[1]) / (values[5] - values[1]) if values[5] != values[1] else 0.5
                        edge_verts[9] = (float(x+1), float(y), lerp(z, z+1, t))
                    elif e == 10:
                        t = (isovalue - values[2]) / (values[6] - values[2]) if values[6] != values[2] else 0.5
                        edge_verts[10] = (float(x+1), float(y+1), lerp(z, z+1, t))
                    elif e == 11:
                        t = (isovalue - values[3]) / (values[7] - values[3]) if values[7] != values[3] else 0.5
                        edge_verts[11] = (float(x), float(y+1), lerp(z, z+1, t))

                # Generate triangles (triangulate each edge pair)
                for i in range(0, len(edges), 3):
                    if i + 2 < len(edges):
                        v0 = edge_verts[edges[i]]
                        v1 = edge_verts[edges[i + 1]]
                        v2 = edge_verts[edges[i + 2]]
                        if v0 and v1 and v2:
                            triangles.append((v0, v1, v2))

    return triangles


def compute_normal(vert: Tuple[float, float, float], eps: float = 1e-5) -> Tuple[float, float, float]:
    """Compute approximate surface normal at a vertex using central differences.

    Args:
        vert: Vertex (x, y, z).
        eps: Small offset for gradient estimation.

    Returns:
        Normalized normal vector.
    """
    g: Callable[[float, float, float], float] = lambda x, y, z: x*x + y*y + z*z - 1.0
    dx = (g(vert[0] + eps, vert[1], vert[2]) - g(vert[0] - eps, vert[1], vert[2])) / (2 * eps)
    dy = (g(vert[0], vert[1] + eps, vert[2]) - g(vert[0], vert[1] - eps, vert[2])) / (2 * eps)
    dz = (g(vert[0], vert[1], vert[2] + eps) - g(vert[0], vert[1], vert[2] - eps)) / (2 * eps)
    length = math.sqrt(dx*dx + dy*dy + dz*dz)
    if length < 1e-10:
        return (0.0, 1.0, 0.0)
    return (dx/length, dy/length, dz/length)


def mesh_surface_area(triangles: List[Tuple[Tuple[float, float, float], ...]]) -> float:
    """Compute total surface area of a triangle mesh.

    Args:
        triangles: List of triangle vertices.

    Returns:
        Total surface area.
    """
    total = 0.0
    for tri in triangles:
        a, b, c = tri
        ab = (b[0]-a[0], b[1]-a[1], b[2]-a[2])
        ac = (c[0]-a[0], c[1]-a[1], c[2]-a[2])
        # Cross product magnitude / 2
        cx = ab[1]*ac[2] - ab[2]*ac[1]
        cy = ab[2]*ac[0] - ab[0]*ac[2]
        cz = ab[0]*ac[1] - ab[1]*ac[0]
        area = math.sqrt(cx*cx + cy*cy + cz*cz) / 2.0
        total += area
    return total


def marching_cubes_simple(
    volume: List[List[List[float]]],
    isovalue: float = 0.0,
) -> List[Tuple[Tuple[float, float, float], ...]]:
    """ marching cubes on a 3D numpy-like volume list.

    Args:
        volume: 3D list of floats [z][y][x].
        isovalue: Surface threshold.

    Returns:
        List of triangles.
    """
    nz = len(volume)
    ny = len(volume[0]) if nz > 0 else 0
    nx = len(volume[0][0]) if ny > 0 else 0
    if nx < 2 or ny < 2 or nz < 2:
        return []

    def getter(x: int, y: int, z: int) -> float:
        if 0 <= x < nx and 0 <= y < ny and 0 <= z < nz:
            return volume[z][y][x]
        return 0.0

    return marching_cubes(getter, (nx, ny, nz), isovalue)
