"""Voxel grid utilities for RabAI AutoClick.

Provides:
- Voxel grid creation and manipulation
- 3D occupancy grids
- Ray casting through voxel grids
- Voxel neighbor queries
"""

from typing import List, Tuple, Optional, Set, Callable, Dict
from dataclasses import dataclass, field
import math


@dataclass
class Voxel:
    """A single voxel."""
    x: int
    y: int
    z: int
    value: float = 1.0


@dataclass
class VoxelGrid:
    """3D voxel grid."""
    width: int
    height: int
    depth: int
    voxels: Dict[Tuple[int, int, int], float] = field(default_factory=dict)

    def set_voxel(self, x: int, y: int, z: int, value: float = 1.0) -> None:
        """Set voxel value."""
        if 0 <= x < self.width and 0 <= y < self.height and 0 <= z < self.depth:
            self.voxels[(x, y, z)] = value

    def get_voxel(self, x: int, y: int, z: int) -> Optional[float]:
        """Get voxel value."""
        return self.voxels.get((x, y, z))

    def is_occupied(self, x: int, y: int, z: int) -> bool:
        """Check if voxel is occupied."""
        return (x, y, z) in self.voxels

    def clear(self) -> None:
        """Clear all voxels."""
        self.voxels.clear()


def world_to_voxel(
    wx: float, wy: float, wz: float,
    origin: Tuple[float, float, float],
    voxel_size: float,
) -> Tuple[int, int, int]:
    """Convert world coordinates to voxel indices.

    Args:
        wx, wy, wz: World coordinates.
        origin: Voxel grid origin.
        voxel_size: Size of each voxel.

    Returns:
        (vx, vy, vz) voxel indices.
    """
    return (
        int((wx - origin[0]) / voxel_size),
        int((wy - origin[1]) / voxel_size),
        int((wz - origin[2]) / voxel_size),
    )


def voxel_to_world(
    vx: int, vy: int, vz: int,
    origin: Tuple[float, float, float],
    voxel_size: float,
) -> Tuple[float, float, float]:
    """Convert voxel indices to world coordinates (center of voxel)."""
    return (
        origin[0] + (vx + 0.5) * voxel_size,
        origin[1] + (vy + 0.5) * voxel_size,
        origin[2] + (vz + 0.5) * voxel_size,
    )


def voxel_neighbors(
    x: int, y: int, z: int,
    connectivity: int = 6,
) -> List[Tuple[int, int, int]]:
    """Get neighboring voxel indices.

    Args:
        x, y, z: Center voxel.
        connectivity: 6 (face), 18 (face+edge), or 26 (all neighbors).

    Returns:
        List of neighbor coordinates.
    """
    neighbors: List[Tuple[int, int, int]] = []
    for dx in (-1, 0, 1):
        for dy in (-1, 0, 1):
            for dz in (-1, 0, 1):
                if dx == 0 and dy == 0 and dz == 0:
                    continue
                if connectivity == 6:
                    if sum([dx != 0, dy != 0, dz != 0]) > 1:
                        continue
                elif connectivity == 18:
                    if dx != 0 and dy != 0 and dz != 0:
                        continue
                neighbors.append((x + dx, y + dy, z + dz))
    return neighbors


def voxel_ray_cast(
    origin: Tuple[float, float, float],
    direction: Tuple[float, float, float],
    max_dist: float,
    get_voxel: Callable[[int, int, int], Optional[float]],
    max_steps: int = 1000,
) -> List[Tuple[int, int, int]]:
    """Cast ray through voxel grid using Bresenham-like algorithm.

    Args:
        origin: Ray origin in world coords.
        direction: Normalized direction vector.
        max_dist: Maximum ray length.
        get_voxel: Function(x, y, z) -> value or None.
        max_steps: Maximum iterations.

    Returns:
        List of visited voxel coordinates.
    """
    # Use DDA (Digital Differential Analyzer)
    voxel = world_to_voxel(origin[0], origin[1], origin[2], (0, 0, 0), 1.0)
    visited: List[Tuple[int, int, int]] = [voxel]

    step_x = 1 if direction[0] > 0 else -1
    step_y = 1 if direction[1] > 0 else -1
    step_z = 1 if direction[2] > 0 else -1

    t_delta_x = abs(1.0 / direction[0]) if direction[0] != 0 else float("inf")
    t_delta_y = abs(1.0 / direction[1]) if direction[1] != 0 else float("inf")
    t_delta_z = abs(1.0 / direction[2]) if direction[2] != 0 else float("inf")

    t_max_x = t_delta_x * (1.0 - (origin[0] % 1.0) if step_x > 0 else (origin[0] % 1.0))
    t_max_y = t_delta_y * (1.0 - (origin[1] % 1.0) if step_y > 0 else (origin[1] % 1.0))
    t_max_z = t_delta_z * (1.0 - (origin[2] % 1.0) if step_z > 0 else (origin[2] % 1.0))

    t = 0.0
    for _ in range(max_steps):
        if t > max_dist:
            break
        if get_voxel(*voxel) is not None:
            break

        if t_max_x < t_max_y:
            if t_max_x < t_max_z:
                voxel = (voxel[0] + step_x, voxel[1], voxel[2])
                t = t_max_x
                t_max_x += t_delta_x
            else:
                voxel = (voxel[0], voxel[1], voxel[2] + step_z)
                t = t_max_z
                t_max_z += t_delta_z
        else:
            if t_max_y < t_max_z:
                voxel = (voxel[0], voxel[1] + step_y, voxel[2])
                t = t_max_y
                t_max_y += t_delta_y
            else:
                voxel = (voxel[0], voxel[1], voxel[2] + step_z)
                t = t_max_z
                t_max_z += t_delta_z

        visited.append(voxel)

    return visited


def voxel_flood_fill(
    start: Tuple[int, int, int],
    is_free: Callable[[int, int, int], bool],
    bounds: Tuple[int, int, int],
) -> Set[Tuple[int, int, int]]:
    """Flood fill to find connected free space.

    Args:
        start: Starting voxel.
        is_free: Function to check if voxel is free.
        bounds: (max_x, max_y, max_z) grid bounds.

    Returns:
        Set of reachable voxels.
    """
    max_x, max_y, max_z = bounds
    visited: Set[Tuple[int, int, int]] = set()
    stack = [start]

    while stack:
        v = stack.pop()
        if v in visited:
            continue
        if not (0 <= v[0] < max_x and 0 <= v[1] < max_y and 0 <= v[2] < max_z):
            continue
        if not is_free(*v):
            continue
        visited.add(v)
        for n in voxel_neighbors(*v, connectivity=6):
            if n not in visited:
                stack.append(n)

    return visited


def fill_box_voxels(
    x0: int, y0: int, z0: int,
    x1: int, y1: int, z1: int,
    value: float = 1.0,
) -> List[Tuple[int, int, int, float]]:
    """Generate voxel coordinates for a filled box.

    Args:
        x0, y0, z0: Box minimum corner.
        x1, y1, z1: Box maximum corner (exclusive).
        value: Voxel value.

    Returns:
        List of (x, y, z, value) tuples.
    """
    voxels: List[Tuple[int, int, int, float]] = []
    for z in range(z0, z1):
        for y in range(y0, y1):
            for x in range(x0, x1):
                voxels.append((x, y, z, value))
    return voxels


def marching_voxels_2d(
    grid: List[List[int]],
    threshold: int = 0,
) -> List[Tuple[int, int]]:
    """2D marching squares to extract boundary contours as voxels.

    Args:
        grid: 2D grid of values.
        threshold: Value threshold for boundary.

    Returns:
        List of boundary voxel coordinates.
    """
    if not grid or not grid[0]:
        return []

    height = len(grid)
    width = len(grid[0])
    boundary: List[Tuple[int, int]] = []

    for y in range(height):
        for x in range(width):
            is_above = grid[y][x] >= threshold
            has_below_neighbor = (
                (x > 0 and grid[y][x - 1] < threshold) or
                (x < width - 1 and grid[y][x + 1] < threshold) or
                (y > 0 and grid[y - 1][x] < threshold) or
                (y < height - 1 and grid[y + 1][x] < threshold)
            )
            if is_above and has_below_neighbor:
                boundary.append((x, y))

    return boundary
