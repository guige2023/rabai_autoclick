"""Heatmap generation utilities for RabAI AutoClick.

Provides:
- Click/touch heatmap generation
- Density visualization
- Color mapping utilities
"""

from typing import Dict, List, Optional, Tuple, Any
import math


def generate_click_heatmap(
    points: List[Tuple[float, float]],
    width: int = 1920,
    height: int = 1080,
    radius: int = 30,
    intensity: float = 1.0,
) -> List[List[float]]:
    """Generate a heatmap grid from click points.

    Args:
        points: List of (x, y) click coordinates.
        width: Grid width in pixels.
        height: Grid height in pixels.
        radius: Influence radius for each point.
        intensity: Base intensity multiplier.

    Returns:
        2D list of heat values (normalized 0-1).
    """
    grid: List[List[float]] = [[0.0] * width for _ in range(height)]
    cell_w = width // 100
    cell_h = height // 100
    grid_w = 100
    grid_h = 100

    for px, py in points:
        cx = int(px / width * grid_w)
        cy = int(py / height * grid_h)
        r = max(1, int(radius / width * grid_w))

        for dy in range(-r, r + 1):
            for dx in range(-r, r + 1):
                nx, ny = cx + dx, cy + dy
                if 0 <= nx < grid_w and 0 <= ny < grid_h:
                    dist = math.sqrt(dx * dx + dy * dy)
                    if dist <= r:
                        falloff = 1.0 - (dist / r)
                        grid[ny][nx] += intensity * falloff * falloff

    max_val = max(max(row) for row in grid) if grid else 1.0
    if max_val > 0:
        for y in range(grid_h):
            for x in range(grid_w):
                grid[y][x] /= max_val

    return grid


def heatmap_to_color(
    value: float,
    colormap: str = "hot",
) -> Tuple[int, int, int]:
    """Map a normalized heat value to an RGB color.

    Args:
        value: Normalized heat (0.0 - 1.0).
        colormap: Color map name ('hot', 'cool', 'jet', 'viridis').

    Returns:
        Tuple of (R, G, B) values (0-255).
    """
    value = max(0.0, min(1.0, value))

    if colormap == "hot":
        if value < 0.333:
            return (int(value * 3 * 255), 0, 0)
        elif value < 0.666:
            return (255, int((value - 0.333) * 3 * 255), 0)
        else:
            return (255, 255, int((value - 0.666) * 3 * 255))

    elif colormap == "cool":
        r = int(value * 255)
        b = int((1.0 - value) * 255)
        return (r, int(255 * math.sin(value * math.pi)), b)

    elif colormap == "jet":
        if value < 0.25:
            return (0, int(value * 4 * 255), 255)
        elif value < 0.5:
            return (0, 255, int(255 - (value - 0.25) * 4 * 255))
        elif value < 0.75:
            return (int((value - 0.5) * 4 * 255), 255, 0)
        else:
            return (255, int(255 - (value - 0.75) * 4 * 255), 0)

    else:  # viridis fallback
        r = int((0.267 + 0.283 * value) * 255)
        g = int((0.004 + 0.503 * value) * 255)
        b = int((0.329 + 0.497 * value) * 255)
        return (r, g, b)


def render_heatmap_image(
    heatmap: List[List[float]],
    colormap: str = "hot",
    alpha: float = 0.7,
) -> List[List[Tuple[int, int, int, int]]]:
    """Render a heatmap grid as RGBA pixel data.

    Args:
        heatmap: 2D heat values.
        colormap: Color map for coloring.
        alpha: Opacity (0.0 transparent, 1.0 opaque).

    Returns:
        2D grid of RGBA tuples.
    """
    height = len(heatmap)
    width = len(heatmap[0]) if height > 0 else 0
    result: List[List[Tuple[int, int, int, int]]] = []

    for y in range(height):
        row: List[Tuple[int, int, int, int]] = []
        for x in range(width):
            r, g, b = heatmap_to_color(heatmap[y][x], colormap)
            a = int(alpha * heatmap[y][x] * 255) if heatmap[y][x] > 0 else 0
            row.append((r, g, b, max(a, 0)))
        result.append(row)

    return result


def compute_hotspot_regions(
    heatmap: List[List[float]],
    threshold: float = 0.7,
    min_points: int = 3,
) -> List[Dict[str, Any]]:
    """Extract hotspot regions from a heatmap.

    Args:
        heatmap: 2D normalized heat values.
        threshold: Value threshold for hotspot detection.
        min_points: Minimum points to form a hotspot.

    Returns:
        List of hotspot region dicts with bounds and center.
    """
    height = len(heatmap)
    width = len(heatmap[0]) if height > 0 else 0
    visited: List[List[bool]] = [[False] * width for _ in range(height)]
    regions: List[Dict[str, Any]] = []

    def flood_fill(x: int, y: int) -> List[Tuple[int, int]]:
        if not (0 <= x < width and 0 <= y < height):
            return []
        if visited[y][x] or heatmap[y][x] < threshold:
            return []
        visited[y][x] = True
        points = [(x, y)]
        points += flood_fill(x + 1, y)
        points += flood_fill(x - 1, y)
        points += flood_fill(x, y + 1)
        points += flood_fill(x, y - 1)
        return points

    for y in range(height):
        for x in range(width):
            if not visited[y][x] and heatmap[y][x] >= threshold:
                region_points = flood_fill(x, y)
                if len(region_points) >= min_points:
                    xs = [p[0] for p in region_points]
                    ys = [p[1] for p in region_points]
                    regions.append({
                        "center_x": sum(xs) / len(xs),
                        "center_y": sum(ys) / len(ys),
                        "min_x": min(xs),
                        "min_y": min(ys),
                        "max_x": max(xs),
                        "max_y": max(ys),
                        "width": max(xs) - min(xs) + 1,
                        "height": max(ys) - min(ys) + 1,
                        "count": len(region_points),
                        "avg_intensity": sum(heatmap[py][px] for px, py in region_points) / len(region_points),
                    })

    return sorted(regions, key=lambda r: r["count"], reverse=True)


def blend_heatmaps(
    heatmaps: List[List[List[float]]],
    weights: Optional[List[float]] = None,
) -> List[List[float]]:
    """Blend multiple heatmaps with optional weights.

    Args:
        heatmaps: List of 2D heatmap grids.
        weights: Per-heatmap weight (default equal).

    Returns:
        Blended 2D heatmap.
    """
    if not heatmaps:
        return []
    if len(heatmaps) == 1:
        return heatmaps[0]

    height = len(heatmaps[0])
    width = len(heatmaps[0][0]) if height > 0 else 0

    if weights is None:
        weights = [1.0 / len(heatmaps)] * len(heatmaps)
    else:
        total = sum(weights)
        weights = [w / total for w in weights]

    blended: List[List[float]] = [[0.0] * width for _ in range(height)]
    for hm, w in zip(heatmaps, weights):
        for y in range(height):
            for x in range(width):
                if y < len(hm) and x < len(hm[y]):
                    blended[y][x] += hm[y][x] * w

    max_val = max(max(row) for row in blended) if blended else 1.0
    if max_val > 0:
        for y in range(height):
            for x in range(width):
                blended[y][x] /= max_val

    return blended


def spatial_smooth_heatmap(
    heatmap: List[List[float]],
    kernel_size: int = 3,
    iterations: int = 1,
) -> List[List[float]]:
    """Apply Gaussian-like smoothing to a heatmap.

    Args:
        heatmap: Input 2D heat values.
        kernel_size: Smoothing kernel size (odd).
        iterations: Number of smoothing passes.

    Returns:
        Smoothed heatmap.
    """
    height = len(heatmap)
    width = len(heatmap[0]) if height > 0 else 0
    result = [row[:] for row in heatmap]

    for _ in range(iterations):
        smoothed: List[List[float]] = [[0.0] * width for _ in range(height)]
        half = kernel_size // 2

        for y in range(height):
            for x in range(width):
                total = 0.0
                count = 0
                for dy in range(-half, half + 1):
                    for dx in range(-half, half + 1):
                        nx, ny = x + dx, y + dy
                        if 0 <= nx < width and 0 <= ny < height:
                            weight = 1.0 / (1 + abs(dx) + abs(dy))
                            total += result[ny][nx] * weight
                            count += weight
                smoothed[y][x] = total / count if count > 0 else 0.0

        result = smoothed

    return result
