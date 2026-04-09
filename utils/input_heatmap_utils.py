"""Input Heatmap Utilities.

Generates heatmaps from input/touch data for visualization.

Example:
    >>> from input_heatmap_utils import InputHeatmapGenerator
    >>> gen = InputHeatmapGenerator(width=1920, height=1080)
    >>> gen.add_points([(100, 200), (150, 250)])
    >>> img = gen.generate()
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Tuple


@dataclass
class HeatmapPoint:
    """A point in the heatmap."""
    x: float
    y: float
    intensity: float = 1.0


class InputHeatmapGenerator:
    """Generates heatmaps from input points."""

    def __init__(self, width: int = 1920, height: int = 1080, radius: int = 30):
        """Initialize heatmap generator.

        Args:
            width: Heatmap width in pixels.
            height: Heatmap height in pixels.
            radius: Influence radius for each point.
        """
        self.width = width
        self.height = height
        self.radius = radius
        self.points: List[HeatmapPoint] = []

    def add_point(self, x: float, y: float, intensity: float = 1.0) -> None:
        """Add a point to the heatmap.

        Args:
            x: X coordinate.
            y: Y coordinate.
            intensity: Point intensity (0.0 to 1.0).
        """
        self.points.append(HeatmapPoint(x, y, max(0.0, min(1.0, intensity))))

    def add_points(self, points: List[Tuple[float, float]], intensity: float = 1.0) -> None:
        """Add multiple points.

        Args:
            points: List of (x, y) tuples.
            intensity: Default intensity for all points.
        """
        for x, y in points:
            self.add_point(x, y, intensity)

    def generate(self) -> List[List[float]]:
        """Generate heatmap as 2D grid.

        Returns:
            2D list of heat values (0.0 to 1.0).
        """
        grid_x = max(1, self.width // 10)
        grid_y = max(1, self.height // 10)
        grid = [[0.0 for _ in range(grid_x)] for _ in range(grid_y)]

        cell_w = self.width / grid_x
        cell_h = self.height / grid_y

        for point in self.points:
            cx = int(point.x / cell_w)
            cy = int(point.y / cell_h)
            r = max(1, self.radius // max(int(cell_w), int(cell_h)))

            for dy in range(-r, r + 1):
                for dx in range(-r, r + 1):
                    nx, ny = cx + dx, cy + dy
                    if 0 <= nx < grid_x and 0 <= ny < grid_y:
                        dist = math.sqrt(dx * dx + dy * dy)
                        if dist <= r:
                            falloff = 1.0 - (dist / r)
                            grid[ny][nx] += falloff * point.intensity

        max_val = max(max(row) for row in grid) if grid else 1.0
        if max_val > 0:
            grid = [[v / max_val for v in row] for row in grid]

        return grid

    def get_hotspots(self, threshold: float = 0.7) -> List[Tuple[int, int]]:
        """Get hotspot coordinates.

        Args:
            threshold: Minimum intensity for hotspot.

        Returns:
            List of (x, y) coordinates above threshold.
        """
        grid = self.generate()
        hotspots = []
        cell_w = self.width / len(grid[0])
        cell_h = self.height / len(grid)
        for y, row in enumerate(grid):
            for x, val in enumerate(row):
                if val >= threshold:
                    hotspots.append((int(x * cell_w), int(y * cell_h)))
        return hotspots
