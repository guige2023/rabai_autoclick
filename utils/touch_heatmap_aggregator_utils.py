"""
Touch heatmap aggregation utilities.

This module provides utilities for aggregating touch points into
heatmaps for visualizing interaction density patterns.
"""

from __future__ import annotations

import math
from typing import List, Tuple, Dict, Optional, Any
from dataclasses import dataclass, field


# Type aliases
Point2D = Tuple[float, float]
TouchPoint = Tuple[float, float, float]  # x, y, pressure


@dataclass
class HeatmapCell:
    """A single cell in the heatmap grid."""
    x: int
    y: int
    intensity: float = 0.0
    hit_count: int = 0
    total_pressure: float = 0.0


@dataclass
class HeatmapConfig:
    """Configuration for heatmap generation."""
    cell_size: int = 10
    radius: int = 2
    decay: str = "gaussian"  # gaussian, linear, exponential
    intensity_scale: float = 1.0
    min_intensity: float = 0.0


class TouchHeatmap:
    """Manages a touch interaction heatmap."""

    def __init__(self, width: int, height: int, config: Optional[HeatmapConfig] = None):
        self.width = width
        self.height = height
        self.config = config or HeatmapConfig()
        self._cells: Dict[Tuple[int, int], HeatmapCell] = {}

        # Calculate grid dimensions
        self.grid_width = (width + self.config.cell_size - 1) // self.config.cell_size
        self.grid_height = (height + self.config.cell_size - 1) // self.config.cell_size

    def add_touch_point(self, x: float, y: float, pressure: float = 1.0) -> None:
        """
        Add a touch point to the heatmap.

        Args:
            x: X coordinate.
            y: Y coordinate.
            pressure: Touch pressure (0-1).
        """
        # Convert to grid coordinates
        gx = int(x / self.config.cell_size)
        gy = int(y / self.config.cell_size)

        # Add to radius范围内的所有cells
        radius = self.config.radius
        for dx in range(-radius, radius + 1):
            for dy in range(-radius, radius + 1):
                cx, cy = gx + dx, gy + dy
                if cx < 0 or cx >= self.grid_width or cy < 0 or cy >= self.grid_height:
                    continue

                key = (cx, cy)
                if key not in self._cells:
                    self._cells[key] = HeatmapCell(x=cx, y=cy)

                cell = self._cells[key]
                cell.hit_count += 1
                cell.total_pressure += pressure

                # Calculate distance-based intensity contribution
                dist = math.sqrt(dx * dx + dy * dy)
                if dist <= radius:
                    if self.config.decay == "gaussian":
                        contrib = math.exp(-dist * dist / (2 * (radius / 2) ** 2))
                    elif self.config.decay == "linear":
                        contrib = max(0.0, 1.0 - dist / radius)
                    else:  # exponential
                        contrib = math.exp(-dist)
                    cell.intensity += contrib * pressure * self.config.intensity_scale

    def add_touch_batch(self, points: List[TouchPoint]) -> None:
        """Add multiple touch points at once."""
        for x, y, pressure in points:
            self.add_touch_point(x, y, pressure)

    def get_intensity_at(self, x: float, y: float) -> float:
        """Get interpolated intensity at a specific point."""
        gx = int(x / self.config.cell_size)
        gy = int(y / self.config.cell_size)
        key = (gx, gy)
        if key in self._cells:
            return max(self.config.min_intensity, self._cells[key].intensity)
        return 0.0

    def get_hotspots(self, min_intensity: float = 1.0) -> List[Tuple[int, int, float]]:
        """
        Get cells with intensity above threshold.

        Returns:
            List of (grid_x, grid_y, intensity) tuples sorted by intensity.
        """
        hotspots = [
            (cell.x, cell.y, cell.intensity)
            for cell in self._cells.values()
            if cell.intensity >= min_intensity
        ]
        hotspots.sort(key=lambda h: h[2], reverse=True)
        return hotspots

    def get_max_intensity(self) -> float:
        """Get the maximum intensity in the heatmap."""
        if not self._cells:
            return 0.0
        return max(c.intensity for c in self._cells.values())

    def normalize(self, target_max: float = 1.0) -> None:
        """Normalize all intensities to 0-target_max range."""
        max_int = self.get_max_intensity()
        if max_int < 1e-10:
            return
        scale = target_max / max_int
        for cell in self._cells.values():
            cell.intensity *= scale

    def clear(self) -> None:
        """Clear all heatmap data."""
        self._cells.clear()

    def to_grid(self) -> List[List[float]]:
        """Export heatmap as 2D grid of intensities."""
        grid: List[List[float]] = [
            [0.0] * self.grid_width for _ in range(self.grid_height)
        ]
        for (gx, gy), cell in self._cells.items():
            if 0 <= gx < self.grid_width and 0 <= gy < self.grid_height:
                grid[gy][gx] = cell.intensity
        return grid

    def get_cell_bounds(self, gx: int, gy: int) -> Tuple[int, int, int, int]:
        """Get pixel bounds for a grid cell."""
        return (
            gx * self.config.cell_size,
            gy * self.config.cell_size,
            (gx + 1) * self.config.cell_size,
            (gy + 1) * self.config.cell_size,
        )
