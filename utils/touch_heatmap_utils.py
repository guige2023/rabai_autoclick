"""
Touch Heatmap Utilities for UI Automation.

This module provides utilities for building and analyzing
touch heatmaps in UI automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from enum import Enum


@dataclass
class HeatmapPoint:
    """A point in a heatmap with intensity value."""
    x: float
    y: float
    intensity: float
    timestamp: float


@dataclass
class HeatmapCell:
    """A cell in a grid-based heatmap."""
    x_index: int
    y_index: int
    total_intensity: float = 0.0
    touch_count: int = 0
    avg_pressure: float = 0.0
    max_intensity: float = 0.0


@dataclass
class HeatmapConfig:
    """Configuration for heatmap generation."""
    grid_width: int = 50
    grid_height: int = 50
    radius: float = 20.0
    decay_factor: float = 0.95
    max_intensity: float = 1.0
    normalization_enabled: bool = True


class TouchHeatmap:
    """Builds and analyzes touch heatmaps."""

    def __init__(
        self,
        screen_width: float,
        screen_height: float,
        config: Optional[HeatmapConfig] = None,
    ) -> None:
        self._screen_width = screen_width
        self._screen_height = screen_height
        self._config = config or HeatmapConfig()
        self._cells: Dict[Tuple[int, int], HeatmapCell] = {}
        self._raw_points: List[HeatmapPoint] = []
        self._grid: List[List[float]] = []
        self._initialize_grid()

    def _initialize_grid(self) -> None:
        """Initialize the heatmap grid."""
        self._grid = [
            [0.0] * self._config.grid_width
            for _ in range(self._config.grid_height)
        ]

    def add_touch(
        self,
        x: float,
        y: float,
        intensity: float = 1.0,
        pressure: float = 0.5,
        timestamp: Optional[float] = None,
    ) -> None:
        """Add a touch point to the heatmap."""
        if timestamp is None:
            timestamp = time.time()

        point = HeatmapPoint(x=x, y=y, intensity=intensity, timestamp=timestamp)
        self._raw_points.append(point)

        cell_x = int(x / self._screen_width * self._config.grid_width)
        cell_y = int(y / self._screen_height * self._config.grid_height)

        cell_x = max(0, min(self._config.grid_width - 1, cell_x))
        cell_y = max(0, min(self._config.grid_height - 1, cell_y))

        cell = self._cells.get((cell_x, cell_y))
        if cell is None:
            cell = HeatmapCell(x_index=cell_x, y_index=cell_y)
            self._cells[(cell_x, cell_y)] = cell

        cell.touch_count += 1
        cell.total_intensity += intensity
        cell.max_intensity = max(cell.max_intensity, intensity)
        cell.avg_pressure = (cell.avg_pressure * (cell.touch_count - 1) + pressure) / cell.touch_count

        self._grid[cell_y][cell_x] += intensity

    def add_touch_radial(
        self,
        x: float,
        y: float,
        intensity: float = 1.0,
        radius: Optional[float] = None,
    ) -> None:
        """Add a touch with radial falloff to surrounding cells."""
        if radius is None:
            radius = self._config.radius

        cell_radius = int(radius / self._screen_width * self._config.grid_width)
        center_cell_x = int(x / self._screen_width * self._config.grid_width)
        center_cell_y = int(y / self._screen_height * self._config.grid_height)

        for dy in range(-cell_radius, cell_radius + 1):
            for dx in range(-cell_radius, cell_radius + 1):
                cell_x = center_cell_x + dx
                cell_y = center_cell_y + dy

                if cell_x < 0 or cell_x >= self._config.grid_width:
                    continue
                if cell_y < 0 or cell_y >= self._config.grid_height:
                    continue

                dist = math.sqrt(dx * dx + dy * dy)
                if dist > cell_radius:
                    continue

                falloff = 1.0 - (dist / cell_radius)
                self._grid[cell_y][cell_x] += intensity * falloff

    def get_cell(self, x: float, y: float) -> Optional[HeatmapCell]:
        """Get the heatmap cell at the given coordinates."""
        cell_x = int(x / self._screen_width * self._config.grid_width)
        cell_y = int(y / self._screen_height * self._config.grid_height)
        return self._cells.get((cell_x, cell_y))

    def get_intensity(self, x: float, y: float) -> float:
        """Get the intensity value at the given coordinates."""
        cell_x = int(x / self._screen_width * self._config.grid_width)
        cell_y = int(y / self._screen_height * self._config.grid_height)

        if cell_x < 0 or cell_x >= self._config.grid_width:
            return 0.0
        if cell_y < 0 or cell_y >= self._config.grid_height:
            return 0.0

        return self._grid[cell_y][cell_x]

    def get_hotspots(self, threshold: float = 0.8) -> List[Tuple[float, float]]:
        """Find hotspots above the given intensity threshold."""
        hotspots: List[Tuple[float, float]] = []

        for cell_x in range(self._config.grid_width):
            for cell_y in range(self._config.grid_height):
                if self._grid[cell_y][cell_x] >= threshold:
                    cx = (cell_x + 0.5) / self._config.grid_width * self._screen_width
                    cy = (cell_y + 0.5) / self._config.grid_height * self._screen_height
                    hotspots.append((cx, cy))

        return hotspots

    def normalize(self) -> None:
        """Normalize all intensity values to 0-1 range."""
        max_val = 0.0
        for row in self._grid:
            for val in row:
                max_val = max(max_val, val)

        if max_val > 0:
            for y in range(self._config.grid_height):
                for x in range(self._config.grid_width):
                    self._grid[y][x] /= max_val

    def get_grid(self) -> List[List[float]]:
        """Get the raw heatmap grid."""
        return [row[:] for row in self._grid]

    def get_point_count(self) -> int:
        """Get the number of raw touch points."""
        return len(self._raw_points)

    def clear(self) -> None:
        """Clear all heatmap data."""
        self._raw_points.clear()
        self._cells.clear()
        self._initialize_grid()


def create_touch_heatmap(
    screen_width: float,
    screen_height: float,
    **kwargs: Any,
) -> TouchHeatmap:
    """Create a touch heatmap with the specified screen dimensions."""
    config = HeatmapConfig(
        grid_width=kwargs.get("grid_width", 50),
        grid_height=kwargs.get("grid_height", 50),
        radius=kwargs.get("radius", 20.0),
        decay_factor=kwargs.get("decay_factor", 0.95),
    )
    return TouchHeatmap(screen_width, screen_height, config)
