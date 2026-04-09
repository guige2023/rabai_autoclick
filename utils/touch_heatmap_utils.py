"""
Touch heatmap generation utilities.

Generate and analyze heatmaps of touch interactions.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class HeatmapPoint:
    """A point in a touch heatmap."""
    x: float
    y: float
    intensity: float = 1.0


@dataclass
class GridCell:
    """A cell in the heatmap grid."""
    x: int
    y: int
    total_intensity: float = 0.0
    hit_count: int = 0


class TouchHeatmap:
    """Generate heatmap from touch points."""
    
    def __init__(self, width: int, height: int, cell_size: int = 10):
        self.width = width
        self.height = height
        self.cell_size = cell_size
        self.cols = (width + cell_size - 1) // cell_size
        self.rows = (height + cell_size - 1) // cell_size
        self._grid: list[list[GridCell]] = [
            [GridCell(x=c, y=r) for c in range(self.cols)]
            for r in range(self.rows)
        ]
        self._points: list[HeatmapPoint] = []
    
    def add_point(self, x: float, y: float, intensity: float = 1.0) -> None:
        """Add a point to the heatmap."""
        self._points.append(HeatmapPoint(x, y, intensity))
        
        col = int(x / self.cell_size)
        row = int(y / self.cell_size)
        
        if 0 <= col < self.cols and 0 <= row < self.rows:
            cell = self._grid[row][col]
            cell.total_intensity += intensity
            cell.hit_count += 1
    
    def add_path(self, points: list[tuple[float, float]], intensity: float = 1.0) -> None:
        """Add a path of points to the heatmap."""
        for x, y in points:
            self.add_point(x, y, intensity)
    
    def get_cell(self, col: int, row: int) -> Optional[GridCell]:
        """Get a specific grid cell."""
        if 0 <= col < self.cols and 0 <= row < self.rows:
            return self._grid[row][col]
        return None
    
    def get_intensity_at(self, x: float, y: float) -> float:
        """Get interpolated intensity at a point."""
        col = x / self.cell_size
        row = y / self.cell_size
        
        col0 = int(col)
        row0 = int(row)
        
        fx = col - col0
        fy = row - row0
        
        intensity = 0.0
        total_weight = 0.0
        
        for dy in range(2):
            for dx in range(2):
                c = col0 + dx
                r = row0 + dy
                
                if 0 <= c < self.cols and 0 <= r < self.rows:
                    cell = self._grid[r][c]
                    weight = (1 - abs(dx - fx)) * (1 - abs(dy - fy))
                    intensity += cell.total_intensity * weight
                    total_weight += weight
        
        return intensity / total_weight if total_weight > 0 else 0.0
    
    def get_hotspots(self, threshold: float = 0.5) -> list[GridCell]:
        """Get cells above intensity threshold."""
        hotspots = []
        for row in self._grid:
            for cell in row:
                max_possible = self.cell_size * self.cell_size
                normalized = cell.total_intensity / max_possible if max_possible > 0 else 0
                if normalized >= threshold:
                    hotspots.append(cell)
        
        return hotspots
    
    def get_centroid(self) -> tuple[float, float]:
        """Get the centroid of all touch points."""
        if not self._points:
            return 0.0, 0.0
        
        total_x = sum(p.x * p.intensity for p in self._points)
        total_y = sum(p.y * p.intensity for p in self._points)
        total_intensity = sum(p.intensity for p in self._points)
        
        if total_intensity == 0:
            return 0.0, 0.0
        
        return total_x / total_intensity, total_y / total_intensity


class HeatmapRenderer:
    """Render heatmap to image data."""
    
    @staticmethod
    def render_to_grayscale(heatmap: TouchHeatmap) -> list[list[int]]:
        """Render heatmap as grayscale values (0-255)."""
        max_intensity = 0.0
        for row in heatmap._grid:
            for cell in row:
                if cell.total_intensity > max_intensity:
                    max_intensity = cell.total_intensity
        
        if max_intensity == 0:
            return [[0] * heatmap.cols for _ in range(heatmap.rows)]
        
        result = []
        for row in heatmap._grid:
            row_data = []
            for cell in row:
                normalized = cell.total_intensity / max_intensity
                gray = int(255 * normalized)
                row_data.append(gray)
            result.append(row_data)
        
        return result
    
    @staticmethod
    def render_to_rgb(heatmap: TouchHeatmap) -> list[list[tuple[int, int, int]]]:
        """Render heatmap as RGB values (cold to hot)."""
        max_intensity = 0.0
        for row in heatmap._grid:
            for cell in row:
                if cell.total_intensity > max_intensity:
                    max_intensity = cell.total_intensity
        
        if max_intensity == 0:
            return [[(0, 0, 0)] * heatmap.cols for _ in range(heatmap.rows)]
        
        result = []
        for row in heatmap._grid:
            row_data = []
            for cell in row:
                normalized = cell.total_intensity / max_intensity
                
                if normalized < 0.5:
                    t = normalized * 2
                    r = int(0)
                    g = int(255 * t)
                    b = int(255 * (1 - t))
                else:
                    t = (normalized - 0.5) * 2
                    r = int(255 * t)
                    g = int(255 * (1 - t))
                    b = int(0)
                
                row_data.append((r, g, b))
            result.append(row_data)
        
        return result
