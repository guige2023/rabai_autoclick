"""Touch heatmap aggregator for visualizing touch density."""
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class HeatmapCell:
    """Single cell in a heatmap grid."""
    x: int
    y: int
    count: int = 0
    total_pressure: float = 0.0


class TouchHeatmapAggregator:
    """Aggregates touch points into a density heatmap.
    
    Builds a grid-based heatmap showing where touches
    are most concentrated.
    
    Example:
        aggregator = TouchHeatmapAggregator(grid_size=20)
        aggregator.add_touch(100, 200, pressure=0.5)
        hot_spots = aggregator.get_hot_spots(threshold=0.8)
    """

    def __init__(self, grid_size: int = 20, width: int = 1920, height: int = 1080) -> None:
        self._grid_size = grid_size
        self._width = width
        self._height = height
        self._cols = (width + grid_size - 1) // grid_size
        self._rows = (height + grid_size - 1) // grid_size
        self._cells: Dict[Tuple[int, int], HeatmapCell] = {}
        self._total_touches = 0

    def add_touch(self, x: float, y: float, pressure: float = 1.0) -> None:
        """Add a touch point to the heatmap."""
        col = int(x // self._grid_size)
        row = int(y // self._grid_size)
        col = max(0, min(col, self._cols - 1))
        row = max(0, min(row, self._rows - 1))
        key = (col, row)
        if key not in self._cells:
            self._cells[key] = HeatmapCell(x=col, y=row)
        self._cells[key].count += 1
        self._cells[key].total_pressure += pressure
        self._total_touches += 1

    def add_touches(self, points: List[Tuple[float, float]]) -> None:
        """Add multiple touch points."""
        for x, y in points:
            self.add_touch(x, y)

    def get_density(self, col: int, row: int) -> float:
        """Get density (0-1) for a cell."""
        cell = self._cells.get((col, row))
        if not cell or self._total_touches == 0:
            return 0.0
        return cell.count / self._total_touches

    def get_hot_spots(self, threshold: float = 0.8) -> List[Tuple[int, int]]:
        """Get cell coordinates that exceed density threshold."""
        if not self._cells:
            return []
        max_count = max(c.count for c in self._cells.values())
        if max_count == 0:
            return []
        return [(col, row) for (col, row), cell in self._cells.items()
                if cell.count / max_count >= threshold]

    def get_grid(self) -> List[List[float]]:
        """Get the full heatmap as a 2D grid of densities."""
        return [[self.get_density(c, r) for c in range(self._cols)] for r in range(self._rows)]

    def clear(self) -> None:
        """Clear all heatmap data."""
        self._cells.clear()
        self._total_touches = 0

    def get_stats(self) -> Dict:
        """Get heatmap statistics."""
        if not self._cells:
            return {"total_touches": 0, "unique_cells": 0}
        counts = [c.count for c in self._cells.values()]
        return {
            "total_touches": self._total_touches,
            "unique_cells": len(self._cells),
            "avg_per_cell": sum(counts) / len(counts),
            "max_per_cell": max(counts),
        }
