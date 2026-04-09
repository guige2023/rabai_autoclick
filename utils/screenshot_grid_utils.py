"""
Screenshot Grid Analysis Utilities

Provides utilities for analyzing screenshots using grid-based
region decomposition and comparison.
"""

from typing import List, Tuple, Optional, Dict, Callable
from dataclasses import dataclass
from enum import Enum
import math


class GridDivisionStrategy(Enum):
    """Strategies for dividing screen into grid regions."""
    FIXED = "fixed"
    ADAPTIVE = "adaptive"
    QUADTREE = "quadtree"
    CONTENT_AWARE = "content_aware"


@dataclass(frozen=True)
class GridRegion:
    """Represents a rectangular region in a grid."""
    row: int
    col: int
    x: float
    y: float
    width: float
    height: float
    
    @property
    def center(self) -> Tuple[float, float]:
        """Get center coordinates of the region."""
        return (self.x + self.width / 2, self.y + self.height / 2)
    
    @property
    def corners(self) -> List[Tuple[float, float]]:
        """Get corner coordinates of the region."""
        return [
            (self.x, self.y),
            (self.x + self.width, self.y),
            (self.x + self.width, self.y + self.height),
            (self.x, self.y + self.height)
        ]
    
    def contains_point(self, x: float, y: float) -> bool:
        """Check if a point is within this region."""
        return self.x <= x < self.x + self.width and self.y <= y < self.y + self.height
    
    def overlaps(self, other: "GridRegion") -> bool:
        """Check if this region overlaps with another."""
        return not (
            self.x + self.width <= other.x or
            other.x + other.width <= self.x or
            self.y + self.height <= other.y or
            other.y + other.height <= self.y
        )
    
    def intersection(self, other: "GridRegion") -> Optional["GridRegion"]:
        """Get the intersection region with another grid region."""
        if not self.overlaps(other):
            return None
        
        x = max(self.x, other.x)
        y = max(self.y, other.y)
        width = min(self.x + self.width, other.x + other.width) - x
        height = min(self.y + self.height, other.y + other.height) - y
        
        return GridRegion(
            row=min(self.row, other.row),
            col=min(self.col, other.col),
            x=x, y=y, width=width, height=height
        )


@dataclass
class GridStats:
    """Statistics for a grid cell."""
    cell_index: int
    row: int
    col: int
    mean_intensity: float
    variance: float
    edge_density: float
    is_uniform: bool = False


class ScreenGridAnalyzer:
    """
    Analyzes screenshots using grid-based decomposition.
    
    Divides screen into regions for localized analysis, change detection,
    and feature extraction.
    
    Example:
        >>> analyzer = ScreenGridAnalyzer(rows=4, cols=4)
        >>> regions = analyzer.get_regions()
        >>> print(f"Total regions: {len(regions)}")
    """
    
    def __init__(
        self,
        screen_width: float,
        screen_height: float,
        rows: int = 4,
        cols: int = 4,
        strategy: GridDivisionStrategy = GridDivisionStrategy.FIXED
    ) -> None:
        """
        Initialize screen grid analyzer.
        
        Args:
            screen_width: Total screen width in pixels.
            screen_height: Total screen height in pixels.
            rows: Number of grid rows.
            cols: Number of grid columns.
            strategy: Grid division strategy.
        """
        self._width = screen_width
        self._height = screen_height
        self._rows = rows
        self._cols = cols
        self._strategy = strategy
        self._regions: List[GridRegion] = []
        self._build_regions()
    
    def _build_regions(self) -> None:
        """Build grid region definitions."""
        cell_width = self._width / self._cols
        cell_height = self._height / self._rows
        
        self._regions = []
        for row in range(self._rows):
            for col in range(self._cols):
                region = GridRegion(
                    row=row,
                    col=col,
                    x=col * cell_width,
                    y=row * cell_height,
                    width=cell_width,
                    height=cell_height
                )
                self._regions.append(region)
    
    def get_regions(self) -> List[GridRegion]:
        """Get all grid regions."""
        return self._regions.copy()
    
    def get_region_at(
        self,
        row: int,
        col: int
    ) -> Optional[GridRegion]:
        """Get region at specific grid coordinates."""
        index = row * self._cols + col
        if 0 <= index < len(self._regions):
            return self._regions[index]
        return None
    
    def get_region_containing_point(
        self,
        x: float,
        y: float
    ) -> Optional[GridRegion]:
        """Get the region containing a specific point."""
        for region in self._regions:
            if region.contains_point(x, y):
                return region
        return None
    
    def get_neighbor_regions(
        self,
        row: int,
        col: int,
        radius: int = 1
    ) -> List[GridRegion]:
        """Get neighboring regions within a radius."""
        neighbors: List[GridRegion] = []
        for r in range(max(0, row - radius), min(self._rows, row + radius + 1)):
            for c in range(max(0, col - radius), min(self._cols, col + radius + 1)):
                if r != row or c != col:
                    if (region := self.get_region_at(r, c)) is not None:
                        neighbors.append(region)
        return neighbors
    
    def get_region_row(self, row: int) -> List[GridRegion]:
        """Get all regions in a specific row."""
        return [r for r in self._regions if r.row == row]
    
    def get_region_col(self, col: int) -> List[GridRegion]:
        """Get all regions in a specific column."""
        return [r for r in self._regions if r.col == col]


class AdaptiveGridAnalyzer(ScreenGridAnalyzer):
    """
    Adaptive grid analyzer that adjusts division based on content.
    
    Uses content analysis to determine grid boundaries for more
    meaningful region decomposition.
    """
    
    def __init__(
        self,
        screen_width: float,
        screen_height: float,
        max_rows: int = 8,
        max_cols: int = 8,
        min_cell_size: float = 50.0
    ) -> None:
        """
        Initialize adaptive grid analyzer.
        
        Args:
            screen_width: Total screen width in pixels.
            screen_height: Total screen height in pixels.
            max_rows: Maximum number of rows to create.
            max_cols: Maximum number of columns to create.
            min_cell_size: Minimum cell size in pixels.
        """
        self._max_rows = max_rows
        self._max_cols = max_cols
        self._min_cell_size = min_cell_size
        super().__init__(
            screen_width,
            screen_height,
            rows=min(max_rows, int(screen_height / min_cell_size)),
            cols=min(max_cols, int(screen_width / min_cell_size)),
            strategy=GridDivisionStrategy.ADAPTIVE
        )


class QuadTreeGridAnalyzer:
    """
    Quadtree-based grid analyzer for hierarchical region decomposition.
    
    Recursively divides screen into quadrants for multi-resolution
    analysis and change detection.
    """
    
    def __init__(
        self,
        screen_width: float,
        screen_height: float,
        max_depth: int = 4,
        min_cell_size: float = 20.0
    ) -> None:
        """
        Initialize quadtree grid analyzer.
        
        Args:
            screen_width: Total screen width in pixels.
            screen_height: Total screen height in pixels.
            max_depth: Maximum quadtree depth.
            min_cell_size: Minimum cell size before stopping subdivision.
        """
        self._width = screen_width
        self._height = screen_height
        self._max_depth = max_depth
        self._min_cell_size = min_cell_size
        self._root: QuadNode = QuadNode(0, 0, screen_width, screen_height, 0)
        self._build_tree()
    
    def _build_tree(self) -> None:
        """Build quadtree structure."""
        self._subdivide(self._root)
    
    def _subdivide(self, node: QuadNode) -> None:
        """Recursively subdivide a node."""
        if node.depth >= self._max_depth:
            return
        if node.width < self._min_cell_size * 2 or node.height < self._min_cell_size * 2:
            return
        
        half_w = node.width / 2
        half_h = node.height / 2
        
        node.children = [
            QuadNode(node.x, node.y, half_w, half_h, node.depth + 1),
            QuadNode(node.x + half_w, node.y, half_w, half_h, node.depth + 1),
            QuadNode(node.x, node.y + half_h, half_w, half_h, node.depth + 1),
            QuadNode(node.x + half_w, node.y + half_h, half_w, half_h, node.depth + 1)
        ]
        
        for child in node.children:
            self._subdivide(child)
    
    def get_all_cells(self) -> List[GridRegion]:
        """Get all leaf cells in the quadtree."""
        return self._collect_cells(self._root)
    
    def _collect_cells(self, node: QuadNode) -> List[GridRegion]:
        """Recursively collect all leaf cells."""
        if node.children is None:
            return [GridRegion(0, 0, node.x, node.y, node.width, node.height)]
        
        cells: List[GridRegion] = []
        for child in node.children:
            cells.extend(self._collect_cells(child))
        return cells


class QuadNode:
    """Represents a node in the quadtree."""
    
    def __init__(
        self,
        x: float,
        y: float,
        width: float,
        height: float,
        depth: int
    ) -> None:
        self.x = x
        self.y = y
        self.width = width
        self.height = height
        self.depth = depth
        self.children: Optional[List[QuadNode]] = None
    
    @property
    def center(self) -> Tuple[float, float]:
        """Get center of the node."""
        return (self.x + self.width / 2, self.y + self.height / 2)


def create_grid_analyzer(
    screen_width: float,
    screen_height: float,
    strategy: GridDivisionStrategy = GridDivisionStrategy.FIXED,
    rows: int = 4,
    cols: int = 4
) -> ScreenGridAnalyzer:
    """
    Factory function to create a grid analyzer.
    
    Args:
        screen_width: Screen width in pixels.
        screen_height: Screen height in pixels.
        strategy: Grid division strategy.
        rows: Number of rows (for FIXED strategy).
        cols: Number of columns (for FIXED strategy).
        
    Returns:
        Configured ScreenGridAnalyzer instance.
    """
    if strategy == GridDivisionStrategy.QUADTREE:
        return QuadTreeGridAnalyzer(screen_width, screen_height)
    elif strategy == GridDivisionStrategy.ADAPTIVE:
        return AdaptiveGridAnalyzer(screen_width, screen_height)
    else:
        return ScreenGridAnalyzer(
            screen_width,
            screen_height,
            rows=rows,
            cols=cols,
            strategy=strategy
        )


def calculate_grid_coverage(
    regions: List[GridRegion],
    total_area: float
) -> Dict[str, float]:
    """
    Calculate coverage statistics for grid regions.
    
    Args:
        regions: List of grid regions.
        total_area: Total screen area.
        
    Returns:
        Dictionary with coverage statistics.
    """
    covered_area = sum(r.width * r.height for r in regions)
    
    return {
        "total_regions": len(regions),
        "covered_area": covered_area,
        "coverage_ratio": covered_area / total_area if total_area > 0 else 0.0,
        "average_region_size": covered_area / len(regions) if regions else 0.0
    }


def merge_adjacent_regions(
    regions: List[GridRegion],
    threshold: float = 0.8
) -> List[GridRegion]:
    """
    Merge adjacent regions with similar properties.
    
    Args:
        regions: List of regions to merge.
        threshold: Similarity threshold (0.0 to 1.0).
        
    Returns:
        List of merged regions.
    """
    if len(regions) < 2:
        return regions
    
    merged: List[GridRegion] = regions.copy()
    changed = True
    
    while changed:
        changed = False
        new_merged: List[GridRegion] = []
        used = set()
        
        for i, region in enumerate(merged):
            if i in used:
                continue
            
            for j, other in enumerate(merged):
                if i >= j or j in used:
                    continue
                
                if (region.row == other.row and
                    abs(region.col - other.col) == 1 and
                    abs(region.height - other.height) < region.height * (1 - threshold)):
                    
                    new_region = GridRegion(
                        row=region.row,
                        col=min(region.col, other.col),
                        x=region.x,
                        y=region.y,
                        width=region.width + other.width,
                        height=region.height
                    )
                    new_merged.append(new_region)
                    used.add(i)
                    used.add(j)
                    changed = True
                    break
            
            if i not in used:
                new_merged.append(region)
        
        merged = new_merged
    
    return merged
