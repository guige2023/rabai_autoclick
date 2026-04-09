"""
UI Grid Utilities - Grid-based UI element organization and navigation.

This module provides utilities for working with grid-based UI layouts,
enabling grid navigation, cell identification, and grid-based element
targeting for automation workflows.

Author: rabai_autoclick team
License: MIT
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Callable, Iterator, Optional, Sequence


@dataclass
class GridCell:
    """Represents a single cell in a grid layout.
    
    Attributes:
        row: Zero-based row index.
        column: Zero-based column index.
        bounds: (x, y, width, height) tuple defining cell boundaries.
        element: Optional associated UI element.
    """
    row: int
    column: int
    bounds: tuple[int, int, int, int]
    element: Optional[object] = None


@dataclass
class GridConfig:
    """Configuration for grid layout.
    
    Attributes:
        rows: Number of rows in the grid.
        columns: Number of columns in the grid.
        cell_width: Width of each cell in pixels.
        cell_height: Height of each cell in pixels.
        origin_x: X coordinate of grid origin (top-left corner).
        origin_y: Y coordinate of grid origin (top-left corner).
        row_spacing: Vertical spacing between rows.
        column_spacing: Horizontal spacing between columns.
    """
    rows: int
    columns: int
    cell_width: int
    cell_height: int
    origin_x: int = 0
    origin_y: int = 0
    row_spacing: int = 0
    column_spacing: int = 0


class GridLayout:
    """Manages a grid-based layout for UI element organization.
    
    This class provides methods for creating grids, navigating between
    cells, and mapping coordinates to grid cells.
    
    Example:
        >>> config = GridConfig(rows=3, columns=4, cell_width=100, cell_height=50)
        >>> grid = GridLayout(config)
        >>> cell = grid.get_cell_at(150, 75)
        >>> print(f"Cell: row={cell.row}, col={cell.column}")
    """
    
    def __init__(self, config: GridConfig) -> None:
        """Initialize grid layout with configuration.
        
        Args:
            config: Grid configuration settings.
        """
        self.config = config
        self._cells: dict[tuple[int, int], GridCell] = {}
        self._build_grid()
    
    def _build_grid(self) -> None:
        """Build the internal grid structure."""
        for row in range(self.config.rows):
            for col in range(self.config.columns):
                x = self.config.origin_x + col * (
                    self.config.cell_width + self.config.column_spacing
                )
                y = self.config.origin_y + row * (
                    self.config.cell_height + self.config.row_spacing
                )
                bounds = (x, y, self.config.cell_width, self.config.cell_height)
                self._cells[(row, col)] = GridCell(
                    row=row,
                    column=col,
                    bounds=bounds
                )
    
    def get_cell(self, row: int, column: int) -> Optional[GridCell]:
        """Get cell at specific row and column.
        
        Args:
            row: Zero-based row index.
            column: Zero-based column index.
            
        Returns:
            GridCell if coordinates are valid, None otherwise.
        """
        return self._cells.get((row, column))
    
    def get_cell_at(self, x: int, y: int) -> Optional[GridCell]:
        """Get cell containing the given coordinates.
        
        Args:
            x: X coordinate to query.
            y: Y coordinate to query.
            
        Returns:
            GridCell containing the coordinates, or None if outside grid.
        """
        for cell in self._cells.values():
            bx, by, bw, bh = cell.bounds
            if bx <= x < bx + bw and by <= y < by + bh:
                return cell
        return None
    
    def get_neighbors(
        self,
        row: int,
        column: int,
        include_diagonals: bool = False
    ) -> list[GridCell]:
        """Get neighboring cells.
        
        Args:
            row: Row of the center cell.
            column: Column of the center cell.
            include_diagonals: Whether to include diagonal neighbors.
            
        Returns:
            List of neighboring GridCells.
        """
        neighbors = []
        offsets = [
            (-1, 0), (1, 0), (0, -1), (0, 1)  # Cardinal directions
        ]
        if include_diagonals:
            offsets.extend([(-1, -1), (-1, 1), (1, -1), (1, 1)])
        
        for dr, dc in offsets:
            neighbor = self.get_cell(row + dr, column + dc)
            if neighbor:
                neighbors.append(neighbor)
        return neighbors
    
    def get_row(self, row: int) -> list[GridCell]:
        """Get all cells in a row.
        
        Args:
            row: Zero-based row index.
            
        Returns:
            List of GridCells in the row, left to right.
        """
        return [self.get_cell(row, col) for col in range(self.config.columns)]
    
    def get_column(self, column: int) -> list[GridCell]:
        """Get all cells in a column.
        
        Args:
            column: Zero-based column index.
            
        Returns:
            List of GridCells in the column, top to bottom.
        """
        return [self.get_cell(row, column) for row in range(self.config.rows)]
    
    def cell_to_index(self, row: int, column: int) -> int:
        """Convert cell coordinates to linear index.
        
        Args:
            row: Zero-based row index.
            column: Zero-based column index.
            
        Returns:
            Linear index (row * columns + column).
        """
        return row * self.config.columns + column
    
    def index_to_cell(self, index: int) -> Optional[GridCell]:
        """Convert linear index to cell coordinates.
        
        Args:
            index: Linear index.
            
        Returns:
            GridCell at the index, or None if out of bounds.
        """
        row = index // self.config.columns
        column = index % self.config.columns
        return self.get_cell(row, column)
    
    def iterate_cells(self) -> Iterator[GridCell]:
        """Iterate over all cells in row-major order.
        
        Yields:
            GridCell for each position in the grid.
        """
        for row in range(self.config.rows):
            for col in range(self.config.columns):
                cell = self.get_cell(row, col)
                if cell:
                    yield cell
    
    def filter_cells(
        self,
        predicate: Callable[[GridCell], bool]
    ) -> list[GridCell]:
        """Filter cells based on a predicate.
        
        Args:
            predicate: Function that returns True for cells to keep.
            
        Returns:
            List of cells matching the predicate.
        """
        return [cell for cell in self.iterate_cells() if predicate(cell)]
    
    def find_cell_nearest_to(
        self,
        x: int,
        y: int,
        predicate: Optional[Callable[[GridCell], bool]] = None
    ) -> Optional[GridCell]:
        """Find cell nearest to given coordinates.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            predicate: Optional filter for candidate cells.
            
        Returns:
            GridCell nearest to coordinates, or None if no matches.
        """
        candidates = self.filter_cells(predicate) if predicate else list(self.iterate_cells())
        if not candidates:
            return None
        
        def distance(cell: GridCell) -> float:
            bx, by, _, _ = cell.bounds
            center_x = bx + self.config.cell_width / 2
            center_y = by + self.config.cell_height / 2
            return math.sqrt((x - center_x) ** 2 + (y - center_y) ** 2)
        
        return min(candidates, key=distance)
    
    def get_grid_bounds(self) -> tuple[int, int, int, int]:
        """Get the bounding box of the entire grid.
        
        Returns:
            (x, y, width, height) tuple for the grid.
        """
        width = self.config.columns * (
            self.config.cell_width + self.config.column_spacing
        ) - self.config.column_spacing
        height = self.config.rows * (
            self.config.cell_height + self.config.row_spacing
        ) - self.config.row_spacing
        return (
            self.config.origin_x,
            self.config.origin_y,
            width,
            height
        )


@dataclass
class GridNavigator:
    """Navigation helper for grid-based UI layouts.
    
    Provides methods for navigating between cells with various
    strategies (spiral, zigzag, etc.).
    
    Attributes:
        grid: The GridLayout to navigate.
        current_row: Current row position.
        current_column: Current column position.
    """
    grid: GridLayout
    current_row: int = 0
    current_column: int = 0
    
    def move_to(self, row: int, column: int) -> bool:
        """Move to a specific cell.
        
        Args:
            row: Target row.
            column: Target column.
            
        Returns:
            True if move succeeded, False if out of bounds.
        """
        cell = self.grid.get_cell(row, column)
        if cell:
            self.current_row = row
            self.current_column = column
            return True
        return False
    
    def move_up(self) -> bool:
        """Move one cell up.
        
        Returns:
            True if move succeeded, False if at top edge.
        """
        return self.move_to(self.current_row - 1, self.current_column)
    
    def move_down(self) -> bool:
        """Move one cell down.
        
        Returns:
            True if move succeeded, False if at bottom edge.
        """
        return self.move_to(self.current_row + 1, self.current_column)
    
    def move_left(self) -> bool:
        """Move one cell left.
        
        Returns:
            True if move succeeded, False if at left edge.
        """
        return self.move_to(self.current_row, self.current_column - 1)
    
    def move_right(self) -> bool:
        """Move one cell right.
        
        Returns:
            True if move succeeded, False if right edge.
        """
        return self.move_to(self.current_row, self.current_column + 1)
    
    def move_to_start_of_row(self) -> bool:
        """Move to the first column of the current row.
        
        Returns:
            True always (move always succeeds within grid).
        """
        self.current_column = 0
        return True
    
    def move_to_end_of_row(self) -> bool:
        """Move to the last column of the current row.
        
        Returns:
            True always.
        """
        self.current_column = self.grid.config.columns - 1
        return True
    
    def get_current_cell(self) -> Optional[GridCell]:
        """Get the current cell.
        
        Returns:
            Current GridCell or None.
        """
        return self.grid.get_cell(self.current_row, self.current_column)
    
    def spiral_traversal(self) -> Iterator[GridCell]:
        """Generate cells in spiral order (clockwise from top-left).
        
        Yields:
            GridCells in spiral order.
        """
        visited: set[tuple[int, int]] = set()
        directions = [(0, 1), (1, 0), (0, -1), (-1, 0)]
        dir_idx = 0
        row, col = 0, 0
        
        for _ in range(self.grid.config.rows * self.grid.config.columns):
            cell = self.grid.get_cell(row, col)
            if cell:
                visited.add((row, col))
                yield cell
            
            next_row = row + directions[dir_idx][0]
            next_col = col + directions[dir_idx][1]
            
            if (
                0 <= next_row < self.grid.config.rows
                and 0 <= next_col < self.grid.config.columns
                and (next_row, next_col) not in visited
            ):
                row, col = next_row, next_col
            else:
                dir_idx = (dir_idx + 1) % 4
                row += directions[dir_idx][0]
                col += directions[dir_idx][1]


def create_grid_from_elements(
    elements: Sequence[object],
    rows: int,
    columns: int,
    origin_x: int = 0,
    origin_y: int = 0,
    cell_width: int = 100,
    cell_height: int = 50
) -> GridLayout:
    """Create a grid layout populated with UI elements.
    
    Args:
        elements: Sequence of UI elements to place in grid.
        rows: Number of rows.
        columns: Number of columns.
        origin_x: X coordinate of grid origin.
        origin_y: Y coordinate of grid origin.
        cell_width: Width of each cell.
        cell_height: Height of each cell.
        
    Returns:
        GridLayout populated with elements.
    """
    config = GridConfig(
        rows=rows,
        columns=columns,
        cell_width=cell_width,
        cell_height=cell_height,
        origin_x=origin_x,
        origin_y=origin_y
    )
    grid = GridLayout(config)
    
    for idx, element in enumerate(elements):
        cell = grid.index_to_cell(idx)
        if cell:
            cell.element = element
    
    return grid
