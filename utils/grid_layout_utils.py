"""
Grid Layout Utilities

Provides utilities for working with grid layouts
in UI automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class GridPosition:
    """Position in a grid layout."""
    row: int
    column: int
    row_span: int = 1
    column_span: int = 1


@dataclass
class GridCell:
    """Represents a cell in a grid layout."""
    position: GridPosition
    bounds: tuple[int, int, int, int]
    element: dict[str, Any] | None = None


class GridLayout:
    """
    Represents a grid layout.
    
    Provides utilities for positioning and
    navigating grid-based UIs.
    """

    def __init__(self, rows: int, columns: int) -> None:
        self._rows = rows
        self._columns = columns
        self._cells: dict[GridPosition, GridCell] = {}

    def set_cell(
        self,
        position: GridPosition,
        bounds: tuple[int, int, int, int],
        element: dict[str, Any] | None = None,
    ) -> None:
        """Set cell content and bounds."""
        self._cells[position] = GridCell(
            position=position,
            bounds=bounds,
            element=element,
        )

    def get_cell(self, row: int, column: int) -> GridCell | None:
        """Get cell at row/column."""
        for cell in self._cells.values():
            pos = cell.position
            if (pos.row <= row < pos.row + pos.row_span and
                    pos.column <= column < pos.column + pos.column_span):
                return cell
        return None

    def get_cell_at_point(self, x: int, y: int) -> GridCell | None:
        """Get cell containing a point."""
        for cell in self._cells.values():
            x1, y1, x2, y2 = cell.bounds
            if x1 <= x <= x2 and y1 <= y <= y2:
                return cell
        return None

    def get_neighbors(
        self,
        row: int,
        column: int,
        radius: int = 1,
    ) -> list[GridCell]:
        """Get neighboring cells."""
        neighbors = []
        for r in range(row - radius, row + radius + 1):
            for c in range(column - radius, column + radius + 1):
                if r == row and c == column:
                    continue
                cell = self.get_cell(r, c)
                if cell:
                    neighbors.append(cell)
        return neighbors

    @property
    def rows(self) -> int:
        """Get number of rows."""
        return self._rows

    @property
    def columns(self) -> int:
        """Get number of columns."""
        return self._columns
