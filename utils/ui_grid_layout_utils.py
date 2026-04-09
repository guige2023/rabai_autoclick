"""UI grid layout utilities for UI automation.

Provides utilities for analyzing UI layouts, extracting grid structures,
and managing element positions in grid-based layouts.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set, Tuple


@dataclass
class GridCell:
    """Represents a cell in a grid layout."""
    row: int
    col: int
    x: float
    y: float
    width: float
    height: float
    element_id: Optional[str] = None
    is_occupied: bool = False


@dataclass
class GridPosition:
    """Represents a position in a grid."""
    row: int
    col: int
    row_span: int = 1
    col_span: int = 1
    
    @property
    def is_valid(self) -> bool:
        return self.row >= 0 and self.col >= 0 and self.row_span > 0 and self.col_span > 0


@dataclass
class LayoutRegion:
    """A rectangular region in a layout."""
    x: float
    y: float
    width: float
    height: float
    elements: List[str] = field(default_factory=list)


class GridExtractor:
    """Extracts grid structure from UI layouts.
    
    Analyzes element positions to identify grid-based
    layouts and extract grid parameters.
    """
    
    def __init__(self, tolerance: float = 5.0) -> None:
        """Initialize the grid extractor.
        
        Args:
            tolerance: Tolerance for detecting grid lines.
        """
        self.tolerance = tolerance
        self._x_lines: List[float] = []
        self._y_lines: List[float] = []
    
    def extract_from_elements(
        self,
        elements: List[Tuple[str, float, float, float, float]]
    ) -> Tuple[int, int, List[GridCell]]:
        """Extract grid structure from element positions.
        
        Args:
            elements: List of (id, x, y, width, height) tuples.
            
        Returns:
            Tuple of (rows, cols, grid_cells).
        """
        if not elements:
            return (0, 0, [])
        
        x_coords = []
        y_coords = []
        
        for elem_id, x, y, width, height in elements:
            x_coords.extend([x, x + width])
            y_coords.extend([y, y + height])
        
        self._x_lines = self._detect_lines(x_coords)
        self._y_lines = self._detect_lines(y_coords)
        
        rows = len(self._y_lines) - 1
        cols = len(self._x_lines) - 1
        
        cells = []
        
        for row in range(rows):
            for col in range(cols):
                x = self._x_lines[col]
                y = self._y_lines[row]
                width = self._x_lines[col + 1] - x
                height = self._y_lines[row + 1] - y
                
                cell = GridCell(
                    row=row,
                    col=col,
                    x=x,
                    y=y,
                    width=width,
                    height=height
                )
                cells.append(cell)
        
        return (rows, cols, cells)
    
    def _detect_lines(self, coords: List[float]) -> List[float]:
        """Detect grid lines from coordinates.
        
        Args:
            coords: List of coordinates.
            
        Returns:
            List of grid line positions.
        """
        if not coords:
            return []
        
        sorted_coords = sorted(set(coords))
        
        lines = [sorted_coords[0]]
        current_group = [sorted_coords[0]]
        
        for coord in sorted_coords[1:]:
            if coord - current_group[-1] <= self.tolerance:
                current_group.append(coord)
            else:
                lines.append(sum(current_group) / len(current_group))
                current_group = [coord]
        
        if current_group:
            lines.append(sum(current_group) / len(current_group))
        
        return sorted(lines)
    
    def assign_elements_to_cells(
        self,
        elements: List[Tuple[str, float, float, float, float]],
        cells: List[GridCell]
    ) -> List[GridCell]:
        """Assign elements to grid cells.
        
        Args:
            elements: List of (id, x, y, width, height) tuples.
            cells: Grid cells.
            
        Returns:
            Cells with assigned elements.
        """
        element_to_cell = {}
        
        for elem_id, x, y, width, height in elements:
            center_x = x + width / 2
            center_y = y + height / 2
            
            for cell in cells:
                if (cell.x <= center_x <= cell.x + cell.width and
                    cell.y <= center_y <= cell.y + cell.height):
                    cell.element_id = elem_id
                    cell.is_occupied = True
                    element_to_cell[elem_id] = cell
                    break
        
        return cells
    
    def get_cell_at_position(
        self,
        x: float,
        y: float,
        cells: List[GridCell]
    ) -> Optional[GridCell]:
        """Get cell at a specific position.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            cells: Grid cells.
            
        Returns:
            Cell at position or None.
        """
        for cell in cells:
            if (cell.x <= x <= cell.x + cell.width and
                cell.y <= y <= cell.y + cell.height):
                return cell
        return None


class GridNavigator:
    """Navigates through grid-based layouts.
    
    Provides utilities for moving between cells,
    finding adjacent elements, and grid traversal.
    """
    
    def __init__(self, rows: int, cols: int) -> None:
        """Initialize the grid navigator.
        
        Args:
            rows: Number of rows.
            cols: Number of columns.
        """
        self.rows = rows
        self.cols = cols
        self._cell_map: Dict[Tuple[int, int], GridCell] = {}
        self._element_to_cell: Dict[str, GridCell] = {}
    
    def register_cell(self, cell: GridCell) -> None:
        """Register a cell in the navigator.
        
        Args:
            cell: Grid cell to register.
        """
        self._cell_map[(cell.row, cell.col)] = cell
        if cell.element_id:
            self._element_to_cell[cell.element_id] = cell
    
    def get_cell(self, row: int, col: int) -> Optional[GridCell]:
        """Get cell at position.
        
        Args:
            row: Row index.
            col: Column index.
            
        Returns:
            Cell or None.
        """
        return self._cell_map.get((row, col))
    
    def get_element_cell(self, element_id: str) -> Optional[GridCell]:
        """Get cell containing element.
        
        Args:
            element_id: Element identifier.
            
        Returns:
            Cell or None.
        """
        return self._element_to_cell.get(element_id)
    
    def move_right(self, element_id: str) -> Optional[GridCell]:
        """Move to cell to the right of element.
        
        Args:
            element_id: Element ID.
            
        Returns:
            Cell to the right or None.
        """
        cell = self.get_element_cell(element_id)
        if not cell:
            return None
        
        return self.get_cell(cell.row, cell.col + 1)
    
    def move_left(self, element_id: str) -> Optional[GridCell]:
        """Move to cell to the left of element.
        
        Args:
            element_id: Element ID.
            
        Returns:
            Cell to the left or None.
        """
        cell = self.get_element_cell(element_id)
        if not cell:
            return None
        
        return self.get_cell(cell.row, cell.col - 1)
    
    def move_up(self, element_id: str) -> Optional[GridCell]:
        """Move to cell above element.
        
        Args:
            element_id: Element ID.
            
        Returns:
            Cell above or None.
        """
        cell = self.get_element_cell(element_id)
        if not cell:
            return None
        
        return self.get_cell(cell.row - 1, cell.col)
    
    def move_down(self, element_id: str) -> Optional[GridCell]:
        """Move to cell below element.
        
        Args:
            element_id: Element ID.
            
        Returns:
            Cell below or None.
        """
        cell = self.get_element_cell(element_id)
        if not cell:
            return None
        
        return self.get_cell(cell.row + 1, cell.col)
    
    def get_adjacent_cells(self, element_id: str) -> List[GridCell]:
        """Get all adjacent cells.
        
        Args:
            element_id: Element ID.
            
        Returns:
            List of adjacent cells.
        """
        cell = self.get_element_cell(element_id)
        if not cell:
            return []
        
        adjacent = []
        for dr, dc in [(-1, 0), (1, 0), (0, -1), (0, 1)]:
            neighbor = self.get_cell(cell.row + dr, cell.col + dc)
            if neighbor:
                adjacent.append(neighbor)
        
        return adjacent


class LayoutAnalyzer:
    """Analyzes UI layouts for patterns and structure.
    
    Provides utilities for detecting layout patterns,
    computing alignment, and identifying layout issues.
    """
    
    def __init__(self) -> None:
        """Initialize the layout analyzer."""
        pass
    
    def detect_alignment(
        self,
        elements: List[Tuple[str, float, float, float, float]]
    ) -> Dict[str, List[str]]:
        """Detect alignment groups among elements.
        
        Args:
            elements: List of (id, x, y, width, height) tuples.
            
        Returns:
            Dictionary mapping alignment type to aligned element groups.
        """
        alignment: Dict[str, List[str]] = {
            "left": [],
            "right": [],
            "top": [],
            "bottom": [],
            "center_x": [],
            "center_y": []
        }
        
        left_groups = defaultdict(list)
        right_groups = defaultdict(list)
        top_groups = defaultdict(list)
        bottom_groups = defaultdict(list)
        
        for elem_id, x, y, width, height in elements:
            left_groups[int(x)].append(elem_id)
            right_groups[int(x + width)].append(elem_id)
            top_groups[int(y)].append(elem_id)
            bottom_groups[int(y + height)].append(elem_id)
        
        for group in left_groups.values():
            if len(group) > 1:
                alignment["left"].extend(group)
        
        for group in right_groups.values():
            if len(group) > 1:
                alignment["right"].extend(group)
        
        for group in top_groups.values():
            if len(group) > 1:
                alignment["top"].extend(group)
        
        for group in bottom_groups.values():
            if len(group) > 1:
                alignment["bottom"].extend(group)
        
        return {k: list(set(v)) for k, v in alignment.items()}
    
    def compute_spacing(
        self,
        elements: List[Tuple[str, float, float, float, float]]
    ) -> Dict[str, float]:
        """Compute spacing statistics between elements.
        
        Args:
            elements: List of (id, x, y, width, height) tuples.
            
        Returns:
            Dictionary of spacing statistics.
        """
        if len(elements) < 2:
            return {}
        
        sorted_by_x = sorted(elements, key=lambda e: e[1])
        sorted_by_y = sorted(elements, key=lambda e: e[2])
        
        h_gaps = []
        for i in range(len(sorted_by_x) - 1):
            gap = sorted_by_x[i + 1][1] - (sorted_by_x[i][1] + sorted_by_x[i][3])
            if gap > 0:
                h_gaps.append(gap)
        
        v_gaps = []
        for i in range(len(sorted_by_y) - 1):
            gap = sorted_by_y[i + 1][2] - (sorted_by_y[i][2] + sorted_by_y[i][4])
            if gap > 0:
                v_gaps.append(gap)
        
        result = {}
        
        if h_gaps:
            result["avg_horizontal_gap"] = sum(h_gaps) / len(h_gaps)
            result["min_horizontal_gap"] = min(h_gaps)
            result["max_horizontal_gap"] = max(h_gaps)
        
        if v_gaps:
            result["avg_vertical_gap"] = sum(v_gaps) / len(v_gaps)
            result["min_vertical_gap"] = min(v_gaps)
            result["max_vertical_gap"] = max(v_gaps)
        
        return result
    
    def detect_distribution(
        self,
        elements: List[Tuple[str, float, float, float, float]]
    ) -> str:
        """Detect element distribution pattern.
        
        Args:
            elements: List of (id, x, y, width, height) tuples.
            
        Returns:
            Distribution type: "uniform", "increasing", "decreasing", or "irregular".
        """
        if len(elements) < 3:
            return "irregular"
        
        sorted_by_x = sorted(elements, key=lambda e: e[1])
        
        gaps = []
        for i in range(len(sorted_by_x) - 1):
            gap = sorted_by_x[i + 1][1] - sorted_by_x[i][1]
            gaps.append(gap)
        
        avg_gap = sum(gaps) / len(gaps)
        
        variance = sum((g - avg_gap) ** 2 for g in gaps) / len(gaps)
        
        if variance < 4:
            return "uniform"
        
        increasing = all(gaps[i] <= gaps[i + 1] for i in range(len(gaps) - 1))
        decreasing = all(gaps[i] >= gaps[i + 1] for i in range(len(gaps) - 1))
        
        if increasing:
            return "increasing"
        if decreasing:
            return "decreasing"
        
        return "irregular"


def extract_grid_layout(
    elements: List[Tuple[str, float, float, float, float]],
    tolerance: float = 5.0
) -> Tuple[int, int, List[GridCell]]:
    """Extract grid layout from elements.
    
    Args:
        elements: List of (id, x, y, width, height) tuples.
        tolerance: Grid line tolerance.
        
    Returns:
        Tuple of (rows, cols, cells).
    """
    extractor = GridExtractor(tolerance)
    return extractor.extract_from_elements(elements)
