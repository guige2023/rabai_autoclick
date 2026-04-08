"""
Layout analysis utilities for UI structure analysis.

Analyzes UI element layouts including grid detection, alignment,
spacing, and grouping patterns.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class LayoutGrid:
    """Detected grid structure in a UI."""
    columns: int
    rows: int
    cell_width: float
    cell_height: float
    origin_x: float
    origin_y: float
    gutter_x: float
    gutter_y: float


@dataclass
class AlignmentResult:
    """Alignment analysis result."""
    is_aligned: bool
    axis: str  # "horizontal", "vertical", or "both"
    deviation: float
    aligned_elements: list[str]


@dataclass
class SpacingResult:
    """Spacing analysis between elements."""
    min_spacing: float
    max_spacing: float
    avg_spacing: float
    is_consistent: bool
    consistent_threshold: float = 2.0


@dataclass
class ElementRect:
    """Rectangle representation of an element."""
    element_id: str
    x: float
    y: float
    width: float
    height: float

    @property
    def x2(self) -> float:
        return self.x + self.width

    @property
    def y2(self) -> float:
        return self.y + self.height

    @property
    def center_x(self) -> float:
        return self.x + self.width / 2

    @property
    def center_y(self) -> float:
        return self.y + self.height / 2


class LayoutAnalyzer:
    """Analyzes UI layouts for grids, alignment, and spacing."""

    def detect_grid(self, elements: list[ElementRect], tolerance: float = 5.0) -> Optional[LayoutGrid]:
        """Detect if elements form a grid layout."""
        if len(elements) < 4:
            return None

        # Find column positions
        x_positions = sorted(set(e.x for e in elements))
        y_positions = sorted(set(e.y for e in elements))

        # Find column gaps
        col_gaps = []
        for i in range(len(x_positions) - 1):
            gap = x_positions[i + 1] - x_positions[i]
            if gap > tolerance:
                col_gaps.append(gap)

        # Find row gaps
        row_gaps = []
        for i in range(len(y_positions) - 1):
            gap = y_positions[i + 1] - y_positions[i]
            if gap > tolerance:
                row_gaps.append(gap)

        if len(col_gaps) == 0 and len(row_gaps) == 0:
            return None

        cell_width = col_gaps[0] if col_gaps else elements[0].width
        cell_height = row_gaps[0] if row_gaps else elements[0].height

        columns = len(x_positions)
        rows = len(y_positions)

        return LayoutGrid(
            columns=columns,
            rows=rows,
            cell_width=cell_width,
            cell_height=cell_height,
            origin_x=x_positions[0] if x_positions else 0,
            origin_y=y_positions[0] if y_positions else 0,
            gutter_x=sum(col_gaps) / len(col_gaps) if col_gaps else 0,
            gutter_y=sum(row_gaps) / len(row_gaps) if row_gaps else 0,
        )

    def check_horizontal_alignment(self, elements: list[ElementRect], threshold: float = 2.0) -> AlignmentResult:
        """Check if elements are horizontally aligned."""
        if not elements:
            return AlignmentResult(False, "horizontal", 0.0, [])

        y_values = [e.y for e in elements]
        if len(y_values) < 2:
            return AlignmentResult(False, "horizontal", 0.0, [])

        most_common_y = self._most_common_value(y_values, threshold)
        aligned = [e.element_id for e in elements if abs(e.y - most_common_y) <= threshold]

        deviation = sum(abs(e.y - most_common_y) for e in elements) / len(elements)

        return AlignmentResult(
            is_aligned=len(aligned) >= 2,
            axis="horizontal",
            deviation=deviation,
            aligned_elements=aligned,
        )

    def check_vertical_alignment(self, elements: list[ElementRect], threshold: float = 2.0) -> AlignmentResult:
        """Check if elements are vertically aligned."""
        if not elements:
            return AlignmentResult(False, "vertical", 0.0, [])

        x_values = [e.x for e in elements]
        if len(x_values) < 2:
            return AlignmentResult(False, "vertical", 0.0, [])

        most_common_x = self._most_common_value(x_values, threshold)
        aligned = [e.element_id for e in elements if abs(e.x - most_common_x) <= threshold]

        deviation = sum(abs(e.x - most_common_x) for e in elements) / len(elements)

        return AlignmentResult(
            is_aligned=len(aligned) >= 2,
            axis="vertical",
            deviation=deviation,
            aligned_elements=aligned,
        )

    def analyze_spacing(self, elements: list[ElementRect], axis: str = "horizontal") -> SpacingResult:
        """Analyze spacing between elements along an axis."""
        if len(elements) < 2:
            return SpacingResult(0.0, 0.0, 0.0, True)

        if axis == "horizontal":
            sorted_elements = sorted(elements, key=lambda e: e.x)
            spacings = []
            for i in range(len(sorted_elements) - 1):
                e1 = sorted_elements[i]
                e2 = sorted_elements[i + 1]
                spacing = e2.x - e1.x2
                spacings.append(spacing)
        else:
            sorted_elements = sorted(elements, key=lambda e: e.y)
            spacings = []
            for i in range(len(sorted_elements) - 1):
                e1 = sorted_elements[i]
                e2 = sorted_elements[i + 1]
                spacing = e2.y - e1.y2
                spacings.append(spacing)

        if not spacings:
            return SpacingResult(0.0, 0.0, 0.0, True)

        min_sp = min(spacings)
        max_sp = max(spacings)
        avg_sp = sum(spacings) / len(spacings)

        return SpacingResult(
            min_spacing=min_sp,
            max_spacing=max_sp,
            avg_spacing=avg_sp,
            is_consistent=max_sp - min_sp <= 2.0,
        )

    def find_centered_elements(self, container_width: float, elements: list[ElementRect]) -> list[str]:
        """Find elements that are horizontally centered in a container."""
        container_center = container_width / 2
        threshold = 2.0
        return [
            e.element_id for e in elements
            if abs(e.center_x - container_center) <= threshold
        ]

    def detect_overlapping(self, elements: list[ElementRect]) -> list[tuple[str, str]]:
        """Detect pairs of overlapping elements."""
        overlaps = []
        for i in range(len(elements)):
            for j in range(i + 1, len(elements)):
                e1, e2 = elements[i], elements[j]
                if self._rects_intersect(e1, e2):
                    overlaps.append((e1.element_id, e2.element_id))
        return overlaps

    def _rects_intersect(self, a: ElementRect, b: ElementRect) -> bool:
        return not (a.x2 < b.x or b.x2 < a.x or a.y2 < b.y or b.y2 < a.y)

    def _most_common_value(self, values: list[float], threshold: float) -> float:
        if not values:
            return 0.0
        sorted_vals = sorted(values)
        best_val = sorted_vals[0]
        best_count = 1
        current_val = sorted_vals[0]
        current_count = 1

        for i in range(1, len(sorted_vals)):
            if abs(sorted_vals[i] - current_val) <= threshold:
                current_count += 1
            else:
                if current_count > best_count:
                    best_count = current_count
                    best_val = current_val
                current_val = sorted_vals[i]
                current_count = 1

        if current_count > best_count:
            best_val = current_val

        return best_val


__all__ = ["LayoutAnalyzer", "LayoutGrid", "AlignmentResult", "SpacingResult", "ElementRect"]
