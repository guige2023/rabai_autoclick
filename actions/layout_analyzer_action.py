"""
Layout Analyzer Action Module.

Analyzes UI layouts for responsive design validation,
alignment checking, spacing measurement, and visual hierarchy
assessment.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class AlignmentResult:
    """Result of alignment check."""
    aligned: bool
    axis: str
    offset: float
    elements: list[str]


class LayoutAnalyzer:
    """Analyzes UI layouts for design compliance."""

    def __init__(self):
        """Initialize layout analyzer."""
        pass

    def check_horizontal_alignment(
        self,
        elements: list[dict],
        tolerance: float = 2.0,
    ) -> AlignmentResult:
        """
        Check if elements are horizontally aligned.

        Args:
            elements: List of elements with 'bounds'.
            tolerance: Pixel tolerance for alignment.

        Returns:
            AlignmentResult.
        """
        if len(elements) < 2:
            return AlignmentResult(True, "horizontal", 0.0, [])

        y_positions = []
        ids = []

        for elem in elements:
            bounds = elem.get("bounds", (0, 0, 0, 0))
            y_positions.append((bounds[1] + bounds[3]) / 2)
            ids.append(elem.get("id", "unknown"))

        reference = y_positions[0]
        max_offset = 0.0
        misaligned = []

        for i, y in enumerate(y_positions[1:], 1):
            offset = abs(y - reference)
            if offset > tolerance:
                misaligned.append(ids[i])
            max_offset = max(max_offset, offset)

        return AlignmentResult(
            aligned=len(misaligned) == 0,
            axis="horizontal",
            offset=max_offset,
            elements=ids,
        )

    def check_vertical_alignment(
        self,
        elements: list[dict],
        tolerance: float = 2.0,
    ) -> AlignmentResult:
        """
        Check if elements are vertically aligned.

        Args:
            elements: List of elements with 'bounds'.
            tolerance: Pixel tolerance.

        Returns:
            AlignmentResult.
        """
        if len(elements) < 2:
            return AlignmentResult(True, "vertical", 0.0, [])

        x_positions = []
        ids = []

        for elem in elements:
            bounds = elem.get("bounds", (0, 0, 0, 0))
            x_positions.append((bounds[0] + bounds[2]) / 2)
            ids.append(elem.get("id", "unknown"))

        reference = x_positions[0]
        max_offset = 0.0
        misaligned = []

        for i, x in enumerate(x_positions[1:], 1):
            offset = abs(x - reference)
            if offset > tolerance:
                misaligned.append(ids[i])
            max_offset = max(max_offset, offset)

        return AlignmentResult(
            aligned=len(misaligned) == 0,
            axis="vertical",
            offset=max_offset,
            elements=ids,
        )

    def measure_spacing(
        self,
        element1: dict,
        element2: dict,
    ) -> dict[str, float]:
        """
        Measure spacing between two elements.

        Args:
            element1: First element with bounds.
            element2: Second element with bounds.

        Returns:
            Dict with 'top', 'bottom', 'left', 'right' spacing.
        """
        b1 = element1.get("bounds", (0, 0, 0, 0))
        b2 = element2.get("bounds", (0, 0, 0, 0))

        return {
            "top": b2[1] - b1[3],
            "bottom": b1[1] - b2[3],
            "left": b2[0] - b1[2],
            "right": b1[0] - b2[2],
        }

    def detect_overlaps(
        self,
        elements: list[dict],
    ) -> list[tuple[str, str]]:
        """
        Detect overlapping element pairs.

        Args:
            elements: List of elements with bounds and id.

        Returns:
            List of (id1, id2) tuples for overlapping pairs.
        """
        overlaps = []

        for i, elem1 in enumerate(elements):
            b1 = elem1.get("bounds", (0, 0, 0, 0))
            id1 = elem1.get("id", f"elem_{i}")

            for j, elem2 in enumerate(elements[i + 1:], i + 1):
                b2 = elem2.get("bounds", (0, 0, 0, 0))
                id2 = elem2.get("id", f"elem_{j}")

                if self._bounds_overlap(b1, b2):
                    overlaps.append((id1, id2))

        return overlaps

    def analyze_grid_layout(
        self,
        elements: list[dict],
        expected_cols: int,
    ) -> dict:
        """
        Analyze grid layout structure.

        Args:
            elements: Elements in grid.
            expected_cols: Expected number of columns.

        Returns:
            Analysis results.
        """
        if not elements:
            return {"valid": False, "reason": "no elements"}

        bounds_list = [e.get("bounds", (0, 0, 0, 0)) for e in elements]

        col_widths = {}
        for i, bounds in enumerate(bounds_list):
            x = bounds[0]
            col_widths.setdefault(x, []).append(bounds[2] - bounds[0])

        row_heights = {}
        for i, bounds in enumerate(bounds_list):
            y = bounds[1]
            row_heights.setdefault(y, []).append(bounds[3] - bounds[1])

        actual_cols = len(col_widths)

        return {
            "valid": actual_cols == expected_cols,
            "expected_cols": expected_cols,
            "actual_cols": actual_cols,
            "row_count": len(row_heights),
            "element_count": len(elements),
        }

    @staticmethod
    def _bounds_overlap(
        b1: tuple[int, int, int, int],
        b2: tuple[int, int, int, int],
    ) -> bool:
        """Check if two bounding boxes overlap."""
        return not (b1[2] <= b2[0] or b1[0] >= b2[2] or b1[3] <= b2[1] or b1[1] >= b2[3])
