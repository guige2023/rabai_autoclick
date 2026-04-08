"""
Data Comparison Action Module.

Compares datasets side-by-side: field-by-field diff,
semantic similarity, and visual diff generation.
"""
from typing import Any, Optional
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class FieldDiff:
    """Difference for a single field."""
    field: str
    left_value: Any
    right_value: Any
    diff_type: str  # added, removed, modified, unchanged


@dataclass
class ComparisonResult:
    """Result of dataset comparison."""
    left_count: int
    right_count: int
    field_diffs: list[FieldDiff]
    similarity_score: float
    summary: dict[str, int]


class DataComparisonAction(BaseAction):
    """Compare datasets side-by-side."""

    def __init__(self) -> None:
        super().__init__("data_comparison")

    def execute(self, context: dict, params: dict) -> dict:
        """
        Compare two datasets.

        Args:
            context: Execution context
            params: Parameters:
                - left: Left dataset (list of dicts)
                - right: Right dataset (list of dicts)
                - key_field: Primary key for row matching
                - ignore_fields: Fields to ignore in comparison
                - tolerance: Numeric tolerance for float comparison

        Returns:
            ComparisonResult with diff details
        """
        left = params.get("left", [])
        right = params.get("right", [])
        key_field = params.get("key_field", "id")
        ignore_fields = set(params.get("ignore_fields", []))
        tolerance = params.get("tolerance", 0.001)

        left_by_key = {str(r.get(key_field, "")): r for r in left}
        right_by_key = {str(r.get(key_field, "")): r for r in right}

        all_keys = set(left_by_key.keys()) | set(right_by_key.keys())

        field_diffs: list[FieldDiff] = []
        total_differences = 0

        for key in sorted(all_keys):
            l_rec = left_by_key.get(key, {})
            r_rec = right_by_key.get(key, {})

            all_fields = set(l_rec.keys()) | set(r_rec.keys())
            all_fields -= ignore_fields

            for field in all_fields:
                l_val = l_rec.get(field)
                r_val = r_rec.get(field)

                if l_val is None and r_val is not None:
                    field_diffs.append(FieldDiff(field, None, r_val, "added"))
                    total_differences += 1
                elif l_val is not None and r_val is None:
                    field_diffs.append(FieldDiff(field, l_val, None, "removed"))
                    total_differences += 1
                elif l_val != r_val:
                    if isinstance(l_val, (int, float)) and isinstance(r_val, (int, float)):
                        if abs(l_val - r_val) <= tolerance:
                            continue
                    field_diffs.append(FieldDiff(field, l_val, r_val, "modified"))
                    total_differences += 1

        total_fields = sum(len(set(r.keys()) - ignore_fields) for r in left + right)
        similarity = 1.0 - (total_differences / total_fields) if total_fields > 0 else 1.0

        added = sum(1 for d in field_diffs if d.diff_type == "added")
        removed = sum(1 for d in field_diffs if d.diff_type == "removed")
        modified = sum(1 for d in field_diffs if d.diff_type == "modified")

        return ComparisonResult(
            left_count=len(left),
            right_count=len(right),
            field_diffs=field_diffs[:100],
            similarity_score=similarity,
            summary={"added": added, "removed": removed, "modified": modified, "total": total_differences}
        ).__dict__
