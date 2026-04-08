"""
Data Diff Action Module.

Compares two datasets and produces detailed differences: added rows,
removed rows, modified rows, and field-level changes with fuzzy matching.
"""
from typing import Any, Optional
from dataclasses import dataclass
from actions.base_action import BaseAction


@dataclass
class DiffResult:
    """Result of dataset comparison."""
    added: list[dict[str, Any]]
    removed: list[dict[str, Any]]
    modified: list[dict[str, Any]]
    unchanged: list[dict[str, Any]]
    summary: dict[str, int]


class DataDiffAction(BaseAction):
    """Compare two datasets and produce diff."""

    def __init__(self) -> None:
        super().__init__("data_diff")

    def execute(self, context: dict, params: dict) -> DiffResult:
        """
        Compare two datasets.

        Args:
            context: Execution context
            params: Parameters:
                - left: Left dataset (list of dicts)
                - right: Right dataset (list of dicts)
                - key_field: Primary key field for matching rows
                - ignore_fields: Fields to ignore in comparison
                - fuzzy_text: Enable fuzzy text comparison (default: False)
                - tolerance: Numeric tolerance for float comparison (default: 0.001)

        Returns:
            DiffResult with added, removed, modified, and unchanged records
        """
        left = params.get("left", [])
        right = params.get("right", [])
        key_field = params.get("key_field", "id")
        ignore_fields = set(params.get("ignore_fields", []))
        tolerance = params.get("tolerance", 0.001)

        left_by_key: dict[str, dict] = {str(r.get(key_field, "")): r for r in left}
        right_by_key: dict[str, dict] = {str(r.get(key_field, "")): r for r in right}

        left_keys = set(left_by_key.keys())
        right_keys = set(right_by_key.keys())

        added_keys = right_keys - left_keys
        removed_keys = left_keys - right_keys
        common_keys = left_keys & right_keys

        added: list[dict] = [right_by_key[k] for k in added_keys]
        removed: list[dict] = [left_by_key[k] for k in removed_keys]

        modified: list[dict] = []
        unchanged: list[dict] = []

        for key in common_keys:
            l_record = left_by_key[key]
            r_record = right_by_key[key]

            l_filtered = {k: v for k, v in l_record.items() if k not in ignore_fields}
            r_filtered = {k: v for k, v in r_record.items() if k not in ignore_fields}

            if l_filtered == r_filtered:
                unchanged.append(r_record)
            else:
                changes = self._compute_field_changes(l_filtered, r_filtered, tolerance)
                modified.append({
                    "key": key,
                    "left": l_record,
                    "right": r_record,
                    "changes": changes
                })

        return DiffResult(
            added=added,
            removed=removed,
            modified=modified,
            unchanged=unchanged,
            summary={
                "added": len(added),
                "removed": len(removed),
                "modified": len(modified),
                "unchanged": len(unchanged),
                "total_left": len(left),
                "total_right": len(right)
            }
        )

    def _compute_field_changes(self, left: dict, right: dict, tolerance: float) -> list[dict[str, Any]]:
        """Compute field-level changes between two records."""
        changes = []
        all_keys = set(left.keys()) | set(right.keys())

        for key in all_keys:
            l_val = left.get(key)
            r_val = right.get(key)

            if l_val == r_val:
                continue

            if isinstance(l_val, (int, float)) and isinstance(r_val, (int, float)):
                if abs(l_val - r_val) <= tolerance:
                    continue
                changes.append({
                    "field": key,
                    "old": l_val,
                    "new": r_val,
                    "change_type": "modified"
                })
            elif isinstance(l_val, str) and isinstance(r_val, str):
                if l_val.strip() == r_val.strip():
                    continue
                changes.append({
                    "field": key,
                    "old": l_val,
                    "new": r_val,
                    "change_type": "modified"
                })
            else:
                changes.append({
                    "field": key,
                    "old": l_val,
                    "new": r_val,
                    "change_type": "modified"
                })

        return changes
