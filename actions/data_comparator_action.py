"""
Data Comparator Action Module.

Compares datasets, objects, and records with configurable
 fuzzy matching and difference reporting.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, Union
from dataclasses import dataclass, field
from enum import Enum
import difflib
import logging

logger = logging.getLogger(__name__)


class DiffType(Enum):
    """Type of difference detected."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


@dataclass
class FieldDiff:
    """Difference in a single field."""
    field: str
    diff_type: DiffType
    old_value: Any = None
    new_value: Any = None
    similarity: float = 1.0


@dataclass
class RecordDiff:
    """Difference between two records."""
    record_id: Any
    diff_type: DiffType
    field_diffs: list[FieldDiff] = field(default_factory=list)
    score: float = 1.0


@dataclass
class ComparisonResult:
    """Result of a comparison operation."""
    total_records: int = 0
    added: int = 0
    removed: int = 0
    modified: int = 0
    unchanged: int = 0
    record_diffs: list[RecordDiff] = field(default_factory=list)
    similarity_score: float = 0.0


class DataComparatorAction:
    """
    Data comparison engine with fuzzy matching support.

    Compares datasets, detects additions, removals, modifications,
    and provides detailed diff reports.

    Example:
        comparator = DataComparatorAction()
        result = comparator.compare(dataset_a, dataset_b, key_field="id")
    """

    def __init__(
        self,
        fuzzy_threshold: float = 0.8,
        ignore_fields: Optional[list[str]] = None,
    ) -> None:
        self.fuzzy_threshold = fuzzy_threshold
        self.ignore_fields = ignore_fields or ["_updated_at", "_version"]

    def compare(
        self,
        left: list[dict[str, Any]],
        right: list[dict[str, Any]],
        key_field: str = "id",
        ignore_fields: Optional[list[str]] = None,
    ) -> ComparisonResult:
        """Compare two datasets and return detailed differences."""
        ignore = set(self.ignore_fields + (ignore_fields or []))

        left_index = {r.get(key_field): r for r in left}
        right_index = {r.get(key_field): r for r in right}

        left_keys = set(left_index.keys())
        right_keys = set(right_index.keys())

        added_keys = right_keys - left_keys
        removed_keys = left_keys - right_keys
        common_keys = left_keys & right_keys

        record_diffs: list[RecordDiff] = []
        total_similarity = 0.0

        for key in common_keys:
            left_record = left_index[key]
            right_record = right_index[key]
            record_diff = self._compare_records(
                key, left_record, right_record, ignore
            )
            record_diffs.append(record_diff)
            total_similarity += record_diff.score

        similarity_score = (
            total_similarity / len(record_diffs) if record_diffs else 1.0
        )

        added = len(added_keys)
        removed = len(removed_keys)
        modified = sum(1 for d in record_diffs if d.diff_type == DiffType.MODIFIED)
        unchanged = sum(1 for d in record_diffs if d.diff_type == DiffType.UNCHANGED)

        return ComparisonResult(
            total_records=len(left) + len(right),
            added=added,
            removed=removed,
            modified=modified,
            unchanged=unchanged,
            record_diffs=record_diffs,
            similarity_score=similarity_score,
        )

    def _compare_records(
        self,
        record_id: Any,
        left: dict[str, Any],
        right: dict[str, Any],
        ignore_fields: set[str],
    ) -> RecordDiff:
        """Compare two individual records."""
        all_fields = set(left.keys()) | set(right.keys())
        filtered_fields = [f for f in all_fields if f not in ignore_fields]

        field_diffs: list[FieldDiff] = []
        modifications = 0

        for field_name in filtered_fields:
            left_val = left.get(field_name)
            right_val = right.get(field_name)

            if left_val == right_val:
                continue

            diff_type = DiffType.MODIFIED
            similarity = self._calculate_similarity(left_val, right_val)

            if similarity < self.fuzzy_threshold:
                modifications += 1

            field_diffs.append(FieldDiff(
                field=field_name,
                diff_type=diff_type,
                old_value=left_val,
                new_value=right_val,
                similarity=similarity,
            ))

        if modifications == 0 and not field_diffs:
            diff_type = DiffType.UNCHANGED
            score = 1.0
        else:
            diff_type = DiffType.MODIFIED
            score = 1.0 - (modifications / max(len(field_diffs), 1))

        return RecordDiff(
            record_id=record_id,
            diff_type=diff_type,
            field_diffs=field_diffs,
            score=score,
        )

    def _calculate_similarity(self, left: Any, right: Any) -> float:
        """Calculate similarity between two values."""
        if left is None or right is None:
            return 0.0

        if isinstance(left, str) and isinstance(right, str):
            return difflib.SequenceMatcher(None, left, right).ratio()

        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            if left == right:
                return 1.0
            max_val = max(abs(left), abs(right))
            if max_val == 0:
                return 1.0
            return 1.0 - min(abs(left - right) / max_val, 1.0)

        if type(left) != type(right):
            return 0.0

        return 1.0 if left == right else 0.0

    def compare_objects(
        self,
        left: dict[str, Any],
        right: dict[str, Any],
        ignore_fields: Optional[list[str]] = None,
    ) -> RecordDiff:
        """Compare two objects/records."""
        ignore = set(self.ignore_fields + (ignore_fields or []))
        return self._compare_records(None, left, right, ignore)

    def generate_diff_report(
        self,
        result: ComparisonResult,
        format: str = "text",
    ) -> str:
        """Generate a human-readable diff report."""
        if format == "json":
            return self._generate_json_report(result)
        return self._generate_text_report(result)

    def _generate_text_report(self, result: ComparisonResult) -> str:
        """Generate plain text diff report."""
        lines = [
            "=" * 60,
            "DATA COMPARISON REPORT",
            "=" * 60,
            f"Total Records: {result.total_records}",
            f"Similarity Score: {result.similarity_score:.2%}",
            "",
            f"  Added:    {result.added}",
            f"  Removed:  {result.removed}",
            f"  Modified: {result.modified}",
            f"  Unchanged: {result.unchanged}",
            "",
        ]

        if result.record_diffs:
            lines.append("-" * 60)
            lines.append("DETAILED DIFFERENCES")
            lines.append("-" * 60)

            for diff in result.record_diffs:
                if diff.diff_type == DiffType.UNCHANGED:
                    continue

                lines.append(f"\n[{diff.diff_type.value.upper()}] Record: {diff.record_id}")
                for field_diff in diff.field_diffs:
                    lines.append(
                        f"  {field_diff.field}: {field_diff.old_value} -> {field_diff.new_value}"
                    )

        return "\n".join(lines)

    def _generate_json_report(self, result: ComparisonResult) -> str:
        """Generate JSON diff report."""
        import json
        data = {
            "summary": {
                "total_records": result.total_records,
                "added": result.added,
                "removed": result.removed,
                "modified": result.modified,
                "unchanged": result.unchanged,
                "similarity_score": result.similarity_score,
            },
            "diffs": [
                {
                    "record_id": d.record_id,
                    "type": d.diff_type.value,
                    "score": d.score,
                    "field_diffs": [
                        {
                            "field": f.field,
                            "type": f.diff_type.value,
                            "old": f.old_value,
                            "new": f.new_value,
                            "similarity": f.similarity,
                        }
                        for f in d.field_diffs
                    ],
                }
                for d in result.record_diffs
            ],
        }
        return json.dumps(data, indent=2)
