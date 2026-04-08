"""Data Comparison Action Module.

Provides data comparison, diff generation, and
similarity analysis for structured and unstructured data.
"""

from typing import Any, Dict, List, Optional, Callable, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import difflib
from datetime import datetime


class DiffOperation(Enum):
    """Types of diff operations."""
    ADD = "add"
    REMOVE = "remove"
    CHANGE = "change"
    EQUAL = "equal"


@dataclass
class DiffEntry:
    """Represents a single diff entry."""
    operation: DiffOperation
    path: str
    old_value: Any = None
    new_value: Any = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "operation": self.operation.value,
            "path": self.path,
            "old_value": self.old_value,
            "new_value": self.new_value,
            "metadata": self.metadata,
        }


@dataclass
class ComparisonResult:
    """Result of data comparison."""
    are_equal: bool
    diffs: List[DiffEntry]
    similarity_score: float
    added_count: int = 0
    removed_count: int = 0
    changed_count: int = 0
    unchanged_count: int = 0

    def __post_init__(self):
        self.added_count = sum(1 for d in self.diffs if d.operation == DiffOperation.ADD)
        self.removed_count = sum(1 for d in self.diffs if d.operation == DiffOperation.REMOVE)
        self.changed_count = sum(1 for d in self.diffs if d.operation == DiffOperation.CHANGE)
        self.unchanged_count = sum(1 for d in self.diffs if d.operation == DiffOperation.EQUAL)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "are_equal": self.are_equal,
            "similarity_score": self.similarity_score,
            "added_count": self.added_count,
            "removed_count": self.removed_count,
            "changed_count": self.changed_count,
            "unchanged_count": self.unchanged_count,
            "diffs": [d.to_dict() for d in self.diffs],
        }


@dataclass
class SchemaDiff:
    """Difference between two schemas."""
    field_name: str
    diff_type: str
    old_type: Optional[str] = None
    new_type: Optional[str] = None
    description: str = ""


class DataComparator:
    """Compares data structures."""

    def __init__(self, ignore_fields: Optional[List[str]] = None):
        self.ignore_fields = ignore_fields or []

    def compare(
        self,
        left: Any,
        right: Any,
        path: str = "",
    ) -> ComparisonResult:
        """Compare two data structures."""
        diffs = []
        self._compare_values(left, right, path, diffs)

        total_fields = len(diffs)
        unchanged = sum(1 for d in diffs if d.operation == DiffOperation.EQUAL)
        similarity = unchanged / max(total_fields, 1)

        return ComparisonResult(
            are_equal=len([d for d in diffs if d.operation != DiffOperation.EQUAL]) == 0,
            diffs=diffs,
            similarity_score=similarity,
        )

    def _compare_values(
        self,
        left: Any,
        right: Any,
        path: str,
        diffs: List[DiffEntry],
    ):
        """Recursively compare values."""
        if isinstance(left, dict) and isinstance(right, dict):
            self._compare_dicts(left, right, path, diffs)
        elif isinstance(left, list) and isinstance(right, list):
            self._compare_lists(left, right, path, diffs)
        else:
            self._compare_primitives(left, right, path, diffs)

    def _compare_dicts(
        self,
        left: dict,
        right: dict,
        path: str,
        diffs: List[DiffEntry],
    ):
        """Compare two dictionaries."""
        all_keys = set(left.keys()) | set(right.keys())

        for key in sorted(all_keys):
            if key in self.ignore_fields:
                continue

            new_path = f"{path}.{key}" if path else key
            left_val = left.get(key, None)
            right_val = right.get(key, None)

            if key not in left:
                diffs.append(DiffEntry(
                    operation=DiffOperation.ADD,
                    path=new_path,
                    new_value=right_val,
                ))
            elif key not in right:
                diffs.append(DiffEntry(
                    operation=DiffOperation.REMOVE,
                    path=new_path,
                    old_value=left_val,
                ))
            else:
                self._compare_values(left_val, right_val, new_path, diffs)

    def _compare_lists(
        self,
        left: list,
        right: list,
        path: str,
        diffs: List[DiffEntry],
    ):
        """Compare two lists."""
        max_len = max(len(left), len(right))

        for i in range(max_len):
            new_path = f"{path}[{i}]"

            if i >= len(left):
                diffs.append(DiffEntry(
                    operation=DiffOperation.ADD,
                    path=new_path,
                    new_value=right[i],
                ))
            elif i >= len(right):
                diffs.append(DiffEntry(
                    operation=DiffOperation.REMOVE,
                    path=new_path,
                    old_value=left[i],
                ))
            else:
                self._compare_values(left[i], right[i], new_path, diffs)

    def _compare_primitives(
        self,
        left: Any,
        right: Any,
        path: str,
        diffs: List[DiffEntry],
    ):
        """Compare primitive values."""
        if left == right:
            diffs.append(DiffEntry(
                operation=DiffOperation.EQUAL,
                path=path,
                old_value=left,
                new_value=right,
            ))
        else:
            diffs.append(DiffEntry(
                operation=DiffOperation.CHANGE,
                path=path,
                old_value=left,
                new_value=right,
            ))


class TextComparator:
    """Compares text content."""

    def compare(
        self,
        left: str,
        right: str,
        context_lines: int = 3,
    ) -> Dict[str, Any]:
        """Compare two text strings."""
        left_lines = left.splitlines(keepends=True)
        right_lines = right.splitlines(keepends=True)

        differ = difflib.SequenceMatcher(None, left, right)
        op_codes = differ.get_opcodes()

        unified_diff = list(difflib.unified_diff(
            left_lines,
            right_lines,
            n=context_lines,
        ))

        return {
            "are_equal": left == right,
            "similarity_score": differ.ratio(),
            "changes": [
                {
                    "type": self._get_operation_name(code),
                    "i1": i1,
                    "i2": i2,
                    "j1": j1,
                    "j2": j2,
                }
                for code, i1, i2, j1, j2 in op_codes
            ],
            "unified_diff": "".join(unified_diff),
            "left_lines": len(left_lines),
            "right_lines": len(right_lines),
        }

    def _get_operation_name(self, code: str) -> str:
        """Get human-readable operation name."""
        return {
            "replace": "change",
            "delete": "remove",
            "insert": "add",
            "equal": "equal",
        }.get(code, code)


class SchemaComparator:
    """Compares data schemas."""

    def compare(
        self,
        left_schema: Dict[str, Any],
        right_schema: Dict[str, Any],
    ) -> List[SchemaDiff]:
        """Compare two schemas."""
        diffs = []

        left_fields = set(left_schema.keys())
        right_fields = set(right_schema.keys())

        for field_name in left_fields - right_fields:
            diffs.append(SchemaDiff(
                field_name=field_name,
                diff_type="removed",
                old_type=left_schema[field_name].get("type"),
                description=f"Field '{field_name}' removed",
            ))

        for field_name in right_fields - left_fields:
            diffs.append(SchemaDiff(
                field_name=field_name,
                diff_type="added",
                new_type=right_schema[field_name].get("type"),
                description=f"Field '{field_name}' added",
            ))

        for field_name in left_fields & right_fields:
            left_type = left_schema[field_name].get("type")
            right_type = right_schema[field_name].get("type")

            if left_type != right_type:
                diffs.append(SchemaDiff(
                    field_name=field_name,
                    diff_type="type_change",
                    old_type=left_type,
                    new_type=right_type,
                    description=f"Type changed from '{left_type}' to '{right_type}'",
                ))

        return diffs


class SimilarityAnalyzer:
    """Analyzes similarity between data structures."""

    def jaccard_similarity(self, left: Any, right: Any) -> float:
        """Calculate Jaccard similarity coefficient."""
        if isinstance(left, dict) and isinstance(right, dict):
            left_keys = set(left.keys())
            right_keys = set(right.keys())
            intersection = left_keys & right_keys
            union = left_keys | right_keys
            return len(intersection) / len(union) if union else 1.0

        elif isinstance(left, list) and isinstance(right, list):
            left_set = set(str(x) for x in left)
            right_set = set(str(x) for x in right)
            intersection = left_set & right_set
            union = left_set | right_set
            return len(intersection) / len(union) if union else 1.0

        return 1.0 if left == right else 0.0

    def cosine_similarity(self, left: Dict[str, float], right: Dict[str, float]) -> float:
        """Calculate cosine similarity between vectors."""
        all_keys = set(left.keys()) | set(right.keys())

        left_vec = [left.get(k, 0) for k in all_keys]
        right_vec = [right.get(k, 0) for k in all_keys]

        dot_product = sum(a * b for a, b in zip(left_vec, right_vec))
        left_mag = sum(a * a for a in left_vec) ** 0.5
        right_mag = sum(b * b for b in right_vec) ** 0.5

        if left_mag * right_mag == 0:
            return 0.0

        return dot_product / (left_mag * right_mag)

    def levenshtein_distance(self, left: str, right: str) -> int:
        """Calculate Levenshtein edit distance."""
        if len(left) < len(right):
            return self.levenshtein_distance(right, left)

        if len(right) == 0:
            return len(left)

        previous_row = list(range(len(right) + 1))

        for i, c1 in enumerate(left):
            current_row = [i + 1]
            for j, c2 in enumerate(right):
                insertions = previous_row[j + 1] + 1
                deletions = current_row[j] + 1
                substitutions = previous_row[j] + (c1 != c2)
                current_row.append(min(insertions, deletions, substitutions))
            previous_row = current_row

        return previous_row[-1]

    def string_similarity(self, left: str, right: str) -> float:
        """Calculate string similarity using Levenshtein distance."""
        if not left and not right:
            return 1.0

        distance = self.levenshtein_distance(left, right)
        max_len = max(len(left), len(right))

        return 1.0 - (distance / max_len)


class DataComparisonAction:
    """High-level data comparison action."""

    def __init__(
        self,
        comparator: Optional[DataComparator] = None,
        text_comparator: Optional[TextComparator] = None,
        schema_comparator: Optional[SchemaComparator] = None,
        similarity_analyzer: Optional[SimilarityAnalyzer] = None,
    ):
        self.comparator = comparator or DataComparator()
        self.text_comparator = text_comparator or TextComparator()
        self.schema_comparator = schema_comparator or SchemaComparator()
        self.similarity_analyzer = similarity_analyzer or SimilarityAnalyzer()

    def compare_data(
        self,
        left: Any,
        right: Any,
        ignore_fields: Optional[List[str]] = None,
    ) -> ComparisonResult:
        """Compare two data structures."""
        if ignore_fields:
            comparator = DataComparator(ignore_fields)
            return comparator.compare(left, right)
        return self.comparator.compare(left, right)

    def compare_text(
        self,
        left: str,
        right: str,
    ) -> Dict[str, Any]:
        """Compare two text strings."""
        return self.text_comparator.compare(left, right)

    def compare_schema(
        self,
        left_schema: Dict[str, Any],
        right_schema: Dict[str, Any],
    ) -> List[SchemaDiff]:
        """Compare two schemas."""
        return self.schema_comparator.compare(left_schema, right_schema)

    def calculate_similarity(
        self,
        left: Any,
        right: Any,
        method: str = "jaccard",
    ) -> float:
        """Calculate similarity between two values."""
        if method == "jaccard":
            return self.similarity_analyzer.jaccard_similarity(left, right)
        elif method == "cosine":
            if isinstance(left, dict) and isinstance(right, dict):
                return self.similarity_analyzer.cosine_similarity(left, right)
            return 0.0
        elif method == "levenshtein":
            if isinstance(left, str) and isinstance(right, str):
                return self.similarity_analyzer.string_similarity(left, right)
            return 0.0
        return 0.0

    def generate_diff_report(
        self,
        left: Any,
        right: Any,
    ) -> Dict[str, Any]:
        """Generate a comprehensive diff report."""
        result = self.compare_data(left, right)
        return result.to_dict()


# Module exports
__all__ = [
    "DataComparisonAction",
    "DataComparator",
    "TextComparator",
    "SchemaComparator",
    "SimilarityAnalyzer",
    "ComparisonResult",
    "DiffEntry",
    "SchemaDiff",
    "DiffOperation",
]
