"""Data Comparator Action Module.

Provides deep comparison of data structures with diff
generation, schema comparison, and similarity scoring.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class DiffType(Enum):
    """Type of difference."""
    ADDED = "added"
    REMOVED = "removed"
    CHANGED = "changed"
    UNCHANGED = "unchanged"


@dataclass
class Diff:
    """Single difference."""
    path: str
    diff_type: DiffType
    old_value: Any = None
    new_value: Any = None
    children: List["Diff"] = field(default_factory=list)


@dataclass
class ComparisonResult:
    """Comparison result."""
    equal: bool
    diffs: List[Diff] = field(default_factory=list)
    similarity_score: float = 1.0


from enum import Enum


class DataComparatorAction:
    """Data structure comparator.

    Example:
        comparator = DataComparatorAction()

        result = comparator.compare(
            {"a": 1, "b": 2},
            {"a": 1, "c": 3}
        )

        print(result.equal)  # False
        for diff in result.diffs:
            print(f"{diff.path}: {diff.diff_type}")
    """

    def __init__(
        self,
        ignore_fields: Optional[List[str]] = None,
        tolerance: float = 0.0,
    ) -> None:
        self.ignore_fields = ignore_fields or []
        self.tolerance = tolerance

    def compare(
        self,
        left: Any,
        right: Any,
        path: str = "",
    ) -> ComparisonResult:
        """Compare two data structures.

        Args:
            left: Left data structure
            right: Right data structure
            path: Current path for tracking

        Returns:
            ComparisonResult with diffs
        """
        diffs: List[Diff] = []

        if self._should_ignore(path):
            return ComparisonResult(equal=True, diffs=[], similarity_score=1.0)

        if type(left) != type(right):
            diffs.append(Diff(
                path=path or "/",
                diff_type=DiffType.CHANGED,
                old_value=left,
                new_value=right,
            ))
            return ComparisonResult(equal=False, diffs=diffs, similarity_score=0.0)

        if isinstance(left, dict):
            result = self._compare_dicts(left, right, path)
            return result

        elif isinstance(left, (list, tuple)):
            result = self._compare_lists(left, right, path)
            return result

        else:
            if self._values_equal(left, right):
                return ComparisonResult(equal=True, diffs=[], similarity_score=1.0)
            else:
                diffs.append(Diff(
                    path=path or "/",
                    diff_type=DiffType.CHANGED,
                    old_value=left,
                    new_value=right,
                ))
                return ComparisonResult(equal=False, diffs=diffs, similarity_score=0.0)

    def _compare_dicts(
        self,
        left: Dict,
        right: Dict,
        path: str,
    ) -> ComparisonResult:
        """Compare two dictionaries."""
        diffs: List[Diff] = []
        all_keys = set(left.keys()) | set(right.keys())
        changed_count = 0

        for key in all_keys:
            key_path = f"{path}/{key}" if path else f"/{key}"

            if self._should_ignore(key_path):
                continue

            if key not in left:
                diffs.append(Diff(
                    path=key_path,
                    diff_type=DiffType.ADDED,
                    new_value=right[key],
                ))
                changed_count += 1

            elif key not in right:
                diffs.append(Diff(
                    path=key_path,
                    diff_type=DiffType.REMOVED,
                    old_value=left[key],
                ))
                changed_count += 1

            else:
                sub_result = self.compare(left[key], right[key], key_path)
                diffs.extend(sub_result.diffs)
                if not sub_result.equal:
                    changed_count += 1

        total_keys = len(all_keys)
        similarity = 1.0 - (changed_count / total_keys) if total_keys > 0 else 1.0

        return ComparisonResult(
            equal=len(diffs) == 0,
            diffs=diffs,
            similarity_score=similarity,
        )

    def _compare_lists(
        self,
        left: List,
        right: List,
        path: str,
    ) -> ComparisonResult:
        """Compare two lists."""
        diffs: List[Diff] = []
        max_len = max(len(left), len(right))
        changed_count = 0

        for i in range(max_len):
            index_path = f"{path}[{i}]"

            if i >= len(left):
                diffs.append(Diff(
                    path=index_path,
                    diff_type=DiffType.ADDED,
                    new_value=right[i],
                ))
                changed_count += 1

            elif i >= len(right):
                diffs.append(Diff(
                    path=index_path,
                    diff_type=DiffType.REMOVED,
                    old_value=left[i],
                ))
                changed_count += 1

            else:
                sub_result = self.compare(left[i], right[i], index_path)
                diffs.extend(sub_result.diffs)
                if not sub_result.equal:
                    changed_count += 1

        similarity = 1.0 - (changed_count / max_len) if max_len > 0 else 1.0

        return ComparisonResult(
            equal=len(diffs) == 0,
            diffs=diffs,
            similarity_score=similarity,
        )

    def _values_equal(self, left: Any, right: Any) -> bool:
        """Check if values are equal."""
        if left == right:
            return True

        if isinstance(left, (int, float)) and isinstance(right, (int, float)):
            return abs(left - right) <= self.tolerance

        return False

    def _should_ignore(self, path: str) -> bool:
        """Check if path should be ignored."""
        for ignore_field in self.ignore_fields:
            if path == ignore_field or path.startswith(f"{ignore_field}/"):
                return True
        return False

    def get_summary(self, result: ComparisonResult) -> Dict[str, Any]:
        """Get comparison summary."""
        added = sum(1 for d in result.diffs if d.diff_type == DiffType.ADDED)
        removed = sum(1 for d in result.diffs if d.diff_type == DiffType.REMOVED)
        changed = sum(1 for d in result.diffs if d.diff_type == DiffType.CHANGED)

        return {
            "equal": result.equal,
            "total_diffs": len(result.diffs),
            "added": added,
            "removed": removed,
            "changed": changed,
            "similarity_score": result.similarity_score,
        }
