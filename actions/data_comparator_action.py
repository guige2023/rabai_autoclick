"""
Data Comparator Action Module.

Compares data structures with detailed diff reports,
supports nested structures and custom comparators.
"""

from __future__ import annotations

from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DiffType(Enum):
    """Type of difference found."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    TYPE_CHANGED = "type_changed"


@dataclass
class DiffEntry:
    """Single difference entry."""
    path: str
    diff_type: DiffType
    old_value: Any = None
    new_value: Any = None


@dataclass
class ComparisonResult:
    """Result of data comparison."""
    equal: bool
    diffs: list[DiffEntry]
    added_count: int = 0
    removed_count: int = 0
    modified_count: int = 0


class DataComparatorAction:
    """
    Data comparison with detailed diff reporting.

    Compares nested data structures with
    configurable comparison strategies.

    Example:
        comparator = DataComparatorAction()
        result = comparator.compare(data_a, data_b)
        print(f"Equal: {result.equal}")
    """

    def __init__(
        self,
        ignore_keys: Optional[set[str]] = None,
        case_sensitive: bool = True,
        float_tolerance: float = 0.0,
    ) -> None:
        self.ignore_keys = ignore_keys or set()
        self.case_sensitive = case_sensitive
        self.float_tolerance = float_tolerance

    def compare(
        self,
        a: Any,
        b: Any,
        path: str = "",
    ) -> ComparisonResult:
        """Compare two data structures."""
        diffs: list[DiffEntry] = []

        self._compare_recursive(a, b, path, diffs)

        added = sum(1 for d in diffs if d.diff_type == DiffType.ADDED)
        removed = sum(1 for d in diffs if d.diff_type == DiffType.REMOVED)
        modified = sum(1 for d in diffs if d.diff_type in (DiffType.MODIFIED, DiffType.TYPE_CHANGED))

        return ComparisonResult(
            equal=len(diffs) == 0,
            diffs=diffs,
            added_count=added,
            removed_count=removed,
            modified_count=modified,
        )

    def _compare_recursive(
        self,
        a: Any,
        b: Any,
        path: str,
        diffs: list[DiffEntry],
    ) -> None:
        """Recursively compare values."""
        key = path.split(".")[-1] if path else ""

        if key in self.ignore_keys:
            return

        if self._values_equal(a, b):
            return

        type_a = type(a)
        type_b = type(b)

        if type_a != type_b:
            diffs.append(DiffEntry(
                path=path or "root",
                diff_type=DiffType.TYPE_CHANGED,
                old_value=a,
                new_value=b,
            ))
            return

        if isinstance(a, dict):
            self._compare_dicts(a, b, path, diffs)
        elif isinstance(a, (list, tuple)):
            self._compare_lists(a, b, path, diffs)
        else:
            diffs.append(DiffEntry(
                path=path or "root",
                diff_type=DiffType.MODIFIED,
                old_value=a,
                new_value=b,
            ))

    def _compare_dicts(
        self,
        a: dict,
        b: dict,
        path: str,
        diffs: list[DiffEntry],
    ) -> None:
        """Compare two dictionaries."""
        all_keys = set(a.keys()) | set(b.keys())

        for key in all_keys:
            if key in self.ignore_keys:
                continue

            new_path = f"{path}.{key}" if path else key

            if key not in b:
                diffs.append(DiffEntry(
                    path=new_path,
                    diff_type=DiffType.REMOVED,
                    old_value=a[key],
                ))
            elif key not in a:
                diffs.append(DiffEntry(
                    path=new_path,
                    diff_type=DiffType.ADDED,
                    new_value=b[key],
                ))
            else:
                self._compare_recursive(a[key], b[key], new_path, diffs)

    def _compare_lists(
        self,
        a: list,
        b: list,
        path: str,
        diffs: list[DiffEntry],
    ) -> None:
        """Compare two lists."""
        max_len = max(len(a), len(len))

        for i in range(max_len):
            new_path = f"{path}[{i}]"

            if i >= len(a):
                diffs.append(DiffEntry(
                    path=new_path,
                    diff_type=DiffType.ADDED,
                    new_value=b[i],
                ))
            elif i >= len(b):
                diffs.append(DiffEntry(
                    path=new_path,
                    diff_type=DiffType.REMOVED,
                    old_value=a[i],
                ))
            else:
                self._compare_recursive(a[i], b[i], new_path, diffs)

    def _values_equal(self, a: Any, b: Any) -> bool:
        """Check if two values are equal."""
        if self.float_tolerance > 0:
            if isinstance(a, float) and isinstance(b, float):
                return abs(a - b) <= self.float_tolerance

        if not self.case_sensitive and isinstance(a, str) and isinstance(b, str):
            return a.lower() == b.lower()

        return a == b

    def get_diff_summary(
        self,
        result: ComparisonResult,
    ) -> str:
        """Get human-readable diff summary."""
        if result.equal:
            return "Data structures are equal"

        lines = [
            f"Differences found: {len(result.diffs)}",
            f"  Added: {result.added_count}",
            f"  Removed: {result.removed_count}",
            f"  Modified: {result.modified_count}",
            "",
        ]

        for diff in result.diffs[:10]:
            lines.append(f"  {diff.diff_type.name}: {diff.path}")
            if diff.old_value is not None:
                lines.append(f"    old: {diff.old_value}")
            if diff.new_value is not None:
                lines.append(f"    new: {diff.new_value}")

        if len(result.diffs) > 10:
            lines.append(f"  ... and {len(result.diffs) - 10} more")

        return "\n".join(lines)
