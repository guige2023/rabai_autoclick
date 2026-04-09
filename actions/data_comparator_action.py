"""
Data Comparator Action Module

Provides data comparison and diff functionality for UI automation workflows.
Supports deep comparison of nested structures, fuzzy matching, and
structured diff generation.

Author: AI Agent
Version: 1.0.0
"""

from __future__ import annotations

import difflib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class DiffType(Enum):
    """Diff type enumeration."""
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    UNCHANGED = auto()


class ComparisonMode(Enum):
    """Comparison mode."""
    STRICT = auto()
    LENIENT = auto()
    FUZZY = auto()


@dataclass
class DiffEntry:
    """Single diff entry."""
    diff_type: DiffType
    path: str
    old_value: Any = None
    new_value: Any = None
    description: str = ""


@dataclass
class ComparisonResult:
    """Comparison result."""
    equal: bool
    diffs: list[DiffEntry] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    timestamp: float = field(default_factory=lambda: datetime.utcnow().timestamp())


class DataComparator:
    """
    Deep data comparator with diff generation.

    Example:
        >>> comparator = DataComparator()
        >>> result = comparator.compare({"a": 1}, {"a": 2})
        >>> print(result.equal)  # False
    """

    def __init__(
        self,
        mode: ComparisonMode = ComparisonMode.STRICT,
        tolerance: float = 1e-9,
        ignore_keys: Optional[list[str]] = None,
    ) -> None:
        self.mode = mode
        self.tolerance = tolerance
        self.ignore_keys = set(ignore_keys or [])

    def compare(self, old: Any, new: Any, path: str = "") -> ComparisonResult:
        """Compare two data structures."""
        diffs: list[DiffEntry] = []
        metrics: dict[str, Any] = {"total_paths": 0, "diffs_count": 0}

        self._compare_values(old, new, path or "root", diffs, metrics)

        metrics["diffs_count"] = len(diffs)
        return ComparisonResult(
            equal=len(diffs) == 0,
            diffs=diffs,
            metrics=metrics,
        )

    def _compare_values(
        self,
        old: Any,
        new: Any,
        path: str,
        diffs: list[DiffEntry],
        metrics: dict[str, Any],
    ) -> None:
        """Compare values recursively."""
        metrics["total_paths"] += 1

        if self._is_same_type(old, new) and old == new:
            return

        if self.mode == ComparisonMode.LENIENT:
            if self._is_numeric(old) and self._is_numeric(new):
                if abs(float(old) - float(new)) <= self.tolerance:
                    return

        if isinstance(old, dict) and isinstance(new, dict):
            self._compare_dicts(old, new, path, diffs, metrics)
        elif isinstance(old, (list, tuple)) and isinstance(new, (list, tuple)):
            self._compare_lists(old, new, path, diffs, metrics)
        else:
            diffs.append(DiffEntry(
                diff_type=DiffType.MODIFIED,
                path=path,
                old_value=old,
                new_value=new,
                description=f"Values differ: {old!r} vs {new!r}",
            ))

    def _compare_dicts(
        self,
        old: dict,
        new: dict,
        path: str,
        diffs: list[DiffEntry],
        metrics: dict[str, Any],
    ) -> None:
        """Compare dictionaries."""
        all_keys = set(old.keys()) | set(new.keys())

        for key in sorted(all_keys):
            if key in self.ignore_keys:
                continue

            key_path = f"{path}.{key}" if path else key
            old_value = old.get(key)
            new_value = new.get(key)

            if key not in old:
                diffs.append(DiffEntry(
                    diff_type=DiffType.ADDED,
                    path=key_path,
                    new_value=new_value,
                    description=f"Key added: {key}",
                ))
            elif key not in new:
                diffs.append(DiffEntry(
                    diff_type=DiffType.REMOVED,
                    path=key_path,
                    old_value=old_value,
                    description=f"Key removed: {key}",
                ))
            else:
                self._compare_values(old_value, new_value, key_path, diffs, metrics)

    def _compare_lists(
        self,
        old: list,
        new: list,
        path: str,
        diffs: list[DiffEntry],
        metrics: dict[str, Any],
    ) -> None:
        """Compare lists."""
        max_len = max(len(old), len(new))

        for i in range(max_len):
            item_path = f"{path}[{i}]"

            if i >= len(old):
                diffs.append(DiffEntry(
                    diff_type=DiffType.ADDED,
                    path=item_path,
                    new_value=new[i],
                    description=f"Item added at index {i}",
                ))
            elif i >= len(new):
                diffs.append(DiffEntry(
                    diff_type=DiffType.REMOVED,
                    path=item_path,
                    old_value=old[i],
                    description=f"Item removed at index {i}",
                ))
            else:
                self._compare_values(old[i], new[i], item_path, diffs, metrics)

    def _is_same_type(self, a: Any, b: Any) -> bool:
        """Check if values have same type."""
        return type(a) == type(b)

    def _is_numeric(self, value: Any) -> bool:
        """Check if value is numeric."""
        return isinstance(value, (int, float))


class StringComparator:
    """
    String comparison with multiple algorithms.

    Example:
        >>> comp = StringComparator()
        >>> ratio = comp.similarity("hello", "helo")
        >>> print(ratio)  # ~0.8
    """

    def similarity(self, s1: str, s2: str) -> float:
        """Calculate similarity ratio (0-1)."""
        if not s1 and not s2:
            return 1.0
        if not s1 or not s2:
            return 0.0
        return difflib.SequenceMatcher(None, s1, s2).ratio()

    def diff_lines(self, old: str, new: str) -> list[DiffEntry]:
        """Generate line-by-line diff."""
        diffs: list[DiffEntry] = []
        old_lines = old.splitlines(keepends=True)
        new_lines = new.splitlines(keepends=True)

        matcher = difflib.SequenceMatcher(None, old_lines, new_lines)

        for tag, i1, i2, j1, j2 in matcher.get_opcodes():
            if tag == "replace":
                for i, line in enumerate(old_lines[i1:i2]):
                    diffs.append(DiffEntry(
                        diff_type=DiffType.REMOVED,
                        path=f"line:{i1+i+1}",
                        old_value=line.rstrip("\n"),
                        description=f"Removed line {i1+i+1}",
                    ))
                for i, line in enumerate(new_lines[j1:j2]):
                    diffs.append(DiffEntry(
                        diff_type=DiffType.ADDED,
                        path=f"line:{j1+i+1}",
                        new_value=line.rstrip("\n"),
                        description=f"Added line {j1+i+1}",
                    ))
            elif tag == "delete":
                for i, line in enumerate(old_lines[i1:i2]):
                    diffs.append(DiffEntry(
                        diff_type=DiffType.REMOVED,
                        path=f"line:{i1+i+1}",
                        old_value=line.rstrip("\n"),
                        description=f"Removed line {i1+i+1}",
                    ))
            elif tag == "insert":
                for i, line in enumerate(new_lines[j1:j2]):
                    diffs.append(DiffEntry(
                        diff_type=DiffType.ADDED,
                        path=f"line:{j1+i+1}",
                        new_value=line.rstrip("\n"),
                        description=f"Added line {j1+i+1}",
                    ))

        return diffs

    def unified_diff(self, old: str, new: str, context: int = 3) -> str:
        """Generate unified diff string."""
        return "".join(difflib.unified_diff(
            old.splitlines(keepends=True),
            new.splitlines(keepends=True),
            n=context,
        ))


class FuzzyComparator:
    """
    Fuzzy comparator for approximate matching.

    Example:
        >>> comp = FuzzyComparator()
        >>> result = comp.match("hello world", "hello worl", threshold=0.9)
    """

    def __init__(self, threshold: float = 0.8) -> None:
        self.threshold = threshold
        self._string_comp = StringComparator()

    def match(self, old: Any, new: Any, threshold: Optional[float] = None) -> bool:
        """Check if values match within threshold."""
        threshold = threshold or self.threshold

        if isinstance(old, str) and isinstance(new, str):
            return self._string_comp.similarity(old, new) >= threshold

        if isinstance(old, (int, float)) and isinstance(new, (int, float)):
            return abs(old - new) <= threshold

        if isinstance(old, dict) and isinstance(new, dict):
            return self._match_dicts(old, new, threshold)

        if isinstance(old, (list, tuple)) and isinstance(new, (list, tuple)):
            return self._match_lists(old, new, threshold)

        return old == new

    def _match_dicts(self, old: dict, new: dict, threshold: float) -> bool:
        """Fuzzy match dictionaries."""
        all_keys = set(old.keys()) | set(new.keys())
        match_count = 0

        for key in all_keys:
            if key in old and key in new:
                if self.match(old[key], new[key], threshold):
                    match_count += 1
            elif key in old and key not in new:
                pass
            else:
                pass

        return match_count / len(all_keys) >= threshold if all_keys else True

    def _match_lists(self, old: list, new: list, threshold: float) -> bool:
        """Fuzzy match lists."""
        max_len = max(len(old), len(new))
        if max_len == 0:
            return True

        match_count = 0
        min_len = min(len(old), len(new))

        for i in range(min_len):
            if self.match(old[i], new[i], threshold):
                match_count += 1

        return match_count / max_len >= threshold


class StructuredDiffGenerator:
    """
    Generates structured diff reports.

    Example:
        >>> generator = StructuredDiffGenerator()
        >>> report = generator.generate(old_data, new_data)
    """

    def __init__(self, comparator: Optional[DataComparator] = None) -> None:
        self.comparator = comparator or DataComparator()

    def generate(self, old: Any, new: Any) -> dict[str, Any]:
        """Generate structured diff report."""
        result = self.comparator.compare(old, new)

        return {
            "equal": result.equal,
            "summary": {
                "total_diffs": len(result.diffs),
                "added": sum(1 for d in result.diffs if d.diff_type == DiffType.ADDED),
                "removed": sum(1 for d in result.diffs if d.diff_type == DiffType.REMOVED),
                "modified": sum(1 for d in result.diffs if d.diff_type == DiffType.MODIFIED),
            },
            "diffs": [
                {
                    "type": d.diff_type.name,
                    "path": d.path,
                    "old_value": d.old_value,
                    "new_value": d.new_value,
                    "description": d.description,
                }
                for d in result.diffs
            ],
            "metrics": result.metrics,
        }

    def generate_json(self, old: Any, new: Any, indent: int = 2) -> str:
        """Generate JSON diff report."""
        import json
        return json.dumps(self.generate(old, new), indent=indent, default=str)
