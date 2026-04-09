"""Data Diff and Compare Engine.

This module provides data comparison:
- Record-level diffing
- Field-level change tracking
- Deep object comparison
- Diff summarization

Example:
    >>> from actions.data_diff_action import DataDiffer
    >>> differ = DataDiffer()
    >>> diffs = differ.diff(record_a, record_b)
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class DiffType(Enum):
    """Types of differences."""
    ADDED = "added"
    REMOVED = "removed"
    CHANGED = "changed"
    UNCHANGED = "unchanged"


@dataclass
class DiffEntry:
    """A single difference entry."""
    path: str
    diff_type: DiffType
    old_value: Any = None
    new_value: Any = None
    field_type: str = ""


@dataclass
class DiffResult:
    """Result of a diff operation."""
    has_changes: bool
    added_count: int
    removed_count: int
    changed_count: int
    diffs: list[DiffEntry]
    summary: str = ""


class DataDiffer:
    """Compares data structures and records."""

    def __init__(self, ignore_fields: Optional[list[str]] = None) -> None:
        """Initialize the data differ.

        Args:
            ignore_fields: Fields to ignore in comparisons.
        """
        self._ignore_fields = set(ignore_fields or [])
        self._lock = threading.Lock()
        self._stats = {"comparisons": 0, "changes_found": 0}

    def diff(
        self,
        old: dict[str, Any],
        new: dict[str, Any],
        path: str = "",
    ) -> DiffResult:
        """Diff two records.

        Args:
            old: Old record.
            new: New record.
            path: Current path for nested diffs.

        Returns:
            DiffResult with all differences.
        """
        self._stats["comparisons"] += 1
        diffs = []

        all_keys = set(old.keys()) | set(new.keys())

        for key in all_keys:
            if key in self._ignore_fields:
                continue

            current_path = f"{path}.{key}" if path else key
            old_val = old.get(key)
            new_val = new.get(key)

            if key not in old:
                diffs.append(DiffEntry(path=current_path, diff_type=DiffType.ADDED, new_value=new_val))
                self._stats["changes_found"] += 1
            elif key not in new:
                diffs.append(DiffEntry(path=current_path, diff_type=DiffType.REMOVED, old_value=old_val))
                self._stats["changes_found"] += 1
            elif self._values_differ(old_val, new_val):
                if isinstance(old_val, dict) and isinstance(new_val, dict):
                    nested = self.diff(old_val, new_val, current_path)
                    diffs.extend(nested.diffs)
                else:
                    diffs.append(DiffEntry(path=current_path, diff_type=DiffType.CHANGED, old_value=old_val, new_value=new_val))
                    self._stats["changes_found"] += 1

        added = sum(1 for d in diffs if d.diff_type == DiffType.ADDED)
        removed = sum(1 for d in diffs if d.diff_type == DiffType.REMOVED)
        changed = sum(1 for d in diffs if d.diff_type == DiffType.CHANGED)

        return DiffResult(
            has_changes=len(diffs) > 0,
            added_count=added,
            removed_count=removed,
            changed_count=changed,
            diffs=diffs,
            summary=f"{added} added, {removed} removed, {changed} changed",
        )

    def diff_list(
        self,
        old: list[dict[str, Any]],
        new: list[dict[str, Any]],
        key_field: str = "id",
    ) -> DiffResult:
        """Diff two lists of records.

        Args:
            old: Old list.
            new: New list.
            key_field: Field to use as key for matching.

        Returns:
            DiffResult with list differences.
        """
        old_index = {r.get(key_field): r for r in old}
        new_index = {r.get(key_field): r for r in new}

        all_keys = set(old_index.keys()) | set(new_index.keys())
        diffs = []

        for key in all_keys:
            old_rec = old_index.get(key)
            new_rec = new_index.get(key)

            if old_rec is None:
                diffs.append(DiffEntry(path=f"[{key}]", diff_type=DiffType.ADDED, new_value=new_rec))
                self._stats["changes_found"] += 1
            elif new_rec is None:
                diffs.append(DiffEntry(path=f"[{key}]", diff_type=DiffType.REMOVED, old_value=old_rec))
                self._stats["changes_found"] += 1
            else:
                rec_diff = self.diff(old_rec, new_rec, path=f"[{key}]")
                diffs.extend(rec_diff.diffs)

        added = sum(1 for d in diffs if d.diff_type == DiffType.ADDED)
        removed = sum(1 for d in diffs if d.diff_type == DiffType.REMOVED)
        changed = sum(1 for d in diffs if d.diff_type == DiffType.CHANGED)

        return DiffResult(
            has_changes=len(diffs) > 0,
            added_count=added,
            removed_count=removed,
            changed_count=changed,
            diffs=diffs,
            summary=f"{added} added, {removed} removed, {changed} changed",
        )

    def _values_differ(self, old: Any, new: Any) -> bool:
        """Check if two values differ."""
        if type(old) != type(new):
            return True
        if isinstance(old, dict):
            return old != new
        if isinstance(old, (list, tuple)):
            return old != new
        return old != new

    def get_stats(self) -> dict[str, int]:
        """Get diff statistics."""
        with self._lock:
            return dict(self._stats)
