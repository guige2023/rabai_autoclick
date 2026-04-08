"""
Data Delta Action Module.

Computes differences between data versions,
supports structural diff, value diff, and patch application.
"""

from __future__ import annotations

from typing import Any, Optional
from dataclasses import dataclass, field
from enum import Enum
import logging

logger = logging.getLogger(__name__)


class DeltaType(Enum):
    """Types of data changes."""
    ADDED = "added"
    REMOVED = "removed"
    MODIFIED = "modified"
    UNCHANGED = "unchanged"


@dataclass
class DeltaEntry:
    """Single change entry."""
    path: str
    delta_type: DeltaType
    old_value: Any = None
    new_value: Any = None


@dataclass
class DeltaResult:
    """Result of delta computation."""
    changes: list[DeltaEntry]
    added_count: int = 0
    removed_count: int = 0
    modified_count: int = 0
    unchanged_count: int = 0


class DataDeltaAction:
    """
    Computes structural and value differences between data.

    Supports nested dicts, lists, and scalar values.
    Generates change entries with path, type, and values.

    Example:
        delta = DataDeltaAction()
        result = delta.diff(old_data, new_data)
        print(f"Added: {result.added_count}")
    """

    def __init__(
        self,
        ignore_keys: Optional[set[str]] = None,
        case_sensitive: bool = True,
    ) -> None:
        self.ignore_keys = ignore_keys or set()
        self.case_sensitive = case_sensitive

    def diff(
        self,
        old_data: Any,
        new_data: Any,
        path: str = "",
    ) -> DeltaResult:
        """Compute delta between two data structures."""
        changes: list[DeltaEntry] = []
        self._diff_recursive(old_data, new_data, path, changes)

        added = sum(1 for c in changes if c.delta_type == DeltaType.ADDED)
        removed = sum(1 for c in changes if c.delta_type == DeltaType.REMOVED)
        modified = sum(1 for c in changes if c.delta_type == DeltaType.MODIFIED)
        unchanged = sum(1 for c in changes if c.delta_type == DeltaType.UNCHANGED)

        return DeltaResult(
            changes=changes,
            added_count=added,
            removed_count=removed,
            modified_count=modified,
            unchanged_count=unchanged,
        )

    def _diff_recursive(
        self,
        old_data: Any,
        new_data: Any,
        path: str,
        changes: list[DeltaEntry],
    ) -> None:
        """Recursively compute differences."""
        if self._should_ignore(path):
            return

        if type(old_data) != type(new_data):
            changes.append(DeltaEntry(
                path=path or "/",
                delta_type=DeltaType.MODIFIED,
                old_value=old_data,
                new_value=new_data,
            ))
            return

        if isinstance(old_data, dict):
            self._diff_dict(old_data, new_data, path, changes)

        elif isinstance(old_data, (list, tuple)):
            self._diff_list(old_data, new_data, path, changes)

        else:
            if not self._values_equal(old_data, new_data):
                changes.append(DeltaEntry(
                    path=path or "/",
                    delta_type=DeltaType.MODIFIED,
                    old_value=old_data,
                    new_value=new_data,
                ))
            else:
                changes.append(DeltaEntry(
                    path=path or "/",
                    delta_type=DeltaType.UNCHANGED,
                    old_value=old_data,
                    new_value=new_data,
                ))

    def _diff_dict(
        self,
        old_dict: dict,
        new_dict: dict,
        path: str,
        changes: list[DeltaEntry],
    ) -> None:
        """Diff two dictionaries."""
        all_keys = set(old_dict.keys()) | set(new_dict.keys())

        for key in sorted(all_keys):
            if self._should_ignore(key):
                continue
            new_path = f"{path}.{key}" if path else key

            if key not in new_dict:
                changes.append(DeltaEntry(
                    path=new_path,
                    delta_type=DeltaType.REMOVED,
                    old_value=old_dict[key],
                ))
            elif key not in old_dict:
                changes.append(DeltaEntry(
                    path=new_path,
                    delta_type=DeltaType.ADDED,
                    new_value=new_dict[key],
                ))
            else:
                self._diff_recursive(old_dict[key], new_dict[key], new_path, changes)

    def _diff_list(
        self,
        old_list: list,
        new_list: list,
        path: str,
        changes: list[DeltaEntry],
    ) -> None:
        """Diff two lists."""
        max_len = max(len(old_list), len(new_list))

        for idx in range(max_len):
            new_path = f"{path}[{idx}]"

            if idx >= len(old_list):
                changes.append(DeltaEntry(
                    path=new_path,
                    delta_type=DeltaType.ADDED,
                    new_value=new_list[idx],
                ))
            elif idx >= len(new_list):
                changes.append(DeltaEntry(
                    path=new_path,
                    delta_type=DeltaType.REMOVED,
                    old_value=old_list[idx],
                ))
            else:
                self._diff_recursive(old_list[idx], new_list[idx], new_path, changes)

    def _should_ignore(self, key: str) -> bool:
        """Check if a key should be ignored."""
        return key in self.ignore_keys

    def _values_equal(self, a: Any, b: Any) -> bool:
        """Check if two values are equal."""
        if not self.case_sensitive and isinstance(a, str) and isinstance(b, str):
            return a.lower() == b.lower()
        return a == b

    def apply_patch(
        self,
        base: Any,
        delta: DeltaResult,
    ) -> Any:
        """Apply a delta result to base data to produce new data."""
        import copy
        result = copy.deepcopy(base)

        for entry in delta.changes:
            if entry.delta_type == DeltaType.UNCHANGED:
                continue
            self._apply_entry(result, entry)

        return result

    def _apply_entry(self, data: Any, entry: DeltaEntry) -> None:
        """Apply a single delta entry to data."""
        path = entry.path.strip("/")
        parts = path.split(".") if path else []

        current = data
        for part in parts[:-1]:
            if part not in current:
                return
            current = current[part]

        key = parts[-1] if parts else None

        if entry.delta_type == DeltaType.ADDED:
            if key:
                current[key] = entry.new_value

        elif entry.delta_type == DeltaType.REMOVED:
            if key and key in current:
                del current[key]

        elif entry.delta_type == DeltaType.MODIFIED:
            if key:
                current[key] = entry.new_value
