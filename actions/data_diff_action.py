"""
Data Diff Action Module.

Compare and compute differences between data sets.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union


@dataclass
class DiffEntry:
    """A difference entry."""
    path: str
    diff_type: str
    old_value: Any = None
    new_value: Any = None


@dataclass
class DiffResult:
    """Result of a diff operation."""
    added: List[DiffEntry] = field(default_factory=list)
    removed: List[DiffEntry] = field(default_factory=list)
    modified: List[DiffEntry] = field(default_factory=list)
    unchanged: int = 0


class DataDiffAction:
    """
    Compute differences between data structures.

    Supports dicts, lists, and nested structures.
    """

    def __init__(self) -> None:
        self._ignore_fields: Set[str] = set()

    def set_ignore_fields(self, fields: List[str]) -> None:
        """Set fields to ignore during comparison."""
        self._ignore_fields = set(fields)

    def diff(
        self,
        old: Any,
        new: Any,
        path: str = "",
    ) -> DiffResult:
        """
        Compute diff between two values.

        Args:
            old: Original value
            new: New value
            path: Current path in structure

        Returns:
            DiffResult with all differences
        """
        result = DiffResult()

        if self._is_same(old, new):
            if not self._is_container(old):
                result.unchanged += 1
            return result

        if old is None:
            result.added.append(DiffEntry(path=path, diff_type="added", new_value=new))
        elif new is None:
            result.removed.append(DiffEntry(path=path, diff_type="removed", old_value=old))
        elif self._is_container(old) and self._is_container(new):
            self._diff_containers(old, new, path, result)
        else:
            result.modified.append(DiffEntry(
                path=path,
                diff_type="modified",
                old_value=old,
                new_value=new,
            ))

        return result

    def _is_same(self, old: Any, new: Any) -> bool:
        """Check if values are the same."""
        return old == new

    def _is_container(self, value: Any) -> bool:
        """Check if value is a container (dict or list)."""
        return isinstance(value, (dict, list))

    def _diff_containers(
        self,
        old: Any,
        new: Any,
        path: str,
        result: DiffResult,
    ) -> None:
        """Diff two containers."""
        if isinstance(old, dict) and isinstance(new, dict):
            self._diff_dicts(old, new, path, result)
        elif isinstance(old, list) and isinstance(new, list):
            self._diff_lists(old, new, path, result)
        else:
            result.modified.append(DiffEntry(
                path=path,
                diff_type="modified",
                old_value=old,
                new_value=new,
            ))

    def _diff_dicts(
        self,
        old: Dict[str, Any],
        new: Dict[str, Any],
        path: str,
        result: DiffResult,
    ) -> None:
        """Diff two dictionaries."""
        all_keys = set(old.keys()) | set(new.keys())

        for key in sorted(all_keys):
            if key in self._ignore_fields:
                continue

            key_path = f"{path}.{key}" if path else key

            if key not in old:
                result.added.append(DiffEntry(
                    path=key_path,
                    diff_type="added",
                    new_value=new[key],
                ))
            elif key not in new:
                result.removed.append(DiffEntry(
                    path=key_path,
                    diff_type="removed",
                    old_value=old[key],
                ))
            else:
                sub_result = self.diff(old[key], new[key], key_path)
                self._merge_diff_results(result, sub_result)

    def _diff_lists(
        self,
        old: List[Any],
        new: List[Any],
        path: str,
        result: DiffResult,
    ) -> None:
        """Diff two lists."""
        max_len = max(len(old), len(new))

        for i in range(max_len):
            idx_path = f"{path}[{i}]"

            if i >= len(old):
                result.added.append(DiffEntry(
                    path=idx_path,
                    diff_type="added",
                    new_value=new[i],
                ))
            elif i >= len(new):
                result.removed.append(DiffEntry(
                    path=idx_path,
                    diff_type="removed",
                    old_value=old[i],
                ))
            else:
                sub_result = self.diff(old[i], new[i], idx_path)
                self._merge_diff_results(result, sub_result)

    def _merge_diff_results(
        self,
        target: DiffResult,
        source: DiffResult,
    ) -> None:
        """Merge source diff result into target."""
        target.added.extend(source.added)
        target.removed.extend(source.removed)
        target.modified.extend(source.modified)
        target.unchanged += source.unchanged

    def diff_summary(self, result: DiffResult) -> Dict[str, Any]:
        """Get summary of diff."""
        return {
            "added_count": len(result.added),
            "removed_count": len(result.removed),
            "modified_count": len(result.modified),
            "unchanged_count": result.unchanged,
            "total_changes": len(result.added) + len(result.removed) + len(result.modified),
        }

    def apply_diff(
        self,
        data: Any,
        diffs: List[DiffEntry],
    ) -> Any:
        """
        Apply diff entries to data.

        Args:
            data: Original data
            diffs: List of diffs to apply

        Returns:
            Modified data
        """
        if isinstance(data, dict):
            return self._apply_dict_diff(data, diffs)
        elif isinstance(data, list):
            return self._apply_list_diff(data, diffs)
        return data

    def _apply_dict_diff(
        self,
        data: Dict[str, Any],
        diffs: List[DiffEntry],
    ) -> Dict[str, Any]:
        """Apply diffs to a dictionary."""
        result = data.copy()

        for diff in diffs:
            path_parts = diff.path.split(".")

            if diff.diff_type == "added":
                current = result
                for part in path_parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                current[path_parts[-1]] = diff.new_value

            elif diff.diff_type == "removed":
                current = result
                for part in path_parts[:-1]:
                    current = current.get(part, {})
                current.pop(path_parts[-1], None)

            elif diff.diff_type == "modified":
                current = result
                for part in path_parts[:-1]:
                    current = current.get(part, {})
                current[path_parts[-1]] = diff.new_value

        return result

    def _apply_list_diff(
        self,
        data: List[Any],
        diffs: List[DiffEntry],
    ) -> List[Any]:
        """Apply diffs to a list."""
        import re

        result = data.copy()

        for diff in diffs:
            match = re.match(r'\[(\d+)\]', diff.path)
            if not match:
                continue

            idx = int(match.group(1))

            if diff.diff_type == "added":
                result.insert(idx, diff.new_value)
            elif diff.diff_type == "removed":
                if 0 <= idx < len(result):
                    result.pop(idx)
            elif diff.diff_type == "modified":
                if 0 <= idx < len(result):
                    result[idx] = diff.new_value

        return result
