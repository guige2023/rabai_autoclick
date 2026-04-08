"""Data Diff Action Module.

Provides structured diff generation for data structures,
patch application, and three-way merging.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union
import logging

logger = logging.getLogger(__name__)


class DiffOperation(Enum):
    """Diff operation type."""
    ADD = "add"
    REMOVE = "remove"
    MODIFY = "modify"
    MOVE = "move"


@dataclass
class DiffEntry:
    """Single diff entry."""
    path: str
    operation: DiffOperation
    old_value: Any = None
    new_value: Any = None
    old_index: Optional[int] = None
    new_index: Optional[int] = None


@dataclass
class DiffResult:
    """Complete diff result."""
    entries: List[DiffEntry] = field(default_factory=list)
    added_count: int = 0
    removed_count: int = 0
    modified_count: int = 0


class DataDiffAction:
    """Structured diff generator.

    Example:
        differ = DataDiffAction()

        diff = differ.diff(
            {"a": 1, "b": 2},
            {"a": 1, "c": 3}
        )

        print(f"Added: {diff.added_count}")
        for entry in diff.entries:
            print(f"{entry.operation}: {entry.path}")
    """

    def __init__(self) -> None:
        pass

    def diff(
        self,
        left: Any,
        right: Any,
        path: str = "",
    ) -> DiffResult:
        """Generate diff between two data structures.

        Args:
            left: Original data
            right: Modified data
            path: Current path for tracking

        Returns:
            DiffResult with all differences
        """
        result = DiffResult()

        if type(left) != type(right):
            result.entries.append(DiffEntry(
                path=path or "/",
                operation=DiffOperation.MODIFY,
                old_value=left,
                new_value=right,
            ))
            result.modified_count += 1
            return result

        if isinstance(left, dict):
            self._diff_dicts(left, right, path, result)
        elif isinstance(left, list):
            self._diff_lists(left, right, path, result)
        else:
            if left != right:
                result.entries.append(DiffEntry(
                    path=path or "/",
                    operation=DiffOperation.MODIFY,
                    old_value=left,
                    new_value=right,
                ))
                result.modified_count += 1

        return result

    def _diff_dicts(
        self,
        left: Dict,
        right: Dict,
        path: str,
        result: DiffResult,
    ) -> None:
        """Diff two dictionaries."""
        all_keys = set(left.keys()) | set(right.keys())

        for key in all_keys:
            key_path = f"{path}.{key}" if path else key

            if key not in left:
                result.entries.append(DiffEntry(
                    path=key_path,
                    operation=DiffOperation.ADD,
                    new_value=right[key],
                ))
                result.added_count += 1

            elif key not in right:
                result.entries.append(DiffEntry(
                    path=key_path,
                    operation=DiffOperation.REMOVE,
                    old_value=left[key],
                ))
                result.removed_count += 1

            else:
                self.diff(left[key], right[key], key_path, result)

    def _diff_lists(
        self,
        left: List,
        right: List,
        path: str,
        result: DiffResult,
    ) -> None:
        """Diff two lists using LCS algorithm."""
        lcs = self._longest_common_subsequence(left, right)

        left_idx = 0
        right_idx = 0
        lcs_idx = 0

        while left_idx < len(left) or right_idx < len(right):
            if lcs_idx < len(lcs):
                lcs_item = lcs[lcs_idx]

                while left_idx < len(left) and left[left_idx] != lcs_item:
                    result.entries.append(DiffEntry(
                        path=f"{path}[{left_idx}]",
                        operation=DiffOperation.REMOVE,
                        old_value=left[left_idx],
                        old_index=left_idx,
                    ))
                    result.removed_count += 1
                    left_idx += 1

                while right_idx < len(right) and right[right_idx] != lcs_item:
                    result.entries.append(DiffEntry(
                        path=f"{path}[{right_idx}]",
                        operation=DiffOperation.ADD,
                        new_value=right[right_idx],
                        new_index=right_idx,
                    ))
                    result.added_count += 1
                    right_idx += 1

                lcs_idx += 1
                left_idx += 1
                right_idx += 1
            else:
                while left_idx < len(left):
                    result.entries.append(DiffEntry(
                        path=f"{path}[{left_idx}]",
                        operation=DiffOperation.REMOVE,
                        old_value=left[left_idx],
                        old_index=left_idx,
                    ))
                    result.removed_count += 1
                    left_idx += 1

                while right_idx < len(right):
                    result.entries.append(DiffEntry(
                        path=f"{path}[{right_idx}]",
                        operation=DiffOperation.ADD,
                        new_value=right[right_idx],
                        new_index=right_idx,
                    ))
                    result.added_count += 1
                    right_idx += 1

    def _longest_common_subsequence(
        self,
        left: List,
        right: List,
    ) -> List:
        """Compute LCS of two lists."""
        m, n = len(left), len(right)
        dp = [[0] * (n + 1) for _ in range(m + 1)]

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if left[i - 1] == right[j - 1]:
                    dp[i][j] = dp[i - 1][j - 1] + 1
                else:
                    dp[i][j] = max(dp[i - 1][j], dp[i][j - 1])

        lcs = []
        i, j = m, n
        while i > 0 and j > 0:
            if left[i - 1] == right[j - 1]:
                lcs.insert(0, left[i - 1])
                i -= 1
                j -= 1
            elif dp[i - 1][j] > dp[i][j - 1]:
                i -= 1
            else:
                j -= 1

        return lcs

    def apply_patch(
        self,
        data: Any,
        diff: DiffResult,
    ) -> Any:
        """Apply diff to data.

        Args:
            data: Original data
            diff: Diff to apply

        Returns:
            Patched data
        """
        if isinstance(data, dict):
            return self._apply_dict_patch(data, diff)
        elif isinstance(data, list):
            return self._apply_list_patch(data, diff)
        return data

    def _apply_dict_patch(
        self,
        data: Dict,
        diff: DiffResult,
    ) -> Dict:
        """Apply patch to dictionary."""
        result = dict(data)

        for entry in diff.entries:
            if entry.operation == DiffOperation.ADD:
                result[entry.path] = entry.new_value
            elif entry.operation == DiffOperation.REMOVE:
                result.pop(entry.path, None)
            elif entry.operation == DiffOperation.MODIFY:
                result[entry.path] = entry.new_value

        return result

    def _apply_list_patch(
        self,
        data: List,
        diff: DiffResult,
    ) -> List:
        """Apply patch to list."""
        result = list(data)

        for entry in sorted(
            [e for e in diff.entries if e.operation == DiffOperation.ADD],
            key=lambda x: x.new_index or 0,
            reverse=True
        ):
            if entry.new_index is not None:
                result.insert(entry.new_index, entry.new_value)

        for entry in sorted(
            [e for e in diff.entries if e.operation == DiffOperation.REMOVE],
            key=lambda x: x.old_index or 0,
            reverse=True
        ):
            if entry.old_index is not None:
                result.pop(entry.old_index)

        return result

    def merge_patches(
        self,
        base: Any,
        diff1: DiffResult,
        diff2: DiffResult,
    ) -> Any:
        """Merge two patches from same base.

        Args:
            base: Base data
            diff1: First diff
            diff2: Second diff

        Returns:
            Merged result
        """
        merged = self.apply_patch(base, diff1)
        merged = self.apply_patch(merged, diff2)
        return merged
