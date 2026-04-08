"""Diff utilities for RabAI AutoClick.

Provides:
- Text diff generation (unified, side-by-side)
- Structural diff for nested objects
- Line-by-line diff with context
- Diff statistics and metrics
"""

from __future__ import annotations

import difflib
import json
from typing import (
    Any,
    Dict,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Union,
)


class DiffSegment(NamedTuple):
    """A segment of a diff."""
    tag: str  # 'equal', 'insert', 'delete', 'replace'
    text: str


class DiffResult(NamedTuple):
    """Result of a text diff operation."""
    segments: List[DiffSegment]
    added_lines: int
    removed_lines: int
    unchanged_lines: int


def text_diff(
    old_text: str,
    new_text: str,
    context: int = 3,
    ignore_whitespace: bool = False,
) -> DiffResult:
    """Generate a structured diff between two text strings.

    Args:
        old_text: Original text.
        new_text: New text.
        context: Number of context lines around changes.
        ignore_whitespace: Whether to ignore whitespace changes.

    Returns:
        DiffResult with segments and statistics.
    """
    if ignore_whitespace:
        old_lines = old_text.splitlines(keepends=True)
        new_lines = new_text.splitlines(keepends=True)
        old_text = "".join(line.rstrip() + "\n" for line in old_lines)
        new_text = "".join(line.rstrip() + "\n" for line in new_lines)

    old_lines = old_text.splitlines(keepends=True)
    new_lines = new_text.splitlines(keepends=True)

    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    segments: List[DiffSegment] = []
    added = 0
    removed = 0
    unchanged = 0

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            segments.append(DiffSegment("equal", "".join(old_lines[i1:i2])))
            unchanged += i2 - i1
        elif tag == "delete":
            segments.append(DiffSegment("delete", "".join(old_lines[i1:i2])))
            removed += i2 - i1
        elif tag == "insert":
            segments.append(DiffSegment("insert", "".join(new_lines[j1:j2])))
            added += j2 - j1
        elif tag == "replace":
            segments.append(DiffSegment("delete", "".join(old_lines[i1:i2])))
            segments.append(DiffSegment("insert", "".join(new_lines[j1:j2])))
            removed += i2 - i1
            added += j2 - j1

    return DiffResult(
        segments=segments,
        added_lines=added,
        removed_lines=removed,
        unchanged_lines=unchanged,
    )


def unified_diff(
    old_text: str,
    new_text: str,
    old_name: str = "old",
    new_name: str = "new",
    context: int = 3,
) -> str:
    """Generate a unified diff string.

    Args:
        old_text: Original text.
        new_text: New text.
        old_name: Filename for old version.
        new_name: Filename for new version.
        context: Number of context lines.

    Returns:
        Unified diff string.
    """
    old_lines = old_text.splitlines(keepends=True)
    new_lines = new_text.splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            old_lines,
            new_lines,
            fromfile=old_name,
            tofile=new_name,
            n=context,
        )
    )


def side_by_side_diff(
    old_text: str,
    new_text: str,
    width: int = 80,
) -> str:
    """Generate a side-by-side diff view.

    Args:
        old_text: Original text.
        new_text: New text.
        width: Total display width.

    Returns:
        Side-by-side diff string.
    """
    half = max(width // 2 - 6, 20)
    old_lines = old_text.splitlines()
    new_lines = new_text.splitlines()

    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    result_lines: List[str] = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for o_line, n_line in zip(old_lines[i1:i2], new_lines[j1:j2]):
                result_lines.append(
                    f"{' ' * half} | {trunc(o_line, half)}"
                )
        elif tag == "delete":
            for line in old_lines[i1:i2]:
                result_lines.append(
                    f"{trunc(line, half):<{half}} | "
                )
        elif tag == "insert":
            for line in new_lines[j1:j2]:
                result_lines.append(
                    f"{' ' * half} | +{trunc(line, half-1)}"
                )
        elif tag == "replace":
            for o_line, n_line in zip(old_lines[i1:i2], new_lines[j1:j2]):
                result_lines.append(
                    f"{trunc(o_line, half):<{half}} | +{trunc(n_line, half-1)}"
                )

    return "\n".join(result_lines)


def trunc(s: str, max_len: int) -> str:
    """Truncate a string to max_len."""
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


def dict_diff(
    old_dict: Dict[str, Any],
    new_dict: Dict[str, Any],
    path: str = "",
) -> List[Tuple[str, Any, Any]]:
    """Compare two dictionaries and return differences.

    Args:
        old_dict: Original dictionary.
        new_dict: New dictionary.
        path: Current path for nested comparisons.

    Returns:
        List of (key_path, old_value, new_value) for changed keys.
    """
    diffs: List[Tuple[str, Any, Any]] = []
    all_keys = set(old_dict.keys()) | set(new_dict.keys())

    for key in sorted(all_keys):
        current_path = f"{path}.{key}" if path else key
        old_val = old_dict.get(key, None)
        new_val = new_dict.get(key, None)

        if isinstance(old_val, dict) and isinstance(new_val, dict):
            diffs.extend(dict_diff(old_val, new_val, current_path))
        elif old_val != new_val:
            diffs.append((current_path, old_val, new_val))

    return diffs


def json_diff(
    old_json: Union[str, Dict],
    new_json: Union[str, Dict],
) -> List[Tuple[str, Any, Any]]:
    """Compare two JSON values (str or dict).

    Args:
        old_json: Original JSON.
        new_json: New JSON.

    Returns:
        List of (path, old_value, new_value) for changed values.
    """
    old_dict = json.loads(old_json) if isinstance(old_json, str) else old_json
    new_dict = json.loads(new_json) if isinstance(new_json, str) else new_json
    return dict_diff(old_dict, new_dict)


def diff_statistics(diff_result: DiffResult) -> Dict[str, Any]:
    """Compute statistics from a DiffResult.

    Args:
        diff_result: Result from text_diff().

    Returns:
        Dict with statistics.
    """
    total = (
        diff_result.added_lines
        + diff_result.removed_lines
        + diff_result.unchanged_lines
    )
    change_rate = (
        (diff_result.added_lines + diff_result.removed_lines) / total
        if total > 0
        else 0.0
    )
    return {
        "total_lines": total,
        "added": diff_result.added_lines,
        "removed": diff_result.removed_lines,
        "unchanged": diff_result.unchanged_lines,
        "change_rate": round(change_rate, 4),
    }


__all__ = [
    "DiffSegment",
    "DiffResult",
    "text_diff",
    "unified_diff",
    "side_by_side_diff",
    "dict_diff",
    "json_diff",
    "diff_statistics",
]
