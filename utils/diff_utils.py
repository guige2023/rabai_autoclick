"""
Text and structure diff utilities.

Provides:
- Line-by-line text diff
- Structure diff for nested data
- Diff formatting (unified, side-by-side)
- Semantic diff operations
"""

from __future__ import annotations

import difflib
import json
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class DiffOp(Enum):
    """Diff operation types."""

    EQUAL = "equal"
    INSERT = "insert"
    DELETE = "delete"
    REPLACE = "replace"


@dataclass
class DiffHunk:
    """A contiguous block of changes."""

    old_start: int
    old_count: int
    new_start: int
    new_count: int
    changes: list[DiffChange]


@dataclass
class DiffChange:
    """A single change in a diff."""

    operation: DiffOp
    old_line: Optional[int]
    new_line: Optional[int]
    old_text: str
    new_text: str


@dataclass
class TextDiff:
    """Result of a text diff operation."""

    hunks: list[DiffHunk] = field(default_factory=list)
    total_additions: int = 0
    total_deletions: int = 0
    total_replacements: int = 0


def diff_strings(old: str, new: str, context_lines: int = 3) -> TextDiff:
    """
    Compute diff between two strings.

    Args:
        old: Original text
        new: New text
        context_lines: Number of context lines around changes

    Returns:
        TextDiff with hunks and statistics
    """
    old_lines = old.splitlines(keepends=True)
    new_lines = new.splitlines(keepends=True)

    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    result = TextDiff()

    for group in matcher.get_grouped_opcodes(context_lines):
        hunk = DiffHunk(
            old_start=group[0][1] + 1,
            old_count=group[-1][2] - group[0][1],
            new_start=group[0][3] + 1,
            new_count=group[-1][4] - group[0][3],
            changes=[],
        )

        for tag, i1, i2, j1, j2 in group:
            if tag == "equal":
                op = DiffOp.EQUAL
                for idx in range(i1, i2):
                    hunk.changes.append(DiffChange(op, idx + 1, idx + 1, old_lines[idx].rstrip("\n"), new_lines[idx].rstrip("\n")))
            elif tag == "delete":
                op = DiffOp.DELETE
                result.total_deletions += i2 - i1
                for idx in range(i1, i2):
                    hunk.changes.append(DiffChange(op, idx + 1, None, old_lines[idx].rstrip("\n"), ""))
            elif tag == "insert":
                op = DiffOp.INSERT
                result.total_additions += j2 - j1
                for idx in range(j1, j2):
                    hunk.changes.append(DiffChange(op, None, idx + 1, "", new_lines[idx].rstrip("\n")))
            elif tag == "replace":
                op = DiffOp.REPLACE
                result.total_replacements += max(i2 - i1, j2 - j1)
                for idx in range(i1, i2):
                    hunk.changes.append(DiffChange(DiffOp.DELETE, idx + 1, None, old_lines[idx].rstrip("\n"), ""))
                for idx in range(j1, j2):
                    hunk.changes.append(DiffChange(DiffOp.INSERT, None, idx + 1, "", new_lines[idx].rstrip("\n")))

        result.hunks.append(hunk)

    return result


def format_unified_diff(old: str, new: str, old_name: str = "a", new_name: str = "b", context_lines: int = 3) -> str:
    """
    Format diff as unified diff output.

    Args:
        old: Original text
        new: New text
        old_name: Name for old file
        new_name: Name for new file
        context_lines: Lines of context

    Returns:
        Unified diff string
    """
    old_lines = old.splitlines(keepends=True)
    new_lines = new.splitlines(keepends=True)

    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    lines: list[str] = []

    for group in matcher.get_grouped_opcodes(context_lines):
        old_start = group[0][1]
        old_end = group[-1][2]
        new_start = group[0][3]
        new_end = group[-1][4]

        lines.append(f"@@ -{old_start + 1},{old_end - old_start} +{new_start + 1},{new_end - new_start} @@")

        for tag, i1, i2, j1, j2 in group:
            if tag == "equal":
                for idx in range(i1, i2):
                    lines.append(f" {old_lines[idx].rstrip()}")
            elif tag == "delete":
                for idx in range(i1, i2):
                    lines.append(f"-{old_lines[idx].rstrip()}")
            elif tag == "insert":
                for idx in range(j1, j2):
                    lines.append(f"+{new_lines[idx].rstrip()}")
            elif tag == "replace":
                for idx in range(i1, i2):
                    lines.append(f"-{old_lines[idx].rstrip()}")
                for idx in range(j1, j2):
                    lines.append(f"+{new_lines[idx].rstrip()}")

    if not lines:
        return ""

    header = f"--- {old_name}\n+++ {new_name}\n"
    return header + "\n".join(lines)


def format_side_by_side_diff(old: str, new: str, width: int = 80) -> str:
    """
    Format diff as side-by-side view.

    Args:
        old: Original text
        new: New text
        width: Total display width

    Returns:
        Side-by-side diff string
    """
    old_lines = old.splitlines()
    new_lines = new.splitlines()

    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    half_width = (width - 6) // 2
    lines: list[str] = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for idx in range(i1, i2):
                left = old_lines[idx][:half_width].ljust(half_width)
                right = new_lines[idx][:half_width].ljust(half_width)
                lines.append(f" {left} | {right}")
        elif tag == "delete":
            for idx in range(i1, i2):
                left = old_lines[idx][:half_width].ljust(half_width)
                lines.append(f"-{left} |")
        elif tag == "insert":
            for idx in range(j1, j2):
                right = new_lines[idx][:half_width].ljust(half_width)
                lines.append(f" | {right}")
        elif tag == "replace":
            old_count = i2 - i1
            new_count = j2 - j1
            for idx in range(i1, i2):
                left = old_lines[idx][:half_width].ljust(half_width)
                new_idx = j1 + (idx - i1) if idx - i1 < new_count else j1
                if new_idx < j2:
                    right = new_lines[new_idx][:half_width].ljust(half_width)
                else:
                    right = " " * half_width
                lines.append(f"-{left} | {right}")

    return "\n".join(lines)


def diff_json(old_json: Any, new_json: Any, path: str = "") -> list[dict[str, Any]]:
    """
    Compute structural diff between two JSON values.

    Args:
        old_json: Original JSON value
        new_json: New JSON value
        path: Current JSON path (for recursion)

    Returns:
        List of differences with paths and values
    """
    diffs: list[dict[str, Any]] = []

    if type(old_json) != type(new_json):
        diffs.append({"path": path, "type": "type_changed", "old": old_json, "new": new_json})
        return diffs

    if isinstance(old_json, dict):
        all_keys = set(old_json.keys()) | set(new_json.keys())
        for key in all_keys:
            child_path = f"{path}.{key}" if path else key
            if key not in old_json:
                diffs.append({"path": child_path, "type": "added", "value": new_json[key]})
            elif key not in new_json:
                diffs.append({"path": child_path, "type": "removed", "value": old_json[key]})
            else:
                diffs.extend(diff_json(old_json[key], new_json[key], child_path))

    elif isinstance(old_json, list):
        max_len = max(len(old_json), len(new_json))
        for idx in range(max_len):
            child_path = f"{path}[{idx}]"
            if idx >= len(old_json):
                diffs.append({"path": child_path, "type": "added", "value": new_json[idx]})
            elif idx >= len(new_json):
                diffs.append({"path": child_path, "type": "removed", "value": old_json[idx]})
            else:
                diffs.extend(diff_json(old_json[idx], new_json[idx], child_path))

    else:
        if old_json != new_json:
            diffs.append({"path": path, "type": "changed", "old": old_json, "new": new_json})

    return diffs


def apply_diff(original: str, diff_text: str) -> str:
    """
    Apply a unified diff to text.

    Args:
        original: Original text
        diff_text: Unified diff text

    Returns:
        Modified text

    Raises:
        ValueError: If diff cannot be applied
    """
    lines = original.splitlines(keepends=True)
    result = []
    diff_lines = diff_text.splitlines()

    old_idx = 0
    i = 0

    while i < len(diff_lines):
        line = diff_lines[i]

        if line.startswith("@@"):
            match = re.match(r"@@ -(\d+)(?:,(\d+))? \+(\d+)(?:,(\d+))? @@", line)
            if not match:
                raise ValueError(f"Invalid hunk header: {line}")

            start_old = int(match.group(1)) - 1
            start_new = int(match.group(3)) - 1
            i += 1

            while old_idx < start_old and result is not None:
                result.append(lines[old_idx])
                old_idx += 1

            j = 0
            while i < len(diff_lines) and not diff_lines[i].startswith("@@"):
                if diff_lines[i].startswith("-"):
                    old_idx += 1
                elif diff_lines[i].startswith("+"):
                    result.append(diff_lines[i][1:] + ("\n" if not diff_lines[i].endswith("\n") else ""))
                elif diff_lines[i].startswith(" "):
                    result.append(lines[old_idx])
                    old_idx += 1
                i += 1

    while old_idx < len(lines):
        result.append(lines[old_idx])
        old_idx += 1

    return "".join(result)


def word_diff(old: str, new: str) -> str:
    """
    Compute word-level diff and return combined text with markers.

    Args:
        old: Original text
        new: New text

    Returns:
        String with [-removed-] and [+added+] markers
    """
    old_words = old.split()
    new_words = new.split()

    matcher = difflib.SequenceMatcher(None, old_words, new_words)
    result: list[str] = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            result.extend(old_words[i1:i2])
        elif tag == "delete":
            for word in old_words[i1:i2]:
                result.append(f"[-{word}-]")
        elif tag == "insert":
            for word in new_words[j1:j2]:
                result.append(f"[+{word}+]")
        elif tag == "replace":
            for word in old_words[i1:i2]:
                result.append(f"[-{word}-]")
            for word in new_words[j1:j2]:
                result.append(f"[+{word}+]")

    return " ".join(result)


def semantic_clean_diff(diff_text: str) -> str:
    """
    Clean up a unified diff by removing trivial changes.

    Args:
        diff_text: Unified diff text

    Returns:
        Cleaned diff text
    """
    lines = diff_text.splitlines()
    cleaned: list[str] = []

    skip_pattern = re.compile(r"^\s*$")

    i = 0
    while i < len(lines):
        line = lines[i]
        if skip_pattern.match(line):
            i += 1
            continue
        cleaned.append(line)
        i += 1

    return "\n".join(cleaned)


def similarity_ratio(s1: str, s2: str) -> float:
    """
    Calculate similarity ratio between two strings.

    Args:
        s1: First string
        s2: Second string

    Returns:
        Similarity ratio between 0 and 1
    """
    if not s1 and not s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    matcher = difflib.SequenceMatcher(None, s1, s2)
    return matcher.ratio()


def unified_diff_stats(diff_text: str) -> dict[str, int]:
    """
    Get statistics from unified diff text.

    Args:
        diff_text: Unified diff output

    Returns:
        Dictionary with additions, deletions, hunks count
    """
    lines = diff_text.splitlines()
    stats = {"additions": 0, "deletions": 0, "hunks": 0}

    for line in lines:
        if line.startswith("@@"):
            stats["hunks"] += 1
        elif line.startswith("+") and not line.startswith("+++"):
            stats["additions"] += 1
        elif line.startswith("-") and not line.startswith("---"):
            stats["deletions"] += 1

    return stats
