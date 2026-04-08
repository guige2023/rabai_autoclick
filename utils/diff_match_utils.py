"""Diff and patch utilities for text comparison.

Provides text differencing and patching operations
for change tracking and synchronization.
"""

import difflib
from typing import List, Optional, Tuple


DiffChange = Tuple[str, str, int]  # (op, text, line_no)


def diff_lines(text1: str, text2: str) -> List[DiffChange]:
    """Compute line-by-line diff between two texts.

    Args:
        text1: Original text.
        text2: Modified text.

    Returns:
        List of (op, text, line_no) where op is "+", "-", or " ".
    """
    diff: List[DiffChange] = []
    matcher = difflib.SequenceMatcher(None, text1.splitlines(), text2.splitlines())
    line_no1, line_no2 = 0, 0
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for line in text1.splitlines()[i1:i2]:
                line_no1 += 1
                diff.append((" ", line, line_no1))
        elif tag == "delete":
            for line in text1.splitlines()[i1:i2]:
                line_no1 += 1
                diff.append(("-", line, line_no1))
        elif tag == "insert":
            for line in text2.splitlines()[j1:j2]:
                line_no2 += 1
                diff.append(("+", line, line_no2))
        elif tag == "replace":
            for line in text1.splitlines()[i1:i2]:
                line_no1 += 1
                diff.append(("-", line, line_no1))
            for line in text2.splitlines()[j1:j2]:
                line_no2 += 1
                diff.append(("+", line, line_no2))
    return diff


def format_diff_text(diff: List[DiffChange]) -> str:
    """Format diff changes as a readable string.

    Args:
        diff: List of diff changes from diff_lines().

    Returns:
        Formatted diff string with +/- prefixes.
    """
    lines = []
    for op, text, line_no in diff:
        lines.append(f"{op} {line_no:4d}: {text}")
    return "\n".join(lines)


def unified_diff(
    text1: str,
    text2: str,
    from_label: str = "original",
    to_label: str = "modified",
    context: int = 3,
) -> str:
    """Generate unified diff format string.

    Args:
        text1: Original text.
        text2: Modified text.
        from_label: Label for original file.
        to_label: Label for modified file.
        context: Number of context lines around changes.

    Returns:
        Unified diff formatted string.
    """
    lines1 = text1.splitlines(keepends=True)
    lines2 = text2.splitlines(keepends=True)
    return "".join(difflib.unified_diff(
        lines1, lines2,
        fromfile=from_label,
        tofile=to_label,
        n=context,
    ))


def similarity_ratio(text1: str, text2: str) -> float:
    """Calculate similarity ratio between two texts.

    Args:
        text1: First text.
        text2: Second text.

    Returns:
        Similarity ratio from 0.0 to 1.0.
    """
    if not text1 and not text2:
        return 1.0
    if not text1 or not text2:
        return 0.0
    return difflib.SequenceMatcher(None, text1, text2).ratio()


def has_changes(text1: str, text2: str) -> bool:
    """Check if two texts differ.

    Args:
        text1: First text.
        text2: Second text.

    Returns:
        True if texts differ.
    """
    return text1 != text2


def changed_lines(text1: str, text2: str) -> Tuple[List[int], List[int]]:
    """Get line numbers that changed between texts.

    Args:
        text1: Original text.
        text2: Modified text.

    Returns:
        Tuple of (deleted_line_nos, added_line_nos).
    """
    diff = diff_lines(text1, text2)
    deleted = [ln for op, _, ln in diff if op == "-"]
    added = [ln for op, _, ln in diff if op == "+"]
    return (deleted, added)
