"""
Text and structure diff utilities.
"""

import difflib
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum


class ChangeType(Enum):
    """Type of change in a diff."""
    EQUAL = "equal"
    INSERT = "insert"
    DELETE = "delete"
    REPLACE = "replace"


@dataclass
class DiffChunk:
    """A single chunk of diff output."""
    change_type: ChangeType
    old_text: str
    new_text: str
    old_line_start: Optional[int] = None
    old_line_end: Optional[int] = None
    new_line_start: Optional[int] = None
    new_line_end: Optional[int] = None


class DiffResult:
    """Container for diff operation results."""

    def __init__(self, chunks: List[DiffChunk]):
        self.chunks = chunks

    @property
    def has_changes(self) -> bool:
        return any(c.change_type != ChangeType.EQUAL for c in self.chunks)

    @property
    def insertions(self) -> int:
        return sum(1 for c in self.chunks if c.change_type == ChangeType.INSERT)

    @property
    def deletions(self) -> int:
        return sum(1 for c in self.chunks if c.change_type == ChangeType.DELETE)

    def unified_diff(
        self,
        old_label: str = "old",
        new_label: str = "new",
        context: int = 3
    ) -> str:
        """Generate unified diff format string."""
        old_lines = []
        new_lines = []
        for chunk in self.chunks:
            if chunk.change_type == ChangeType.EQUAL:
                old_lines.extend(chunk.old_text.splitlines(keepends=True))
                new_lines.extend(chunk.new_text.splitlines(keepends=True))
            elif chunk.change_type == ChangeType.DELETE:
                old_lines.extend(chunk.old_text.splitlines(keepends=True))
            elif chunk.change_type == ChangeType.INSERT:
                new_lines.extend(chunk.new_text.splitlines(keepends=True))
            elif chunk.change_type == ChangeType.REPLACE:
                old_lines.extend(chunk.old_text.splitlines(keepends=True))
                new_lines.extend(chunk.new_text.splitlines(keepends=True))
        return "".join(difflib.unified_diff(
            old_lines, new_lines,
            fromfile=old_label, tofile=new_label,
            n=context
        ))
