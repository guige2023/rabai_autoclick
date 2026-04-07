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


def diff_text(
    old_text: str,
    new_text: str,
) -> DiffResult:
    """Compute diff between two text strings."""
    old_lines = old_text.splitlines(keepends=True)
    new_lines = new_text.splitlines(keepends=True)
    matcher = difflib.SequenceMatcher(None, old_lines, new_lines)
    chunks = []

    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        old_block = "".join(old_lines[i1:i2])
        new_block = "".join(new_lines[j1:j2])

        if tag == "equal":
            chunks.append(DiffChunk(
                change_type=ChangeType.EQUAL,
                old_text=old_block, new_text=new_block,
                old_line_start=i1 + 1, old_line_end=i2,
                new_line_start=j1 + 1, new_line_end=j2,
            ))
        elif tag == "replace":
            chunks.append(DiffChunk(
                change_type=ChangeType.REPLACE,
                old_text=old_block, new_text=new_block,
                old_line_start=i1 + 1, old_line_end=i2,
                new_line_start=j1 + 1, new_line_end=j2,
            ))
        elif tag == "delete":
            chunks.append(DiffChunk(
                change_type=ChangeType.DELETE,
                old_text=old_block, new_text="",
                old_line_start=i1 + 1, old_line_end=i2,
            ))
        elif tag == "insert":
            chunks.append(DiffChunk(
                change_type=ChangeType.INSERT,
                old_text="", new_text=new_block,
                new_line_start=j1 + 1, new_line_end=j2,
            ))

    return DiffResult(chunks)


def diff_words(old: str, new: str) -> Iterator[Tuple[ChangeType, str]]:
    """Diff at word level."""
    matcher = difflib.SequenceMatcher(None, old.split(), new.split())
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for word in old.split()[i1:i2]:
                yield (ChangeType.EQUAL, word)
        elif tag == "replace":
            for word in old.split()[i1:i2]:
                yield (ChangeType.DELETE, word)
            for word in new.split()[j1:j2]:
                yield (ChangeType.INSERT, word)
        elif tag == "delete":
            for word in old.split()[i1:i2]:
                yield (ChangeType.DELETE, word)
        elif tag == "insert":
            for word in new.split()[j1:j2]:
                yield (ChangeType.INSERT, word)


def diff_chars(old: str, new: str) -> Iterator[Tuple[ChangeType, str]]:
    """Diff at character level."""
    matcher = difflib.SequenceMatcher(None, old, new)
    for tag, i1, i2, j1, j2 in matcher.get_opcodes():
        if tag == "equal":
            for char in old[i1:i2]:
                yield (ChangeType.EQUAL, char)
        elif tag == "replace":
            for char in old[i1:i2]:
                yield (ChangeType.DELETE, char)
            for char in new[j1:j2]:
                yield (ChangeType.INSERT, char)
        elif tag == "delete":
            for char in old[i1:i2]:
                yield (ChangeType.DELETE, char)
        elif tag == "insert":
            for char in new[j1:j2]:
                yield (ChangeType.INSERT, char)


def diff_structured(
    old_dict: Dict[str, Any],
    new_dict: Dict[str, Any],
    path: str = ""
) -> List[Dict[str, Any]]:
    """Diff two dictionaries and return structured change list."""
    changes = []
    all_keys = set(old_dict.keys()) | set(new_dict.keys())

    for key in sorted(all_keys):
        current_path = f"{path}.{key}" if path else key

        if key not in new_dict:
            changes.append({
                "path": current_path,
                "change_type": ChangeType.DELETE,
                "old_value": old_dict[key],
                "new_value": None,
            })
        elif key not in old_dict:
            changes.append({
                "path": current_path,
                "change_type": ChangeType.INSERT,
                "old_value": None,
                "new_value": new_dict[key],
            })
        elif old_dict[key] != new_dict[key]:
            if isinstance(old_dict[key], dict) and isinstance(new_dict[key], dict):
                changes.extend(diff_structured(old_dict[key], new_dict[key], current_path))
            else:
                changes.append({
                    "path": current_path,
                    "change_type": ChangeType.REPLACE,
                    "old_value": old_dict[key],
                    "new_value": new_dict[key],
                })

    return changes


def diff_lists(
    old_list: List[Any],
    new_list: List[Any],
    key: Optional[Callable[[Any], Any]] = None
) -> Tuple[List[Any], List[Any], List[Any]]:
    """Diff two lists, optionally matching by key function."""
    if key is None:
        key = lambda x: x
    old_keys = {key(x) for x in old_list}
    new_keys = {key(x) for x in new_list}
    added = [x for x in new_list if key(x) in new_keys - old_keys]
    removed = [x for x in old_list if key(x) in old_keys - new_keys]
    common = [x for x in new_list if key(x) in old_keys & new_keys]
    return (added, removed, common)
