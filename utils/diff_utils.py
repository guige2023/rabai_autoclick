"""Diff and patch utilities: JSON diff, text diff, and structural comparison."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import Enum
from typing import Any

__all__ = [
    "DiffOperation",
    "DiffEntry",
    "diff",
    "patch",
    "unpatch",
    "deep_equal",
]


class DiffOperation(Enum):
    ADD = "add"
    REMOVE = "remove"
    CHANGE = "change"
    UNCHANGED = "unchanged"


@dataclass
class DiffEntry:
    """A single diff entry."""
    path: str
    operation: DiffOperation
    old_value: Any = None
    new_value: Any = None

    def __repr__(self) -> str:
        if self.operation == DiffOperation.REMOVE:
            return f"- {self.path}: {self.old_value!r}"
        elif self.operation == DiffOperation.ADD:
            return f"+ {self.path}: {self.new_value!r}"
        elif self.operation == DiffOperation.CHANGE:
            return f"~ {self.path}: {self.old_value!r} -> {self.new_value!r}"
        return f"  {self.path}"


def deep_equal(a: Any, b: Any) -> bool:
    """Check deep equality between two values."""
    if type(a) != type(b):
        return False
    if isinstance(a, dict):
        if set(a.keys()) != set(b.keys()):
            return False
        return all(deep_equal(a[k], b[k]) for k in a)
    if isinstance(a, (list, tuple)):
        if len(a) != len(b):
            return False
        return all(deep_equal(x, y) for x, y in zip(a, b))
    return a == b


def diff(old: Any, new: Any, path: str = "") -> list[DiffEntry]:
    """Compute the diff between two values, returning a list of DiffEntries."""
    entries: list[DiffEntry] = []

    if deep_equal(old, new):
        return entries

    if isinstance(old, dict) and isinstance(new, dict):
        all_keys = set(old.keys()) | set(new.keys())
        for key in sorted(all_keys):
            child_path = f"{path}.{key}" if path else key
            if key not in old:
                entries.append(DiffEntry(child_path, DiffOperation.ADD, new_value=new[key]))
            elif key not in new:
                entries.append(DiffEntry(child_path, DiffOperation.REMOVE, old_value=old[key]))
            else:
                child_entries = diff(old[key], new[key], child_path)
                entries.extend(child_entries)

    elif isinstance(old, list) and isinstance(new, list):
        max_len = max(len(old), len(new))
        for i in range(max_len):
            child_path = f"{path}[{i}]"
            if i >= len(old):
                entries.append(DiffEntry(child_path, DiffOperation.ADD, new_value=new[i]))
            elif i >= len(new):
                entries.append(DiffEntry(child_path, DiffOperation.REMOVE, old_value=old[i]))
            else:
                entries.extend(diff(old[i], new[i], child_path))

    else:
        entries.append(DiffEntry(
            path if path else "/",
            DiffOperation.CHANGE,
            old_value=old,
            new_value=new,
        ))

    return entries


def patch(data: Any, entries: list[DiffEntry]) -> Any:
    """Apply a list of diff entries to a data structure, returning the modified copy."""
    import copy
    result = copy.deepcopy(data)

    for entry in entries:
        if entry.operation == DiffOperation.UNCHANGED:
            continue

        parts = entry.path.strip("/").split(".")
        current = result
        for part in parts[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]

        last = parts[-1] if parts else entry.path
        if entry.operation == DiffOperation.REMOVE:
            if isinstance(current, dict):
                current.pop(last, None)
            elif isinstance(current, list):
                try:
                    idx = int(last)
                    current.pop(idx)
                except (ValueError, IndexError):
                    pass
        elif entry.operation == DiffOperation.ADD or entry.operation == DiffOperation.CHANGE:
            current[last] = entry.new_value

    return result


def unpatch(data: Any, entries: list[DiffEntry]) -> Any:
    """Reverse-apply a list of diff entries."""
    reverse_ops = {
        DiffOperation.ADD: DiffOperation.REMOVE,
        DiffOperation.REMOVE: DiffOperation.ADD,
        DiffOperation.CHANGE: DiffOperation.CHANGE,
    }

    def reverse_entry(e: DiffEntry) -> DiffEntry:
        return DiffEntry(
            path=e.path,
            operation=reverse_ops.get(e.operation, e.operation),
            old_value=e.new_value,
            new_value=e.old_value,
        )

    return patch(data, [reverse_entry(e) for e in entries])


def diff_to_json(entries: list[DiffEntry]) -> str:
    """Serialize diff entries to JSON."""
    return json.dumps([
        {"path": e.path, "op": e.operation.value, "old": e.old_value, "new": e.new_value}
        for e in entries
    ], default=str)


def diff_from_json(json_str: str) -> list[DiffEntry]:
    """Deserialize diff entries from JSON."""
    data = json.loads(json_str)
    return [
        DiffEntry(
            path=d["path"],
            operation=DiffOperation(d["op"]),
            old_value=d.get("old"),
            new_value=d.get("new"),
        )
        for d in data
    ]
