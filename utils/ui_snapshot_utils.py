"""UI snapshot utilities.

This module provides utilities for capturing and comparing
UI state snapshots.
"""

from __future__ import annotations

import time
import hashlib
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class ElementSnapshot:
    """Snapshot of a single UI element."""
    role: str
    name: str
    value: str
    rect: tuple[int, int, int, int]  # x, y, width, height
    visible: bool
    enabled: bool
    focused: bool
    attributes: Dict[str, Any] = field(default_factory=dict)
    children: List["ElementSnapshot"] = field(default_factory=list)


@dataclass
class UISnapshot:
    """A complete UI snapshot."""
    timestamp: float
    app_name: str
    window_title: str
    elements: List[ElementSnapshot] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def element_count(self) -> int:
        """Count total elements including nested."""
        count = len(self.elements)
        for elem in self.elements:
            count += _count_nested(elem)
        return count

    def find_by_role(self, role: str) -> List[ElementSnapshot]:
        """Find elements by role."""
        results: List[ElementSnapshot] = []
        for elem in self.elements:
            _find_by_role(elem, role, results)
        return results

    def find_by_name(self, name: str) -> List[ElementSnapshot]:
        """Find elements by name (substring match)."""
        results: List[ElementSnapshot] = []
        for elem in self.elements:
            _find_by_name(elem, name, results)
        return results

    def hash(self) -> str:
        """Generate a hash of the snapshot."""
        data = self._serialize()
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()[:16]

    def _serialize(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "app_name": self.app_name,
            "window_title": self.window_title,
            "elements": [self._serialize_element(e) for e in self.elements],
        }

    def _serialize_element(self, elem: ElementSnapshot) -> Dict[str, Any]:
        return {
            "role": elem.role,
            "name": elem.name,
            "value": elem.value,
            "rect": elem.rect,
            "visible": elem.visible,
            "enabled": elem.enabled,
            "focused": elem.focused,
            "attributes": elem.attributes,
            "children": [self._serialize_element(c) for c in elem.children],
        }


def _count_nested(elem: ElementSnapshot) -> int:
    count = len(elem.children)
    for child in elem.children:
        count += _count_nested(child)
    return count


def _find_by_role(elem: ElementSnapshot, role: str, results: List[ElementSnapshot]) -> None:
    if elem.role == role:
        results.append(elem)
    for child in elem.children:
        _find_by_role(child, role, results)


def _find_by_name(elem: ElementSnapshot, name: str, results: List[ElementSnapshot]) -> None:
    if name.lower() in elem.name.lower():
        results.append(elem)
    for child in elem.children:
        _find_by_name(child, name, results)


class SnapshotComparator:
    """Compares UI snapshots for changes."""

    def __init__(self) -> None:
        self._snapshots: List[UISnapshot] = []

    def add(self, snapshot: UISnapshot) -> None:
        self._snapshots.append(snapshot)

    def has_changed_since(self, index: int) -> bool:
        if index >= len(self._snapshots):
            return False
        return self._snapshots[-1].hash() != self._snapshots[index].hash()

    def diff_since(self, index: int) -> Optional[SnapshotDiff]:
        if index >= len(self._snapshots):
            return None
        old = self._snapshots[index]
        new = self._snapshots[-1]
        return compare_snapshots(old, new)


@dataclass
class SnapshotDiff:
    """Differences between two snapshots."""
    added: List[ElementSnapshot]
    removed: List[ElementSnapshot]
    changed: List[tuple[ElementSnapshot, ElementSnapshot]]
    timestamp_delta: float


def compare_snapshots(old: UISnapshot, new: UISnapshot) -> SnapshotDiff:
    """Compare two snapshots.

    Args:
        old: Previous snapshot.
        new: Current snapshot.

    Returns:
        SnapshotDiff with changes.
    """
    old_map = {e.name: e for e in _flatten_elements(old.elements)}
    new_map = {e.name: e for e in _flatten_elements(new.elements)}

    added = [new_map[k] for k in new_map if k not in old_map]
    removed = [old_map[k] for k in old_map if k not in new_map]
    changed = []

    for name in old_map:
        if name in new_map:
            if not _elements_equal(old_map[name], new_map[name]):
                changed.append((old_map[name], new_map[name]))

    return SnapshotDiff(
        added=added,
        removed=removed,
        changed=changed,
        timestamp_delta=new.timestamp - old.timestamp,
    )


def _flatten_elements(elems: List[ElementSnapshot]) -> List[ElementSnapshot]:
    result: List[ElementSnapshot] = []
    for elem in elems:
        result.append(elem)
        result.extend(_flatten_elements(elem.children))
    return result


def _elements_equal(a: ElementSnapshot, b: ElementSnapshot) -> bool:
    return (
        a.role == b.role
        and a.name == b.name
        and a.value == b.value
        and a.rect == b.rect
        and a.visible == b.visible
        and a.enabled == b.enabled
        and a.focused == b.focused
    )


__all__ = [
    "ElementSnapshot",
    "UISnapshot",
    "SnapshotComparator",
    "SnapshotDiff",
    "compare_snapshots",
]
