"""UI snapshot utilities for UI automation.

Captures and compares UI snapshots for change detection
and automation verification.
"""

from __future__ import annotations

import hashlib
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class ElementSnapshot:
    """A snapshot of a single UI element.

    Attributes:
        element_id: Unique element identifier.
        role: Element role.
        name: Element name/label.
        value: Current value.
        bounds: Bounding box (x, y, width, height).
        states: Active states.
        children: IDs of child elements.
        hash: Content hash of this element.
    """
    element_id: str
    role: str = ""
    name: str = ""
    value: Any = None
    bounds: tuple[float, float, float, float] = (0, 0, 0, 0)
    states: tuple[str, ...] = ()
    children: tuple[str, ...] = ()
    hash: str = ""


@dataclass
class UISnapshot:
    """A complete UI snapshot.

    Attributes:
        snapshot_id: Unique snapshot identifier.
        window_id: ID of the window this snapshot is from.
        timestamp: When the snapshot was taken.
        elements: All elements in this snapshot.
        root_elements: IDs of top-level elements.
        window_title: Window title at snapshot time.
        window_state: Window state (normal, maximized, etc.).
        element_count: Total number of elements.
        content_hash: Hash of entire snapshot content.
    """
    snapshot_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    window_id: str = ""
    timestamp: float = field(default_factory=time.time)
    elements: list[ElementSnapshot] = field(default_factory=list)
    root_elements: tuple[str, ...] = ()
    window_title: str = ""
    window_state: str = "normal"
    element_count: int = 0
    content_hash: str = ""

    def get_element(self, element_id: str) -> Optional[ElementSnapshot]:
        """Get an element by ID."""
        for elem in self.elements:
            if elem.element_id == element_id:
                return elem
        return None

    def compute_hash(self) -> str:
        """Compute content hash for this snapshot."""
        content = f"{self.window_title}:{self.window_state}:{len(self.elements)}"
        for elem in self.elements:
            content += f":{elem.role}:{elem.name}:{elem.hash}"
        return hashlib.md5(content.encode()).hexdigest()


@dataclass
class SnapshotDiff:
    """Difference between two UI snapshots.

    Attributes:
        added: Elements in new but not in old.
        removed: Elements in old but not in new.
        changed: Elements that exist in both but differ.
        unchanged: Elements that are identical.
    """
    added: list[ElementSnapshot] = field(default_factory=list)
    removed: list[ElementSnapshot] = field(default_factory=list)
    changed: list[tuple[ElementSnapshot, ElementSnapshot]] = field(
        default_factory=list
    )
    unchanged: list[ElementSnapshot] = field(default_factory=list)


class UISnapshotManager:
    """Manages UI snapshots for change detection."""

    def __init__(self) -> None:
        """Initialize snapshot manager."""
        self._snapshots: dict[str, UISnapshot] = {}
        self._change_callbacks: list[Callable[[str, UISnapshot, SnapshotDiff], None]] = []

    def take_snapshot(
        self,
        window_id: str,
        window_title: str = "",
        window_state: str = "normal",
        root_elements: Optional[list[str]] = None,
    ) -> UISnapshot:
        """Take a snapshot of the current UI state."""
        snapshot = UISnapshot(
            window_id=window_id,
            window_title=window_title,
            window_state=window_state,
            root_elements=tuple(root_elements or []),
        )
        self._snapshots[snapshot.snapshot_id] = snapshot
        return snapshot

    def add_element_to_snapshot(
        self,
        snapshot_id: str,
        element: ElementSnapshot,
    ) -> bool:
        """Add an element to a snapshot."""
        snapshot = self._snapshots.get(snapshot_id)
        if not snapshot:
            return False
        snapshot.elements.append(element)
        snapshot.element_count = len(snapshot.elements)
        return True

    def get_snapshot(self, snapshot_id: str) -> Optional[UISnapshot]:
        """Get a snapshot by ID."""
        return self._snapshots.get(snapshot_id)

    def get_latest(self, window_id: str) -> Optional[UISnapshot]:
        """Get the most recent snapshot for a window."""
        candidates = [
            s for s in self._snapshots.values()
            if s.window_id == window_id
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.timestamp)

    def diff(
        self,
        old_id: str,
        new_id: str,
    ) -> Optional[SnapshotDiff]:
        """Compute diff between two snapshots."""
        old = self._snapshots.get(old_id)
        new = self._snapshots.get(new_id)
        if not old or not new:
            return None

        old_map = {e.element_id: e for e in old.elements}
        new_map = {e.element_id: e for e in new.elements}

        diff = SnapshotDiff()

        for elem_id, elem in new_map.items():
            if elem_id not in old_map:
                diff.added.append(elem)
            elif elem.hash != old_map[elem_id].hash:
                diff.changed.append((old_map[elem_id], elem))
            else:
                diff.unchanged.append(elem)

        for elem_id, elem in old_map.items():
            if elem_id not in new_map:
                diff.removed.append(elem)

        return diff

    def on_change(
        self,
        callback: Callable[[str, UISnapshot, SnapshotDiff], None],
    ) -> None:
        """Register a callback for snapshot changes."""
        self._change_callbacks.append(callback)

    @property
    def snapshot_count(self) -> int:
        """Return number of stored snapshots."""
        return len(self._snapshots)
