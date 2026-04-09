"""
UI change detection utilities for automation monitoring.

This module provides utilities for detecting and responding to
changes in UI elements and application state.
"""

from __future__ import annotations

import time
import hashlib
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Any, Set
from enum import Enum, auto


class ChangeType(Enum):
    """Types of UI changes."""
    APPEARED = auto()
    DISAPPEARED = auto()
    MODIFIED = auto()
    MOVED = auto()
    RESIZED = auto()
    STYLE_CHANGED = auto()
    STATE_CHANGED = auto()


@dataclass
class UIChange:
    """
    Represents a detected UI change.

    Attributes:
        change_type: Type of change.
        element_id: ID of affected element.
        path: Path to element in UI tree.
        old_value: Previous value/state.
        new_value: New value/state.
        timestamp: When change was detected.
    """
    change_type: ChangeType
    element_id: str
    path: str
    old_value: Any = None
    new_value: Any = None
    timestamp: float = field(default_factory=time.time)


@dataclass
class ElementSnapshot:
    """
    Snapshot of an element's state at a point in time.

    Attributes:
        element_id: Unique identifier.
        role: Accessibility role.
        title: Element title.
        value: Current value.
        bounds: Element bounds.
        enabled: Enabled state.
        focused: Focused state.
        attributes: Additional attributes.
        checksum: Hash of element state.
    """
    element_id: str
    role: str = ""
    title: str = ""
    value: str = ""
    bounds: Optional[tuple] = None
    enabled: bool = True
    focused: bool = False
    attributes: Dict[str, Any] = field(default_factory=dict)
    checksum: str = ""

    @classmethod
    def from_element(cls, element: Dict[str, Any]) -> ElementSnapshot:
        """Create snapshot from element dictionary."""
        content = str(element.get("role", "")) + str(element.get("title", "")) + str(element.get("value", ""))
        checksum = hashlib.md5(content.encode()).hexdigest()

        return cls(
            element_id=element.get("elementId", element.get("identifier", "")),
            role=element.get("role", ""),
            title=element.get("title", ""),
            value=str(element.get("value", "")),
            bounds=element.get("bounds"),
            enabled=element.get("enabled", True),
            focused=element.get("focused", False),
            attributes=element.get("attributes", {}),
            checksum=checksum,
        )

    def has_changed(self, other: ElementSnapshot) -> bool:
        """Check if this snapshot differs from another."""
        return self.checksum != other.checksum


class ChangeDetector:
    """
    Detects changes in UI elements by comparing snapshots.

    Maintains state history and emits change events
    when differences are detected.
    """

    def __init__(self) -> None:
        self._snapshots: Dict[str, ElementSnapshot] = {}
        self._change_handlers: List[Callable[[UIChange], None]] = []
        self._change_history: List[UIChange] = []

    def add_change_handler(self, handler: Callable[[UIChange], None]) -> ChangeDetector:
        """Add a handler for change events."""
        self._change_handlers.append(handler)
        return self

    def update_snapshot(self, element: Dict[str, Any]) -> List[UIChange]:
        """
        Update snapshot for an element and detect changes.

        Returns list of detected changes.
        """
        snapshot = ElementSnapshot.from_element(element)
        element_id = snapshot.element_id

        changes: List[UIChange] = []

        if element_id not in self._snapshots:
            # New element
            change = UIChange(
                change_type=ChangeType.APPEARED,
                element_id=element_id,
                path=element.get("path", ""),
                new_value=snapshot,
            )
            changes.append(change)

        else:
            old_snapshot = self._snapshots[element_id]

            if snapshot.has_changed(old_snapshot):
                # Determine change type
                change_type = ChangeType.MODIFIED

                if old_snapshot.bounds != snapshot.bounds:
                    if old_snapshot.role != snapshot.role:
                        change_type = ChangeType.MOVED
                    else:
                        change_type = ChangeType.RESIZED

                elif old_snapshot.enabled != snapshot.enabled:
                    change_type = ChangeType.STATE_CHANGED

                change = UIChange(
                    change_type=change_type,
                    element_id=element_id,
                    path=element.get("path", ""),
                    old_value=old_snapshot,
                    new_value=snapshot,
                )
                changes.append(change)

        # Update stored snapshot
        self._snapshots[element_id] = snapshot

        # Notify handlers
        for change in changes:
            self._change_history.append(change)
            for handler in self._change_handlers:
                handler(change)

        return changes

    def remove_element(self, element_id: str) -> Optional[UIChange]:
        """Mark an element as removed."""
        if element_id in self._snapshots:
            old_snapshot = self._snapshots.pop(element_id)
            change = UIChange(
                change_type=ChangeType.DISAPPEARED,
                element_id=element_id,
                path="",
                old_value=old_snapshot,
            )
            self._change_history.append(change)
            for handler in self._change_handlers:
                handler(change)
            return change
        return None

    def get_changes_since(self, timestamp: float) -> List[UIChange]:
        """Get all changes after a specific time."""
        return [c for c in self._change_history if c.timestamp >= timestamp]

    def get_element_snapshot(self, element_id: str) -> Optional[ElementSnapshot]:
        """Get current snapshot for an element."""
        return self._snapshots.get(element_id)

    def clear_history(self) -> None:
        """Clear change history."""
        self._change_history.clear()


class PeriodicChecker:
    """
    Periodically checks for UI changes.

    Useful for monitoring an application for changes
    over time without continuous polling.
    """

    def __init__(
        self,
        interval: float = 1.0,
        detector: Optional[ChangeDetector] = None,
    ) -> None:
        self._interval = interval
        self._detector = detector or ChangeDetector()
        self._running: bool = False
        self._snapshot_func: Optional[Callable[[], List[Dict[str, Any]]]] = None
        self._handlers: List[Callable[[List[UIChange]], None]] = []

    def set_snapshot_func(self, func: Callable[[], List[Dict[str, Any]]]) -> PeriodicChecker:
        """Set function that returns current UI elements."""
        self._snapshot_func = func
        return self

    def add_handler(self, handler: Callable[[List[UIChange]], None]) -> PeriodicChecker:
        """Add handler for change batches."""
        self._handlers.append(handler)
        return self

    def start(self) -> PeriodicChecker:
        """Start periodic checking."""
        self._running = True
        return self

    def stop(self) -> None:
        """Stop periodic checking."""
        self._running = False

    def check(self) -> List[UIChange]:
        """
        Perform a single check for changes.

        Returns list of detected changes.
        """
        if not self._snapshot_func:
            return []

        elements = self._snapshot_func()
        all_changes: List[UIChange] = []

        for element in elements:
            changes = self._detector.update_snapshot(element)
            all_changes.extend(changes)

        if all_changes:
            for handler in self._handlers:
                handler(all_changes)

        return all_changes


class ChangeMonitor:
    """
    High-level monitor for UI changes.

    Provides a simple interface for watching specific
    elements or patterns.
    """

    def __init__(self) -> None:
        self._detector = ChangeDetector()
        self._watched_patterns: Dict[str, Callable[[UIChange], bool]] = {}
        self._handlers: Dict[str, List[Callable[[UIChange], None]]] = {}

    def watch(
        self,
        pattern: str,
        handler: Callable[[UIChange], None],
        filter_func: Optional[Callable[[UIChange], bool]] = None,
    ) -> ChangeMonitor:
        """
        Watch for changes matching a pattern.

        Args:
            pattern: Identifier for this watch.
            handler: Callback when matching change detected.
            filter_func: Optional filter for specific changes.
        """
        self._watched_patterns[pattern] = filter_func or (lambda _: True)
        self._handlers.setdefault(pattern, []).append(handler)
        return self

    def unwatch(self, pattern: str) -> bool:
        """Stop watching a pattern."""
        if pattern in self._handlers:
            del self._handlers[pattern]
            del self._watched_patterns[pattern]
            return True
        return False

    def on_change(self, change: UIChange) -> None:
        """Process a change and notify matching watchers."""
        for pattern, filter_func in self._watched_patterns.items():
            if filter_func(change):
                for handler in self._handlers.get(pattern, []):
                    handler(change)

    def add_change(self, element: Dict[str, Any]) -> List[UIChange]:
        """Add an element and detect changes."""
        changes = self._detector.update_snapshot(element)
        for change in changes:
            self.on_change(change)
        return changes


def detect_element_changes(
    old_elements: List[Dict[str, Any]],
    new_elements: List[Dict[str, Any]],
) -> List[UIChange]:
    """
    Compare two element lists and return changes.

    Simple utility function for one-off comparisons.
    """
    detector = ChangeDetector()

    for element in old_elements:
        detector.update_snapshot(element)

    changes: List[UIChange] = []
    for element in new_elements:
        elem_changes = detector.update_snapshot(element)
        changes.extend(elem_changes)

    return changes
