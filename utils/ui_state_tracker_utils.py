"""
UI state tracking and change detection utilities.

This module provides utilities for tracking UI state changes,
detecting modifications, and triggering callbacks on state transitions.
"""

from __future__ import annotations

import time
import hashlib
import copy
from dataclasses import dataclass, field
from typing import Callable, Optional, Any, Dict, List, Set, TypeVar, Generic
from enum import Enum, auto
from abc import ABC, abstractmethod


class ChangeType(Enum):
    """Types of UI state changes."""
    ADDED = auto()
    REMOVED = auto()
    MODIFIED = auto()
    UNCHANGED = auto()


@dataclass
class StateChange:
    """
    Represents a detected state change.

    Attributes:
        path: Dot-notation path to the changed element.
        change_type: Type of change that occurred.
        old_value: Previous value before change.
        new_value: Current value after change.
        timestamp: When the change was detected.
    """
    path: str
    change_type: ChangeType
    old_value: Any = None
    new_value: Any = None
    timestamp: float = field(default_factory=time.time)

    def __str__(self) -> str:
        return f"{self.change_type.name} at {self.path}"


T = TypeVar("T")


@dataclass
class StateSnapshot(Generic[T]):
    """
    Point-in-time snapshot of state.

    Attributes:
        state: The state data at snapshot time.
        timestamp: When the snapshot was taken.
        checksum: Hash of the state for comparison.
    """
    state: T
    timestamp: float = field(default_factory=time.time)
    checksum: str = ""

    @classmethod
    def capture(cls, state: T) -> StateSnapshot[T]:
        """Create a new snapshot from current state."""
        serialized = str(state)
        checksum = hashlib.md5(serialized.encode()).hexdigest()
        return cls(state=copy.deepcopy(state), checksum=checksum)

    def has_changed(self, other: StateSnapshot[T]) -> bool:
        """Check if this snapshot differs from another."""
        return self.checksum != other.checksum


class StateObserver(ABC):
    """Abstract base class for state observers."""

    @abstractmethod
    def on_change(self, change: StateChange) -> None:
        """Called when a state change is detected."""
        pass

    @abstractmethod
    def on_batch_change(self, changes: List[StateChange]) -> None:
        """Called when multiple changes are detected together."""
        pass


class ChangeDetector(Generic[T]):
    """
    Detects changes between state snapshots.

    Performs deep comparison of state objects and reports
    specific changes via registered observers.
    """

    def __init__(self, path_separator: str = ".") -> None:
        self._observers: List[StateObserver] = []
        self._previous: Optional[StateSnapshot[T]] = None
        self._path_separator = path_separator

    def add_observer(self, observer: StateObserver) -> ChangeDetector[T]:
        """Register an observer for change notifications."""
        self._observers.append(observer)
        return self

    def remove_observer(self, observer: StateObserver) -> None:
        """Unregister an observer."""
        self._observers.remove(observer)

    def capture(self, state: T) -> List[StateChange]:
        """
        Capture new state and detect changes from previous.

        Returns list of detected changes.
        """
        current = StateSnapshot.capture(state)
        changes: List[StateChange] = []

        if self._previous is not None:
            changes = self._detect_changes(self._previous.state, current.state)

            # Notify observers
            if changes:
                for observer in self._observers:
                    observer.on_batch_change(changes)
                for change in changes:
                    for observer in self._observers:
                        observer.on_change(change)

        self._previous = current
        return changes

    def _detect_changes(
        self,
        old: Any,
        new: Any,
        path: str = "",
    ) -> List[StateChange]:
        """Recursively detect changes between old and new state."""
        changes: List[StateChange] = []

        if type(old) != type(new):
            changes.append(StateChange(
                path=path or "root",
                change_type=ChangeType.MODIFIED,
                old_value=old,
                new_value=new,
            ))
            return changes

        if isinstance(old, dict):
            old_keys = set(old.keys())
            new_keys = set(new.keys())

            for key in old_keys - new_keys:
                changes.append(StateChange(
                    path=f"{path}{self._path_separator}{key}" if path else key,
                    change_type=ChangeType.REMOVED,
                    old_value=old[key],
                ))

            for key in new_keys - old_keys:
                changes.append(StateChange(
                    path=f"{path}{self._path_separator}{key}" if path else key,
                    change_type=ChangeType.ADDED,
                    new_value=new[key],
                ))

            for key in old_keys & new_keys:
                child_changes = self._detect_changes(
                    old[key],
                    new[key],
                    f"{path}{self._path_separator}{key}" if path else key,
                )
                changes.extend(child_changes)

        elif isinstance(old, (list, tuple)):
            if len(old) != len(new):
                changes.append(StateChange(
                    path=path or "root",
                    change_type=ChangeType.MODIFIED,
                    old_value=old,
                    new_value=new,
                ))
            else:
                for i, (old_item, new_item) in enumerate(zip(old, new)):
                    child_changes = self._detect_changes(old_item, new_item, f"{path}[{i}]")
                    changes.extend(child_changes)

        else:
            if old != new:
                changes.append(StateChange(
                    path=path or "root",
                    change_type=ChangeType.MODIFIED,
                    old_value=old,
                    new_value=new,
                ))

        return changes


class UIStateTracker:
    """
    High-level UI state tracking with automatic diffing.

    Maintains a history of state snapshots and provides
    methods to query state changes over time.
    """

    def __init__(self) -> None:
        self._snapshots: List[StateSnapshot[Dict[str, Any]]] = []
        self._change_detector = ChangeDetector[Dict[str, Any]]()
        self._max_history: int = 100

    def add_observer(self, observer: StateObserver) -> UIStateTracker:
        """Register an observer for state changes."""
        self._change_detector.add_observer(observer)
        return self

    def update(self, state: Dict[str, Any]) -> List[StateChange]:
        """
        Update tracker with new state.

        Returns detected changes since last update.
        """
        changes = self._change_detector.capture(state)

        # Maintain history
        snapshot = StateSnapshot.capture(state)
        self._snapshots.append(snapshot)

        # Trim history if needed
        if len(self._snapshots) > self._max_history:
            self._snapshots = self._snapshots[-self._max_history:]

        return changes

    def get_snapshot(self, index: int) -> Optional[StateSnapshot[Dict[str, Any]]]:
        """Get snapshot at index from end (0 = most recent)."""
        if not self._snapshots:
            return None
        try:
            return self._snapshots[-1 - index]
        except IndexError:
            return None

    def changes_since(self, index: int) -> Optional[List[StateChange]]:
        """Get changes between snapshot at index and most recent."""
        older = self.get_snapshot(index + 1)
        newer = self.get_snapshot(0)
        if older is None or newer is None:
            return None

        detector = ChangeDetector[Dict[str, Any]]()
        detector._previous = older
        return detector._detect_changes(older.state, newer.state)

    @property
    def history_size(self) -> int:
        """Get number of snapshots in history."""
        return len(self._snapshots)


class PropertyObserver(StateObserver):
    """Observer that tracks specific property paths."""

    def __init__(self, paths: Set[str]) -> None:
        self._paths = paths
        self._handlers: Dict[str, List[Callable[[Any], None]]] = {p: [] for p in paths}

    def on_change(self, change: StateChange) -> None:
        """Handle individual change notification."""
        if change.path in self._paths:
            for handler in self._handlers[change.path]:
                handler(change.new_value)

    def on_batch_change(self, changes: List[StateChange]) -> None:
        """Handle batch change notification."""
        pass

    def watch(self, path: str, handler: Callable[[Any], None]) -> None:
        """Register a handler for a property path."""
        if path in self._handlers:
            self._handlers[path].append(handler)
