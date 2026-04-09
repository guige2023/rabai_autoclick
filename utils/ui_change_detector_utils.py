"""
UI change detector utilities.

Detect and track UI changes for automation reliability.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional, Callable, Any
from enum import Enum, auto


class ChangeType(Enum):
    """Types of UI changes."""
    ELEMENT_ADDED = auto()
    ELEMENT_REMOVED = auto()
    ELEMENT_UPDATED = auto()
    ELEMENT_MOVED = auto()
    ELEMENT_VISIBLE = auto()
    ELEMENT_HIDDEN = auto()
    PROPERTY_CHANGED = auto()


@dataclass
class UIChange:
    """Represents a detected UI change."""
    change_type: ChangeType
    element_id: str
    timestamp: float
    details: dict = field(default_factory=dict)


@dataclass
class ElementSnapshot:
    """Snapshot of an element at a point in time."""
    element_id: str
    bounds: tuple[int, int, int, int]
    is_visible: bool
    properties: dict
    timestamp: float


class UIChangeDetector:
    """Detect changes in UI state."""
    
    def __init__(self):
        self._snapshots: dict[str, ElementSnapshot] = {}
        self._change_callbacks: list[Callable[[UIChange], None]] = []
        self._change_history: list[UIChange] = []
        self._max_history = 500
    
    def capture_element(
        self,
        element_id: str,
        bounds: tuple[int, int, int, int],
        is_visible: bool,
        properties: Optional[dict] = None
    ) -> Optional[UIChange]:
        """Capture element state and detect changes."""
        previous = self._snapshots.get(element_id)
        
        current = ElementSnapshot(
            element_id=element_id,
            bounds=bounds,
            is_visible=is_visible,
            properties=properties or {},
            timestamp=time.time()
        )
        
        change = None
        
        if previous is None:
            change = UIChange(
                change_type=ChangeType.ELEMENT_ADDED,
                element_id=element_id,
                timestamp=current.timestamp
            )
        elif not is_visible and previous.is_visible:
            change = UIChange(
                change_type=ChangeType.ELEMENT_HIDDEN,
                element_id=element_id,
                timestamp=current.timestamp
            )
        elif is_visible and not previous.is_visible:
            change = UIChange(
                change_type=ChangeType.ELEMENT_VISIBLE,
                element_id=element_id,
                timestamp=current.timestamp
            )
        elif bounds != previous.bounds:
            change = UIChange(
                change_type=ChangeType.ELEMENT_MOVED,
                element_id=element_id,
                timestamp=current.timestamp,
                details={
                    "old_bounds": previous.bounds,
                    "new_bounds": bounds
                }
            )
        elif properties != previous.properties:
            diff = self._diff_properties(previous.properties, properties)
            if diff:
                change = UIChange(
                    change_type=ChangeType.PROPERTY_CHANGED,
                    element_id=element_id,
                    timestamp=current.timestamp,
                    details=diff
                )
        
        self._snapshots[element_id] = current
        
        if change:
            self._record_change(change)
        
        return change
    
    def remove_element(self, element_id: str) -> Optional[UIChange]:
        """Mark an element as removed."""
        if element_id in self._snapshots:
            change = UIChange(
                change_type=ChangeType.ELEMENT_REMOVED,
                element_id=element_id,
                timestamp=time.time()
            )
            self._record_change(change)
            del self._snapshots[element_id]
            return change
        return None
    
    def _diff_properties(self, old: dict, new: dict) -> dict:
        """Diff two property dictionaries."""
        diff = {}
        
        for key in set(old.keys()) | set(new.keys()):
            if key not in old:
                diff[key] = {"from": None, "to": new[key]}
            elif key not in new:
                diff[key] = {"from": old[key], "to": None}
            elif old[key] != new[key]:
                diff[key] = {"from": old[key], "to": new[key]}
        
        return diff
    
    def _record_change(self, change: UIChange) -> None:
        """Record a detected change."""
        self._change_history.append(change)
        if len(self._change_history) > self._max_history:
            self._change_history.pop(0)
        
        for callback in self._change_callbacks:
            callback(change)
    
    def on_change(self, callback: Callable[[UIChange], None]) -> None:
        """Register callback for change events."""
        self._change_callbacks.append(callback)
    
    def get_change_history(
        self,
        element_id: Optional[str] = None,
        since: Optional[float] = None
    ) -> list[UIChange]:
        """Get change history."""
        changes = self._change_history
        
        if element_id:
            changes = [c for c in changes if c.element_id == element_id]
        
        if since:
            changes = [c for c in changes if c.timestamp >= since]
        
        return changes
    
    def get_snapshot(self, element_id: str) -> Optional[ElementSnapshot]:
        """Get current snapshot of an element."""
        return self._snapshots.get(element_id)
    
    def clear_history(self) -> None:
        """Clear change history."""
        self._change_history.clear()
