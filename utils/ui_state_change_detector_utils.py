"""
UI state change detection utilities.

This module provides utilities for detecting and classifying UI state changes,
including property changes, structural changes, and animation transitions.
"""

from __future__ import annotations

import time
from typing import List, Optional, Dict, Any, Callable, Set
from dataclasses import dataclass, field
from enum import Enum, auto


class ChangeType(Enum):
    """Classification of UI state change."""
    NONE = auto()
    PROPERTY = auto()
    STRUCTURE = auto()
    VISIBILITY = auto()
    FOCUS = auto()
    VALUE = auto()
    ANIMATION = auto()
    LAYOUT = auto()
    MULTIPLE = auto()


@dataclass
class UIChange:
    """A detected UI state change."""
    change_type: ChangeType
    element_path: str
    old_value: Any = None
    new_value: Any = None
    timestamp: float = field(default_factory=time.time)
    animated: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ChangeDetectorConfig:
    """Configuration for change detection."""
    debounce_ms: float = 50.0
    ignore_animations: bool = True
    animation_threshold_ms: float = 300.0
    min_change_importance: float = 0.0


@dataclass
class ElementSnapshot:
    """Snapshot of an element's state at a point in time."""
    path: str
    role: str = ""
    label: str = ""
    value: str = ""
    enabled: bool = True
    visible: bool = True
    focused: bool = False
    bounds: Tuple[int, int, int, int] = (0, 0, 0, 0)
    children_count: int = 0
    timestamp: float = field(default_factory=time.time)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "role": self.role,
            "label": self.label,
            "value": self.value,
            "enabled": self.enabled,
            "visible": self.visible,
            "focused": self.focused,
            "bounds": self.bounds,
            "children_count": self.children_count,
            "timestamp": self.timestamp,
        }


class UIStateChangeDetector:
    """Detects and classifies changes in UI element states."""

    def __init__(self, config: Optional[ChangeDetectorConfig] = None):
        self.config = config or ChangeDetectorConfig()
        self._last_snapshot: Optional[List[ElementSnapshot]] = None
        self._last_change_time: float = 0.0
        self._change_log: List[UIChange] = []

    def detect_changes(
        self,
        old_elements: List[ElementSnapshot],
        new_elements: List[ElementSnapshot],
    ) -> List[UIChange]:
        """
        Detect changes between two element snapshots.

        Args:
            old_elements: Previous element states.
            new_elements: Current element states.

        Returns:
            List of detected UIChange objects.
        """
        changes: List[UIChange] = []
        old_map: Dict[str, ElementSnapshot] = {e.path: e for e in old_elements}
        new_map: Dict[str, ElementSnapshot] = {e.path: e for e in new_elements}

        all_paths: Set[str] = set(old_map.keys()) | set(new_map.keys())

        for path in all_paths:
            old_elem = old_map.get(path)
            new_elem = new_map.get(path)

            if old_elem is None and new_elem is not None:
                changes.append(UIChange(
                    change_type=ChangeType.STRUCTURE,
                    element_path=path,
                    new_value=new_elem.to_dict(),
                    metadata={"action": "added"},
                ))
            elif old_elem is not None and new_elem is None:
                changes.append(UIChange(
                    change_type=ChangeType.STRUCTURE,
                    element_path=path,
                    old_value=old_elem.to_dict(),
                    metadata={"action": "removed"},
                ))
            elif old_elem is not None and new_elem is not None:
                elem_changes = self._detect_element_changes(old_elem, new_elem)
                changes.extend(elem_changes)

        # Debounce
        now = time.time()
        recent_changes = [c for c in changes if (now - c.timestamp) * 1000 > self.config.debounce_ms]
        if not recent_changes and changes:
            # All changes are recent - apply debounce by clearing
            self._last_change_time = now

        self._last_snapshot = new_elements
        self._change_log.extend(changes)
        return changes

    def _detect_element_changes(
        self,
        old_elem: ElementSnapshot,
        new_elem: ElementSnapshot,
    ) -> List[UIChange]:
        """Detect changes within a single element."""
        changes: List[UIChange] = []

        if old_elem.visible != new_elem.visible:
            changes.append(UIChange(
                change_type=ChangeType.VISIBILITY,
                element_path=old_elem.path,
                old_value=old_elem.visible,
                new_value=new_elem.visible,
            ))

        if old_elem.focused != new_elem.focused:
            changes.append(UIChange(
                change_type=ChangeType.FOCUS,
                element_path=old_elem.path,
                old_value=old_elem.focused,
                new_value=new_elem.focused,
            ))

        if old_elem.value != new_elem.value:
            changes.append(UIChange(
                change_type=ChangeType.VALUE,
                element_path=old_elem.path,
                old_value=old_elem.value,
                new_value=new_elem.value,
            ))

        if old_elem.enabled != new_elem.enabled:
            changes.append(UIChange(
                change_type=ChangeType.PROPERTY,
                element_path=old_elem.path,
                old_value=old_elem.enabled,
                new_value=new_elem.enabled,
                metadata={"property": "enabled"},
            ))

        if old_elem.children_count != new_elem.children_count:
            changes.append(UIChange(
                change_type=ChangeType.STRUCTURE,
                element_path=old_elem.path,
                old_value=old_elem.children_count,
                new_value=new_elem.children_count,
                metadata={"property": "children_count"},
            ))

        if old_elem.bounds != new_elem.bounds:
            changes.append(UIChange(
                change_type=ChangeType.LAYOUT,
                element_path=old_elem.path,
                old_value=old_elem.bounds,
                new_value=new_elem.bounds,
            ))

        return changes

    def get_change_summary(self) -> Dict[str, int]:
        """Get a summary of change types detected."""
        summary: Dict[str, int] = {}
        for change in self._change_log:
            key = change.change_type.name
            summary[key] = summary.get(key, 0) + 1
        return summary

    def clear_log(self) -> None:
        """Clear the change log."""
        self._change_log.clear()


def is_animated_change(change: UIChange, threshold_ms: float = 300.0) -> bool:
    """
    Determine if a change is likely animated.

    Args:
        change: The UIChange to evaluate.
        threshold_ms: Time threshold for animation detection.

    Returns:
        True if the change appears animated.
    """
    if change.change_type in (ChangeType.VISIBILITY, ChangeType.LAYOUT, ChangeType.PROPERTY):
        # Heuristic: layout/visibility changes in quick succession are likely animated
        return True
    return change.animated
