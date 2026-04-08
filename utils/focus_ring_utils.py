"""
Focus Ring Utilities for macOS Accessibility.

Provides utilities for tracking, detecting, and interacting with
the macOS accessibility focus ring (keyboard navigation indicator).

Usage:
    from utils.focus_ring_utils import FocusRingTracker, get_focused_element

    tracker = FocusRingTracker()
    tracker.start()
    # ... perform actions ...
    focused = tracker.get_current_focus()
    tracker.stop()
"""

from __future__ import annotations

import time
from typing import Optional, Callable, Dict, Any, List, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum, auto

if TYPE_CHECKING:
    from utils.accessibility_bridge import AccessibilityBridge


class FocusChangeType(Enum):
    """Types of focus changes that can occur."""
    DIRECT = auto()
    TRAVERSED = auto()
    ACTIVATED = auto()
    DEACTIVATED = auto()
    DISMISSED = auto()


@dataclass
class FocusRingEvent:
    """Represents a focus ring change event."""
    change_type: FocusChangeType
    element: Optional[Any]
    role: Optional[str]
    title: Optional[str]
    value: Optional[str]
    timestamp: float = field(default_factory=time.time)
    previous_element: Optional[Any] = None

    def __repr__(self) -> str:
        return (
            f"FocusRingEvent(type={self.change_type.name}, "
            f"role={self.role!r}, title={self.title!r})"
        )


class FocusRingTracker:
    """
    Track focus ring changes in macOS accessibility hierarchy.

    The focus ring indicates which element currently has keyboard
    focus. This tracker monitors focus changes and can invoke
    callbacks when focus moves to specific elements.

    Example:
        tracker = FocusRingTracker(bridge=bridge)
        tracker.on_focus_change(callback=lambda e: print(f"Focused: {e.title}"))
        tracker.start()
    """

    def __init__(
        self,
        bridge: Optional["AccessibilityBridge"] = None,
        poll_interval: float = 0.05,
    ) -> None:
        """
        Initialize the focus ring tracker.

        Args:
            bridge: AccessibilityBridge instance for querying elements.
            poll_interval: Seconds between polling checks (default 0.05s).
        """
        self._bridge = bridge
        self._poll_interval = poll_interval
        self._running = False
        self._callbacks: List[Callable[[FocusRingEvent], None]] = []
        self._last_focused: Optional[Any] = None
        self._event_history: List[FocusRingEvent] = []
        self._max_history = 100

    @property
    def is_running(self) -> bool:
        """Return True if the tracker is actively monitoring."""
        return self._running

    def on_focus_change(
        self,
        callback: Callable[[FocusRingEvent], None],
    ) -> None:
        """
        Register a callback to be invoked on focus changes.

        Args:
            callback: Function called with FocusRingEvent on each change.
        """
        self._callbacks.append(callback)

    def get_current_focus(self) -> Optional[Dict[str, Any]]:
        """
        Get the currently focused element as a dictionary.

        Returns:
            Dictionary with role, title, value, and other attributes,
            or None if no focus can be determined.
        """
        if self._bridge is None:
            return None

        try:
            frontmost = self._bridge.get_frontmost_app()
            if frontmost is None:
                return None

            tree = self._bridge.build_accessibility_tree(frontmost)
            focused = self._find_focused_element(tree)
            if focused:
                return self._element_to_dict(focused)
            return None
        except Exception:
            return None

    def _find_focused_element(
        self,
        tree: Dict[str, Any],
        visited: Optional[set] = None,
    ) -> Optional[Dict[str, Any]]:
        """Recursively search tree for focused element."""
        if visited is None:
            visited = set()

        node_id = id(tree)
        if node_id in visited:
            return None
        visited.add(node_id)

        if tree.get("focused") or tree.get("selected"):
            return tree

        for child in tree.get("children", []):
            result = self._find_focused_element(child, visited)
            if result:
                return result

        return None

    def _element_to_dict(self, element: Dict[str, Any]) -> Dict[str, Any]:
        """Convert element to dictionary with relevant focus info."""
        return {
            "role": element.get("role"),
            "role_description": element.get("role_description"),
            "title": element.get("title"),
            "value": element.get("value"),
            "description": element.get("description"),
            "enabled": element.get("enabled"),
            "focused": element.get("focused", False),
            "selected": element.get("selected", False),
        }

    def _emit_event(self, event: FocusRingEvent) -> None:
        """Emit an event to all registered callbacks."""
        self._event_history.append(event)
        if len(self._event_history) > self._max_history:
            self._event_history.pop(0)
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception:
                pass

    def start(self) -> None:
        """Start tracking focus changes."""
        self._running = True
        self._last_focused = self.get_current_focus()

    def stop(self) -> None:
        """Stop tracking focus changes."""
        self._running = False

    def get_event_history(
        self,
        since: Optional[float] = None,
    ) -> List[FocusRingEvent]:
        """
        Get recorded focus change events.

        Args:
            since: Optional Unix timestamp to filter events after.

        Returns:
            List of FocusRingEvent objects, newest first.
        """
        if since is None:
            return list(reversed(self._event_history))
        return [e for e in reversed(self._event_history) if e.timestamp >= since]


def get_focused_element(bridge: "AccessibilityBridge") -> Optional[Dict[str, Any]]:
    """
    Convenience function to get the currently focused element.

    Args:
        bridge: An AccessibilityBridge instance.

    Returns:
        Dictionary of focused element attributes, or None.
    """
    tracker = FocusRingTracker(bridge=bridge)
    return tracker.get_current_focus()


def wait_for_focus(
    bridge: "AccessibilityBridge",
    role: Optional[str] = None,
    title: Optional[str] = None,
    timeout: float = 5.0,
    poll_interval: float = 0.05,
) -> Optional[Dict[str, Any]]:
    """
    Wait for an element with specified attributes to receive focus.

    Args:
        bridge: AccessibilityBridge instance.
        role: Required role (e.g., "button", "text_field").
        title: Required title value.
        timeout: Maximum seconds to wait.
        poll_interval: Seconds between each check.

    Returns:
        Element dictionary if found, None on timeout.
    """
    tracker = FocusRingTracker(bridge=bridge, poll_interval=poll_interval)
    tracker.start()

    deadline = time.time() + timeout
    while time.time() < deadline:
        focus = tracker.get_current_focus()
        if focus:
            if role and focus.get("role") != role:
                pass
            elif title and focus.get("title") != title:
                pass
            else:
                tracker.stop()
                return focus
        time.sleep(poll_interval)

    tracker.stop()
    return None
