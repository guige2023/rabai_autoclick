"""
UI Change Detector.

Detect changes in UI state by monitoring accessibility trees
and triggering callbacks when elements appear, disappear, or change.

Usage:
    from utils.ui_change_detector import UIChangeDetector, ChangeType

    detector = UIChangeDetector(bridge)
    detector.on_change(ChangeType.ELEMENT_ADDED, callback)
    detector.start()
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from enum import Enum, auto
import time
import hashlib

if TYPE_CHECKING:
    pass


class ChangeType(Enum):
    """Types of UI changes."""
    ELEMENT_ADDED = auto()
    ELEMENT_REMOVED = auto()
    ELEMENT_MODIFIED = auto()
    WINDOW_APPEARED = auto()
    WINDOW_DISAPPEARED = auto()
    APP_ACTIVATED = auto()
    APP_DEACTIVATED = auto()


@dataclass
class UIChange:
    """Represents a detected UI change."""
    change_type: ChangeType
    element: Optional[Dict[str, Any]] = None
    element_role: Optional[str] = None
    element_title: Optional[str] = None
    timestamp: float = field(default_factory=time.time)
    details: Optional[str] = None

    def __repr__(self) -> str:
        return f"UIChange({self.change_type.name}, {self.element_role!r})"


class UIChangeDetector:
    """
    Detect changes in UI state.

    Monitors accessibility trees and emits events when changes
    are detected. Useful for waiting for UI updates after actions.

    Example:
        detector = UIChangeDetector(bridge)
        detector.on_change(ChangeType.ELEMENT_ADDED, my_callback)
        detector.start()

        # Perform action that should add an element
        bridge.click_button("Add")

        change = detector.wait_for_change(
            ChangeType.ELEMENT_ADDED,
            timeout=5.0
        )
    """

    def __init__(
        self,
        bridge: Any,
        poll_interval: float = 0.2,
        max_history: int = 100,
    ) -> None:
        """
        Initialize the change detector.

        Args:
            bridge: AccessibilityBridge instance.
            poll_interval: Seconds between polling checks.
            max_history: Maximum number of changes to retain.
        """
        self._bridge = bridge
        self._poll_interval = poll_interval
        self._max_history = max_history
        self._running = False
        self._callbacks: Dict[ChangeType, List[Callable[[UIChange], None]]] = {
            ct: [] for ct in ChangeType
        }
        self._history: List[UIChange] = []
        self._last_tree_hash: Optional[str] = None
        self._last_window_count = 0
        self._last_app_bundle_id: Optional[str] = None

    def start(self) -> None:
        """Start detecting changes."""
        self._running = True
        self._last_tree_hash = self._get_tree_hash()

    def stop(self) -> None:
        """Stop detecting changes."""
        self._running = False

    @property
    def is_running(self) -> bool:
        return self._running

    def on_change(
        self,
        change_type: ChangeType,
        callback: Callable[[UIChange], None],
    ) -> None:
        """
        Register a callback for a specific change type.

        Args:
            change_type: Type of change to listen for.
            callback: Function called with UIChange on detection.
        """
        if callback not in self._callbacks[change_type]:
            self._callbacks[change_type].append(callback)

    def _emit(self, change: UIChange) -> None:
        """Emit a change to registered callbacks."""
        self._history.append(change)
        if len(self._history) > self._max_history:
            self._history.pop(0)

        for cb in self._callbacks.get(change.change_type, []):
            try:
                cb(change)
            except Exception:
                pass

        for cb in self._callbacks.get(ChangeType.ELEMENT_MODIFIED, []):
            if change.change_type == ChangeType.ELEMENT_MODIFIED:
                try:
                    cb(change)
                except Exception:
                    pass

    def _get_tree_hash(self) -> str:
        """Get a hash of the current accessibility tree."""
        try:
            app = self._bridge.get_frontmost_app()
            if app is None:
                return ""

            tree = self._bridge.build_accessibility_tree(app)
            tree_str = str(sorted(tree.items()))
            return hashlib.md5(tree_str.encode()).hexdigest()
        except Exception:
            return ""

    def _get_window_count(self) -> int:
        """Get the number of windows in the frontmost app."""
        try:
            app = self._bridge.get_frontmost_app()
            if app is None:
                return 0
            tree = self._bridge.build_accessibility_tree(app)
            return self._count_windows(tree)
        except Exception:
            return 0

    def _count_windows(self, node: Dict[str, Any]) -> int:
        """Recursively count windows in a tree."""
        count = 1 if node.get("role") == "window" else 0
        for child in node.get("children", []):
            if isinstance(child, dict):
                count += self._count_windows(child)
        return count

    def poll(self) -> List[UIChange]:
        """
        Poll for changes. Call this in a loop.

        Returns:
            List of detected changes since last poll.
        """
        if not self._running:
            return []

        changes: List[UIChange] = []

        current_hash = self._get_tree_hash()
        if self._last_tree_hash and current_hash != self._last_tree_hash:
            changes.append(UIChange(
                change_type=ChangeType.ELEMENT_MODIFIED,
                details="Tree structure changed",
            ))
        self._last_tree_hash = current_hash

        current_windows = self._get_window_count()
        if self._last_window_count and current_windows != self._last_window_count:
            if current_windows > self._last_window_count:
                changes.append(UIChange(
                    change_type=ChangeType.WINDOW_APPEARED,
                    details=f"Window count: {self._last_window_count} -> {current_windows}",
                ))
            else:
                changes.append(UIChange(
                    change_type=ChangeType.WINDOW_DISAPPEARED,
                    details=f"Window count: {self._last_window_count} -> {current_windows}",
                ))
        self._last_window_count = current_windows

        for change in changes:
            self._emit(change)

        return changes

    def wait_for_change(
        self,
        change_type: ChangeType,
        timeout: float = 10.0,
        role: Optional[str] = None,
        title: Optional[str] = None,
    ) -> Optional[UIChange]:
        """
        Wait for a specific change to occur.

        Args:
            change_type: Type of change to wait for.
            timeout: Maximum seconds to wait.
            role: Optional role filter.
            title: Optional title filter.

        Returns:
            UIChange if detected, None on timeout.
        """
        deadline = time.time() + timeout

        while time.time() < deadline:
            if not self._running:
                self.start()

            changes = self.poll()
            for change in changes:
                if change.change_type == change_type:
                    if role and change.element_role != role:
                        continue
                    if title and change.element_title != title:
                        continue
                    return change

            time.sleep(self._poll_interval)

        return None

    def get_history(
        self,
        since: Optional[float] = None,
        change_type: Optional[ChangeType] = None,
    ) -> List[UIChange]:
        """
        Get the change history.

        Args:
            since: Optional Unix timestamp filter.
            change_type: Optional type filter.

        Returns:
            List of UIChange objects.
        """
        results = list(reversed(self._history))

        if since is not None:
            results = [c for c in results if c.timestamp >= since]
        if change_type is not None:
            results = [c for c in results if c.change_type == change_type]

        return results
