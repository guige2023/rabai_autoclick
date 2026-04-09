"""
Element State Tracker Action Module.

Tracks the state of UI elements over time, detecting changes,
animations, and transient states for robust automation.
"""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


@dataclass
class ElementSnapshot:
    """A snapshot of an element's state at a point in time."""
    element_id: str
    text: str
    visible: bool
    enabled: bool
    bounds: tuple[int, int, int, int]
    attributes: dict[str, str]
    timestamp: float
    checksum: str = ""

    def is_visible_region(self, region: tuple[int, int, int, int]) -> bool:
        """Check if element overlaps with a screen region."""
        bx1, by1, bx2, by2 = self.bounds
        rx1, ry1, rx2, ry2 = region
        return not (bx2 < rx1 or bx1 > rx2 or by2 < ry1 or by1 > ry2)


@dataclass
class StateChange:
    """Represents a detected state change."""
    element_id: str
    change_type: str
    old_value: Any
    new_value: Any
    timestamp: float


class ElementStateTracker:
    """Tracks element state changes over time."""

    def __init__(self, history_size: int = 100):
        """
        Initialize state tracker.

        Args:
            history_size: Number of snapshots to retain per element.
        """
        self.history_size = history_size
        self._snapshots: dict[str, deque[ElementSnapshot]] = {}
        self._change_callbacks: list[Callable[[StateChange], None]] = []

    def capture(
        self,
        element_id: str,
        text: str,
        visible: bool,
        enabled: bool,
        bounds: tuple[int, int, int, int],
        attributes: Optional[dict[str, str]] = None,
    ) -> Optional[StateChange]:
        """
        Capture current state of an element.

        Args:
            element_id: Unique element identifier.
            text: Current element text.
            visible: Current visibility.
            enabled: Current enabled state.
            bounds: Current bounding box (x1, y1, x2, y2).
            attributes: Current element attributes.

        Returns:
            StateChange if state changed, None otherwise.
        """
        if element_id not in self._snapshots:
            self._snapshots[element_id] = deque(maxlen=self.history_size)

        attributes = attributes or {}
        snapshot = ElementSnapshot(
            element_id=element_id,
            text=text,
            visible=visible,
            enabled=enabled,
            bounds=bounds,
            attributes=attributes,
            timestamp=time.time(),
        )

        last = self._get_last_snapshot(element_id)
        change = self._detect_change(last, snapshot) if last else None

        self._snapshots[element_id].append(snapshot)

        if change:
            self._notify_change(change)

        return change

    def get_history(
        self,
        element_id: str,
        since: Optional[float] = None,
    ) -> list[ElementSnapshot]:
        """
        Get state history for an element.

        Args:
            element_id: Element identifier.
            since: Only snapshots after this timestamp.

        Returns:
            List of snapshots, oldest first.
        """
        if element_id not in self._snapshots:
            return []

        history = list(self._snapshots[element_id])
        if since is not None:
            history = [s for s in history if s.timestamp >= since]
        return history

    def is_stable(
        self,
        element_id: str,
        duration: float = 1.0,
    ) -> bool:
        """
        Check if element has been stable (no changes) for duration.

        Args:
            element_id: Element identifier.
            duration: Required stable duration in seconds.

        Returns:
            True if stable for given duration.
        """
        history = self.get_history(element_id)
        if len(history) < 2:
            return False

        last = history[-1]
        now = time.time()
        return (now - last.timestamp) >= duration

    def wait_for_state(
        self,
        element_id: str,
        check_fn: Callable[[ElementSnapshot], bool],
        timeout: float = 10.0,
        poll_interval: float = 0.1,
    ) -> Optional[ElementSnapshot]:
        """
        Wait for element to reach a target state.

        Args:
            element_id: Element to monitor.
            check_fn: Function that returns True when target state reached.
            timeout: Maximum wait time.
            poll_interval: Check interval.

        Returns:
            Snapshot when state reached, or None on timeout.
        """
        start = time.time()
        while time.time() - start < timeout:
            history = self.get_history(element_id)
            if history:
                last = history[-1]
                if check_fn(last):
                    return last
            time.sleep(poll_interval)
        return None

    def on_state_change(
        self,
        callback: Callable[[StateChange], None],
    ) -> None:
        """
        Register a callback for state changes.

        Args:
            callback: Function to call on state change.
        """
        self._change_callbacks.append(callback)

    def _get_last_snapshot(self, element_id: str) -> Optional[ElementSnapshot]:
        """Get the most recent snapshot for an element."""
        if element_id in self._snapshots and self._snapshots[element_id]:
            return self._snapshots[element_id][-1]
        return None

    def _detect_change(
        self,
        old: ElementSnapshot,
        new: ElementSnapshot,
    ) -> Optional[StateChange]:
        """Detect what changed between two snapshots."""
        if old.text != new.text:
            return StateChange(new.element_id, "text", old.text, new.text, new.timestamp)
        if old.visible != new.visible:
            return StateChange(new.element_id, "visible", old.visible, new.visible, new.timestamp)
        if old.enabled != new.enabled:
            return StateChange(new.element_id, "enabled", old.enabled, new.enabled, new.timestamp)
        if old.bounds != new.bounds:
            return StateChange(new.element_id, "bounds", old.bounds, new.bounds, new.timestamp)
        return None

    def _notify_change(self, change: StateChange) -> None:
        """Notify all registered callbacks of a change."""
        for callback in self._change_callbacks:
            try:
                callback(change)
            except Exception:
                pass
