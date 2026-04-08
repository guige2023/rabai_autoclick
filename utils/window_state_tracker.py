"""Window state tracking for UI automation.

Tracks window states (normal, minimized, maximized, fullscreen)
and window lifecycle events for automation workflows.
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class WindowState(Enum):
    """Window state values."""
    NORMAL = auto()
    MINIMIZED = auto()
    MAXIMIZED = auto()
    FULLSCREEN = auto()
    HIDDEN = auto()
    CLOSED = auto()


class WindowEvent(Enum):
    """Window lifecycle events."""
    OPENED = auto()
    CLOSED = auto()
    ACTIVATED = auto()
    DEACTIVATED = auto()
    RESIZED = auto()
    MOVED = auto()
    STATE_CHANGED = auto()
    TITLE_CHANGED = auto()
    FOCUS_GAINED = auto()
    FOCUS_LOST = auto()


@dataclass
class WindowSnapshot:
    """A point-in-time snapshot of a window's state.

    Attributes:
        window_id: Unique window identifier.
        title: Window title at snapshot time.
        process: Process name owning the window.
        state: Current window state.
        bounds: Window bounds as (x, y, width, height).
        is_focused: Whether the window has focus.
        is_visible: Whether the window is visible.
        monitor_id: ID of the monitor containing this window.
        timestamp: Snapshot timestamp.
    """
    window_id: str
    title: str
    process: str = ""
    state: WindowState = WindowState.NORMAL
    bounds: tuple[float, float, float, float] = (0, 0, 0, 0)
    is_focused: bool = False
    is_visible: bool = True
    monitor_id: str = ""
    timestamp: float = field(default_factory=time.time)

    @property
    def x(self) -> float:
        return self.bounds[0]

    @property
    def y(self) -> float:
        return self.bounds[1]

    @property
    def width(self) -> float:
        return self.bounds[2]

    @property
    def height(self) -> float:
        return self.bounds[3]

    @property
    def center(self) -> tuple[float, float]:
        """Return center point."""
        return (self.x + self.width / 2, self.y + self.height / 2)


@dataclass
class WindowHistoryEntry:
    """A historical record of a window state change.

    Attributes:
        snapshot: Window snapshot at this point.
        event: The event that caused this history entry.
        previous_state: The window state before this event.
        duration: How long the window was in previous_state.
    """
    snapshot: WindowSnapshot
    event: WindowEvent
    previous_state: Optional[WindowState] = None
    duration: float = 0.0
    timestamp: float = field(default_factory=time.time)


class WindowStateTracker:
    """Tracks window state changes over time.

    Maintains a history of window state snapshots and events,
    supports state diffing and event callbacks.
    """

    def __init__(self) -> None:
        """Initialize with empty tracking state."""
        self._snapshots: dict[str, WindowSnapshot] = {}
        self._history: dict[str, list[WindowHistoryEntry]] = {}
        self._event_callbacks: dict[
            WindowEvent, list[Callable[[WindowHistoryEntry], None]]
        ] = {}
        self._window_callbacks: dict[
            str, list[Callable[[WindowSnapshot], None]]
        ] = {}

    def update_snapshot(self, snapshot: WindowSnapshot) -> WindowHistoryEntry:
        """Update the snapshot for a window and record history.

        Returns the history entry describing the change.
        """
        prev_snapshot = self._snapshots.get(snapshot.window_id)
        prev_state = prev_snapshot.state if prev_snapshot else None
        event = self._detect_event(prev_snapshot, snapshot)
        duration = 0.0

        if prev_snapshot:
            duration = snapshot.timestamp - prev_snapshot.timestamp

        entry = WindowHistoryEntry(
            snapshot=snapshot,
            event=event,
            previous_state=prev_state,
            duration=duration,
        )

        self._snapshots[snapshot.window_id] = snapshot
        self._history.setdefault(snapshot.window_id, []).append(entry)
        self._notify_event(entry)
        return entry

    def _detect_event(
        self,
        prev: Optional[WindowSnapshot],
        curr: WindowSnapshot,
    ) -> WindowEvent:
        """Detect which event caused the snapshot change."""
        if prev is None:
            return WindowEvent.OPENED

        if prev.state != curr.state:
            return WindowEvent.STATE_CHANGED
        if prev.bounds != curr.bounds:
            if prev.x != curr.x or prev.y != curr.y:
                return WindowEvent.MOVED
            return WindowEvent.RESIZED
        if prev.is_focused != curr.is_focused:
            return (
                WindowEvent.FOCUS_GAINED
                if curr.is_focused
                else WindowEvent.FOCUS_LOST
            )
        if prev.title != curr.title:
            return WindowEvent.TITLE_CHANGED
        return WindowEvent.ACTIVATED

    def get_snapshot(self, window_id: str) -> Optional[WindowSnapshot]:
        """Get the current snapshot for a window."""
        return self._snapshots.get(window_id)

    def get_history(
        self,
        window_id: str,
        limit: int = 0,
    ) -> list[WindowHistoryEntry]:
        """Get history entries for a window.

        Args:
            window_id: The window to get history for.
            limit: Maximum entries to return (0 = all).
        """
        entries = self._history.get(window_id, [])
        if limit > 0:
            return entries[-limit:]
        return list(entries)

    def get_history_for_event(
        self,
        window_id: str,
        event: WindowEvent,
    ) -> list[WindowHistoryEntry]:
        """Get history entries filtered by event type."""
        return [
            e for e in self._history.get(window_id, [])
            if e.event == event
        ]

    def get_state_duration(
        self,
        window_id: str,
        state: WindowState,
    ) -> float:
        """Get total time a window has been in a given state."""
        total = 0.0
        for entry in self._history.get(window_id, []):
            if entry.snapshot.state == state:
                total += entry.duration
        return total

    def get_last_event(self, window_id: str) -> Optional[WindowHistoryEntry]:
        """Get the most recent history entry for a window."""
        history = self._history.get(window_id, [])
        return history[-1] if history else None

    def get_state_at_time(
        self,
        window_id: str,
        timestamp: float,
    ) -> Optional[WindowSnapshot]:
        """Get the window state at a specific timestamp."""
        history = self._history.get(window_id, [])
        for entry in reversed(history):
            if entry.timestamp <= timestamp:
                return entry.snapshot
        return None

    def on_event(
        self,
        event: WindowEvent,
        callback: Callable[[WindowHistoryEntry], None],
    ) -> None:
        """Register a callback for a specific window event."""
        self._event_callbacks.setdefault(event, []).append(callback)

    def on_window(
        self,
        window_id: str,
        callback: Callable[[WindowSnapshot], None],
    ) -> None:
        """Register a callback for any change to a specific window."""
        self._window_callbacks.setdefault(window_id, []).append(callback)

    def _notify_event(self, entry: WindowHistoryEntry) -> None:
        """Notify all relevant callbacks of an event."""
        event = entry.event
        for cb in self._event_callbacks.get(event, []):
            try:
                cb(entry)
            except Exception:
                pass
        for cb in self._window_callbacks.get(entry.snapshot.window_id, []):
            try:
                cb(entry.snapshot)
            except Exception:
                pass

    def get_active_windows(self) -> list[WindowSnapshot]:
        """Return snapshots for all windows that are not closed or hidden."""
        return [
            s for s in self._snapshots.values()
            if s.state not in (WindowState.CLOSED, WindowState.HIDDEN)
            and s.is_visible
        ]

    def get_focused_window(self) -> Optional[WindowSnapshot]:
        """Return the currently focused window, if any."""
        for s in self._snapshots.values():
            if s.is_focused:
                return s
        return None

    @property
    def tracked_windows(self) -> list[str]:
        """Return IDs of all tracked windows."""
        return list(self._snapshots.keys())

    @property
    def all_snapshots(self) -> list[WindowSnapshot]:
        """Return all current snapshots."""
        return list(self._snapshots.values())


class WindowStateDiffer:
    """Compares two window states and produces a diff."""

    @staticmethod
    def diff(
        before: WindowSnapshot,
        after: WindowSnapshot,
    ) -> dict[str, tuple[Any, Any]]:
        """Compare two snapshots and return changed fields.

        Returns dict of field_name -> (old_value, new_value).
        """
        changes: dict[str, tuple[Any, Any]] = {}
        fields = ["title", "state", "bounds", "is_focused", "is_visible"]
        for field in fields:
            old_val = getattr(before, field)
            new_val = getattr(after, field)
            if old_val != new_val:
                changes[field] = (old_val, new_val)
        return changes
