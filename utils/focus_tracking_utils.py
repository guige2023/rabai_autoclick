"""
Focus Tracking Utilities.

Track keyboard focus across UI elements with history,
focus chains, and change notifications.

Usage:
    from utils.focus_tracking_utils import FocusTracker, get_focus_chain

    tracker = FocusTracker()
    tracker.start()
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List, Callable, Deque, TYPE_CHECKING
from dataclasses import dataclass, field
from collections import deque
import time

if TYPE_CHECKING:
    pass


@dataclass
class FocusEntry:
    """A single focus entry in the history."""
    element: Optional[Dict[str, Any]]
    timestamp: float = field(default_factory=time.time)
    role: Optional[str] = None
    title: Optional[str] = None

    def __repr__(self) -> str:
        return f"FocusEntry({self.role!r}, {self.title!r})"


class FocusTracker:
    """
    Track keyboard focus changes with history.

    Example:
        tracker = FocusTracker()
        tracker.on_focus_change(lambda e: print(f"Focus: {e.title}"))
        tracker.start()
    """

    def __init__(
        self,
        max_history: int = 100,
        poll_interval: float = 0.1,
    ) -> None:
        self._max_history = max_history
        self._poll_interval = poll_interval
        self._history: Deque[FocusEntry] = deque(maxlen=max_history)
        self._callbacks: List[Callable[[FocusEntry], None]] = []
        self._running = False
        self._last_focus: Optional[FocusEntry] = None

    def start(self) -> None:
        self._running = True

    def stop(self) -> None:
        self._running = False

    def on_focus_change(
        self,
        callback: Callable[[FocusEntry], None],
    ) -> None:
        if callback not in self._callbacks:
            self._callbacks.append(callback)

    def record(
        self,
        element: Optional[Dict[str, Any]],
    ) -> Optional[FocusEntry]:
        """Record a focus event."""
        entry = FocusEntry(
            element=element,
            role=element.get("role") if element else None,
            title=element.get("title") if element else None,
        )
        self._history.append(entry)
        self._last_focus = entry

        if self._last_focus and entry.role != self._last_focus.role:
            for cb in self._callbacks:
                try:
                    cb(entry)
                except Exception:
                    pass

        return entry

    def get_current(self) -> Optional[FocusEntry]:
        """Get the most recent focus entry."""
        return self._last_focus

    def get_history(
        self,
        since: Optional[float] = None,
    ) -> List[FocusEntry]:
        """Get focus history."""
        if since is None:
            return list(self._history)
        return [e for e in self._history if e.timestamp >= since]


def get_focus_chain(
    history: List[FocusEntry],
    max_depth: int = 5,
) -> List[str]:
    """
    Extract a focus chain (breadcrumb) from focus history.

    Args:
        history: List of FocusEntry objects.
        max_depth: Maximum number of entries to return.

    Returns:
        List of role:title strings.
    """
    chain = []
    for entry in history[-max_depth:]:
        label = f"{entry.role or '?'}"
        if entry.title:
            label += f":{entry.title}"
        chain.append(label)
    return chain
