"""Window visibility monitor for tracking window state changes."""
from typing import Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import time


class VisibilityState(Enum):
    """Window visibility states."""
    VISIBLE = "visible"
    HIDDEN = "hidden"
    MINIMIZED = "minimized"
    MAXIMIZED = "maximized"
    FULLSCREEN = "fullscreen"
    UNKNOWN = "unknown"


@dataclass
class WindowSnapshot:
    """Snapshot of window visibility state."""
    window_id: str
    title: str
    visibility: VisibilityState
    bounds: Optional[tuple] = None
    z_order: int = 0
    timestamp: float = field(default_factory=time.time)


class WindowVisibilityMonitor:
    """Monitors window visibility and state changes.
    
    Tracks which windows are visible, hidden, minimized, etc.,
    and notifies listeners of state changes.
    
    Example:
        monitor = WindowVisibilityMonitor()
        monitor.add_listener(lambda old, new: print(f"State changed"))
        monitor.update_window("win1", "Notepad", VisibilityState.VISIBLE)
    """

    def __init__(self, poll_interval: float = 0.5) -> None:
        self._poll_interval = poll_interval
        self._listeners: List[Callable] = []
        self._state_history: Dict[str, List[WindowSnapshot]] = {}
        self._last_states: Dict[str, WindowSnapshot] = {}

    def add_listener(
        self,
        callback: Callable[[WindowSnapshot, Optional[WindowSnapshot]], None],
    ) -> None:
        """Add a listener for window state changes."""
        self._listeners.append(callback)

    def get_visible_windows(self) -> List[WindowSnapshot]:
        """Get all currently visible windows."""
        return [
            snap for snap in self._last_states.values()
            if snap.visibility == VisibilityState.VISIBLE
        ]

    def get_window_state(self, window_id: str) -> Optional[WindowSnapshot]:
        """Get current state of a specific window."""
        return self._last_states.get(window_id)

    def update_window(
        self,
        window_id: str,
        title: str,
        visibility: VisibilityState,
        bounds: Optional[tuple] = None,
        z_order: int = 0,
    ) -> bool:
        """Update window state and notify if changed."""
        new_snapshot = WindowSnapshot(
            window_id=window_id,
            title=title,
            visibility=visibility,
            bounds=bounds,
            z_order=z_order,
        )
        
        old_snapshot = self._last_states.get(window_id)
        
        if old_snapshot and old_snapshot.visibility != visibility:
            self._notify_change(old_snapshot, new_snapshot)
        
        self._last_states[window_id] = new_snapshot
        
        if window_id not in self._state_history:
            self._state_history[window_id] = []
        self._state_history[window_id].append(new_snapshot)
        
        if len(self._state_history[window_id]) > 1000:
            self._state_history[window_id] = self._state_history[window_id][-500:]
        
        return old_snapshot is None or old_snapshot.visibility != visibility

    def remove_window(self, window_id: str) -> Optional[WindowSnapshot]:
        """Remove a window from monitoring."""
        return self._last_states.pop(window_id, None)

    def get_top_window(self) -> Optional[WindowSnapshot]:
        """Get the topmost visible window."""
        visible = self.get_visible_windows()
        if not visible:
            return None
        return max(visible, key=lambda w: w.z_order)

    def is_window_visible(self, window_id: str) -> bool:
        """Check if a window is currently visible."""
        state = self._last_states.get(window_id)
        return state is not None and state.visibility == VisibilityState.VISIBLE

    def _notify_change(
        self,
        old_state: WindowSnapshot,
        new_state: WindowSnapshot,
    ) -> None:
        """Notify all listeners of a state change."""
        for listener in self._listeners:
            try:
                listener(old_state, new_state)
            except Exception:
                pass

    def clear_history(self) -> None:
        """Clear all state history."""
        self._state_history.clear()
