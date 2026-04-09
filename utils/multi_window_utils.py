"""
Multi-window coordination utilities.

Manage and coordinate actions across multiple application windows.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Optional, Callable
from enum import Enum, auto


class WindowRole(Enum):
    """Role of a window in a workflow."""
    PRIMARY = auto()
    SECONDARY = auto()
    MODAL = auto()
    POPUP = auto()
    TOOLBAR = auto()


@dataclass
class WindowInfo:
    """Information about a managed window."""
    window_id: str
    bundle_id: Optional[str] = None
    title: Optional[str] = None
    role: WindowRole = WindowRole.PRIMARY
    bounds: tuple[int, int, int, int] = (0, 0, 0, 0)
    is_visible: bool = True
    z_order: int = 0


@dataclass
class WindowTransition:
    """Transition specification between windows."""
    from_window: str
    to_window: str
    delay_ms: float = 0
    action: Optional[Callable] = None


class WindowManager:
    """Manage multiple windows for coordinated workflows."""
    
    def __init__(self):
        self._windows: dict[str, WindowInfo] = {}
        self._transition_history: list[WindowTransition] = []
    
    def register_window(self, window: WindowInfo) -> None:
        """Register a window for management."""
        self._windows[window.window_id] = window
    
    def unregister_window(self, window_id: str) -> None:
        """Unregister a window."""
        self._windows.pop(window_id, None)
    
    def get_window(self, window_id: str) -> Optional[WindowInfo]:
        """Get window info by ID."""
        return self._windows.get(window_id)
    
    def get_windows_by_role(self, role: WindowRole) -> list[WindowInfo]:
        """Get all windows with a specific role."""
        return [w for w in self._windows.values() if w.role == role]
    
    def get_windows_by_bundle(self, bundle_id: str) -> list[WindowInfo]:
        """Get all windows for a specific app bundle."""
        return [w for w in self._windows.values() if w.bundle_id == bundle_id]
    
    def update_bounds(self, window_id: str, bounds: tuple[int, int, int, int]) -> bool:
        """Update window bounds."""
        if window_id in self._windows:
            self._windows[window_id].bounds = bounds
            return True
        return False
    
    def set_visibility(self, window_id: str, visible: bool) -> bool:
        """Set window visibility."""
        if window_id in self._windows:
            self._windows[window_id].is_visible = visible
            return True
        return False
    
    def record_transition(self, from_id: str, to_id: str, action: Optional[Callable] = None) -> None:
        """Record a window transition."""
        transition = WindowTransition(
            from_window=from_id,
            to_window=to_id,
            delay_ms=0,
            action=action
        )
        self._transition_history.append(transition)
    
    def get_ordered_windows(self) -> list[WindowInfo]:
        """Get windows ordered by z-order."""
        return sorted(self._windows.values(), key=lambda w: w.z_order)


class WindowOrchestrator:
    """Orchestrate actions across multiple windows."""
    
    def __init__(self, window_manager: WindowManager):
        self.window_manager = window_manager
        self._transition_delays: dict[tuple[str, str], float] = {}
    
    def set_transition_delay(self, from_id: str, to_id: str, delay_ms: float) -> None:
        """Set delay between specific window transitions."""
        self._transition_delays[(from_id, to_id)] = delay_ms
    
    def transition_to(self, from_id: str, to_id: str) -> float:
        """Transition from one window to another."""
        to_window = self.window_manager.get_window(to_id)
        if to_window is None:
            return 0
        
        delay = self._transition_delays.get((from_id, to_id), 0)
        if delay > 0:
            time.sleep(delay / 1000)
        
        self.window_manager.record_transition(from_id, to_id)
        return delay
    
    def broadcast_to_role(self, role: WindowRole, action: Callable[[WindowInfo], None]) -> None:
        """Apply action to all windows with a specific role."""
        for window in self.window_manager.get_windows_by_role(role):
            action(window)
    
    def close_secondary_windows(self) -> int:
        """Close all secondary windows."""
        count = 0
        for window in self.window_manager.get_windows_by_role(WindowRole.SECONDARY):
            if window.is_visible:
                count += 1
        return count
