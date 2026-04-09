"""
Window State Management Utilities for UI Automation.

This module provides utilities for managing window states, positions,
and properties during automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional, Callable


class WindowState(Enum):
    """Window state enumeration."""
    NORMAL = auto()
    MINIMIZED = auto()
    MAXIMIZED = auto()
    FULLSCREEN = auto()
    HIDDEN = auto()
    CLOSED = auto()


class WindowEdge(Enum):
    """Window edge/corner positions."""
    TOP_LEFT = auto()
    TOP = auto()
    TOP_RIGHT = auto()
    LEFT = auto()
    RIGHT = auto()
    BOTTOM_LEFT = auto()
    BOTTOM = auto()
    BOTTOM_RIGHT = auto()
    CENTER = auto()


@dataclass
class WindowInfo:
    """
    Window information data class.
    
    Attributes:
        window_id: Unique window identifier
        title: Window title
        bounds: Window rectangle (x, y, width, height)
        state: Current window state
        process_id: Process ID that owns the window
        is_active: Whether the window is currently active
    """
    window_id: str
    title: str
    bounds: tuple[int, int, int, int]  # x, y, width, height
    state: WindowState = WindowState.NORMAL
    process_id: Optional[int] = None
    is_active: bool = False
    is_visible: bool = True
    monitor: int = 0


@dataclass
class WindowManager:
    """
    Manages window states and operations.
    
    Example:
        manager = WindowManager()
        windows = manager.list_windows()
        manager.set_state("Chrome", WindowState.MAXIMIZED)
    """
    
    def __init__(self):
        self._cache: dict[str, WindowInfo] = {}
        self._cache_timeout: float = 5.0
        self._last_update: float = 0.0
    
    def list_windows(self, refresh: bool = False) -> list[WindowInfo]:
        """
        List all visible windows.
        
        Args:
            refresh: Force refresh of cached data
            
        Returns:
            List of WindowInfo objects
        """
        now = time.time()
        if not refresh and (now - self._last_update) < self._cache_timeout:
            return list(self._cache.values())
        
        # Placeholder - actual implementation would use platform APIs
        windows = self._get_windows_platform()
        self._cache = {w.window_id: w for w in windows}
        self._last_update = now
        return windows
    
    def _get_windows_platform(self) -> list[WindowInfo]:
        """Platform-specific window enumeration."""
        # Placeholder implementation
        return []
    
    def find_window(self, title_pattern: str, exact: bool = False) -> Optional[WindowInfo]:
        """
        Find a window by title pattern.
        
        Args:
            title_pattern: Window title or pattern to match
            exact: Whether to match exactly
            
        Returns:
            WindowInfo if found, None otherwise
        """
        windows = self.list_windows()
        for window in windows:
            if exact:
                if window.title == title_pattern:
                    return window
            else:
                if title_pattern.lower() in window.title.lower():
                    return window
        return None
    
    def set_state(self, window_id: str, state: WindowState) -> bool:
        """
        Set the state of a window.
        
        Args:
            window_id: Window identifier
            state: Desired window state
            
        Returns:
            True if successful, False otherwise
        """
        # Placeholder - actual implementation would use platform APIs
        return True
    
    def set_bounds(self, window_id: str, bounds: tuple[int, int, int, int]) -> bool:
        """
        Set window bounds (position and size).
        
        Args:
            window_id: Window identifier
            bounds: New bounds (x, y, width, height)
            
        Returns:
            True if successful, False otherwise
        """
        # Placeholder - actual implementation would use platform APIs
        return True
    
    def move_to_edge(self, window_id: str, edge: WindowEdge) -> bool:
        """
        Move window to a screen edge.
        
        Args:
            window_id: Window identifier
            edge: Target edge or corner
            
        Returns:
            True if successful, False otherwise
        """
        # Placeholder implementation
        return True
    
    def center_on_screen(self, window_id: str, monitor: int = 0) -> bool:
        """
        Center window on a specific monitor.
        
        Args:
            window_id: Window identifier
            monitor: Monitor index
            
        Returns:
            True if successful, False otherwise
        """
        # Placeholder implementation
        return True
    
    def activate(self, window_id: str) -> bool:
        """
        Activate (bring to front) a window.
        
        Args:
            window_id: Window identifier
            
        Returns:
            True if successful, False otherwise
        """
        # Placeholder - actual implementation would use platform APIs
        return True
    
    def minimize(self, window_id: str) -> bool:
        """Minimize a window."""
        return self.set_state(window_id, WindowState.MINIMIZED)
    
    def maximize(self, window_id: str) -> bool:
        """Maximize a window."""
        return self.set_state(window_id, WindowState.MAXIMIZED)
    
    def restore(self, window_id: str) -> bool:
        """Restore a window to normal state."""
        return self.set_state(window_id, WindowState.NORMAL)
    
    def close(self, window_id: str) -> bool:
        """
        Close a window.
        
        Args:
            window_id: Window identifier
            
        Returns:
            True if successful, False otherwise
        """
        # Placeholder - actual implementation would use platform APIs
        return True


class WindowWatcher:
    """
    Watches for window state changes.
    
    Example:
        def on_change(info: WindowInfo):
            print(f"Window {info.title} changed state")
        
        watcher = WindowWatcher()
        watcher.watch(on_change)
    """
    
    def __init__(self, poll_interval: float = 1.0):
        self.poll_interval = poll_interval
        self._callbacks: list[Callable[[WindowInfo, WindowInfo], None]] = []
        self._running = False
        self._manager = WindowManager()
        self._previous_state: dict[str, WindowInfo] = {}
    
    def watch(self, callback: Callable[[WindowInfo, WindowInfo], None]) -> None:
        """
        Add a callback for window changes.
        
        Args:
            callback: Function that receives (old_info, new_info)
        """
        self._callbacks.append(callback)
    
    def start(self) -> None:
        """Start watching for changes."""
        self._running = True
        self._previous_state = {w.window_id: w for w in self._manager.list_windows()}
    
    def stop(self) -> None:
        """Stop watching for changes."""
        self._running = False
    
    def check_changes(self) -> list[tuple[WindowInfo, WindowInfo]]:
        """
        Check for window changes since last check.
        
        Returns:
            List of (old_info, new_info) tuples for changed windows
        """
        changes = []
        current_windows = {w.window_id: w for w in self._manager.list_windows()}
        
        # Check for changed windows
        for window_id, new_info in current_windows.items():
            if window_id in self._previous_state:
                old_info = self._previous_state[window_id]
                if old_info != new_info:
                    changes.append((old_info, new_info))
                    for callback in self._callbacks:
                        callback(old_info, new_info)
        
        self._previous_state = current_windows
        return changes
