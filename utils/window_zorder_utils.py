"""Window z-order management utilities for UI automation.

Provides utilities for managing window stacking order,
moving windows to front/back, and tracking window hierarchies.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set


@dataclass
class WindowInfo:
    """Information about a window."""
    window_id: str
    title: str
    bundle_id: str
    frame: "Rect"
    is_on_screen: bool = True
    is_minimized: bool = False
    z_order: int = 0
    parent_id: Optional[str] = None
    child_ids: List[str] = field(default_factory=list)
    owner_pid: int = 0


@dataclass
class Rect:
    """Represents a rectangle."""
    x: float
    y: float
    width: float
    height: float
    
    @property
    def left(self) -> float:
        return self.x
    
    @property
    def top(self) -> float:
        return self.y
    
    @property
    def right(self) -> float:
        return self.x + self.width
    
    @property
    def bottom(self) -> float:
        return self.y + self.height
    
    @property
    def center_x(self) -> float:
        return self.x + self.width / 2
    
    @property
    def center_y(self) -> float:
        return self.y + self.height / 2
    
    def contains_point(self, x: float, y: float) -> bool:
        """Check if point is inside rectangle."""
        return self.left <= x <= self.right and self.top <= y <= self.bottom
    
    def intersects(self, other: "Rect") -> bool:
        """Check if this rect intersects another."""
        return not (
            self.right < other.left or
            self.left > other.right or
            self.bottom < other.top or
            self.top > other.bottom
        )


class ZOrderManager:
    """Manages window z-order and stacking.
    
    Provides utilities for raising, lowering, and
    arranging windows in specific z-orders.
    """
    
    def __init__(self) -> None:
        """Initialize the z-order manager."""
        self._windows: Dict[str, WindowInfo] = {}
        self._z_counter = 0
        self._callbacks: List[Callable[[str, str], None]] = []
    
    def register_window(self, window: WindowInfo) -> None:
        """Register a window with the manager.
        
        Args:
            window: Window information.
        """
        self._z_counter += 1
        window.z_order = self._z_counter
        self._windows[window.window_id] = window
        self._update_child_relations()
    
    def unregister_window(self, window_id: str) -> None:
        """Unregister a window.
        
        Args:
            window_id: ID of the window to unregister.
        """
        if window_id in self._windows:
            window = self._windows[window_id]
            if window.parent_id and window.parent_id in self._windows:
                parent = self._windows[window.parent_id]
                if window_id in parent.child_ids:
                    parent.child_ids.remove(window_id)
            
            for child_id in window.child_ids:
                if child_id in self._windows:
                    self._windows[child_id].parent_id = None
            
            del self._windows[window_id]
    
    def raise_window(self, window_id: str) -> bool:
        """Raise a window to the top.
        
        Args:
            window_id: ID of the window to raise.
            
        Returns:
            True if successful.
        """
        if window_id not in self._windows:
            return False
        
        self._z_counter += 1
        self._windows[window_id].z_order = self._z_counter
        self._notify_change("raise", window_id)
        return True
    
    def lower_window(self, window_id: str) -> bool:
        """Lower a window to the bottom.
        
        Args:
            window_id: ID of the window to lower.
            
        Returns:
            True if successful.
        """
        if window_id not in self._windows:
            return False
        
        min_z = min(w.z_order for w in self._windows.values())
        self._windows[window_id].z_order = min_z - 1
        self._notify_change("lower", window_id)
        return True
    
    def move_above(self, window_id: str, target_id: str) -> bool:
        """Move window above another window.
        
        Args:
            window_id: ID of the window to move.
            target_id: ID of the window to place above.
            
        Returns:
            True if successful.
        """
        if window_id not in self._windows or target_id not in self._windows:
            return False
        
        target = self._windows[target_id]
        self._z_counter += 1
        self._windows[window_id].z_order = target.z_order + 0.5
        self._notify_change("move_above", window_id)
        return True
    
    def move_below(self, window_id: str, target_id: str) -> bool:
        """Move window below another window.
        
        Args:
            window_id: ID of the window to move.
            target_id: ID of the window to place below.
            
        Returns:
            True if successful.
        """
        if window_id not in self._windows or target_id not in self._windows:
            return False
        
        target = self._windows[target_id]
        self._z_counter += 1
        self._windows[window_id].z_order = target.z_order - 0.5
        self._notify_change("move_below", window_id)
        return True
    
    def get_windows_sorted(self) -> List[WindowInfo]:
        """Get all windows sorted by z-order (top to bottom).
        
        Returns:
            List of windows sorted by z-order.
        """
        return sorted(
            self._windows.values(),
            key=lambda w: w.z_order,
            reverse=True
        )
    
    def get_window_at_point(
        self,
        x: float,
        y: float,
        only_on_screen: bool = True
    ) -> Optional[WindowInfo]:
        """Get the topmost window at a given point.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            only_on_screen: Only consider windows fully on screen.
            
        Returns:
            Window at point or None.
        """
        sorted_windows = self.get_windows_sorted()
        
        for window in sorted_windows:
            if only_on_screen and not window.is_on_screen:
                continue
            
            if window.is_minimized:
                continue
            
            if window.frame.contains_point(x, y):
                return window
        
        return None
    
    def get_windows_in_rect(
        self,
        rect: Rect,
        partially: bool = True
    ) -> List[WindowInfo]:
        """Get all windows that intersect a rectangle.
        
        Args:
            rect: Rectangle to check.
            partially: If True, include partially intersecting windows.
            
        Returns:
            List of intersecting windows.
        """
        result = []
        
        for window in self._windows.values():
            if window.is_minimized:
                continue
            
            if partially:
                if window.frame.intersects(rect):
                    result.append(window)
            else:
                if rect.contains_rect(window.frame):
                    result.append(window)
        
        return sorted(result, key=lambda w: w.z_order, reverse=True)
    
    def set_parent(
        self,
        child_id: str,
        parent_id: Optional[str]
    ) -> bool:
        """Set parent-child relationship between windows.
        
        Args:
            child_id: ID of the child window.
            parent_id: ID of the parent window (or None to clear).
            
        Returns:
            True if successful.
        """
        if child_id not in self._windows:
            return False
        
        child = self._windows[child_id]
        
        if child.parent_id and child.parent_id in self._windows:
            old_parent = self._windows[child.parent_id]
            if child_id in old_parent.child_ids:
                old_parent.child_ids.remove(child_id)
        
        if parent_id is not None:
            if parent_id not in self._windows:
                return False
            
            parent = self._windows[parent_id]
            child.parent_id = parent_id
            if child_id not in parent.child_ids:
                parent.child_ids.append(child_id)
        else:
            child.parent_id = None
        
        self._notify_change("reparent", child_id)
        return True
    
    def get_children(self, window_id: str) -> List[WindowInfo]:
        """Get child windows of a window.
        
        Args:
            window_id: ID of the parent window.
            
        Returns:
            List of child windows.
        """
        if window_id not in self._windows:
            return []
        
        return [
            self._windows[cid]
            for cid in self._windows[window_id].child_ids
            if cid in self._windows
        ]
    
    def get_siblings(self, window_id: str) -> List[WindowInfo]:
        """Get sibling windows (same parent).
        
        Args:
            window_id: ID of the window.
            
        Returns:
            List of sibling windows.
        """
        if window_id not in self._windows:
            return []
        
        window = self._windows[window_id]
        
        if window.parent_id is None:
            return [
                w for w in self._windows.values()
                if w.parent_id is None and w.window_id != window_id
            ]
        
        return self.get_children(window.parent_id)
    
    def register_change_callback(
        self,
        callback: Callable[[str, str], None]
    ) -> None:
        """Register a callback for z-order changes.
        
        Args:
            callback: Function called on change (event_type, window_id).
        """
        self._callbacks.append(callback)
    
    def _notify_change(self, event_type: str, window_id: str) -> None:
        """Notify callbacks of a change.
        
        Args:
            event_type: Type of change.
            window_id: Affected window ID.
        """
        for callback in self._callbacks:
            callback(event_type, window_id)
    
    def _update_child_relations(self) -> None:
        """Update child relations based on parent IDs."""
        for window in self._windows.values():
            window.child_ids = []
        
        for window in self._windows.values():
            if window.parent_id and window.parent_id in self._windows:
                parent = self._windows[window.parent_id]
                if window.window_id not in parent.child_ids:
                    parent.child_ids.append(window.window_id)
    
    def get_window(self, window_id: str) -> Optional[WindowInfo]:
        """Get window by ID.
        
        Args:
            window_id: Window ID.
            
        Returns:
            Window info or None.
        """
        return self._windows.get(window_id)


class WindowArranger:
    """Arranges windows in specific patterns.
    
    Provides utilities for tiling, cascading, and
    other window arrangement strategies.
    """
    
    def __init__(
        self,
        z_order_manager: Optional[ZOrderManager] = None
    ) -> None:
        """Initialize the window arranger.
        
        Args:
            z_order_manager: Z-order manager to use.
        """
        self.z_order = z_order_manager or ZOrderManager()
    
    def tile_horizontal(
        self,
        window_ids: List[str],
        screen_bounds: Rect
    ) -> bool:
        """Tile windows horizontally.
        
        Args:
            window_ids: IDs of windows to tile.
            screen_bounds: Screen bounds to tile within.
            
        Returns:
            True if successful.
        """
        if not window_ids:
            return False
        
        count = len(window_ids)
        width = screen_bounds.width / count
        height = screen_bounds.height
        
        for i, window_id in enumerate(window_ids):
            window = self.z_order.get_window(window_id)
            if window:
                window.frame.x = screen_bounds.x + i * width
                window.frame.y = screen_bounds.y
                window.frame.width = width
                window.frame.height = height
        
        return True
    
    def tile_vertical(
        self,
        window_ids: List[str],
        screen_bounds: Rect
    ) -> bool:
        """Tile windows vertically.
        
        Args:
            window_ids: IDs of windows to tile.
            screen_bounds: Screen bounds to tile within.
            
        Returns:
            True if successful.
        """
        if not window_ids:
            return False
        
        count = len(window_ids)
        width = screen_bounds.width
        height = screen_bounds.height / count
        
        for i, window_id in enumerate(window_ids):
            window = self.z_order.get_window(window_id)
            if window:
                window.frame.x = screen_bounds.x
                window.frame.y = screen_bounds.y + i * height
                window.frame.width = width
                window.frame.height = height
        
        return True
    
    def cascade(
        self,
        window_ids: List[str],
        screen_bounds: Rect,
        offset_x: float = 30.0,
        offset_y: float = 30.0
    ) -> bool:
        """Cascade windows.
        
        Args:
            window_ids: IDs of windows to cascade.
            screen_bounds: Screen bounds.
            offset_x: Horizontal offset between windows.
            offset_y: Vertical offset between windows.
            
        Returns:
            True if successful.
        """
        if not window_ids:
            return False
        
        base_width = screen_bounds.width * 0.7
        base_height = screen_bounds.height * 0.7
        
        for i, window_id in enumerate(window_ids):
            window = self.z_order.get_window(window_id)
            if window:
                window.frame.x = screen_bounds.x + i * offset_x
                window.frame.y = screen_bounds.y + i * offset_y
                window.frame.width = base_width
                window.frame.height = base_height
                self.z_order.raise_window(window_id)
        
        return True
    
    def maximize(self, window_id: str, screen_bounds: Rect) -> bool:
        """Maximize a window.
        
        Args:
            window_id: ID of the window to maximize.
            screen_bounds: Screen bounds.
            
        Returns:
            True if successful.
        """
        window = self.z_order.get_window(window_id)
        if not window:
            return False
        
        window.frame.x = screen_bounds.x
        window.frame.y = screen_bounds.y
        window.frame.width = screen_bounds.width
        window.frame.height = screen_bounds.height
        self.z_order.raise_window(window_id)
        
        return True
    
    def center(
        self,
        window_id: str,
        screen_bounds: Rect
    ) -> bool:
        """Center a window on screen.
        
        Args:
            window_id: ID of the window to center.
            screen_bounds: Screen bounds.
            
        Returns:
            True if successful.
        """
        window = self.z_order.get_window(window_id)
        if not window:
            return False
        
        window.frame.x = screen_bounds.center_x - window.frame.width / 2
        window.frame.y = screen_bounds.center_y - window.frame.height / 2
        self.z_order.raise_window(window_id)
        
        return True
