"""
UI focus ring utilities.

Track and manage focus rings for accessibility.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class FocusRing:
    """Represents a focus ring around an element."""
    element_id: str
    bounds: tuple[int, int, int, int]
    color: tuple[int, int, int] = (0, 120, 215)
    width: int = 2
    style: str = "solid"
    animation_duration_ms: int = 150


@dataclass
class FocusState:
    """Current focus state."""
    focused_element_id: Optional[str] = None
    focus_ring: Optional[FocusRing] = None
    tab_index: int = 0
    is_modal: bool = False


class FocusRingManager:
    """Manage focus rings for accessibility."""
    
    def __init__(self):
        self._focus_state = FocusState()
        self._focus_history: list[str] = []
        self._max_history = 50
        self._on_focus_change_callbacks = []
    
    def set_focus(
        self,
        element_id: str,
        bounds: tuple[int, int, int, int],
        color: Optional[tuple[int, int, int]] = None
    ) -> FocusRing:
        """Set focus to an element."""
        self._focus_history.append(element_id)
        if len(self._focus_history) > self._max_history:
            self._focus_history.pop(0)
        
        ring = FocusRing(
            element_id=element_id,
            bounds=bounds,
            color=color or (0, 120, 215)
        )
        
        self._focus_state.focused_element_id = element_id
        self._focus_state.focus_ring = ring
        
        for callback in self._on_focus_change_callbacks:
            callback(element_id, ring)
        
        return ring
    
    def clear_focus(self) -> None:
        """Clear current focus."""
        self._focus_state.focused_element_id = None
        self._focus_state.focus_ring = None
        
        for callback in self._on_focus_change_callbacks:
            callback(None, None)
    
    def get_focused_element(self) -> Optional[str]:
        """Get currently focused element ID."""
        return self._focus_state.focused_element_id
    
    def get_focus_ring(self) -> Optional[FocusRing]:
        """Get focus ring for current element."""
        return self._focus_state.focus_ring
    
    def restore_previous_focus(self) -> bool:
        """Restore focus to previous element in history."""
        if len(self._focus_history) < 2:
            return False
        
        self._focus_history.pop()
        previous = self._focus_history[-1]
        
        self._focus_state.focused_element_id = previous
        return True
    
    def on_focus_change(self, callback) -> None:
        """Register callback for focus changes."""
        self._on_focus_change_callbacks.append(callback)


class FocusNavigationManager:
    """Manage keyboard focus navigation."""
    
    def __init__(self, focus_manager: FocusRingManager):
        self.focus_manager = focus_manager
        self._tab_order: list[str] = []
        self._current_index = 0
    
    def set_tab_order(self, element_ids: list[str]) -> None:
        """Set the tab order for navigation."""
        self._tab_order = list(element_ids)
        self._current_index = 0
    
    def move_next(self) -> Optional[str]:
        """Move focus to next element in tab order."""
        if not self._tab_order:
            return None
        
        self._current_index = (self._current_index + 1) % len(self._tab_order)
        return self._tab_order[self._current_index]
    
    def move_previous(self) -> Optional[str]:
        """Move focus to previous element in tab order."""
        if not self._tab_order:
            return None
        
        self._current_index = (self._current_index - 1) % len(self._tab_order)
        return self._tab_order[self._current_index]
    
    def move_to(self, index: int) -> Optional[str]:
        """Move focus to specific index."""
        if 0 <= index < len(self._tab_order):
            self._current_index = index
            return self._tab_order[self._current_index]
        return None
