"""
UI focus ring and accessibility focus tracking utilities.

Tracks focus state for UI elements, manages focus rings,
and provides keyboard navigation support.

Author: Auto-generated
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class FocusEventType(Enum):
    """Types of focus events."""
    GAIN = auto()
    LOSE = auto()
    CHANGE = auto()
    ACTIVATE = auto()


@dataclass
class FocusEvent:
    """A focus-related event."""
    event_type: FocusEventType
    element_id: str
    timestamp: float
    related_element_id: str | None = None
    metadata: dict = field(default_factory=dict)


@dataclass
class FocusableElement:
    """An element that can receive focus."""
    element_id: str
    bounds: tuple[float, float, float, float]  # x, y, width, height
    focusable: bool = True
    tab_index: int = 0
    role: str = ""
    label: str = ""
    enabled: bool = True
    visible: bool = True
    
    @property
    def tab_order(self) -> int:
        """Tab order priority (lower = earlier)."""
        if self.tab_index > 0:
            return self.tab_index
        return 0


@dataclass
class FocusRingStyle:
    """Style configuration for focus ring."""
    color: str = "#007AFF"
    width: float = 2.0
    offset: float = 2.0
    style: str = "solid"  # solid, dashed, dotted
    corner_radius: float = 4.0
    animation: str = "none"  # none, pulse, glow


class FocusRingManager:
    """
    Manages focus rings and focus state for UI elements.
    
    Example:
        manager = FocusRingManager()
        manager.register_element(element)
        manager.set_focus("button_submit")
        ring_bounds = manager.get_ring_bounds("button_submit")
    """
    
    def __init__(self):
        self._elements: dict[str, FocusableElement] = {}
        self._current_focus: str | None = None
        self._focus_history: deque[str] = deque(maxlen=100)
        self._focus_listeners: list[Callable[[FocusEvent], None]] = []
        self._ring_style = FocusRingStyle()
        self._ring_visible = True
    
    def register_element(self, element: FocusableElement) -> None:
        """Register a focusable element."""
        self._elements[element.element_id] = element
    
    def unregister_element(self, element_id: str) -> None:
        """Unregister an element."""
        if element_id in self._elements:
            del self._elements[element_id]
        if self._current_focus == element_id:
            self._current_focus = None
    
    def set_focus(self, element_id: str, force: bool = False) -> bool:
        """
        Set focus to an element.
        
        Args:
            element_id: ID of element to focus
            force: If True, focus even if element doesn't allow focus
            
        Returns:
            True if focus was set successfully
        """
        if element_id not in self._elements:
            return False
        
        element = self._elements[element_id]
        
        if not force:
            if not element.focusable or not element.enabled or not element.visible:
                return False
        
        old_focus = self._current_focus
        self._current_focus = element_id
        self._focus_history.append(element_id)
        
        if old_focus != element_id:
            self._emit_event(FocusEvent(
                event_type=FocusEventType.CHANGE,
                element_id=element_id,
                timestamp=time.time(),
                related_element_id=old_focus,
            ))
        
        return True
    
    def get_focused_element(self) -> FocusableElement | None:
        """Get the currently focused element."""
        if self._current_focus is None:
            return None
        return self._elements.get(self._current_focus)
    
    def clear_focus(self) -> None:
        """Clear current focus."""
        old_focus = self._current_focus
        self._current_focus = None
        
        if old_focus is not None:
            self._emit_event(FocusEvent(
                event_type=FocusEventType.LOSE,
                element_id=old_focus,
                timestamp=time.time(),
            ))
    
    def get_ring_bounds(
        self, element_id: str, padding: float = 4.0
    ) -> tuple[float, float, float, float] | None:
        """
        Get bounds for drawing focus ring around element.
        
        Returns:
            Tuple of (x, y, width, height) or None if element not found
        """
        if element_id not in self._elements:
            return None
        
        element = self._elements[element_id]
        x, y, w, h = element.bounds
        offset = self._ring_style.offset
        
        return (x - offset, y - offset, w + offset * 2, h + offset * 2)
    
    def focus_next(self) -> bool:
        """Move focus to the next tabbable element."""
        if not self._elements:
            return False
        
        focusable = self._get_ordered_focusable()
        if not focusable:
            return False
        
        if self._current_focus is None:
            return self.set_focus(focusable[0].element_id)
        
        current_idx = next(
            (i for i, e in enumerate(focusable) if e.element_id == self._current_focus),
            -1,
        )
        next_idx = (current_idx + 1) % len(focusable)
        
        return self.set_focus(focusable[next_idx].element_id)
    
    def focus_previous(self) -> bool:
        """Move focus to the previous tabbable element."""
        if not self._elements:
            return False
        
        focusable = self._get_ordered_focusable()
        if not focusable:
            return False
        
        if self._current_focus is None:
            return self.set_focus(focusable[-1].element_id)
        
        current_idx = next(
            (i for i, e in enumerate(focusable) if e.element_id == self._current_focus),
            0,
        )
        prev_idx = (current_idx - 1) % len(focusable)
        
        return self.set_focus(focusable[prev_idx].element_id)
    
    def _get_ordered_focusable(self) -> list[FocusableElement]:
        """Get focusable elements in tab order."""
        focusable = [
            e for e in self._elements.values()
            if e.focusable and e.enabled and e.visible
        ]
        return sorted(focusable, key=lambda e: e.tab_order)
    
    def add_listener(self, listener: Callable[[FocusEvent], None]) -> None:
        """Add a focus change listener."""
        self._listeners.append(listener)
    
    def _emit_event(self, event: FocusEvent) -> None:
        """Emit a focus event to all listeners."""
        for listener in self._listeners:
            listener(event)
    
    @property
    def ring_style(self) -> FocusRingStyle:
        """Get current ring style."""
        return self._ring_style
    
    def set_ring_style(self, style: FocusRingStyle) -> None:
        """Set ring style."""
        self._ring_style = style
    
    def set_ring_visible(self, visible: bool) -> None:
        """Show or hide the focus ring."""
        self._ring_visible = visible
    
    def is_ring_visible(self) -> bool:
        """Check if ring is visible."""
        return self._ring_visible
    
    def get_focus_history(self) -> list[str]:
        """Get focus history."""
        return list(self._focus_history)
    
    def reset(self) -> None:
        """Reset focus state."""
        self._current_focus = None
        self._focus_history.clear()


def create_focus_event(
    event_type: FocusEventType,
    element_id: str,
    related_element_id: str | None = None,
) -> FocusEvent:
    """Create a focus event with current timestamp."""
    return FocusEvent(
        event_type=event_type,
        element_id=element_id,
        timestamp=time.time(),
        related_element_id=related_element_id,
    )
