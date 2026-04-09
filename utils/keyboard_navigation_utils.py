"""Keyboard Navigation Utilities.

This module provides keyboard-based navigation utilities for accessible
UI interaction, including focus management, keyboard shortcuts, and
tab order navigation for macOS applications.

Example:
    >>> from keyboard_navigation_utils import FocusManager, KeyboardNavigator
    >>> manager = FocusManager()
    >>> manager.navigate_next()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Dict, List, Optional, Set, Tuple, Any


class FocusDirection(Enum):
    """Focus navigation directions."""
    NEXT = auto()
    PREVIOUS = auto()
    UP = auto()
    DOWN = auto()
    LEFT = auto()
    RIGHT = auto()
    FIRST = auto()
    LAST = auto()


class FocusPolicy(Enum):
    """Focus policy for elements."""
    NONE = auto()
    CLICK = auto()
    Tab = auto()
    STRONG = auto()


@dataclass
class FocusableElement:
    """Represents a focusable UI element.
    
    Attributes:
        element_id: Unique identifier for the element
        label: Accessibility label
        is_focusable: Whether element can receive focus
        is_focused: Whether element currently has focus
        tab_index: Tab order index (-1 if not in tab order)
        rect: Bounding rectangle (x, y, width, height)
    """
    element_id: str
    label: str
    is_focusable: bool = True
    is_focused: bool = False
    tab_index: int = 0
    rect: Tuple[int, int, int, int] = (0, 0, 0, 0)
    custom_actions: Dict[str, Callable] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def x(self) -> int:
        return self.rect[0]
    
    @property
    def y(self) -> int:
        return self.rect[1]
    
    @property
    def width(self) -> int:
        return self.rect[2]
    
    @property
    def height(self) -> int:
        return self.rect[3]
    
    @property
    def center(self) -> Tuple[int, int]:
        return (self.x + self.width // 2, self.y + self.height // 2)


@dataclass
class KeyboardShortcut:
    """Represents a keyboard shortcut binding.
    
    Attributes:
        key: Main key (e.g., 'a', '1', 'f1')
        modifiers: Set of modifier keys (cmd, shift, ctrl, opt)
        action: Callable action to execute
        description: Human-readable description
    """
    key: str
    modifiers: Set[str] = field(default_factory=set)
    action: Optional[Callable[[], None]] = None
    description: str = ""
    is_enabled: bool = True
    scope: Optional[str] = None
    
    @property
    def display_string(self) -> str:
        """Get human-readable shortcut string."""
        parts = []
        if 'ctrl' in self.modifiers:
            parts.append('⌃')
        if 'opt' in self.modifiers or 'alt' in self.modifiers:
            parts.append('⌥')
        if 'shift' in self.modifiers:
            parts.append('⇧')
        if 'cmd' in self.modifiers:
            parts.append('⌘')
        
        key_display = self.key.upper() if len(self.key) == 1 else self.key
        parts.append(key_display)
        
        return ''.join(parts)
    
    def matches(self, key: str, modifiers: Set[str]) -> bool:
        """Check if this shortcut matches a key event."""
        if not self.is_enabled:
            return False
        
        key_lower = key.lower()
        shortcut_key_lower = self.key.lower()
        
        if key_lower != shortcut_key_lower:
            return False
        
        return self.modifiers == modifiers
    
    def execute(self) -> None:
        """Execute the shortcut's action."""
        if self.action:
            self.action()


class FocusManager:
    """Manages focus state and navigation for keyboard interaction.
    
    Provides focus traversal, focus trapping, and focus restoration
    capabilities for accessible keyboard navigation.
    
    Attributes:
        elements: Dictionary of focusable elements
        focused_element: Currently focused element
        focus_cycle: Whether focus cycles through elements
    """
    
    def __init__(self, focus_cycle: bool = True):
        self.elements: Dict[str, FocusableElement] = {}
        self.focused_element: Optional[FocusableElement] = None
        self.focus_cycle: bool = focus_cycle
        self._focus_history: List[str] = []
        self._max_history: int = 10
    
    def register_element(self, element: FocusableElement) -> None:
        """Register a focusable element.
        
        Args:
            element: FocusableElement to register
        """
        self.elements[element.element_id] = element
    
    def unregister_element(self, element_id: str) -> None:
        """Unregister a focusable element.
        
        Args:
            element_id: ID of element to remove
        """
        if element_id in self.elements:
            del self.elements[element_id]
        
        if self.focused_element and self.focused_element.element_id == element_id:
            self.focused_element.is_focused = False
            self.focused_element = None
    
    def get_focusable_elements(self, sorted_by_tab: bool = True) -> List[FocusableElement]:
        """Get all focusable elements.
        
        Args:
            sorted_by_tab: Whether to sort by tab index
            
        Returns:
            List of focusable elements
        """
        focusable = [e for e in self.elements.values() if e.is_focusable]
        
        if sorted_by_tab:
            return sorted(focusable, key=lambda e: e.tab_index)
        
        return focusable
    
    def navigate(self, direction: FocusDirection) -> Optional[FocusableElement]:
        """Navigate focus in a direction.
        
        Args:
            direction: FocusDirection to navigate
            
        Returns:
            New focused element or None
        """
        elements = self.get_focusable_elements(sorted_by_tab=False)
        
        if not elements:
            return None
        
        if not self.focused_element:
            if direction == FocusDirection.NEXT:
                return self._set_focus(elements[0])
            return None
        
        if direction == FocusDirection.FIRST:
            return self._set_focus(elements[0])
        
        if direction == FocusDirection.LAST:
            return self._set_focus(elements[-1])
        
        if direction in (FocusDirection.NEXT, FocusDirection.PREVIOUS):
            return self._navigate_horizontal(elements, direction)
        
        if direction in (FocusDirection.UP, FocusDirection.DOWN, FocusDirection.LEFT, FocusDirection.RIGHT):
            return self._navigate_directional(elements, direction)
        
        return None
    
    def _set_focus(self, element: FocusableElement) -> FocusableElement:
        """Set focus to an element."""
        if self.focused_element:
            self.focused_element.is_focused = False
        
        element.is_focused = True
        self.focused_element = element
        
        self._focus_history.append(element.element_id)
        if len(self._focus_history) > self._max_history:
            self._focus_history.pop(0)
        
        return element
    
    def _navigate_horizontal(
        self,
        elements: List[FocusableElement],
        direction: FocusDirection,
    ) -> Optional[FocusableElement]:
        """Navigate to next/previous element."""
        if not self.focused_element:
            return None
        
        current_idx = -1
        for i, e in enumerate(elements):
            if e.element_id == self.focused_element.element_id:
                current_idx = i
                break
        
        if current_idx == -1:
            return None
        
        if direction == FocusDirection.NEXT:
            next_idx = current_idx + 1
            if next_idx >= len(elements):
                next_idx = 0 if self.focus_cycle else len(elements) - 1
        else:
            next_idx = current_idx - 1
            if next_idx < 0:
                next_idx = len(elements) - 1 if self.focus_cycle else 0
        
        return self._set_focus(elements[next_idx])
    
    def _navigate_directional(
        self,
        elements: List[FocusableElement],
        direction: FocusDirection,
    ) -> Optional[FocusableElement]:
        """Navigate based on element positions."""
        if not self.focused_element:
            return None
        
        current = self.focused_element
        
        candidates = []
        for e in elements:
            if e.element_id == current.element_id:
                continue
            
            if direction == FocusDirection.UP and e.y < current.y:
                candidates.append(e)
            elif direction == FocusDirection.DOWN and e.y > current.y:
                candidates.append(e)
            elif direction == FocusDirection.LEFT and e.x < current.x:
                candidates.append(e)
            elif direction == FocusDirection.RIGHT and e.x > current.x:
                candidates.append(e)
        
        if not candidates:
            return None
        
        if direction in (FocusDirection.UP, FocusDirection.DOWN):
            candidates.sort(key=lambda e: abs(e.x - current.x))
        else:
            candidates.sort(key=lambda e: abs(e.y - current.y))
        
        return self._set_focus(candidates[0])
    
    def navigate_next(self) -> Optional[FocusableElement]:
        """Navigate to next element."""
        return self.navigate(FocusDirection.NEXT)
    
    def navigate_previous(self) -> Optional[FocusableElement]:
        """Navigate to previous element."""
        return self.navigate(FocusDirection.PREVIOUS)
    
    def restore_previous_focus(self) -> bool:
        """Restore focus to previously focused element.
        
        Returns:
            True if restoration successful
        """
        if not self._focus_history:
            return False
        
        previous_id = self._focus_history.pop()
        if previous_id in self.elements:
            self._set_focus(self.elements[previous_id])
            return True
        
        return False


class KeyboardNavigator:
    """Handles keyboard navigation and shortcut execution.
    
    Manages global keyboard shortcuts and focus navigation for
    keyboard-driven UI interaction.
    
    Attributes:
        shortcuts: Dictionary of registered shortcuts
        focus_manager: Associated FocusManager
    """
    
    def __init__(self, focus_manager: Optional[FocusManager] = None):
        self.shortcuts: Dict[str, KeyboardShortcut] = {}
        self.focus_manager = focus_manager or FocusManager()
        self._active_modifiers: Set[str] = set()
        self._pending_key: Optional[str] = None
    
    def register_shortcut(
        self,
        shortcut: KeyboardShortcut,
        shortcut_id: Optional[str] = None,
    ) -> None:
        """Register a keyboard shortcut.
        
        Args:
            shortcut: KeyboardShortcut to register
            shortcut_id: Optional ID (defaults to display_string)
        """
        sid = shortcut_id or shortcut.display_string
        self.shortcuts[sid] = shortcut
    
    def unregister_shortcut(self, shortcut_id: str) -> None:
        """Unregister a keyboard shortcut."""
        if shortcut_id in self.shortcuts:
            del self.shortcuts[shortcut_id]
    
    def handle_key_down(self, key: str, modifiers: Set[str]) -> bool:
        """Handle a key down event.
        
        Args:
            key: Key character
            modifiers: Set of active modifiers
            
        Returns:
            True if shortcut was handled
        """
        self._active_modifiers = modifiers.copy()
        self._pending_key = key
        
        for shortcut in self.shortcuts.values():
            if shortcut.matches(key, modifiers):
                shortcut.execute()
                self._pending_key = None
                return True
        
        return False
    
    def handle_key_up(self, key: str, modifiers: Set[str]) -> None:
        """Handle a key up event."""
        self._pending_key = None
    
    def execute_navigation(self, direction: FocusDirection) -> bool:
        """Execute focus navigation.
        
        Args:
            direction: Direction to navigate
            
        Returns:
            True if navigation occurred
        """
        result = self.focus_manager.navigate(direction)
        return result is not None
    
    def get_shortcut_for_action(self, action: Callable) -> Optional[KeyboardShortcut]:
        """Find shortcut that executes an action.
        
        Args:
            action: Action callable
            
        Returns:
            KeyboardShortcut or None
        """
        for shortcut in self.shortcuts.values():
            if shortcut.action == action:
                return shortcut
        return None
    
    def get_all_shortcuts(self) -> List[KeyboardShortcut]:
        """Get all registered shortcuts."""
        return list(self.shortcuts.values())
    
    def get_shortcuts_sorted(self) -> List[Tuple[str, KeyboardShortcut]]:
        """Get shortcuts sorted by display string."""
        items = [(sid, s) for sid, s in self.shortcuts.items()]
        items.sort(key=lambda x: x[1].display_string)
        return items


class TabOrderManager:
    """Manages tab order for custom UI elements.
    
    Provides programmatic control over focus order independent
    of visual layout.
    """
    
    def __init__(self):
        self._ordered_ids: List[str] = []
        self._element_to_order: Dict[str, int] = {}
    
    def set_order(self, element_ids: List[str]) -> None:
        """Set explicit tab order.
        
        Args:
            element_ids: Ordered list of element IDs
        """
        self._ordered_ids = element_ids.copy()
        self._element_to_order = {
            eid: idx for idx, eid in enumerate(element_ids)
        }
    
    def add_to_order(self, element_id: str, after: Optional[str] = None) -> None:
        """Add element to tab order.
        
        Args:
            element_id: Element to add
            after: Element ID to insert after
        """
        if element_id in self._element_to_order:
            return
        
        if after is None or after not in self._element_to_order:
            self._ordered_ids.append(element_id)
        else:
            after_idx = self._element_to_order[after]
            self._ordered_ids.insert(after_idx + 1, element_id)
        
        self._rebuild_order_map()
    
    def remove_from_order(self, element_id: str) -> None:
        """Remove element from tab order."""
        if element_id in self._element_to_order:
            self._ordered_ids.remove(element_id)
            del self._element_to_order[element_id]
            self._rebuild_order_map()
    
    def _rebuild_order_map(self) -> None:
        """Rebuild the order index map."""
        self._element_to_order = {
            eid: idx for idx, eid in enumerate(self._ordered_ids)
        }
    
    def get_next(self, current_id: str) -> Optional[str]:
        """Get next element in tab order."""
        if current_id not in self._element_to_order:
            return self._ordered_ids[0] if self._ordered_ids else None
        
        current_idx = self._element_to_order[current_id]
        next_idx = (current_idx + 1) % len(self._ordered_ids)
        return self._ordered_ids[next_idx]
    
    def get_previous(self, current_id: str) -> Optional[str]:
        """Get previous element in tab order."""
        if current_id not in self._element_to_order:
            return self._ordered_ids[-1] if self._ordered_ids else None
        
        current_idx = self._element_to_order[current_id]
        prev_idx = (current_idx - 1) % len(self._ordered_ids)
        return self._ordered_ids[prev_idx]
