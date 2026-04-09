"""UI Keyboard Navigation Utilities.

Manages keyboard-based UI navigation and focus traversal.

Example:
    >>> from ui_key_navigation_utils import KeyboardNavigation
    >>> nav = KeyboardNavigation()
    >>> nav.set_focus_order(["btn_ok", "btn_cancel", "input_name"])
    >>> nav.focus_next()
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, List, Optional, Set


class NavigationDirection(Enum):
    """Navigation directions."""
    FORWARD = auto()
    BACKWARD = auto()
    UP = auto()
    DOWN = auto()
    LEFT = auto()
    RIGHT = auto()


@dataclass
class FocusableElement:
    """A focusable UI element."""
    id: str
    role: str
    label: str = ""
    bounds: tuple = (0, 0, 0, 0)
    tab_index: int = 0
    enabled: bool = True


class KeyboardNavigation:
    """Handles keyboard-based UI navigation."""

    def __init__(self):
        """Initialize navigation handler."""
        self._elements: List[FocusableElement] = []
        self._current_index = -1
        self._handlers: dict = {}

    def set_focus_order(self, element_ids: List[str]) -> None:
        """Set the focus traversal order.

        Args:
            element_ids: Ordered list of element IDs.
        """
        self._elements = [
            FocusableElement(id=eid, role="element") for eid in element_ids
        ]
        if self._elements:
            self._current_index = 0

    def add_element(
        self,
        element_id: str,
        role: str = "element",
        tab_index: int = 0,
        label: str = "",
    ) -> None:
        """Add a focusable element.

        Args:
            element_id: Element identifier.
            role: Element role.
            tab_index: Tab order index.
            label: Element label.
        """
        elem = FocusableElement(id=element_id, role=role, label=label, tab_index=tab_index)
        self._elements.append(elem)
        self._elements.sort(key=lambda e: e.tab_index)

    def focus_next(self) -> Optional[str]:
        """Move focus to next element.

        Returns:
            New focused element ID or None.
        """
        if not self._elements:
            return None
        self._current_index = (self._current_index + 1) % len(self._elements)
        return self._elements[self._current_index].id

    def focus_previous(self) -> Optional[str]:
        """Move focus to previous element.

        Returns:
            New focused element ID or None.
        """
        if not self._elements:
            return None
        self._current_index = (self._current_index - 1) % len(self._elements)
        return self._elements[self._current_index].id

    def focus_element(self, element_id: str) -> bool:
        """Set focus to a specific element.

        Args:
            element_id: Element to focus.

        Returns:
            True if element exists and was focused.
        """
        for i, elem in enumerate(self._elements):
            if elem.id == element_id:
                self._current_index = i
                return True
        return False

    def get_current_focus(self) -> Optional[str]:
        """Get currently focused element ID.

        Returns:
            Focused element ID or None.
        """
        if 0 <= self._current_index < len(self._elements):
            return self._elements[self._current_index].id
        return None

    def on_focus_change(self, handler: Callable[[str], None]) -> None:
        """Register focus change handler.

        Args:
            handler: Called when focus changes with element ID.
        """
        self._handlers["focus_change"] = handler
