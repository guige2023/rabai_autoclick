"""
Focus Manager Action Module.

Manages keyboard focus across elements, tracking focus order,
focus trapping, and focus restoration for robust automation.
"""

from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class FocusState:
    """Represents the focus state of an element."""
    element_id: str
    tag: str
    focusable: bool
    tab_index: int = 0
    focused: bool = False


class FocusManager:
    """Manages element focus for keyboard navigation."""

    def __init__(self):
        """Initialize focus manager."""
        self._focus_stack: deque[str] = deque(maxlen=50)
        self._current_focus: Optional[str] = None
        self._element_states: dict[str, FocusState] = {}

    def register_element(
        self,
        element_id: str,
        tag: str,
        tab_index: int = 0,
        focusable: bool = True,
    ) -> None:
        """
        Register an element for focus management.

        Args:
            element_id: Unique element identifier.
            tag: HTML tag name.
            tab_index: Tab order index.
            focusable: Whether element can receive focus.
        """
        state = FocusState(
            element_id=element_id,
            tag=tag,
            focusable=focusable,
            tab_index=tab_index if tab_index != 0 else (0 if focusable else -1),
        )
        self._element_states[element_id] = state

    def unregister_element(self, element_id: str) -> bool:
        """
        Unregister an element.

        Args:
            element_id: Element to remove.

        Returns:
            True if removed, False if not found.
        """
        if element_id in self._element_states:
            del self._element_states[element_id]
            if self._current_focus == element_id:
                self._current_focus = None
            return True
        return False

    def set_focus(self, element_id: str) -> bool:
        """
        Set focus to an element.

        Args:
            element_id: Element to focus.

        Returns:
            True if focused, False if element not found or not focusable.
        """
        if element_id not in self._element_states:
            return False

        state = self._element_states[element_id]
        if not state.focusable:
            return False

        if self._current_focus:
            old_state = self._element_states.get(self._current_focus)
            if old_state:
                old_state.focused = False

        state.focused = True
        self._current_focus = element_id
        self._focus_stack.append(element_id)

        return True

    def get_focused_element(self) -> Optional[str]:
        """
        Get the currently focused element ID.

        Returns:
            Element ID or None.
        """
        return self._current_focus

    def get_next_tab(self) -> Optional[str]:
        """
        Get the next tabbable element.

        Returns:
            Element ID of next tabbable element.
        """
        focusable = [
            e for e in self._element_states.values()
            if e.focusable and e.tab_index >= 0
        ]
        focusable.sort(key=lambda e: e.tab_index)

        if not focusable:
            return None

        if self._current_focus is None:
            return focusable[0].element_id if focusable else None

        current_state = self._element_states.get(self._current_focus)
        if not current_state:
            return focusable[0].element_id if focusable else None

        current_idx = -1
        for i, e in enumerate(focusable):
            if e.element_id == self._current_focus:
                current_idx = i
                break

        next_idx = (current_idx + 1) % len(focusable)
        return focusable[next_idx].element_id

    def get_previous_tab(self) -> Optional[str]:
        """
        Get the previous tabbable element.

        Returns:
            Element ID of previous tabbable element.
        """
        focusable = [
            e for e in self._element_states.values()
            if e.focusable and e.tab_index >= 0
        ]
        focusable.sort(key=lambda e: e.tab_index)

        if not focusable:
            return None

        if self._current_focus is None:
            return focusable[-1].element_id if focusable else None

        current_idx = -1
        for i, e in enumerate(focusable):
            if e.element_id == self._current_focus:
                current_idx = i
                break

        prev_idx = (current_idx - 1) % len(focusable)
        return focusable[prev_idx].element_id

    def restore_previous_focus(self) -> bool:
        """
        Restore focus to the previous element in the stack.

        Returns:
            True if restored, False if stack is empty.
        """
        if len(self._focus_stack) < 2:
            return False

        self._focus_stack.pop()
        previous = self._focus_stack[-1]

        if previous in self._element_states and self._element_states[previous].focusable:
            return self.set_focus(previous)

        return False

    def clear_focus(self) -> None:
        """Clear current focus without restoring."""
        if self._current_focus:
            state = self._element_states.get(self._current_focus)
            if state:
                state.focused = False
        self._current_focus = None
