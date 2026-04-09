"""
Focus Manager Action Module

Manages window focus, input focus, and focus chains
for complex UI automation workflows.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class FocusScope(Enum):
    """Focus scope types."""

    WINDOW = "window"
    APPLICATION = "application"
    SYSTEM = "system"


@dataclass
class FocusElement:
    """Represents a focusable element."""

    id: str
    name: str
    role: str
    bounds: Optional[tuple] = None
    enabled: bool = True
    visible: bool = True


@dataclass
class FocusManagerConfig:
    """Configuration for focus management."""

    default_timeout: float = 5.0
    track_focus_history: bool = True
    max_history: int = 50
    restore_on_error: bool = True


class FocusManager:
    """
    Manages window and element focus.

    Supports focus setting, tracking, restoration,
    and focus chain navigation.
    """

    def __init__(
        self,
        config: Optional[FocusManagerConfig] = None,
        focus_handler: Optional[Any] = None,
    ):
        self.config = config or FocusManagerConfig()
        self.focus_handler = focus_handler
        self._focus_history: List[FocusElement] = []
        self._current_focus: Optional[FocusElement] = None
        self._saved_focus: Optional[FocusElement] = None

    def set_focus(
        self,
        element: FocusElement,
        timeout: Optional[float] = None,
    ) -> bool:
        """
        Set focus to an element.

        Args:
            element: Element to focus
            timeout: Optional timeout

        Returns:
            True if successful
        """
        timeout = timeout or self.config.default_timeout

        try:
            if self.focus_handler:
                result = self.focus_handler.set_focus(element.id)
            else:
                result = True

            if result:
                self._save_to_history(element)
                self._current_focus = element

            return result

        except Exception as e:
            logger.error(f"Set focus failed: {e}")
            if self.config.restore_on_error:
                self.restore_previous()
            return False

    def get_focused_element(self) -> Optional[FocusElement]:
        """Get currently focused element."""
        return self._current_focus

    def clear_focus(self) -> bool:
        """Clear current focus."""
        self._current_focus = None

        if self.focus_handler:
            return self.focus_handler.clear_focus()

        return True

    def save_current_focus(self) -> bool:
        """Save current focus for later restoration."""
        if self._current_focus:
            self._saved_focus = self._current_focus
            return True
        return False

    def restore_saved_focus(self) -> bool:
        """Restore previously saved focus."""
        if self._saved_focus:
            return self.set_focus(self._saved_focus)
        return False

    def restore_previous(self) -> bool:
        """Restore previous focus from history."""
        if len(self._focus_history) >= 2:
            previous = self._focus_history[-2]
            return self.set_focus(previous)
        return False

    def _save_to_history(self, element: FocusElement) -> None:
        """Save element to focus history."""
        self._focus_history.append(element)

        if len(self._focus_history) > self.config.max_history:
            self._focus_history = self._focus_history[-self.config.max_history:]

    def get_focus_history(
        self,
        limit: Optional[int] = None,
    ) -> List[FocusElement]:
        """Get focus history."""
        if limit:
            return self._focus_history[-limit:]
        return self._focus_history.copy()

    def clear_history(self) -> None:
        """Clear focus history."""
        self._focus_history.clear()

    def is_element_focused(self, element: FocusElement) -> bool:
        """Check if element is currently focused."""
        return (
            self._current_focus is not None
            and self._current_focus.id == element.id
        )

    def wait_for_focus(
        self,
        element: FocusElement,
        timeout: Optional[float] = None,
    ) -> bool:
        """Wait for element to gain focus."""
        timeout = timeout or self.config.default_timeout
        deadline = time.time() + timeout

        while time.time() < deadline:
            if self.is_element_focused(element):
                return True
            time.sleep(0.1)

        return False


def create_focus_manager(
    config: Optional[FocusManagerConfig] = None,
) -> FocusManager:
    """Factory function."""
    return FocusManager(config=config)
