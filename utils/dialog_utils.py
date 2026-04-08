"""
Dialog Utilities

Provides utilities for handling dialogs and
modal windows in UI automation.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Any
from enum import Enum, auto


class DialogType(Enum):
    """Types of dialogs."""
    ALERT = auto()
    CONFIRM = auto()
    PROMPT = auto()
    CUSTOM = auto()


class DialogResult(Enum):
    """Possible dialog results."""
    OK = auto()
    CANCEL = auto()
    YES = auto()
    NO = auto()
    CLOSED = auto()


@dataclass
class DialogInfo:
    """Information about a dialog."""
    dialog_type: DialogType
    title: str | None = None
    message: str | None = None
    buttons: list[str] | None = None


class DialogHandler:
    """
    Handles dialog interactions in automation.
    
    Provides methods for detecting, dismissing,
    and interacting with various dialog types.
    """

    def __init__(self) -> None:
        self._handlers: dict[DialogType, Callable[..., DialogResult]] = {}
        self._current_dialog: DialogInfo | None = None

    def register_handler(
        self,
        dialog_type: DialogType,
        handler: Callable[..., DialogResult],
    ) -> None:
        """Register a handler for a specific dialog type."""
        self._handlers[dialog_type] = handler

    def handle(
        self,
        dialog_type: DialogType,
        action: str | None = None,
    ) -> DialogResult:
        """Handle a dialog with the given action."""
        self._current_dialog = DialogInfo(dialog_type=dialog_type)
        if dialog_type in self._handlers:
            handler = self._handlers[dialog_type]
            return handler(action)
        return DialogResult.CLOSED

    def dismiss(self) -> DialogResult:
        """Dismiss the current dialog."""
        if self._current_dialog:
            return self.handle(self._current_dialog.dialog_type, "dismiss")
        return DialogResult.CLOSED

    def accept(self) -> DialogResult:
        """Accept/confirm the current dialog."""
        if self._current_dialog:
            return self.handle(self._current_dialog.dialog_type, "accept")
        return DialogResult.OK

    def get_current_dialog(self) -> DialogInfo | None:
        """Get information about the current dialog."""
        return self._current_dialog


# Default handlers
def default_alert_handler(action: str | None) -> DialogResult:
    """Default alert dialog handler."""
    if action == "accept":
        return DialogResult.OK
    return DialogResult.CLOSED


def default_confirm_handler(action: str | None) -> DialogResult:
    """Default confirm dialog handler."""
    if action == "accept":
        return DialogResult.OK
    elif action == "dismiss":
        return DialogResult.CANCEL
    return DialogResult.CLOSED


def default_prompt_handler(action: str | None, value: str = "") -> DialogResult:
    """Default prompt dialog handler."""
    if action == "accept":
        return DialogResult.OK
    return DialogResult.CANCEL
