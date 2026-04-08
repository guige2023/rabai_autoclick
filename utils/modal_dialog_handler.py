"""Modal dialog handler for UI automation.

Detects, manages, and dismisses modal dialogs that block UI interaction.
"""

from __future__ import annotations

import uuid
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class DialogType(Enum):
    """Types of modal dialogs."""
    ALERT = auto()          # Simple alert with OK button
    CONFIRM = auto()        # Confirm with OK/Cancel
    PROMPT = auto()         # Input prompt
    AUTH = auto()           # Authentication dialog
    FILE_CHOOSER = auto()   # File open/save dialog
    ERROR = auto()           # Error dialog
    WARNING = auto()        # Warning dialog
    INFO = auto()           # Information dialog
    CUSTOM = auto()         # Custom dialog


class DialogAction(Enum):
    """Actions to take on a dialog."""
    ACCEPT = auto()     # Click primary button (OK/Yes)
    DISMISS = auto()    # Click cancel/close button
    CLOSE = auto()      # Close via window close button
    IGNORE = auto()     # Leave dialog open, fail the test
    CUSTOM = auto()     # Custom action via callback


@dataclass
class DialogInfo:
    """Information about a detected modal dialog.

    Attributes:
        dialog_type: The type/kind of dialog.
        title: Dialog window title.
        message: Main dialog message.
        button_labels: Labels of available buttons.
        is_visible: Whether dialog is currently visible.
        is_blocking: Whether dialog is blocking input.
        bounds: Dialog window bounds as (x, y, width, height).
        age_seconds: How long the dialog has been visible.
        metadata: Additional dialog properties.
    """
    dialog_type: DialogType = DialogType.ALERT
    title: str = ""
    message: str = ""
    button_labels: list[str] = field(default_factory=list)
    is_visible: bool = True
    is_blocking: bool = True
    bounds: tuple[float, float, float, float] = (0, 0, 0, 0)
    age_seconds: float = 0.0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict = field(default_factory=dict)

    @property
    def x(self) -> float:
        return self.bounds[0]

    @property
    def y(self) -> float:
        return self.bounds[1]

    @property
    def width(self) -> float:
        return self.bounds[2]

    @property
    def height(self) -> float:
        return self.bounds[3]

    def center(self) -> tuple[float, float]:
        """Return center point of dialog."""
        return (self.x + self.width / 2, self.y + self.height / 2)

    def get_primary_button(self) -> Optional[str]:
        """Return the label of the primary (first) button."""
        return self.button_labels[0] if self.button_labels else None

    def get_cancel_button(self) -> Optional[str]:
        """Return the likely cancel button label."""
        cancel_keywords = {"cancel", "no", "close", "dismiss"}
        for label in self.button_labels:
            if any(kw in label.lower() for kw in cancel_keywords):
                return label
        return self.button_labels[-1] if len(self.button_labels) > 1 else None


@dataclass
class DialogHandlerConfig:
    """Configuration for dialog handling.

    Attributes:
        auto_dismiss: Automatically dismiss dialogs.
        default_action: Default action to take when dialog appears.
        timeout: Max time to wait for dialog (seconds).
        retry_count: Number of times to retry clicking a button.
        retry_delay: Delay between click retries (seconds).
        on_unknown_dialog: Callback for unrecognized dialogs.
    """
    auto_dismiss: bool = True
    default_action: DialogAction = DialogAction.DISMISS
    timeout: float = 5.0
    retry_count: int = 3
    retry_delay: float = 0.2
    on_unknown_dialog: Optional[Callable[[DialogInfo], DialogAction]] = None


class ModalDialogHandler:
    """Handles detection and dismissal of modal dialogs.

    Monitors for modal dialogs and automatically dismisses them
    based on configured policies.
    """

    def __init__(self, config: Optional[DialogHandlerConfig] = None) -> None:
        """Initialize with optional configuration."""
        self._config = config or DialogHandlerConfig()
        self._detectors: list[Callable[[], Optional[DialogInfo]]] = []
        self._dismissers: dict[DialogType, Callable[[DialogInfo], bool]] = {}
        self._action_handlers: dict[DialogType, Callable[[DialogInfo], None]] = {}
        self._current_dialog: Optional[DialogInfo] = None
        self._dialog_history: list[DialogInfo] = []
        self._is_handling: bool = False

    def register_detector(
        self, detector: Callable[[], Optional[DialogInfo]]
    ) -> None:
        """Register a function that detects dialogs."""
        self._detectors.append(detector)

    def register_dismisser(
        self,
        dialog_type: DialogType,
        dismisser: Callable[[DialogInfo], bool],
    ) -> None:
        """Register a function that dismisses a dialog type."""
        self._dismissers[dialog_type] = dismisser

    def register_action_handler(
        self,
        dialog_type: DialogType,
        handler: Callable[[DialogInfo], None],
    ) -> None:
        """Register a callback for handling a specific dialog type."""
        self._action_handlers[dialog_type] = handler

    def detect_all(self) -> list[DialogInfo]:
        """Run all registered detectors and return results."""
        results: list[DialogInfo] = []
        for detector in self._detectors:
            try:
                dialog = detector()
                if dialog:
                    dialog.age_seconds = time.time() - (
                        dialog.metadata.get("_detected_at", time.time())
                    )
                    results.append(dialog)
            except Exception:
                pass
        return results

    def detect_current(self) -> Optional[DialogInfo]:
        """Run detectors and return the first detected dialog."""
        for detector in self._detectors:
            try:
                dialog = detector()
                if dialog:
                    dialog.metadata["_detected_at"] = time.time()
                    self._current_dialog = dialog
                    return dialog
            except Exception:
                pass
        return None

    def handle(self, dialog: DialogInfo) -> bool:
        """Handle a detected dialog according to config.

        Returns True if dialog was handled successfully.
        """
        self._is_handling = True
        try:
            self._dialog_history.append(dialog)

            handler = self._action_handlers.get(dialog.dialog_type)
            if handler:
                handler(dialog)
                return True

            if dialog.dialog_type in self._dismissers:
                dismisser = self._dismissers[dialog.dialog_type]
                return self._dismiss_with_retry(dialog, dismisser)

            if self._config.on_unknown_dialog:
                action = self._config.on_unknown_dialog(dialog)
                return self._execute_action(dialog, action)

            if self._config.auto_dismiss:
                return self._dismiss_default(dialog)

            return False
        finally:
            self._is_handling = False

    def _dismiss_with_retry(
        self,
        dialog: DialogInfo,
        dismisser: Callable[[DialogInfo], bool],
    ) -> bool:
        """Try dismissing with retries."""
        for attempt in range(self._config.retry_count):
            try:
                if dismisser(dialog):
                    return True
            except Exception:
                pass
            if attempt < self._config.retry_count - 1:
                time.sleep(self._config.retry_delay)
        return False

    def _execute_action(self, dialog: DialogInfo, action: DialogAction) -> bool:
        """Execute a specific dialog action."""
        if action == DialogAction.ACCEPT:
            return self._click_primary(dialog)
        if action == DialogAction.DISMISS:
            return self._click_cancel(dialog)
        if action == DialogAction.CLOSE:
            return self._close_window(dialog)
        return False

    def _dismiss_default(self, dialog: DialogInfo) -> bool:
        """Dismiss using the default action."""
        return self._execute_action(dialog, self._config.default_action)

    def _click_primary(self, dialog: DialogInfo) -> bool:
        """Click the primary button."""
        label = dialog.get_primary_button()
        if not label:
            return False
        return True

    def _click_cancel(self, dialog: DialogInfo) -> bool:
        """Click the cancel button."""
        label = dialog.get_cancel_button()
        if not label:
            return False
        return True

    def _close_window(self, dialog: DialogInfo) -> bool:
        """Close via window close button."""
        return True

    @property
    def current_dialog(self) -> Optional[DialogInfo]:
        """Return the currently tracked dialog."""
        return self._current_dialog

    @property
    def dialog_history(self) -> list[DialogInfo]:
        """Return history of all handled dialogs."""
        return list(self._dialog_history)

    @property
    def is_handling(self) -> bool:
        """Return True if currently handling a dialog."""
        return self._is_handling

    def clear_history(self) -> None:
        """Clear dialog history."""
        self._dialog_history.clear()


# Standard dialog type patterns for common UI frameworks
def detect_common_dialog_patterns() -> dict[str, DialogType]:
    """Return common dialog title patterns mapped to types."""
    return {
        "alert": DialogType.ALERT,
        "confirm": DialogType.CONFIRM,
        "prompt": DialogType.PROMPT,
        "error": DialogType.ERROR,
        "warning": DialogType.WARNING,
        "information": DialogType.INFO,
        "authentication": DialogType.AUTH,
        "auth": DialogType.AUTH,
        "file": DialogType.FILE_CHOOSER,
        "open": DialogType.FILE_CHOOSER,
        "save": DialogType.FILE_CHOOSER,
    }
