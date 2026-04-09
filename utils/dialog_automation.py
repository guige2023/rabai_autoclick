"""
Dialog Automation Module.

Provides utilities for automating system and application dialogs,
including modal dialogs, alerts, file dialogs, and custom dialogs.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable


logger = logging.getLogger(__name__)


class DialogType(Enum):
    """Types of dialogs."""
    ALERT = auto()
    CONFIRM = auto()
    PROMPT = auto()
    FILE_OPEN = auto()
    FILE_SAVE = auto()
    DIRECTORY_CHOOSE = auto()
    COLOR_PICK = auto()
    FONT_PICK = auto()
    PRINT = auto()
    CUSTOM = auto()


class DialogResult(Enum):
    """Dialog button/results."""
    OK = auto()
    CANCEL = auto()
    YES = auto()
    NO = auto()
    ABORT = auto()
    RETRY = auto()
    IGNORE = auto()
    CLOSE = auto()


@dataclass
class DialogButton:
    """Represents a button in a dialog."""
    id: str
    label: str
    result: DialogResult
    default: bool = False
    cancel: bool = False
    enabled: bool = True


@dataclass
class DialogElement:
    """Represents an element within a dialog."""
    id: str
    type: str
    label: str | None = None
    value: str = ""
    enabled: bool = True
    visible: bool = True
    children: list[DialogElement] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DialogInfo:
    """Information about a dialog."""
    dialog_type: DialogType
    title: str
    message: str = ""
    buttons: list[DialogButton] = field(default_factory=list)
    elements: list[DialogElement] = field(default_factory=list)
    timeout_ms: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class DialogAction:
    """An action to perform on a dialog."""
    action_type: str
    target_id: str | None = None
    value: str | None = None
    wait_after_ms: float = 0.0


class DialogDetector:
    """
    Detects when dialogs appear.

    Example:
        >>> detector = DialogDetector()
        >>> dialog = await detector.wait_for_dialog(timeout=5.0)
    """

    def __init__(
        self,
        poll_interval_ms: float = 100.0,
        timeout_ms: float = 30000.0
    ) -> None:
        """
        Initialize the dialog detector.

        Args:
            poll_interval_ms: Polling interval.
            timeout_ms: Default timeout.
        """
        self.poll_interval_ms = poll_interval_ms
        self.timeout_ms = timeout_ms
        self._is_monitoring = False

    async def wait_for_dialog(
        self,
        timeout_ms: float | None = None
    ) -> DialogInfo | None:
        """
        Wait for a dialog to appear.

        Args:
            timeout_ms: Timeout in milliseconds.

        Returns:
            DialogInfo if found, None on timeout.
        """
        timeout = timeout_ms or self.timeout_ms
        start_time = time.time()

        while (time.time() - start_time) * 1000 < timeout:
            dialog = self._check_for_dialog()
            if dialog:
                logger.info(f"Dialog detected: {dialog.title}")
                return dialog

            await asyncio.sleep(self.poll_interval_ms / 1000.0)

        logger.debug("Dialog wait timeout")
        return None

    def _check_for_dialog(self) -> DialogInfo | None:
        """Check if a dialog is currently visible."""
        raise NotImplementedError("Subclass must implement _check_for_dialog")


class DialogAutomator:
    """
    Automates dialog interactions.

    Example:
        >>> automator = DialogAutomator()
        >>> await automator.handle_dialog(actions)
    """

    def __init__(self) -> None:
        """Initialize the dialog automator."""
        self._handlers: dict[DialogType, Callable[[DialogInfo], list[DialogAction]]] = {}
        self._default_button_map: dict[DialogType, DialogResult] = {
            DialogType.ALERT: DialogResult.OK,
            DialogType.CONFIRM: DialogResult.OK,
            DialogType.FILE_OPEN: DialogResult.OK,
            DialogType.FILE_SAVE: DialogResult.OK,
        }

    def register_handler(
        self,
        dialog_type: DialogType,
        handler: Callable[[DialogInfo], list[DialogAction]]
    ) -> None:
        """
        Register a handler for a dialog type.

        Args:
            dialog_type: Type of dialog.
            handler: Handler function.
        """
        self._handlers[dialog_type] = handler
        logger.debug(f"Registered handler for {dialog_type}")

    def set_default_button(
        self,
        dialog_type: DialogType,
        result: DialogResult
    ) -> None:
        """
        Set the default button for a dialog type.

        Args:
            dialog_type: Dialog type.
            result: Default result/button.
        """
        self._default_button_map[dialog_type] = result

    async def handle_dialog(
        self,
        dialog: DialogInfo,
        actions: list[DialogAction] | None = None
    ) -> DialogResult:
        """
        Handle a dialog with given or auto-generated actions.

        Args:
            dialog: Dialog information.
            actions: Optional explicit actions.

        Returns:
            DialogResult of the interaction.
        """
        if actions is None:
            actions = self._generate_actions(dialog)

        for action in actions:
            await self._execute_action(action)

            if action.wait_after_ms > 0:
                await asyncio.sleep(action.wait_after_ms / 1000.0)

        result = self._get_button_result(dialog, actions)
        logger.info(f"Dialog handled: {result.name}")
        return result

    def _generate_actions(self, dialog: DialogInfo) -> list[DialogAction]:
        """Generate default actions for a dialog."""
        if dialog.dialog_type in self._handlers:
            return self._handlers[dialog.dialog_type](dialog)

        default_result = self._default_button_map.get(
            dialog.dialog_type,
            DialogResult.OK
        )

        for button in dialog.buttons:
            if button.result == default_result:
                return [DialogAction(
                    action_type="click",
                    target_id=button.id
                )]

        if dialog.buttons:
            return [DialogAction(
                action_type="click",
                target_id=dialog.buttons[0].id
            )]

        return [DialogAction(action_type="close")]

    async def _execute_action(self, action: DialogAction) -> None:
        """Execute a single dialog action."""
        logger.debug(f"Executing action: {action.action_type} on {action.target_id}")

        if action.action_type == "click":
            await self._click_button(action.target_id)
        elif action.action_type == "type":
            await self._type_value(action.target_id, action.value)
        elif action.action_type == "select":
            await self._select_item(action.target_id, action.value)
        elif action.action_type == "close":
            await self._close_dialog()

    async def _click_button(self, button_id: str | None) -> None:
        """Click a button by ID."""
        if not button_id:
            return

        await asyncio.sleep(0.05)

    async def _type_value(
        self,
        target_id: str | None,
        value: str | None
    ) -> None:
        """Type a value into an element."""
        if not target_id or value is None:
            return

        await asyncio.sleep(0.05)

    async def _select_item(
        self,
        target_id: str | None,
        value: str | None
    ) -> None:
        """Select an item."""
        if not target_id or value is None:
            return

        await asyncio.sleep(0.05)

    async def _close_dialog(self) -> None:
        """Close the dialog."""
        await asyncio.sleep(0.05)

    def _get_button_result(
        self,
        dialog: DialogInfo,
        actions: list[DialogAction]
    ) -> DialogResult:
        """Determine the result based on clicked button."""
        if not actions:
            return DialogResult.CLOSE

        for action in actions:
            if action.action_type == "click" and action.target_id:
                for button in dialog.buttons:
                    if button.id == action.target_id:
                        return button.result

        return DialogResult.CLOSE


class AlertAutomator(DialogAutomator):
    """
    Specialized automator for alert dialogs.

    Example:
        >>> automator = AlertAutomator()
        >>> await automator.handle_alert(message, "OK")
    """

    def __init__(self) -> None:
        """Initialize the alert automator."""
        super().__init__()
        self._handlers[DialogType.ALERT] = self._alert_handler

    def _alert_handler(self, dialog: DialogInfo) -> list[DialogAction]:
        """Generate actions for an alert."""
        return [DialogAction(
            action_type="click",
            target_id=dialog.buttons[0].id if dialog.buttons else None
        )]

    async def handle_alert(
        self,
        message: str,
        button_text: str = "OK"
    ) -> None:
        """
        Handle a simple alert.

        Args:
            message: Alert message.
            button_text: Button to click.
        """
        logger.info(f"Handling alert: {message}")
        await asyncio.sleep(0.1)


class FileDialogAutomator(DialogAutomator):
    """
    Specialized automator for file dialogs.

    Example:
        >>> automator = FileDialogAutomator()
        >>> await automator.open_file("/path/to/file.txt")
    """

    def __init__(self) -> None:
        """Initialize the file dialog automator."""
        super().__init__()
        self._handlers[DialogType.FILE_OPEN] = self._file_open_handler
        self._handlers[DialogType.FILE_SAVE] = self._file_save_handler

    def _file_open_handler(self, dialog: DialogInfo) -> list[DialogAction]:
        """Generate actions for file open dialog."""
        actions = [DialogAction(action_type="click", target_id="toolbar")]
        return actions

    def _file_save_handler(self, dialog: DialogInfo) -> list[DialogAction]:
        """Generate actions for file save dialog."""
        actions = [DialogAction(action_type="click", target_id="toolbar")]
        return actions

    async def open_file(
        self,
        file_path: str,
        dialog: DialogInfo | None = None
    ) -> DialogResult:
        """
        Automate opening a file.

        Args:
            file_path: Path to the file.
            dialog: Optional dialog info.

        Returns:
            DialogResult.
        """
        if dialog:
            actions = [
                DialogAction(action_type="type", target_id="path", value=file_path),
                DialogAction(action_type="click", target_id="open_button", wait_after_ms=500)
            ]
            return await self.handle_dialog(dialog, actions)

        logger.info(f"Opening file: {file_path}")
        return DialogResult.OK

    async def save_file(
        self,
        file_path: str,
        dialog: DialogInfo | None = None,
        overwrite: bool = True
    ) -> DialogResult:
        """
        Automate saving a file.

        Args:
            file_path: Path to save to.
            dialog: Optional dialog info.
            overwrite: Whether to overwrite existing file.

        Returns:
            DialogResult.
        """
        if dialog:
            actions = [
                DialogAction(action_type="type", target_id="path", value=file_path),
                DialogAction(
                    action_type="click",
                    target_id="save_button" if overwrite else "cancel_button",
                    wait_after_ms=500
                )
            ]
            return await self.handle_dialog(dialog, actions)

        logger.info(f"Saving file: {file_path}")
        return DialogResult.OK


class DialogWaiter:
    """
    Waits for dialogs with various conditions.

    Example:
        >>> waiter = DialogWaiter(detector)
        >>> dialog = await waiter.wait_for_type(DialogType.ALERT, timeout=10.0)
    """

    def __init__(self, detector: DialogDetector) -> None:
        """
        Initialize the waiter.

        Args:
            detector: DialogDetector to use.
        """
        self.detector = detector

    async def wait_for_type(
        self,
        dialog_type: DialogType,
        timeout_ms: float = 30000.0
    ) -> DialogInfo | None:
        """
        Wait for a specific dialog type.

        Args:
            dialog_type: Type to wait for.
            timeout_ms: Timeout.

        Returns:
            DialogInfo or None.
        """
        while True:
            dialog = await self.detector.wait_for_dialog(timeout_ms=1000)

            if dialog and dialog.dialog_type == dialog_type:
                return dialog

            if not dialog:
                return None

    async def wait_for_message_containing(
        self,
        substring: str,
        timeout_ms: float = 30000.0
    ) -> DialogInfo | None:
        """
        Wait for dialog with message containing substring.

        Args:
            substring: Substring to find.
            timeout_ms: Timeout.

        Returns:
            DialogInfo or None.
        """
        while True:
            dialog = await self.detector.wait_for_dialog(timeout_ms=1000)

            if dialog and substring.lower() in dialog.message.lower():
                return dialog

            if not dialog:
                return None


@dataclass
class DialogSequence:
    """A sequence of dialog interactions."""
    steps: list[tuple[DialogType, list[DialogAction]]] = field(default_factory=list)

    def add_step(
        self,
        dialog_type: DialogType,
        actions: list[DialogAction]
    ) -> DialogSequence:
        """Add a step to the sequence."""
        self.steps.append((dialog_type, actions))
        return self


class DialogSequenceExecutor:
    """
    Executes sequences of dialog interactions.

    Example:
        >>> executor = DialogSequenceExecutor(automator, detector)
        >>> await executor.execute(sequence)
    """

    def __init__(
        self,
        automator: DialogAutomator,
        detector: DialogDetector
    ) -> None:
        """
        Initialize the executor.

        Args:
            automator: DialogAutomator to use.
            detector: DialogDetector to use.
        """
        self.automator = automator
        self.detector = detector

    async def execute(
        self,
        sequence: DialogSequence,
        step_timeout_ms: float = 30000.0
    ) -> list[DialogResult]:
        """
        Execute a dialog sequence.

        Args:
            sequence: DialogSequence to execute.
            step_timeout_ms: Timeout per step.

        Returns:
            List of DialogResults.
        """
        results: list[DialogResult] = []

        for dialog_type, actions in sequence.steps:
            logger.info(f"Waiting for dialog: {dialog_type}")

            dialog = await self.detector.wait_for_dialog(
                timeout_ms=step_timeout_ms
            )

            if not dialog:
                logger.warning(f"Dialog {dialog_type} not found")
                results.append(DialogResult.CANCEL)
                continue

            if dialog.dialog_type != dialog_type:
                logger.warning(
                    f"Expected {dialog_type}, got {dialog.dialog_type}"
                )

            result = await self.automator.handle_dialog(dialog, actions)
            results.append(result)

        return results
