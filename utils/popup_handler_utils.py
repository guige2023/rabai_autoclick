"""
Popup Handler Utilities.

Utilities for detecting, classifying, and handling popup windows,
dialogs, sheets, and other modal UI elements.

Usage:
    from utils.popup_handler_utils import PopupHandler, classify_popup

    handler = PopupHandler(bridge)
    popup = handler.detect_popup()
    if popup:
        handler.auto_handle(popup)
"""

from __future__ import annotations

from typing import Optional, Dict, Any, List, Callable, TYPE_CHECKING
from dataclasses import dataclass
from enum import Enum, auto

if TYPE_CHECKING:
    pass


class PopupType(Enum):
    """Classification of popup types."""
    ALERT = auto()
    CONFIRMATION = auto()
    SHEET = auto()
    DRAWER = auto()
    POPOVER = auto()
    WINDOW = auto()
    MENU = auto()
    TOOLTIP = auto()
    ACTIVITY_INDICATOR = auto()
    UNKNOWN = auto()


class ButtonRole(Enum):
    """Common button roles in dialogs."""
    DEFAULT = auto()
    CANCEL = auto()
    OK = auto()
    YES = auto()
    NO = auto()
    CLOSE = auto()
    SAVE = auto()
    DONT_SAVE = auto()


@dataclass
class PopupInfo:
    """Information about a detected popup."""
    popup_type: PopupType
    title: Optional[str] = None
    message: Optional[str] = None
    buttons: List[Dict[str, Any]] = None
    element: Optional[Dict[str, Any]] = None

    def __post_init__(self) -> None:
        if self.buttons is None:
            self.buttons = []

    @property
    def has_buttons(self) -> bool:
        return len(self.buttons) > 0


class PopupHandler:
    """
    Handle popup windows and dialogs.

    Provides detection, classification, and automated handling
    of various popup types common in macOS applications.

    Example:
        handler = PopupHandler(bridge)
        popup = handler.detect_popup()
        if popup:
            handler.click_button(popup, ButtonRole.CANCEL)
    """

    def __init__(self, bridge: Any) -> None:
        """
        Initialize the popup handler.

        Args:
            bridge: An AccessibilityBridge or similar.
        """
        self._bridge = bridge
        self._handlers: Dict[PopupType, Callable[["PopupInfo"], bool]] = {}

    def register_handler(
        self,
        popup_type: PopupType,
        handler: Callable[["PopupInfo"], bool],
    ) -> None:
        """
        Register a custom handler for a popup type.

        Args:
            popup_type: Type of popup to handle.
            handler: Function that takes PopupInfo and returns True if handled.
        """
        self._handlers[popup_type] = handler

    def detect_popup(
        self,
        app: Optional[Any] = None,
    ) -> Optional[PopupInfo]:
        """
        Detect if a popup is currently visible.

        Args:
            app: Application element (defaults to frontmost).

        Returns:
            PopupInfo if a popup is detected, None otherwise.
        """
        if app is None:
            app = self._bridge.get_frontmost_app()
            if app is None:
                return None

        try:
            tree = self._bridge.build_accessibility_tree(app)
            return self._find_popup(tree)
        except Exception:
            return None

    def _find_popup(
        self,
        node: Dict[str, Any],
    ) -> Optional[PopupInfo]:
        """Recursively search for a popup element."""
        role = node.get("role", "")

        if role in ("alert", "dialog"):
            return self._classify_popup(node)

        if role == "sheet":
            return PopupInfo(
                popup_type=PopupType.SHEET,
                title=node.get("title"),
                message=node.get("value"),
                element=node,
            )

        if role == "drawer":
            return PopupInfo(
                popup_type=PopupType.DRAWER,
                title=node.get("title"),
                element=node,
            )

        for child in node.get("children", []):
            result = self._find_popup(child)
            if result:
                return result

        return None

    def _classify_popup(
        self,
        node: Dict[str, Any],
    ) -> PopupInfo:
        """Classify a popup based on its content."""
        title = node.get("title", "")
        message = node.get("value", "") or node.get("description", "")
        buttons = self._find_buttons(node)

        if any(b in (title + message).lower() for b in ["error", "warning", "alert"]):
            popup_type = PopupType.ALERT
        elif any(b in (title + message).lower() for b in ["confirm", "are you sure", "continue?"]):
            popup_type = PopupType.CONFIRMATION
        elif title.lower().endswith("?"):
            popup_type = PopupType.CONFIRMATION
        elif node.get("role") == "sheet":
            popup_type = PopupType.SHEET
        else:
            popup_type = PopupType.UNKNOWN

        return PopupInfo(
            popup_type=popup_type,
            title=title,
            message=message,
            buttons=buttons,
            element=node,
        )

    def _find_buttons(
        self,
        node: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """Find all buttons in a popup."""
        buttons = []

        role = node.get("role", "")
        if role in ("button", "push_button"):
            buttons.append(node)

        for child in node.get("children", []):
            buttons.extend(self._find_buttons(child))

        return buttons

    def click_button(
        self,
        popup: PopupInfo,
        button_role: ButtonRole,
    ) -> bool:
        """
        Click a button in a popup by role.

        Args:
            popup: PopupInfo of the popup.
            button_role: Role of the button to click.

        Returns:
            True if the button was found and clicked.
        """
        if not popup.buttons:
            return False

        button_labels = {
            ButtonRole.DEFAULT: ["ok", "done", "close"],
            ButtonRole.CANCEL: ["cancel", "abort"],
            ButtonRole.OK: ["ok", "done"],
            ButtonRole.YES: ["yes", "continue", "allow"],
            ButtonRole.NO: ["no", "don't allow"],
            ButtonRole.CLOSE: ["close", "x"],
            ButtonRole.SAVE: ["save"],
            ButtonRole.DONT_SAVE: ["don't save", "discard"],
        }

        targets = button_labels.get(button_role, [])

        for button in popup.buttons:
            title = button.get("title", "").lower()
            if any(t in title for t in targets):
                try:
                    self._bridge.click_element(button)
                    return True
                except Exception:
                    pass

        return False

    def auto_handle(
        self,
        popup: PopupInfo,
        default_action: ButtonRole = ButtonRole.DEFAULT,
    ) -> bool:
        """
        Automatically handle a popup using registered or default handlers.

        Args:
            popup: PopupInfo of the popup.
            default_action: Button role to click by default.

        Returns:
            True if the popup was handled successfully.
        """
        handler = self._handlers.get(popup.popup_type)
        if handler:
            return handler(popup)

        if popup.popup_type == PopupType.ACTIVITY_INDICATOR:
            return False

        return self.click_button(popup, default_action)


def classify_popup(element: Dict[str, Any]) -> PopupType:
    """
    Classify a popup element by its role and content.

    Args:
        element: Element dictionary.

    Returns:
        PopupType classification.
    """
    role = element.get("role", "")
    title = element.get("title", "").lower()
    message = element.get("value", "").lower()

    if role in ("alert", "dialog"):
        if any(w in title + message for w in ["error", "warning", "alert"]):
            return PopupType.ALERT
        if "?" in title:
            return PopupType.CONFIRMATION
        return PopupType.ALERT

    if role == "sheet":
        return PopupType.SHEET
    if role == "drawer":
        return PopupType.DRAWER
    if role == "popover":
        return PopupType.POPOVER

    return PopupType.UNKNOWN
