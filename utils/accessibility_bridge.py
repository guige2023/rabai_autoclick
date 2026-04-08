"""Accessibility bridge utilities for UI automation.

Provides a high-level interface to accessibility APIs
(AX API on macOS, UIA on Windows, AT-SPI on Linux)
for reading and interacting with UI elements.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, Sequence


class AXRole(Enum):
    """Cross-platform accessibility roles."""
    WINDOW = "AXWindow"
    BUTTON = "AXButton"
    TEXT_FIELD = "AXTextField"
    TEXT_AREA = "AXTextArea"
    CHECKBOX = "AXCheckBox"
    RADIO_BUTTON = "AXRadioButton"
    POP_UP_BUTTON = "AXPopUpButton"
    MENU_BAR = "AXMenuBar"
    MENU = "AXMenu"
    MENU_ITEM = "AXMenuItem"
    TABLE = "AXTable"
    ROW = "AXRow"
    CELL = "AXCell"
    GROUP = "AXGroup"
    SHEET = "AXSheet"
    DIALOG = "AXDialog"
    TOOLBAR = "AXToolbar"
    TABLE_HEADER = "AXTableHeader"
    LINK = "AXLink"
    IMAGE = "AXImage"
    STATIC_TEXT = "AXStaticText"
    VALUE_INDICATOR = "AXValueIndicator"
    SLIDER = "AXSlider"
    COLUMN = "AXColumn"
    OUTLINE = "AXOutline"
    OUTLINE_ROW = "AXRow"


@dataclass
class AXAttribute:
    """An accessibility attribute."""
    name: str
    value: Any
    is_readable: bool = True
    is_writable: bool = False


@dataclass
class AXAction:
    """An accessibility action."""
    name: str
    description: str = ""


@dataclass
class AXElement:
    """A cross-platform accessibility element wrapper.

    Attributes:
        element_id: Unique identifier.
        role: The element's accessibility role.
        role_string: Raw role string from the platform API.
        title: Title or label.
        value: Current value.
        description: Accessibility description.
        help: Help text.
        children: Child elements.
        parent: Parent element.
        attributes: Dictionary of accessibility attributes.
        actions: Available actions.
        is_enabled: Whether element is enabled.
        is_focused: Whether element has keyboard focus.
        is_selected: Whether element is selected.
        bounds: Bounding box as (x, y, width, height).
        platform_handle: Platform-specific handle.
    """
    role: AXRole = AXRole.WINDOW
    role_string: str = ""
    title: str = ""
    value: Any = None
    description: str = ""
    help: str = ""
    children: list[AXElement] = field(default_factory=list)
    parent: Optional[AXElement] = None
    element_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    attributes: dict[str, AXAttribute] = field(default_factory=dict)
    actions: list[AXAction] = field(default_factory=list)
    is_enabled: bool = True
    is_focused: bool = False
    is_selected: bool = False
    bounds: tuple[float, float, float, float] = (0, 0, 0, 0)
    platform_handle: Any = None

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

    @property
    def center(self) -> tuple[float, float]:
        return (self.x + self.width / 2, self.y + self.height / 2)

    def get_attribute(self, name: str) -> Optional[Any]:
        """Get an attribute value by name."""
        attr = self.attributes.get(name)
        return attr.value if attr else None

    def has_action(self, action_name: str) -> bool:
        """Check if element supports an action."""
        return any(a.name == action_name for a in self.actions)

    def is_button(self) -> bool:
        """Return True if this is a button."""
        return self.role == AXRole.BUTTON

    def is_text_field(self) -> bool:
        """Return True if this is a text field."""
        return self.role in (AXRole.TEXT_FIELD, AXRole.TEXT_AREA)

    def is_focusable(self) -> bool:
        """Return True if element can receive focus."""
        focusable_roles = {
            AXRole.BUTTON, AXRole.TEXT_FIELD, AXRole.TEXT_AREA,
            AXRole.CHECKBOX, AXRole.RADIO_BUTTON, AXRole.POP_UP_BUTTON,
            AXRole.SLIDER, AXRole.LINK,
        }
        return self.role in focusable_roles and self.is_enabled

    def get_path_from_root(self) -> list[AXElement]:
        """Get path from root element to this element."""
        path: list[AXElement] = []
        current: Optional[AXElement] = self
        while current:
            path.append(current)
            current = current.parent
        return list(reversed(path))

    def find_child_by_role(
        self,
        role: AXRole,
        recursive: bool = False,
    ) -> Optional[AXElement]:
        """Find first child with a given role."""
        for child in self.children:
            if child.role == role:
                return child
            if recursive:
                result = child.find_child_by_role(role, recursive=True)
                if result:
                    return result
        return None

    def find_children_by_role(
        self,
        role: AXRole,
        recursive: bool = False,
    ) -> list[AXElement]:
        """Find all children with a given role."""
        results: list[AXElement] = []
        for child in self.children:
            if child.role == role:
                results.append(child)
            if recursive:
                results.extend(
                    child.find_children_by_role(role, recursive=True)
                )
        return results


class AccessibilityBridge:
    """High-level accessibility API bridge.

    Provides a unified interface to platform accessibility APIs.
    Currently provides the interface; platform implementations
    should be registered via register_platform().
    """

    def __init__(self) -> None:
        """Initialize the accessibility bridge."""
        self._platform_handlers: dict[str, Any] = {}
        self._role_map: dict[str, AXRole] = {}
        self._on_change_callbacks: list[Callable[[str, AXElement], None]] = []

    def register_platform(
        self,
        platform: str,
        handler: Any,
    ) -> None:
        """Register a platform-specific accessibility handler.

        Args:
            platform: Platform name ('macos', 'windows', 'linux').
            handler: Platform handler instance.
        """
        self._platform_handlers[platform] = handler

    def get_platform_handler(self, platform: str) -> Optional[Any]:
        """Get the registered handler for a platform."""
        return self._platform_handlers.get(platform)

    def get_focused_element(self, platform: str) -> Optional[AXElement]:
        """Get the currently focused accessibility element."""
        handler = self._platform_handlers.get(platform)
        if handler and hasattr(handler, "get_focused_element"):
            return handler.get_focused_element()
        return None

    def get_element_at_point(
        self,
        platform: str,
        x: float,
        y: float,
    ) -> Optional[AXElement]:
        """Get the accessibility element at a given screen point."""
        handler = self._platform_handlers.get(platform)
        if handler and hasattr(handler, "get_element_at_point"):
            return handler.get_element_at_point(x, y)
        return None

    def get_window_elements(
        self,
        platform: str,
        window_id: str,
    ) -> Optional[AXElement]:
        """Get the accessibility tree for a window."""
        handler = self._platform_handlers.get(platform)
        if handler and hasattr(handler, "get_window_elements"):
            return handler.get_window_elements(window_id)
        return None

    def perform_action(
        self,
        element: AXElement,
        action_name: str,
    ) -> bool:
        """Perform an accessibility action on an element.

        Returns True if the action was performed successfully.
        """
        if not element.has_action(action_name):
            return False
        if element.platform_handle and hasattr(
            element.platform_handle, "perform_action"
        ):
            return element.platform_handle.perform_action(action_name)
        return False

    def set_value(self, element: AXElement, value: Any) -> bool:
        """Set the value of an accessibility element."""
        if not element.is_writable:
            return False
        if element.platform_handle and hasattr(
            element.platform_handle, "set_value"
        ):
            return element.platform_handle.set_value(value)
        return False

    def focus_element(self, element: AXElement) -> bool:
        """Set keyboard focus to an element."""
        return self.perform_action(element, "AXShowMenu") or \
            self.perform_action(element, "AXRaise")

    def on_change(
        self,
        callback: Callable[[str, AXElement], None],
    ) -> None:
        """Register a callback for accessibility tree changes."""
        self._on_change_callbacks.append(callback)

    @staticmethod
    def map_role_string(role_str: str) -> AXRole:
        """Map a platform role string to a cross-platform AXRole."""
        role_map: dict[str, AXRole] = {
            "AXWindow": AXRole.WINDOW,
            "AXButton": AXRole.BUTTON,
            "AXTextField": AXRole.TEXT_FIELD,
            "AXTextArea": AXRole.TEXT_AREA,
            "AXCheckBox": AXRole.CHECKBOX,
            "AXRadioButton": AXRole.RADIO_BUTTON,
            "AXPopUpButton": AXRole.POP_UP_BUTTON,
            "AXMenuBar": AXRole.MENU_BAR,
            "AXMenu": AXRole.MENU,
            "AXMenuItem": AXRole.MENU_ITEM,
            "AXTable": AXRole.TABLE,
            "AXRow": AXRole.ROW,
            "AXCell": AXRole.CELL,
            "AXGroup": AXRole.GROUP,
            "AXSheet": AXRole.SHEET,
            "AXDialog": AXRole.DIALOG,
            "AXToolbar": AXRole.TOOLBAR,
            "AXTableHeader": AXRole.TABLE_HEADER,
            "AXLink": AXRole.LINK,
            "AXImage": AXRole.IMAGE,
            "AXStaticText": AXRole.STATIC_TEXT,
            "AXValueIndicator": AXRole.VALUE_INDICATOR,
            "AXSlider": AXRole.SLIDER,
            "AXColumn": AXRole.COLUMN,
            "AXOutline": AXRole.OUTLINE,
        }
        return role_map.get(role_str, AXRole.WINDOW)
