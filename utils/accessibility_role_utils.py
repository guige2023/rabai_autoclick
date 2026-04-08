"""
Accessibility role utilities for role-based element analysis.

Provides role classification, role-based filtering,
and accessibility role constant definitions.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# Common accessibility roles
ROLE_BUTTON = "button"
ROLE_CHECKBOX = "checkbox"
ROLE_RADIO_BUTTON = "radiobutton"
ROLE_TEXT_FIELD = "textfield"
ROLE_TEXT_AREA = "textarea"
ROLE_LINK = "link"
ROLE_IMAGE = "image"
ROLE_ICON = "icon"
ROLE_MENU = "menu"
ROLE_MENU_ITEM = "menuitem"
ROLE_MENU_BAR = "menubar"
ROLE_POPUP_BUTTON = "popupbutton"
ROLE_LIST = "list"
ROLE_LIST_ITEM = "listitem"
ROLE_TABLE = "table"
ROLE_TABLE_ROW = "row"
ROLE_TABLE_CELL = "cell"
ROLE_TAB = "tab"
ROLE_TAB_GROUP = "tabgroup"
ROLE_SLIDER = "slider"
ROLE_SWITCH = "switch"
ROLE_PROGRESS_INDICATOR = "progressindicator"
ROLE_SPIN_BUTTON = "spinbutton"
ROLE_COMBO_BOX = "combobox"
ROLE_GROUP = "group"
ROLE_PANEL = "panel"
ROLE_WINDOW = "window"
ROLE_DIALOG = "dialog"
ROLE_ALERT = "alert"
ROLE_TOOLBAR = "toolbar"
ROLE_SHEET = "sheet"
ROLE_UNKNOWN = "unknown"


@dataclass
class RoleInfo:
    """Information about an accessibility role."""
    role: str
    is_interactive: bool = False
    is_focusable: bool = False
    is_text_like: bool = False
    accepts_keyboard_input: bool = False
    has_aria_props: bool = False


_ROLE_INFO_MAP: dict[str, RoleInfo] = {
    ROLE_BUTTON: RoleInfo(ROLE_BUTTON, is_interactive=True, is_focusable=True, has_aria_props=True),
    ROLE_CHECKBOX: RoleInfo(ROLE_CHECKBOX, is_interactive=True, is_focusable=True, is_text_like=True),
    ROLE_RADIO_BUTTON: RoleInfo(ROLE_RADIO_BUTTON, is_interactive=True, is_focusable=True),
    ROLE_TEXT_FIELD: RoleInfo(ROLE_TEXT_FIELD, is_interactive=True, is_focusable=True, is_text_like=True, accepts_keyboard_input=True),
    ROLE_TEXT_AREA: RoleInfo(ROLE_TEXT_AREA, is_interactive=True, is_focusable=True, is_text_like=True, accepts_keyboard_input=True),
    ROLE_LINK: RoleInfo(ROLE_LINK, is_interactive=True, is_focusable=True),
    ROLE_MENU_ITEM: RoleInfo(ROLE_MENU_ITEM, is_interactive=True, is_focusable=True),
    ROLE_POPUP_BUTTON: RoleInfo(ROLE_POPUP_BUTTON, is_interactive=True, is_focusable=True),
    ROLE_SLIDER: RoleInfo(ROLE_SLIDER, is_interactive=True, is_focusable=True),
    ROLE_SWITCH: RoleInfo(ROLE_SWITCH, is_interactive=True, is_focusable=True),
    ROLE_SPIN_BUTTON: RoleInfo(ROLE_SPIN_BUTTON, is_interactive=True, is_focusable=True, accepts_keyboard_input=True),
    ROLE_COMBO_BOX: RoleInfo(ROLE_COMBO_BOX, is_interactive=True, is_focusable=True, accepts_keyboard_input=True),
    ROLE_TAB: RoleInfo(ROLE_TAB, is_interactive=True, is_focusable=True),
    ROLE_DIALOG: RoleInfo(ROLE_DIALOG, has_aria_props=True),
    ROLE_ALERT: RoleInfo(ROLE_ALERT, has_aria_props=True),
}


class AccessibilityRoleHelper:
    """Helper for accessibility role operations."""

    @staticmethod
    def get_role_info(role: str) -> RoleInfo:
        """Get information about a role."""
        return _ROLE_INFO_MAP.get(role, RoleInfo(role=role))

    @staticmethod
    def is_interactive(role: str) -> bool:
        """Check if a role is interactive."""
        info = _ROLE_INFO_MAP.get(role)
        return info.is_interactive if info else False

    @staticmethod
    def is_focusable(role: str) -> bool:
        """Check if a role is focusable."""
        info = _ROLE_INFO_MAP.get(role)
        return info.is_focusable if info else False

    @staticmethod
    def is_text_like(role: str) -> bool:
        """Check if a role contains text content."""
        info = _ROLE_INFO_MAP.get(role)
        return info.is_text_like if info else False

    @staticmethod
    def is_container(role: str) -> bool:
        """Check if a role is a container that can have children."""
        container_roles = {ROLE_MENU, ROLE_LIST, ROLE_TABLE, ROLE_PANEL, ROLE_GROUP, ROLE_TAB_GROUP, ROLE_TOOLBAR, ROLE_MENU_BAR}
        return role in container_roles

    @staticmethod
    def get_keyboard_alternative(role: str) -> Optional[str]:
        """Suggest keyboard alternative for a role."""
        alternatives = {
            ROLE_CHECKBOX: "Space",
            ROLE_RADIO_BUTTON: "Space",
            ROLE_SWITCH: "Space",
            ROLE_BUTTON: "Space or Enter",
            ROLE_LINK: "Enter",
            ROLE_TAB: "Arrow keys",
            ROLE_SLIDER: "Arrow keys",
            ROLE_SPIN_BUTTON: "Arrow keys",
        }
        return alternatives.get(role)

    @staticmethod
    def normalize_role(role: str) -> str:
        """Normalize a role string to standard form."""
        role_lower = role.lower().replace("_", "")
        mappings = {
            "button": ROLE_BUTTON,
            "checkbox": ROLE_CHECKBOX,
            "radiobutton": ROLE_RADIO_BUTTON,
            "textfield": ROLE_TEXT_FIELD,
            "textarea": ROLE_TEXT_AREA,
            "text": ROLE_TEXT_FIELD,
            "link": ROLE_LINK,
            "anchor": ROLE_LINK,
            "image": ROLE_IMAGE,
            "icon": ROLE_ICON,
            "menu": ROLE_MENU,
            "menuitem": ROLE_MENU_ITEM,
            "menubar": ROLE_MENU_BAR,
            "popupbutton": ROLE_POPUP_BUTTON,
            "list": ROLE_LIST,
            "listitem": ROLE_LIST_ITEM,
            "table": ROLE_TABLE,
            "row": ROLE_TABLE_ROW,
            "cell": ROLE_TABLE_CELL,
            "tab": ROLE_TAB,
            "tabgroup": ROLE_TAB_GROUP,
            "slider": ROLE_SLIDER,
            "switch": ROLE_SWITCH,
            "progressindicator": ROLE_PROGRESS_INDICATOR,
            "spinbutton": ROLE_SPIN_BUTTON,
            "combobox": ROLE_COMBO_BOX,
            "group": ROLE_GROUP,
            "panel": ROLE_PANEL,
            "window": ROLE_WINDOW,
            "dialog": ROLE_DIALOG,
            "alert": ROLE_ALERT,
            "toolbar": ROLE_TOOLBAR,
        }
        return mappings.get(role_lower, ROLE_UNKNOWN)


__all__ = [
    "RoleInfo", "AccessibilityRoleHelper",
    "ROLE_BUTTON", "ROLE_CHECKBOX", "ROLE_RADIO_BUTTON", "ROLE_TEXT_FIELD",
    "ROLE_LINK", "ROLE_MENU", "ROLE_LIST", "ROLE_TABLE", "ROLE_DIALOG",
]
