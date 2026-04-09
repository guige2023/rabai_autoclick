"""
Accessibility Action Module

Provides accessibility-based UI automation with screen reader
support, focus management, and assistive technology integration.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class AccessibilityRole(Enum):
    """Accessibility role types."""

    BUTTON = "button"
    CHECKBOX = "checkbox"
    RADIO_BUTTON = "radio_button"
    TEXT_FIELD = "text_field"
    PASSWORD_FIELD = "password_field"
    LINK = "link"
    MENU_ITEM = "menu_item"
    MENU = "menu"
    WINDOW = "window"
    DIALOG = "dialog"
    ALERT = "alert"
    TABLE = "table"
    TABLE_ROW = "table_row"
    TABLE_CELL = "table_cell"
    TREE = "tree"
    TREE_ITEM = "tree_item"
    SLIDER = "slider"
    PROGRESS_BAR = "progress_bar"
    TAB = "tab"
    TAB_LIST = "tab_list"
    UNKNOWN = "unknown"


class AccessibilityState(Enum):
    """Accessibility state flags."""

    FOCUSABLE = "focusable"
    FOCUSED = "focused"
    SELECTED = "selected"
    EXPANDED = "expanded"
    COLLAPSED = "collapsed"
    CHECKED = "checked"
    INDETERMINATE = "indeterminate"
    DISABLED = "disabled"
    READ_ONLY = "read_only"
    BUSY = "busy"
    HIDDEN = "hidden"


@dataclass
class AccessibleElement:
    """Represents an accessibility element."""

    role: AccessibilityRole
    name: str
    description: str = ""
    value: str = ""
    states: Set[AccessibilityState] = field(default_factory=set)
    bounds: Optional[Tuple[int, int, int, int]] = None
    parent: Optional["AccessibleElement"] = None
    children: List["AccessibleElement"] = field(default_factory=list)
    actions: List[str] = field(default_factory=list)
    keyboard_shortcut: Optional[str] = None


@dataclass
class AccessibilityConfig:
    """Configuration for accessibility automation."""

    default_timeout: float = 10.0
    polling_interval: float = 0.1
    include_hidden: bool = False
    traverse_all: bool = True
    cache_tree: bool = True


class AccessibilityAutomation:
    """
    Automates UI via accessibility APIs.

    Supports screen reader interaction, focus management,
    and navigation through accessibility tree.
    """

    def __init__(
        self,
        config: Optional[AccessibilityConfig] = None,
        platform_accessibility: Optional[Any] = None,
    ):
        self.config = config or AccessibilityConfig()
        self.platform_accessibility = platform_accessibility
        self._accessibility_tree: Optional[AccessibleElement] = None
        self._focused_element: Optional[AccessibleElement] = None

    def refresh_tree(self) -> Optional[AccessibleElement]:
        """Refresh the accessibility tree."""
        if self.platform_accessibility:
            self._accessibility_tree = self._build_tree(self.platform_accessibility)
        return self._accessibility_tree

    def _build_tree(self, root: Any) -> AccessibleElement:
        """Build accessibility tree from platform data."""
        role = AccessibilityRole(root.get("role", "unknown"))
        element = AccessibleElement(
            role=role,
            name=root.get("name", ""),
            description=root.get("description", ""),
            value=root.get("value", ""),
            bounds=root.get("bounds"),
            actions=root.get("actions", []),
            keyboard_shortcut=root.get("keyboard_shortcut"),
        )

        for child_data in root.get("children", []):
            child = self._build_tree(child_data)
            child.parent = element
            element.children.append(child)

        return element

    def find_element_by_name(
        self,
        name: str,
        role: Optional[AccessibilityRole] = None,
    ) -> Optional[AccessibleElement]:
        """Find element by name."""
        if not self._accessibility_tree:
            self.refresh_tree()

        return self._find_in_tree(
            self._accessibility_tree,
            lambda e: e.name == name and (role is None or e.role == role)
        )

    def find_element_by_role(
        self,
        role: AccessibilityRole,
        name: Optional[str] = None,
    ) -> List[AccessibleElement]:
        """Find all elements with a given role."""
        if not self._accessibility_tree:
            self.refresh_tree()

        results: List[AccessibleElement] = []
        self._collect_by_role(self._accessibility_tree, role, results)

        if name:
            results = [e for e in results if e.name == name]

        return results

    def _find_in_tree(
        self,
        element: Optional[AccessibleElement],
        predicate: Callable[[AccessibleElement], bool],
    ) -> Optional[AccessibleElement]:
        """Search tree recursively for element matching predicate."""
        if element is None:
            return None

        if predicate(element):
            return element

        for child in element.children:
            found = self._find_in_tree(child, predicate)
            if found:
                return found

        return None

    def _collect_by_role(
        self,
        element: Optional[AccessibleElement],
        role: AccessibilityRole,
        results: List[AccessibleElement],
    ) -> None:
        """Collect all elements with given role."""
        if element is None:
            return

        if element.role == role:
            results.append(element)

        for child in element.children:
            self._collect_by_role(child, role, results)

    def set_focus(self, element: AccessibleElement) -> bool:
        """Set focus to an element."""
        try:
            if self.platform_accessibility:
                return self.platform_accessibility.set_focus(element)
            self._focused_element = element
            return True
        except Exception as e:
            logger.error(f"Set focus failed: {e}")
            return False

    def perform_action(
        self,
        element: AccessibleElement,
        action: str,
    ) -> bool:
        """Perform an accessibility action on an element."""
        if action not in element.actions:
            logger.warning(f"Action '{action}' not available on element")
            return False

        try:
            if self.platform_accessibility:
                return self.platform_accessibility.do_action(element, action)
            return False
        except Exception as e:
            logger.error(f"Action failed: {e}")
            return False

    def click(self, element: AccessibleElement) -> bool:
        """Click an accessible element."""
        return self.perform_action(element, "click")

    def expand(self, element: AccessibleElement) -> bool:
        """Expand an element."""
        return self.perform_action(element, "expand")

    def collapse(self, element: AccessibleElement) -> bool:
        """Collapse an element."""
        return self.perform_action(element, "collapse")

    def select(self, element: AccessibleElement) -> bool:
        """Select an element."""
        return self.perform_action(element, "select")

    def get_focused_element(self) -> Optional[AccessibleElement]:
        """Get currently focused element."""
        return self._focused_element

    def get_table_info(
        self,
        table: AccessibleElement,
    ) -> Dict[str, Any]:
        """Get information about a table element."""
        if table.role != AccessibilityRole.TABLE:
            return {}

        rows = []
        for row_elem in table.children:
            if row_elem.role == AccessibilityRole.TABLE_ROW:
                cells = [cell.name for cell in row_elem.children if cell.role == AccessibilityRole.TABLE_CELL]
                rows.append(cells)

        return {
            "row_count": len(rows),
            "col_count": len(rows[0]) if rows else 0,
            "rows": rows,
        }


def create_accessibility_automation(
    config: Optional[AccessibilityConfig] = None,
) -> AccessibilityAutomation:
    """Factory function to create AccessibilityAutomation."""
    return AccessibilityAutomation(config=config)
