"""
Accessibility Role Hierarchy.

Utilities for understanding and navigating the macOS accessibility
role hierarchy, including parent-child relationships and role grouping.

Usage:
    from utils.accessibility_role_hierarchy import (
        RoleHierarchy, get_parent_roles, is_interactive
    )

    if is_interactive(element):
        print("Element is interactive")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Set, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass


INTERACTIVE_ROLES: Set[str] = {
    "button", "push_button", "radio_button", "check_box",
    "text_field", "text_area", "combo_box", "pop_up_button",
    "menu_item", "link", "tab", "slider", "incrementor",
    "color_well", "search_field", "toggle",
}

CONTAINER_ROLES: Set[str] = {
    "window", "dialog", "sheet", "alert", "drawer",
    "popover", "menu", "menu_bar", "toolbar", "group",
    "radio_group", "split_group", "tab_group", "scroll_area",
    "ruler", "table", "outline", "browser",
}

TEXT_ROLES: Set[str] = {
    "text_field", "text_area", "static_text",
    "group", "description", "heading", "paragraph",
}

WINDOW_ROLES: Set[str] = {
    "window", "dialog", "sheet", "alert", "drawer",
    "popover", "menu", "toolbar",
}


@dataclass
class RoleInfo:
    """Information about an accessibility role."""
    role: str
    is_interactive: bool = False
    is_container: bool = False
    is_text: bool = False
    is_window: bool = False
    parent_roles: List[str] = None

    def __post_init__(self) -> None:
        if self.parent_roles is None:
            self.parent_roles = []


class RoleHierarchy:
    """
    Navigate and query the accessibility role hierarchy.

    Example:
        hierarchy = RoleHierarchy()
        info = hierarchy.get_role_info("button")
        print(f"Interactive: {info.is_interactive}")
    """

    _instance: Optional["RoleHierarchy"] = None

    def __new__(cls) -> "RoleHierarchy":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._init()
        return cls._instance

    def _init(self) -> None:
        """Initialize role information."""
        self._role_info: Dict[str, RoleInfo] = {}
        self._parent_map: Dict[str, List[str]] = {
            "button": ["push_button"],
            "push_button": ["button"],
            "radio_button": ["radio_group"],
            "check_box": ["group"],
            "text_field": ["text_area"],
            "menu_item": ["menu"],
            "menu": ["menu_bar"],
            "tab": ["tab_group"],
            "scroll_area": ["split_group"],
            "table": ["browser"],
            "outline": ["browser"],
            "row": ["table", "outline"],
            "cell": ["row"],
            "column": ["table"],
        }

        for role in INTERACTIVE_ROLES:
            self._ensure_role(role)
            self._role_info[role].is_interactive = True

        for role in CONTAINER_ROLES:
            self._ensure_role(role)
            self._role_info[role].is_container = True

        for role in TEXT_ROLES:
            self._ensure_role(role)
            self._role_info[role].is_text = True

        for role in WINDOW_ROLES:
            self._ensure_role(role)
            self._role_info[role].is_window = True

    def _ensure_role(self, role: str) -> None:
        """Ensure a role exists in the info dictionary."""
        if role not in self._role_info:
            self._role_info[role] = RoleInfo(role=role)

    def get_role_info(self, role: str) -> RoleInfo:
        """
        Get RoleInfo for a role.

        Args:
            role: Role string.

        Returns:
            RoleInfo object.
        """
        if role not in self._role_info:
            self._ensure_role(role)
        return self._role_info[role]

    def get_parent_roles(self, role: str) -> List[str]:
        """
        Get potential parent roles for a given role.

        Args:
            role: Child role.

        Returns:
            List of parent role names.
        """
        return self._parent_map.get(role, [])

    def is_interactive(self, element: Dict[str, Any]) -> bool:
        """
        Check if an element has an interactive role.

        Args:
            element: Element dictionary.

        Returns:
            True if the element's role is interactive.
        """
        role = element.get("role", "")
        return role in INTERACTIVE_ROLES

    def is_container(self, element: Dict[str, Any]) -> bool:
        """
        Check if an element has a container role.

        Args:
            element: Element dictionary.

        Returns:
            True if the element's role is a container.
        """
        role = element.get("role", "")
        return role in CONTAINER_ROLES

    def get_ancestor_roles(
        self,
        element: Dict[str, Any],
        max_depth: int = 10,
    ) -> List[str]:
        """
        Get the chain of ancestor roles for an element.

        Args:
            element: Element dictionary.
            max_depth: Maximum depth to traverse.

        Returns:
            List of ancestor role strings, starting from direct parent.
        """
        roles = []
        current = element
        depth = 0

        while depth < max_depth:
            parent = current.get("parent") or current.get("_parent")
            if parent is None:
                break
            roles.append(parent.get("role", "unknown"))
            current = parent
            depth += 1

        return roles

    def get_common_ancestor(
        self,
        element_a: Dict[str, Any],
        element_b: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Find the common ancestor of two elements.

        Args:
            element_a: First element.
            element_b: Second element.

        Returns:
            Common ancestor element or None.
        """
        ancestors_a = self._get_ancestor_set(element_a)
        current = element_b

        while current:
            if id(current) in ancestors_a:
                return current
            parent = current.get("parent") or current.get("_parent")
            current = parent

        return None

    def _get_ancestor_set(
        self,
        element: Dict[str, Any],
    ) -> Set[int]:
        """Get set of ancestor element IDs."""
        ancestors: Set[int] = set()
        current = element.get("parent") or element.get("_parent")

        while current:
            ancestors.add(id(current))
            current = current.get("parent") or current.get("_parent")

        return ancestors

    def is_descendant_of(
        self,
        element: Dict[str, Any],
        ancestor_role: str,
    ) -> bool:
        """
        Check if an element is a descendant of a role type.

        Args:
            element: Element to check.
            ancestor_role: Role name to look for in ancestors.

        Returns:
            True if any ancestor has the specified role.
        """
        ancestor_roles = self.get_ancestor_roles(element)
        return ancestor_role in ancestor_roles


def is_interactive(element: Dict[str, Any]) -> bool:
    """Check if an element has an interactive role."""
    return RoleHierarchy().is_interactive(element)


def is_container(element: Dict[str, Any]) -> bool:
    """Check if an element has a container role."""
    return RoleHierarchy().is_container(element)


def get_parent_roles(role: str) -> List[str]:
    """Get parent roles for a given role."""
    return RoleHierarchy().get_parent_roles(role)
