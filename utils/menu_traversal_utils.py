"""
Menu Traversal Utilities.

Utilities for traversing and interacting with menu bars,
dropdown menus, context menus, and menu items.

Usage:
    from utils.menu_traversal_utils import MenuTraversalHelper

    helper = MenuTraversalHelper(bridge)
    item = helper.find_menu_item("File", "Open")
    helper.click_menu_item(item)
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass

if TYPE_CHECKING:
    pass


@dataclass
class MenuItemInfo:
    """Information about a menu item."""
    title: str
    role: str = "menu_item"
    enabled: bool = True
    has_submenu: bool = False
    shortcut: Optional[str] = None
    element: Optional[Dict[str, Any]] = None


class MenuTraversalHelper:
    """
    Helper for traversing and interacting with menus.

    Provides utilities for navigating menu bars, finding items,
    and performing actions on menu elements.

    Example:
        helper = MenuTraversalHelper(bridge)
        items = helper.get_menu_bar_items()
        for item in items:
            print(f"Menu: {item.title}")
    """

    def __init__(self, bridge: Any) -> None:
        """
        Initialize the menu traversal helper.

        Args:
            bridge: AccessibilityBridge or similar instance.
        """
        self._bridge = bridge

    def get_menu_bar_items(
        self,
        app: Optional[Any] = None,
    ) -> List[MenuItemInfo]:
        """
        Get all top-level menu bar items.

        Args:
            app: Application element (defaults to frontmost).

        Returns:
            List of MenuItemInfo objects.
        """
        if app is None:
            app = self._bridge.get_frontmost_app()
            if app is None:
                return []

        try:
            tree = self._bridge.build_accessibility_tree(app)
            return self._extract_menu_bar_items(tree)
        except Exception:
            return []

    def _extract_menu_bar_items(
        self,
        tree: Dict[str, Any],
    ) -> List[MenuItemInfo]:
        """Extract menu bar items from tree."""
        items: List[MenuItemInfo] = []

        def traverse(node: Dict[str, Any]) -> None:
            role = node.get("role", "")
            if role == "menu_bar_item":
                item = MenuItemInfo(
                    title=node.get("title", ""),
                    role=role,
                    enabled=node.get("enabled", True),
                    element=node,
                )
                items.append(item)
            elif role == "menu":
                for child in node.get("children", []):
                    if isinstance(child, dict):
                        traverse(child)

        for child in tree.get("children", []):
            if isinstance(child, dict):
                traverse(child)

        return items

    def find_menu_item(
        self,
        menu_title: str,
        item_title: str,
    ) -> Optional[MenuItemInfo]:
        """
        Find a specific menu item within a menu.

        Args:
            menu_title: Title of the menu (e.g., "File").
            item_title: Title of the item to find.

        Returns:
            MenuItemInfo if found, None otherwise.
        """
        items = self.get_menu_bar_items()

        for item in items:
            if item.title == menu_title:
                submenu = self._get_submenu_items(item.element)
                for subitem in submenu:
                    if subitem.title == item_title:
                        return subitem

        return None

    def _get_submenu_items(
        self,
        menu_element: Optional[Dict[str, Any]],
    ) -> List[MenuItemInfo]:
        """Get items from a submenu."""
        if menu_element is None:
            return []

        items: List[MenuItemInfo] = []
        for child in menu_element.get("children", []):
            if isinstance(child, dict) and child.get("role") == "menu_item":
                items.append(MenuItemInfo(
                    title=child.get("title", ""),
                    role="menu_item",
                    enabled=child.get("enabled", True),
                    has_submenu=bool(child.get("children")),
                    element=child,
                ))
        return items

    def click_menu_item(
        self,
        item: MenuItemInfo,
    ) -> bool:
        """
        Click a menu item.

        Args:
            item: MenuItemInfo to click.

        Returns:
            True if successful.
        """
        if item.element is None:
            return False

        try:
            bounds = item.element.get("rect", {})
            cx = bounds.get("x", 0) + bounds.get("width", 0) / 2
            cy = bounds.get("y", 0) + bounds.get("height", 0) / 2
            self._bridge.click_at(cx, cy)
            return True
        except Exception:
            return False

    def open_menu(
        self,
        menu_title: str,
    ) -> bool:
        """
        Open a menu by title.

        Args:
            menu_title: Title of the menu to open.

        Returns:
            True if successful.
        """
        items = self.get_menu_bar_items()
        for item in items:
            if item.title == menu_title:
                return self.click_menu_item(item)
        return False

    def select_item_by_shortcut(
        self,
        shortcut: str,
    ) -> bool:
        """
        Select a menu item by its keyboard shortcut.

        Args:
            shortcut: Shortcut string like "Cmd+O".

        Returns:
            True if the shortcut was found and invoked.
        """
        import re
        pattern = r"(Cmd|Ctrl|Option|Shift)\+?"

        all_items = self.get_all_menu_items()

        for item in all_items:
            if item.shortcut and self._shortcuts_match(item.shortcut, shortcut):
                return self.click_menu_item(item)

        return False

    def _shortcuts_match(
        self,
        a: str,
        b: str,
    ) -> bool:
        """Check if two shortcut strings represent the same shortcut."""
        import re
        clean = lambda s: re.sub(r"[^\w]", "", s.lower())
        return clean(a) == clean(b)

    def get_all_menu_items(
        self,
        app: Optional[Any] = None,
    ) -> List[MenuItemInfo]:
        """
        Get all menu items from all menus.

        Args:
            app: Application element (defaults to frontmost).

        Returns:
            Flat list of all MenuItemInfo objects.
        """
        if app is None:
            app = self._bridge.get_frontmost_app()
            if app is None:
                return []

        all_items: List[MenuItemInfo] = []

        try:
            tree = self._bridge.build_accessibility_tree(app)

            def traverse(node: Dict[str, Any]) -> None:
                role = node.get("role", "")
                if role == "menu_item":
                    all_items.append(MenuItemInfo(
                        title=node.get("title", ""),
                        role=role,
                        enabled=node.get("enabled", True),
                        has_submenu=bool(node.get("children")),
                        shortcut=node.get("shortcut"),
                        element=node,
                    ))
                for child in node.get("children", []):
                    if isinstance(child, dict):
                        traverse(child)

            for child in tree.get("children", []):
                if isinstance(child, dict):
                    traverse(child)

        except Exception:
            pass

        return all_items


def find_menu_item_by_path(
    bridge: Any,
    path: List[str],
) -> Optional[MenuItemInfo]:
    """
    Find a menu item by path (e.g., ["File", "Open"]).

    Args:
        bridge: AccessibilityBridge instance.
        path: List of menu and item titles.

    Returns:
        MenuItemInfo if found, None otherwise.
    """
    helper = MenuTraversalHelper(bridge)

    if len(path) == 1:
        items = helper.get_menu_bar_items()
        for item in items:
            if item.title == path[0]:
                return item
        return None

    menu_title = path[0]
    item_title = path[-1]
    return helper.find_menu_item(menu_title, item_title)
