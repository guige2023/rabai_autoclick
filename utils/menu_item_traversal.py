"""
Menu Item Traversal Utilities.

Utilities for traversing, locating, and interacting with
menu bar items and dropdown menus in macOS applications.

Usage:
    from utils.menu_item_traversal import MenuTraverser, find_menu_item

    traverser = MenuTraverser(bridge)
    item = traverser.find_item(role="menu_item", title="Copy")
    traverser.click(item)
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass
from enum import Enum, auto

if TYPE_CHECKING:
    from utils.accessibility_bridge import AccessibilityBridge


class MenuTraversalOrder(Enum):
    """Order in which to traverse menu items."""
    LINEAR = auto()
    RECURSIVE = auto()
    BREADTH_FIRST = auto()
    DEPTH_FIRST = auto()


@dataclass
class MenuItemLocation:
    """Represents the location path to a specific menu item."""
    app_name: str
    menu_bar_path: List[str]
    item_title: str
    item_index: int
    element: Optional[Any] = None

    @property
    def full_path(self) -> str:
        """Return the full dotted path to the item."""
        path = [self.app_name] + self.menu_bar_path + [self.item_title]
        return " > ".join(path)


@dataclass
class MenuItem:
    """Represents a menu item in a menu hierarchy."""
    title: str
    role: str
    enabled: bool
    selected: bool
    has_submenu: bool
    element: Optional[Any] = None
    children: List["MenuItem"] = None
    shortcut: Optional[str] = None

    def __post_init__(self) -> None:
        if self.children is None:
            self.children = []

    def __repr__(self) -> str:
        return f"MenuItem({self.title!r}, enabled={self.enabled})"


class MenuTraverser:
    """
    Traverse and interact with macOS menu items.

    Provides methods for finding menu items by title, role, or
    keyboard shortcut, and performing actions on them.

    Example:
        traverser = MenuTraverser(bridge)
        # Find Edit > Copy
        item = traverser.find_item(
            path=["Edit", "Copy"],
            app_name="Finder"
        )
        if item:
            traverser.click(item)
    """

    def __init__(self, bridge: "AccessibilityBridge") -> None:
        """
        Initialize the menu traverser.

        Args:
            bridge: An AccessibilityBridge instance.
        """
        self._bridge = bridge

    def get_menu_bar_items(
        self,
        app: Optional[Any] = None,
    ) -> List[MenuItem]:
        """
        Get all menu bar items for an application.

        Args:
            app: Application element (defaults to frontmost).

        Returns:
            List of top-level MenuItem objects.
        """
        if app is None:
            app = self._bridge.get_frontmost_app()
            if app is None:
                return []

        try:
            tree = self._bridge.build_accessibility_tree(app)
            return self._extract_menu_bar(tree)
        except Exception:
            return []

    def _extract_menu_bar(
        self,
        tree: Dict[str, Any],
    ) -> List[MenuItem]:
        """Extract menu bar items from accessibility tree."""
        items: List[MenuItem] = []

        for child in tree.get("children", []):
            role = child.get("role", "")
            if role in ("menu_bar", "menu_bar_item"):
                items.extend(self._parse_menu_items(child))
                break
            elif role == "menu_item" and child.get("title"):
                items.append(self._dict_to_menu_item(child))

        return items

    def _parse_menu_items(
        self,
        node: Dict[str, Any],
        depth: int = 0,
    ) -> List[MenuItem]:
        """Recursively parse menu items from a tree node."""
        items: List[MenuItem] = []

        for child in node.get("children", []):
            role = child.get("role", "")
            if role == "menu_item":
                item = self._dict_to_menu_item(child)
                if child.get("children"):
                    item.children = self._parse_menu_items(child, depth + 1)
                    item.has_submenu = bool(item.children)
                items.append(item)
            elif role == "menu" and depth > 0:
                items.extend(self._parse_menu_items(child, depth + 1))

        return items

    def _dict_to_menu_item(
        self,
        d: Dict[str, Any],
    ) -> MenuItem:
        """Convert a dictionary to a MenuItem."""
        return MenuItem(
            title=d.get("title", ""),
            role=d.get("role", "menu_item"),
            enabled=d.get("enabled", True),
            selected=d.get("selected", False),
            has_submenu=bool(d.get("children")),
            element=d,
            shortcut=d.get("shortcut"),
        )

    def find_item(
        self,
        title: Optional[str] = None,
        path: Optional[List[str]] = None,
        role: str = "menu_item",
        app_name: Optional[str] = None,
    ) -> Optional[MenuItemLocation]:
        """
        Find a menu item by title and optional path.

        Args:
            title: Exact title of the item to find.
            path: List of menu names to navigate (e.g., ["Edit", "Format"]).
            role: Role to match (default "menu_item").
            app_name: Name of the application.

        Returns:
            MenuItemLocation if found, None otherwise.
        """
        if path is None:
            path = []

        app = self._get_app_by_name(app_name) if app_name else None
        menu_items = self.get_menu_bar_items(app)

        if not path:
            for item in menu_items:
                if title and item.title == title:
                    return self._make_location(app_name or "", [], title, item)
            return None

        current_level = menu_items
        for i, path_name in enumerate(path[:-1]):
            found = None
            for item in current_level:
                if item.title == path_name:
                    found = item
                    break
            if found is None:
                return None
            current_level = found.children

        target_title = path[-1] if path else title
        if target_title is None:
            return None

        for item in current_level:
            if item.title == target_title:
                return self._make_location(
                    app_name or "",
                    path,
                    item.title,
                    item,
                )
        return None

    def _make_location(
        self,
        app_name: str,
        path: List[str],
        item_title: str,
        item: MenuItem,
    ) -> MenuItemLocation:
        """Create a MenuItemLocation from components."""
        return MenuItemLocation(
            app_name=app_name,
            menu_bar_path=path,
            item_title=item_title,
            item_index=0,
            element=item.element,
        )

    def _get_app_by_name(self, name: str) -> Optional[Any]:
        """Get an application element by name."""
        try:
            running = self._bridge.get_running_applications()
            for app in running:
                if app.get("name") == name:
                    return app
        except Exception:
            pass
        return None

    def click(
        self,
        item: MenuItem,
        verify: bool = True,
    ) -> bool:
        """
        Click a menu item.

        Args:
            item: MenuItem to click.
            verify: Whether to verify the click succeeded.

        Returns:
            True if click was successful.
        """
        if item.element is None:
            return False

        try:
            role = item.element.get("role", "")
            if role in ("menu_item", "menu_bar_item"):
                bounds = item.element.get("rect", {})
                cx = bounds.get("x", 0) + bounds.get("width", 0) / 2
                cy = bounds.get("y", 0) + bounds.get("height", 0) / 2
                self._bridge.click_at(cx, cy)
                return True
        except Exception:
            return False
        return False

    def activate_menu_path(
        self,
        path: List[str],
        app_name: Optional[str] = None,
    ) -> bool:
        """
        Activate a menu by path (e.g., ["File", "Open"]).

        This opens each menu in the path sequentially.

        Args:
            path: List of menu names to open.
            app_name: Application name.

        Returns:
            True if the full path was opened successfully.
        """
        app = self._get_app_by_name(app_name) if app_name else None
        menu_items = self.get_menu_bar_items(app)
        current = menu_items

        for i, name in enumerate(path):
            found = None
            for item in current:
                if item.title == name:
                    found = item
                    break
            if found is None:
                return False

            self.click(found)
            import time
            time.sleep(0.1)

            if i < len(path) - 1:
                current = found.children

        return True


def find_menu_item(
    bridge: "AccessibilityBridge",
    title: str,
    path: Optional[List[str]] = None,
    app_name: Optional[str] = None,
) -> Optional[MenuItemLocation]:
    """
    Convenience function to find a menu item.

    Args:
        bridge: AccessibilityBridge instance.
        title: Item title to find.
        path: Optional path to the item.
        app_name: Optional app name.

    Returns:
        MenuItemLocation if found, None otherwise.
    """
    traverser = MenuTraverser(bridge)
    return traverser.find_item(title=title, path=path, app_name=app_name)
