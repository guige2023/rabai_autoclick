"""
Context Menu Action Module

Handles context menus, popups, and right-click menus
for UI automation workflows.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class MenuItemType(Enum):
    """Context menu item types."""

    ACTION = "action"
    SEPARATOR = "separator"
    SUBMENU = "submenu"
    CHECKBOX = "checkbox"
    RADIO = "radio"


@dataclass
class MenuItem:
    """Represents a menu item."""

    id: str
    label: str
    item_type: MenuItemType = MenuItemType.ACTION
    enabled: bool = True
    visible: bool = True
    checked: bool = False
    accelerator: Optional[str] = None
    icon: Optional[str] = None
    submenu: Optional[List["MenuItem"]] = None


@dataclass
class ContextMenuConfig:
    """Configuration for context menus."""

    auto_dismiss: bool = True
    dismiss_timeout: float = 5.0
    submenu_delay: float = 0.2
    track_mouse: bool = True


class ContextMenuHandler:
    """
    Handles context menus and popups.

    Supports building menus, handling selections,
    and managing menu visibility.
    """

    def __init__(
        self,
        config: Optional[ContextMenuConfig] = None,
        menu_renderer: Optional[Callable[[List[MenuItem]], None]] = None,
    ):
        self.config = config or ContextMenuConfig()
        self.menu_renderer = menu_renderer
        self._current_menu: Optional[List[MenuItem]] = None
        self._callbacks: Dict[str, Callable[[], None]] = {}
        self._is_visible: bool = False
        self._mouse_position: Tuple[int, int] = (0, 0)

    def build_menu(
        self,
        items: List[MenuItem],
        callback: Optional[Callable[[str], None]] = None,
    ) -> None:
        """
        Build and show a context menu.

        Args:
            items: Menu items to display
            callback: Callback when item is selected
        """
        self._current_menu = items
        self._is_visible = True

        if callback:
            self._menu_callback = callback

        if self.menu_renderer:
            self.menu_renderer(items)

    def show_at(
        self,
        x: int,
        y: int,
        items: List[MenuItem],
    ) -> None:
        """
        Show context menu at coordinates.

        Args:
            x: X coordinate
            y: Y coordinate
            items: Menu items
        """
        self._mouse_position = (x, y)
        self.build_menu(items)

        if self.menu_renderer:
            self.menu_renderer(items)

    def hide(self) -> None:
        """Hide the current menu."""
        self._current_menu = None
        self._is_visible = False

    def select_item(self, item_id: str) -> bool:
        """
        Programmatically select a menu item.

        Args:
            item_id: Item identifier

        Returns:
            True if item was selected
        """
        if not self._current_menu:
            return False

        for item in self._current_menu:
            if item.id == item_id and item.enabled:
                self._trigger_callback(item_id)
                self.hide()
                return True

        return False

    def _trigger_callback(self, item_id: str) -> None:
        """Trigger callback for item selection."""
        if hasattr(self, "_menu_callback") and self._menu_callback:
            try:
                self._menu_callback(item_id)
            except Exception as e:
                logger.error(f"Menu callback failed: {e}")

    def register_callback(
        self,
        item_id: str,
        callback: Callable[[], None],
    ) -> None:
        """Register a callback for a menu item."""
        self._callbacks[item_id] = callback

    def update_item(
        self,
        item_id: str,
        updates: Dict[str, Any],
    ) -> bool:
        """Update a menu item's properties."""
        if not self._current_menu:
            return False

        for item in self._current_menu:
            if item.id == item_id:
                for key, value in updates.items():
                    if hasattr(item, key):
                        setattr(item, key, value)
                return True

        return False

    def enable_item(self, item_id: str, enabled: bool = True) -> bool:
        """Enable or disable a menu item."""
        return self.update_item(item_id, {"enabled": enabled})

    def show_item(self, item_id: str, visible: bool = True) -> bool:
        """Show or hide a menu item."""
        return self.update_item(item_id, {"visible": visible})

    def get_current_menu(self) -> Optional[List[MenuItem]]:
        """Get current menu items."""
        return self._current_menu.copy() if self._current_menu else None

    def is_visible(self) -> bool:
        """Check if menu is visible."""
        return self._is_visible

    def get_mouse_position(self) -> Tuple[int, int]:
        """Get last known mouse position."""
        return self._mouse_position


def create_context_menu_handler(
    config: Optional[ContextMenuConfig] = None,
) -> ContextMenuHandler:
    """Factory function to create ContextMenuHandler."""
    return ContextMenuHandler(config=config)
