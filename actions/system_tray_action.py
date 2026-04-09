"""
System Tray Action Module

Manages system tray icon, menu, and notifications
for background automation workflows.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class TrayIconState(Enum):
    """Tray icon states."""

    NORMAL = "normal"
    WARNING = "warning"
    ERROR = "error"
    BUSY = "busy"


@dataclass
class TrayMenuItem:
    """Represents a tray menu item."""

    id: str
    label: str
    icon: Optional[str] = None
    enabled: bool = True
    checked: bool = False
    separator: bool = False
    submenu: Optional[List["TrayMenuItem"]] = None


@dataclass
class TrayConfig:
    """Configuration for system tray."""

    icon_path: Optional[str] = None
    tooltip: str = "Automation"
    default_state: TrayIconState = TrayIconState.NORMAL
    show_menu_on_left_click: bool = True
    show_notification_on_right_click: bool = False


@dataclass
class TrayEvent:
    """Represents a tray event."""

    event_type: str
    item_id: Optional[str] = None
    timestamp: float = field(default_factory=time.time)


class SystemTrayManager:
    """
    Manages system tray icon and menu.

    Supports icon states, menu items, event handling,
    and tooltip management.
    """

    def __init__(
        self,
        config: Optional[TrayConfig] = None,
        tray_handler: Optional[Any] = None,
    ):
        self.config = config or TrayConfig()
        self.tray_handler = tray_handler
        self._menu_items: Dict[str, TrayMenuItem] = {}
        self._callbacks: Dict[str, Callable[[TrayEvent], None]] = {}
        self._icon_state: TrayIconState = config.default_state if config else TrayIconState.NORMAL
        self._is_visible: bool = False

    def initialize(self) -> bool:
        """Initialize the system tray icon."""
        try:
            if self.tray_handler:
                self.tray_handler.create_icon(
                    self.config.icon_path,
                    self.config.tooltip,
                )
                self._is_visible = True
                logger.info("System tray initialized")
                return True
            return False
        except Exception as e:
            logger.error(f"Tray initialization failed: {e}")
            return False

    def add_menu_item(
        self,
        item: TrayMenuItem,
        callback: Optional[Callable[[], None]] = None,
    ) -> bool:
        """
        Add a menu item.

        Args:
            item: Menu item to add
            callback: Callback when item is clicked

        Returns:
            True if successful
        """
        self._menu_items[item.id] = item

        if callback:
            self._callbacks[item.id] = callback

        if self.tray_handler:
            self.tray_handler.add_menu_item(item)

        return True

    def remove_menu_item(self, item_id: str) -> bool:
        """Remove a menu item."""
        if item_id in self._menu_items:
            del self._menu_items[item_id]

            if self.tray_handler:
                self.tray_handler.remove_menu_item(item_id)

            return True
        return False

    def update_menu_item(
        self,
        item_id: str,
        updates: Dict[str, Any],
    ) -> bool:
        """Update a menu item's properties."""
        if item_id not in self._menu_items:
            return False

        item = self._menu_items[item_id]
        for key, value in updates.items():
            if hasattr(item, key):
                setattr(item, key, value)

        if self.tray_handler:
            self.tray_handler.update_menu_item(item)

        return True

    def set_icon_state(self, state: TrayIconState) -> bool:
        """
        Set tray icon state.

        Args:
            state: New icon state

        Returns:
            True if successful
        """
        self._icon_state = state

        if self.tray_handler:
            return self.tray_handler.set_icon_state(state.value)

        return True

    def set_tooltip(self, tooltip: str) -> bool:
        """Set tray tooltip."""
        if self.tray_handler:
            return self.tray_handler.set_tooltip(tooltip)
        return True

    def show_notification(
        self,
        title: str,
        message: str,
        icon: Optional[str] = None,
    ) -> bool:
        """Show a notification from the tray."""
        if self.tray_handler:
            return self.tray_handler.show_notification(title, message, icon)
        return False

    def handle_event(self, event: TrayEvent) -> None:
        """Handle a tray event."""
        if event.item_id and event.item_id in self._callbacks:
            try:
                self._callbacks[event.item_id]()
            except Exception as e:
                logger.error(f"Tray callback failed: {e}")

    def show(self) -> bool:
        """Show the tray icon."""
        if self.tray_handler and not self._is_visible:
            self.tray_handler.show()
            self._is_visible = True
        return self._is_visible

    def hide(self) -> bool:
        """Hide the tray icon."""
        if self.tray_handler and self._is_visible:
            self.tray_handler.hide()
            self._is_visible = False
        return not self._is_visible

    def get_menu_items(self) -> List[TrayMenuItem]:
        """Get all menu items."""
        return list(self._menu_items.values())

    def destroy(self) -> None:
        """Destroy the tray icon."""
        if self.tray_handler:
            self.tray_handler.destroy()
        self._menu_items.clear()
        self._callbacks.clear()
        self._is_visible = False


def create_system_tray_manager(
    config: Optional[TrayConfig] = None,
) -> SystemTrayManager:
    """Factory function to create SystemTrayManager."""
    return SystemTrayManager(config=config)
