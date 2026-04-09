"""
System Tray Manager Action Module.

Manages system tray icon interactions for background automation,
including menu creation, notifications, and tray icon updates.
"""

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class TrayMenuItem:
    """A menu item in the system tray."""
    label: str
    action: Optional[Callable[[], None]] = None
    enabled: bool = True
    separator: bool = False


@dataclass
class TrayNotification:
    """A tray notification."""
    title: str
    message: str
    icon: str = "info"
    duration: float = 5.0


class SystemTrayManager:
    """Manages system tray icon and menu."""

    def __init__(self):
        """Initialize system tray manager."""
        self._menu_items: list[TrayMenuItem] = []
        self._icon_path: Optional[str] = None
        self._tooltip: str = ""
        self._visible: bool = False
        self._notification_handler: Optional[Callable[[TrayNotification], None]] = None

    def set_icon(self, icon_path: str) -> None:
        """Set the tray icon."""
        self._icon_path = icon_path

    def set_tooltip(self, tooltip: str) -> None:
        """Set the tray tooltip."""
        self._tooltip = tooltip

    def add_menu_item(
        self,
        label: str,
        action: Optional[Callable[[], None]] = None,
        enabled: bool = True,
    ) -> None:
        """Add a menu item."""
        item = TrayMenuItem(label=label, action=action, enabled=enabled)
        self._menu_items.append(item)

    def add_separator(self) -> None:
        """Add a menu separator."""
        self._menu_items.append(TrayMenuItem("", separator=True))

    def remove_menu_item(self, label: str) -> bool:
        """Remove a menu item by label."""
        for i, item in enumerate(self._menu_items):
            if item.label == label:
                self._menu_items.pop(i)
                return True
        return False

    def show(self) -> None:
        """Show the tray icon."""
        self._visible = True

    def hide(self) -> None:
        """Hide the tray icon."""
        self._visible = False

    def is_visible(self) -> bool:
        """Check if tray icon is visible."""
        return self._visible

    def notify(
        self,
        title: str,
        message: str,
        icon: str = "info",
    ) -> None:
        """Show a notification from the tray."""
        if self._notification_handler:
            notification = TrayNotification(
                title=title,
                message=message,
                icon=icon,
            )
            self._notification_handler(notification)

    def on_notification(
        self,
        handler: Callable[[TrayNotification], None],
    ) -> None:
        """Register notification handler."""
        self._notification_handler = handler

    def get_menu_items(self) -> list[TrayMenuItem]:
        """Get all menu items."""
        return list(self._menu_items)
