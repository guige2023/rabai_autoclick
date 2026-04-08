"""Menu navigation utilities for automation of macOS menu bar and context menus.

Provides helpers for opening, navigating, and selecting items from
application menus, the macOS menu bar, and context menus during
GUI automation.

Example:
    >>> from utils.menu_utils import open_menu, select_menu_item, menu_bar_item
    >>> open_menu('File', 'Open...')
    >>> select_menu_item('Edit', 'Paste')
    >>> menu_bar_item('Safari', 'View', 'Reload')
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional

__all__ = [
    "open_menu",
    "select_menu_item",
    "menu_bar_item",
    "open_about_window",
    "close_menu",
    "MenuError",
]


class MenuError(Exception):
    """Raised when a menu operation fails."""
    pass


def _run_script(script: str) -> bool:
    """Run an osascript and return success status."""
    try:
        subprocess.run(["osascript", "-e", script], timeout=10, check=True)
        return True
    except Exception:
        return False


def open_menu(*path: str) -> bool:
    """Open a menu path (e.g., open_menu('File', 'Open...')).

    Uses System Events to activate the menu bar item.

    Args:
        *path: Menu path components.

    Returns:
        True if successful.

    Example:
        >>> open_menu('File', 'Open...')
        >>> open_menu('Edit', 'Find', 'Find and Replace...')
    """
    if not path:
        raise MenuError("Menu path cannot be empty")

    path_str = ", ".join(f'"{p}"' for p in path)
    script = f"""
    tell application "System Events"
        tell process "FrontMost"
            click menu bar item {path_str} of menu 1 of menu bar 1
        end tell
    end tell
    """
    return _run_script(script)


def select_menu_item(*path: str) -> bool:
    """Select a menu item via its path (opens parent menus automatically).

    Args:
        *path: Full path to the menu item.

    Returns:
        True if successful.

    Example:
        >>> select_menu_item('Edit', 'Paste')
        >>> select_menu_item('Format', 'Font', 'Bold')
    """
    if not path:
        raise MenuError("Menu item path cannot be empty")

    path_str = ", ".join(f'"{p}"' for p in path)
    script = f"""
    tell application "System Events"
        tell process "FrontMost"
            click menu item {path_str} of menu 1 of menu bar 1
        end tell
    end tell
    """
    return _run_script(script)


def menu_bar_item(app_name: str, *menu_path: str) -> bool:
    """Click a menu bar item for a specific application.

    Activates the application first, then navigates its menu bar.

    Args:
        app_name: Application name (e.g., 'Safari').
        *menu_path: Menu item path.

    Returns:
        True if successful.

    Example:
        >>> menu_bar_item('Safari', 'View', 'Reload')
    """
    if not menu_path:
        raise MenuError("Menu path cannot be empty")

    # Activate the app first
    activate_script = f'tell application "{app_name}" to activate'
    try:
        subprocess.run(["osascript", "-e", activate_script], timeout=5)
        time.sleep(0.2)
    except Exception:
        return False

    path_str = ", ".join(f'"{p}"' for p in menu_path)
    script = f"""
    tell application "System Events"
        tell process "{app_name}"
            click menu item {path_str} of menu 1 of menu bar 1
        end tell
    end tell
    """
    return _run_script(script)


def open_about_window(app_name: Optional[str] = None) -> bool:
    """Open the About window for an application (or system).

    Args:
        app_name: Application name (uses frontmost if None).

    Returns:
        True if successful.
    """
    if app_name:
        return select_menu_item(app_name, f"About {app_name}")
    return select_menu_item("Apple", f"About This Mac")


def close_menu() -> bool:
    """Close an open menu by pressing Escape.

    Returns:
        True if successful.
    """
    try:
        subprocess.run(
            ["osascript", "-e", 'tell application "System Events" to keystroke (ASCII character 27)'],
            timeout=5,
            check=True,
        )
        return True
    except Exception:
        return False


def menu_item_exists(*path: str) -> bool:
    """Check if a menu item path exists (is accessible).

    Args:
        *path: Menu path to check.

    Returns:
        True if the menu item can be found.
    """
    if not path:
        return False

    path_str = ", ".join(f'"{p}"' for p in path)
    script = f"""
    tell application "System Events"
        tell process "FrontMost"
            try
                set menuItem to menu item {path_str} of menu 1 of menu bar 1
                set isEnabled to enabled of menuItem
                return isEnabled
            on error
                return false
            end try
        end tell
    end tell
    """
    try:
        result = subprocess.check_output(
            ["osascript", "-e", script],
            timeout=5,
        )
        return result.strip() == "true"
    except Exception:
        return False
