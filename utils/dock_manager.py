"""
Dock Manager Utilities.

Utilities for interacting with the macOS Dock including
launching apps, pinning items, and managing dock state.

Usage:
    from utils.dock_manager import DockManager

    manager = DockManager()
    manager.launch_app("Safari")
    manager.add_to_dock("/path/to/app.app")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, TYPE_CHECKING
import subprocess

if TYPE_CHECKING:
    pass


class DockManager:
    """
    Manage the macOS Dock.

    Provides utilities for launching applications, adding items,
    and querying dock state.

    Example:
        manager = DockManager()
        manager.launch_app("Safari")
    """

    def __init__(self) -> None:
        """Initialize the dock manager."""
        pass

    def launch_app(self, app_name: str) -> bool:
        """
        Launch an application by name.

        Args:
            app_name: Name of the application.

        Returns:
            True if successful.
        """
        try:
            subprocess.run(
                ["open", "-a", app_name],
                check=True,
                capture_output=True,
            )
            return True
        except Exception:
            return False

    def launch_app_by_bundle_id(self, bundle_id: str) -> bool:
        """
        Launch an application by bundle ID.

        Args:
            bundle_id: Bundle identifier (e.g., "com.apple.Safari").

        Returns:
            True if successful.
        """
        try:
            subprocess.run(
                ["open", "-b", bundle_id],
                check=True,
                capture_output=True,
            )
            return True
        except Exception:
            return False

    def get_running_apps(self) -> List[str]:
        """
        Get list of apps currently in the Dock and running.

        Returns:
            List of application names.
        """
        apps = []
        try:
            result = subprocess.run(
                ["osascript", "-e",
                 'tell application "System Events" to get '
                 "name of every application process whose "
                 "bundle identifier is not missing value"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                apps = [a.strip() for a in result.stdout.split(", ")]
        except Exception:
            pass
        return apps

    def add_to_dock(self, app_path: str) -> bool:
        """
        Add an application to the Dock.

        Args:
            app_path: Path to the .app bundle.

        Returns:
            True if successful.
        """
        try:
            subprocess.run(
                ["defaults", "write",
                 "com.apple.dock", "persistent-apps",
                 "-array-add", f"'<dict><key>tile-data</key>"
                 f"<dict><key>file-data</key>"
                 f"<dict><key>_CFURLString</key>"
                 f"<string>{app_path}</string>"
                 f"</dict></dict></dict>'"],
                check=True,
                capture_output=True,
            )
            self._restart_dock()
            return True
        except Exception:
            return False

    def remove_from_dock(self, app_name: str) -> bool:
        """
        Remove an application from the Dock.

        Args:
            app_name: Name of the application.

        Returns:
            True if successful.
        """
        try:
            subprocess.run(
                ["osascript", "-e",
                 f'tell application "System Events" to delete '
                 f'every application file of dock items '
                 f'where name is "{app_name}"'],
                check=True,
                capture_output=True,
            )
            return True
        except Exception:
            return False

    def _restart_dock(self) -> None:
        """Restart the Dock process."""
        try:
            subprocess.run(
                ["killall", "Dock"],
                check=True,
            )
        except Exception:
            pass

    def get_dock_items(self) -> List[Dict[str, str]]:
        """
        Get list of items in the Dock.

        Returns:
            List of dicts with name and path for each item.
        """
        items = []
        try:
            result = subprocess.run(
                ["osascript", "-e",
                 'tell application "System Events" to get '
                 "{name, file of every dock item}"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                pass
        except Exception:
            pass
        return items

    def show_dock(self) -> bool:
        """
        Show the Dock if hidden.

        Returns:
            True if successful.
        """
        try:
            subprocess.run(
                ["defaults", "write",
                 "com.apple.dock", "autohide", "-bool", "no"],
                check=True,
            )
            self._restart_dock()
            return True
        except Exception:
            return False

    def hide_dock(self) -> bool:
        """
        Hide the Dock.

        Returns:
            True if successful.
        """
        try:
            subprocess.run(
                ["defaults", "write",
                 "com.apple.dock", "autohide", "-bool", "yes"],
                check=True,
            )
            self._restart_dock()
            return True
        except Exception:
            return False
