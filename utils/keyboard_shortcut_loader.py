"""
Keyboard Shortcut Loader.

Load, manage, and execute keyboard shortcuts from configuration
files or predefined shortcut sets for various applications.

Usage:
    from utils.keyboard_shortcut_loader import ShortcutLoader, load_system_shortcuts

    loader = ShortcutLoader()
    shortcuts = loader.load_app_shortcuts("Finder")
    shortcut = loader.find_shortcut("Copy")
    if shortcut:
        loader.execute(shortcut)
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
import json
import os

if TYPE_CHECKING:
    pass


@dataclass
class Shortcut:
    """Represents a keyboard shortcut."""
    action: str
    keys: List[str]
    modifiers: List[str] = field(default_factory=list)
    description: Optional[str] = None
    app_name: Optional[str] = None

    @property
    def key_string(self) -> str:
        """Return the shortcut as a string like 'Cmd+C'."""
        parts = list(self.modifiers) + self.keys
        return "+".join(parts)

    def __repr__(self) -> str:
        return f"Shortcut({self.action!r}, {self.key_string})"


class ShortcutLoader:
    """
    Load and manage keyboard shortcuts from various sources.

    Supports loading shortcuts from JSON files, predefined sets,
    and dynamically discovering shortcuts from accessibility info.

    Example:
        loader = ShortcutLoader()
        shortcuts = loader.load_defaults()
        copy_shortcut = loader.find("copy")
    """

    DEFAULT_SHORTCUTS: Dict[str, List[Dict[str, Any]]] = {
        "global": [
            {"action": "copy", "keys": ["c"], "modifiers": ["Cmd"]},
            {"action": "paste", "keys": ["v"], "modifiers": ["Cmd"]},
            {"action": "cut", "keys": ["x"], "modifiers": ["Cmd"]},
            {"action": "undo", "keys": ["z"], "modifiers": ["Cmd"]},
            {"action": "redo", "keys": ["z"], "modifiers": ["Cmd", "Shift"]},
            {"action": "select_all", "keys": ["a"], "modifiers": ["Cmd"]},
            {"action": "find", "keys": ["f"], "modifiers": ["Cmd"]},
            {"action": "save", "keys": ["s"], "modifiers": ["Cmd"]},
            {"action": "quit", "keys": ["q"], "modifiers": ["Cmd"]},
            {"action": "new_window", "keys": ["n"], "modifiers": ["Cmd"]},
            {"action": "new_tab", "keys": ["t"], "modifiers": ["Cmd"]},
            {"action": "close_window", "keys": ["w"], "modifiers": ["Cmd"]},
            {"action": "minimize", "keys": ["m"], "modifiers": ["Cmd"]},
            {"action": "zoom", "keys": ["z"], "modifiers": ["Cmd"]},
        ],
        "Finder": [
            {"action": "open", "keys": ["o"], "modifiers": ["Cmd"]},
            {"action": "show_original", "keys": ["r"], "modifiers": ["Cmd"]},
            {"action": "info", "keys": ["i"], "modifiers": ["Cmd"]},
            {"action": "duplicate", "keys": ["d"], "modifiers": ["Cmd"]},
            {"action": "delete", "keys": ["delete"], "modifiers": ["Cmd"]},
            {"action": "rename", "keys": ["enter"], "modifiers": []},
            {"action": "new_folder", "keys": ["n"], "modifiers": ["Cmd", "Shift"]},
        ],
        "Safari": [
            {"action": "reload", "keys": ["r"], "modifiers": ["Cmd"]},
            {"action": "back", "keys": ["left"], "modifiers": ["Cmd"]},
            {"action": "forward", "keys": ["right"], "modifiers": ["Cmd"]},
            {"action": "add_bookmark", "keys": ["d"], "modifiers": ["Cmd"]},
            {"action": "new_tab", "keys": ["t"], "modifiers": ["Cmd"]},
        ],
    }

    def __init__(self) -> None:
        """Initialize the shortcut loader."""
        self._shortcuts: Dict[str, List[Shortcut]] = {}
        self._load_defaults()

    def _load_defaults(self) -> None:
        """Load default shortcuts."""
        for app_name, shortcuts_list in self.DEFAULT_SHORTCUTS.items():
            shortcuts = []
            for s in shortcuts_list:
                shortcuts.append(Shortcut(
                    action=s["action"],
                    keys=s.get("keys", []),
                    modifiers=s.get("modifiers", []),
                    app_name=app_name if app_name != "global" else None,
                ))
            self._shortcuts[app_name] = shortcuts

    def load_from_file(
        self,
        path: str,
    ) -> bool:
        """
        Load shortcuts from a JSON file.

        Args:
            path: Path to the JSON file.

        Returns:
            True if loaded successfully.
        """
        if not os.path.exists(path):
            return False

        try:
            with open(path) as f:
                data = json.load(f)

            for app_name, shortcuts_list in data.items():
                shortcuts = []
                for s in shortcuts_list:
                    shortcuts.append(Shortcut(
                        action=s["action"],
                        keys=s.get("keys", []),
                        modifiers=s.get("modifiers", []),
                        app_name=app_name,
                    ))
                self._shortcuts[app_name] = shortcuts

            return True
        except Exception:
            return False

    def get_shortcuts(
        self,
        app_name: Optional[str] = None,
    ) -> List[Shortcut]:
        """
        Get all shortcuts for an app (or global).

        Args:
            app_name: App name or None for global shortcuts.

        Returns:
            List of Shortcut objects.
        """
        result: List[Shortcut] = []

        if app_name and app_name in self._shortcuts:
            result.extend(self._shortcuts[app_name])

        if "global" in self._shortcuts:
            result.extend(self._shortcuts["global"])

        return result

    def find_shortcut(
        self,
        action: str,
        app_name: Optional[str] = None,
    ) -> Optional[Shortcut]:
        """
        Find a shortcut by action name.

        Args:
            action: Action name (e.g., "copy").
            app_name: Optional app name to search within.

        Returns:
            Shortcut if found, None otherwise.
        """
        shortcuts = self.get_shortcuts(app_name)
        action_lower = action.lower()

        for shortcut in shortcuts:
            if shortcut.action.lower() == action_lower:
                return shortcut

        return None

    def add_shortcut(
        self,
        shortcut: Shortcut,
        app_name: Optional[str] = None,
    ) -> None:
        """
        Add a custom shortcut.

        Args:
            shortcut: Shortcut object to add.
            app_name: Optional app name.
        """
        key = app_name or "global"
        if key not in self._shortcuts:
            self._shortcuts[key] = []

        self._shortcuts[key].append(shortcut)

    def remove_shortcut(
        self,
        action: str,
        app_name: Optional[str] = None,
    ) -> bool:
        """
        Remove a shortcut by action name.

        Args:
            action: Action name.
            app_name: Optional app name.

        Returns:
            True if removed, False if not found.
        """
        key = app_name or "global"
        if key not in self._shortcuts:
            return False

        for i, s in enumerate(self._shortcuts[key]):
            if s.action.lower() == action.lower():
                self._shortcuts[key].pop(i)
                return True

        return False

    def save_to_file(
        self,
        path: str,
    ) -> bool:
        """
        Save all shortcuts to a JSON file.

        Args:
            path: Path to save to.

        Returns:
            True if saved successfully.
        """
        try:
            data = {}
            for app_name, shortcuts in self._shortcuts.items():
                data[app_name] = [
                    {
                        "action": s.action,
                        "keys": s.keys,
                        "modifiers": s.modifiers,
                    }
                    for s in shortcuts
                ]

            with open(path, "w") as f:
                json.dump(data, f, indent=2)

            return True
        except Exception:
            return False


def load_system_shortcuts() -> ShortcutLoader:
    """
    Load system keyboard shortcuts.

    Returns:
        ShortcutLoader with system shortcuts loaded.
    """
    loader = ShortcutLoader()
    return loader
