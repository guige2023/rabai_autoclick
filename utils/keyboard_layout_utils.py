"""
Keyboard layout and shortcut utilities.

Provides keyboard shortcut definition, parsing,
and platform-specific shortcut rendering.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class Shortcut:
    """Keyboard shortcut definition."""
    key: str
    modifiers: list[str] = field(default_factory=list)
    description: str = ""

    def __str__(self) -> str:
        parts = self.modifiers + [self.key]
        return "+".join(parts)

    def render(self, platform: str | None = None) -> str:
        """Render shortcut for specific platform."""
        if platform is None:
            platform = sys.platform

        keys = []
        if platform == "darwin":
            mod_map = {"ctrl": "⌃", "alt": "⌥", "shift": "⇧", "meta": "⌘"}
            for mod in self.modifiers:
                keys.append(mod_map.get(mod.lower(), mod))
            key_map = {
                "left": "←", "right": "→", "up": "↑", "down": "↓",
                "enter": "↩", "escape": "⎋", "space": "␣",
                "backspace": "⌫", "delete": "⌦", "tab": "⇥",
                "home": "↖", "end": "↘", "pageup": "⇞", "pagedown": "⇟",
            }
            keys.append(key_map.get(self.key.lower(), self.key.upper()))
        else:
            mod_map = {"ctrl": "Ctrl+", "alt": "Alt+", "shift": "Shift+", "meta": "Win+"}
            for mod in self.modifiers:
                keys.append(mod_map.get(mod.lower(), mod + "+"))
            keys.append(self.key.upper())

        return "".join(keys)

    def matches(
        self,
        key: str,
        modifiers: list[str] | None = None,
    ) -> bool:
        """Check if key/modifier combo matches this shortcut."""
        if key.lower() != self.key.lower():
            return False
        mods = set(m.lower() for m in (modifiers or []))
        expected = set(m.lower() for m in self.modifiers)
        return mods == expected


# Common modifier keys
MODIFIERS = ["ctrl", "alt", "shift", "meta"]

# Modifier key codes
MODIFIER_CODES = {
    "ctrl": {"darwin": "control", "win32": "control", "linux": "control"},
    "alt": {"darwin": "option", "win32": "alt", "linux": "alt"},
    "shift": {"darwin": "shift", "win32": "shift", "linux": "shift"},
    "meta": {"darwin": "meta", "win32": "meta", "linux": "meta"},
}


def parse_shortcut_string(s: str) -> Shortcut:
    """
    Parse shortcut string like 'Ctrl+Shift+S'.

    Args:
        s: Shortcut string

    Returns:
        Shortcut object
    """
    parts = s.replace(" ", "").split("+")
    if len(parts) < 2:
        raise ValueError(f"Invalid shortcut string: {s}")
    key = parts[-1]
    modifiers = [m.lower() for m in parts[:-1]]
    return Shortcut(key=key, modifiers=modifiers)


def get_modifier_code(modifier: str, platform: str | None = None) -> str:
    """Get platform-specific modifier code."""
    if platform is None:
        platform = sys.platform
    return MODIFIER_CODES.get(modifier.lower(), {}).get(platform, modifier)


@dataclass
class ShortcutRegistry:
    """Registry of keyboard shortcuts."""

    def __init__(self):
        self._shortcuts: dict[str, Shortcut] = {}

    def register(
        self,
        name: str,
        shortcut: Shortcut,
    ) -> None:
        self._shortcuts[name] = shortcut

    def get(self, name: str) -> Shortcut | None:
        return self._shortcuts.get(name)

    def find(
        self,
        key: str,
        modifiers: list[str] | None = None,
    ) -> str | None:
        """Find shortcut name matching key/modifiers."""
        for name, shortcut in self._shortcuts.items():
            if shortcut.matches(key, modifiers):
                return name
        return None

    def render_all(self, platform: str | None = None) -> dict[str, str]:
        """Render all shortcuts for platform."""
        return {name: s.render(platform) for name, s in self._shortcuts.items()}


# Predefined common shortcuts
COMMON_SHORTCUTS = {
    "copy": Shortcut("c", ["ctrl"], "Copy"),
    "paste": Shortcut("v", ["ctrl"], "Paste"),
    "cut": Shortcut("x", ["ctrl"], "Cut"),
    "undo": Shortcut("z", ["ctrl"], "Undo"),
    "redo": Shortcut("z", ["ctrl", "shift"], "Redo"),
    "select_all": Shortcut("a", ["ctrl"], "Select All"),
    "save": Shortcut("s", ["ctrl"], "Save"),
    "find": Shortcut("f", ["ctrl"], "Find"),
    "quit": Shortcut("q", ["ctrl"], "Quit"),
    "close": Shortcut("w", ["ctrl"], "Close"),
    "new_tab": Shortcut("t", ["ctrl"], "New Tab"),
    "close_tab": Shortcut("w", ["ctrl"], "Close Tab"),
    "refresh": Shortcut("r", ["ctrl"], "Refresh"),
    "fullscreen": Shortcut("f", ["ctrl", "shift"], "Fullscreen"),
    "zoom_in": Shortcut("=", ["ctrl"], "Zoom In"),
    "zoom_out": Shortcut("-", ["ctrl"], "Zoom Out"),
    "reset_zoom": Shortcut("0", ["ctrl"], "Reset Zoom"),
}
