"""Hotkey combination parsing and formatting utilities.

This module provides utilities for parsing hotkey strings like
'Ctrl+Shift+A' into structured representations and vice versa.
"""

from __future__ import annotations

import platform
from typing import NamedTuple


IS_MACOS = platform.system() == "Darwin"
IS_LINUX = platform.system() == "Linux"
IS_WINDOWS = platform.system() == "Windows"


# Modifier key constants
MODIFIERS = {"ctrl", "control", "alt", "option", "shift", "cmd", "command", "super", "win"}
SPECIAL_KEYS = {
    "space": "space",
    "enter": "enter",
    "return": "return",
    "tab": "tab",
    "escape": "escape",
    "esc": "escape",
    "up": "up",
    "down": "down",
    "left": "left",
    "right": "right",
    "home": "home",
    "end": "end",
    "pageup": "pageup",
    "pagedown": "pagedown",
    "delete": "delete",
    "backspace": "backspace",
    "f1": "f1", "f2": "f2", "f3": "f3", "f4": "f4",
    "f5": "f5", "f6": "f6", "f7": "f7", "f8": "f8",
    "f9": "f9", "f10": "f10", "f11": "f11", "f12": "f12",
}


class HotkeyCombination(NamedTuple):
    """Represents a parsed hotkey combination."""
    modifiers: tuple[str, ...]
    key: str
    
    def to_pyautogui(self) -> str:
        """Convert to pyautogui hotkey format."""
        parts = list(self.modifiers) + [self.key]
        return "+".join(parts)
    
    def to_mac(self) -> str:
        """Convert to macOS keyboard shortcut format."""
        parts = []
        for mod in self.modifiers:
            if mod in ("ctrl", "control"):
                parts.append("⌃")
            elif mod in ("alt", "option"):
                parts.append("⌥")
            elif mod in ("cmd", "command"):
                parts.append("⌘")
            elif mod == "shift":
                parts.append("⇧")
        parts.append(self.key.upper())
        return "+".join(parts)
    
    def to_windows(self) -> str:
        """Convert to Windows hotkey format."""
        parts = []
        for mod in self.modifiers:
            if mod in ("ctrl", "control"):
                parts.append("Ctrl")
            elif mod == "alt":
                parts.append("Alt")
            elif mod == "shift":
                parts.append("Shift")
            elif mod in ("cmd", "command", "super", "win"):
                parts.append("Win")
        parts.append(self.key.upper())
        return "+".join(parts)
    
    def __str__(self) -> str:
        if IS_MACOS:
            return self.to_mac()
        elif IS_WINDOWS:
            return self.to_windows()
        return self.to_pyautogui()


def parse_hotkey(hotkey_str: str) -> HotkeyCombination:
    """Parse a hotkey string into a structured combination.
    
    Supports formats:
    - 'Ctrl+Shift+A'
    - 'Cmd+Option+Z'
    - 'ctrl-c' (hyphen separator)
    - 'ctrl shift alt p'
    
    Args:
        hotkey_str: String representation of the hotkey.
    
    Returns:
        HotkeyCombination instance.
    
    Raises:
        ValueError: If the hotkey string is invalid.
    """
    # Normalize separators
    normalized = hotkey_str.replace("-", "+").replace(" ", "+")
    parts = [p.strip().lower() for p in normalized.split("+") if p.strip()]
    
    if not parts:
        raise ValueError(f"Invalid hotkey string: {hotkey_str}")
    
    modifiers = []
    key = None
    
    for part in parts:
        if part in MODIFIERS:
            modifiers.append(part)
        elif part in SPECIAL_KEYS:
            key = SPECIAL_KEYS[part]
        elif len(part) == 1:
            key = part.lower()
        elif part.startswith("f") and part[1:].isdigit():
            key = part.lower()
        else:
            key = part.lower()
    
    if key is None:
        raise ValueError(f"No key found in hotkey string: {hotkey_str}")
    
    return HotkeyCombination(tuple(modifiers), key)


def format_hotkey(
    modifiers: list[str],
    key: str,
    format: str = "default",
) -> str:
    """Format a hotkey from separate parts.
    
    Args:
        modifiers: List of modifier key names.
        key: Main key name.
        format: Output format ('default', 'mac', 'windows', 'linux').
    
    Returns:
        Formatted hotkey string.
    """
    combo = HotkeyCombination(tuple(modifiers), key)
    
    if format == "mac":
        return combo.to_mac()
    elif format == "windows":
        return combo.to_windows()
    else:
        return combo.to_pyautogui()


def is_valid_hotkey(hotkey_str: str) -> bool:
    """Check if a string is a valid hotkey combination.
    
    Args:
        hotkey_str: String to validate.
    
    Returns:
        True if valid, False otherwise.
    """
    try:
        parse_hotkey(hotkey_str)
        return True
    except ValueError:
        return False


def normalize_modifier(modifier: str) -> str:
    """Normalize a modifier key name to a canonical form.
    
    Args:
        modifier: Modifier key name (any case).
    
    Returns:
        Normalized modifier name.
    """
    modifier = modifier.lower()
    
    if modifier in ("ctrl", "control"):
        return "ctrl"
    elif modifier in ("alt", "option"):
        return "alt"
    elif modifier in ("cmd", "command"):
        return "cmd"
    elif modifier in ("super", "win", "meta"):
        return "super"
    elif modifier == "shift":
        return "shift"
    return modifier
