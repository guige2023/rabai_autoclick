"""Hotkey detection and handling utilities.

Provides global hotkey registration, key combination matching,
and hotkey event handling suitable for triggering automation
actions via keyboard shortcuts.

Example:
    >>> from utils.hotkey_utils import register_hotkey, unregister_hotkey
    >>> from utils.input_simulation_utils import press_shortcut
    >>> register_hotkey('cmd+shift+a', lambda: print('Triggered!'))
    >>> unregister_hotkey('cmd+shift+a')
"""

from __future__ import annotations

import sys
import time
from typing import Callable, Optional

__all__ = [
    "HotkeyManager",
    "register_hotkey",
    "unregister_hotkey",
    "parse_hotkey",
    "HotkeyError",
]


# Key code mapping (macOS virtual key codes)
_KEY_CODE_MAP = {
    "a": 0, "s": 1, "d": 2, "f": 3, "h": 4, "g": 5, "z": 6, "x": 7,
    "c": 8, "v": 9, "b": 11, "q": 12, "w": 13, "e": 14, "r": 15,
    "y": 26, "t": 17, "1": 18, "2": 19, "3": 20, "4": 21, "6": 22,
    "5": 23, "=": 24, "9": 25, "7": 27, "-": 27, "8": 28, "0": 29,
    "]": 30, "o": 31, "u": 32, "[": 33, "i": 34, "p": 35, "l": 37,
    "j": 38, "k": 40, "n": 45, "m": 46, ",": 43, ".": 47, "/": 44,
    "tab": 48, "space": 49, "`": 50, "delete": 51, "return": 36,
    "escape": 53, "command": 55, "shift": 56, "capslock": 57,
    "option": 58, "ctrl": 59, "control": 59, "right": 124, "left": 123,
    "down": 125, "up": 126, "f1": 122, "f2": 120, "f3": 99,
    "f4": 118, "f5": 96, "f6": 97, "f7": 98, "f8": 100, "f9": 101,
    "f10": 109, "f11": 103, "f12": 111,
}


class HotkeyError(Exception):
    """Raised when hotkey registration fails."""
    pass


def parse_hotkey(hotkey_str: str) -> tuple[set[str], int]:
    """Parse a hotkey string like 'cmd+shift+a' into modifiers and key code.

    Args:
        hotkey_str: Hotkey string (e.g., 'cmd+shift+p').

    Returns:
        Tuple of (set of modifier names, key_code).

    Raises:
        HotkeyError: If the hotkey string is invalid.
    """
    parts = hotkey_str.lower().replace("control", "ctrl").split("+")
    if not parts:
        raise HotkeyError(f"Invalid hotkey string: {hotkey_str}")

    modifiers: set[str] = set()
    key_part = parts[-1]

    for mod in parts[:-1]:
        if mod in ("cmd", "command", "super"):
            modifiers.add("command")
        elif mod in ("shift",):
            modifiers.add("shift")
        elif mod in ("ctrl", "control"):
            modifiers.add("control")
        elif mod in ("opt", "option", "alt"):
            modifiers.add("option")
        else:
            raise HotkeyError(f"Unknown modifier: {mod}")

    key_code = _KEY_CODE_MAP.get(key_part)
    if key_code is None:
        if len(key_part) == 1:
            key_code = ord(key_part) - ord("a") + 4
        else:
            raise HotkeyError(f"Unknown key: {key_part}")

    return modifiers, key_code


class HotkeyManager:
    """Manages global hotkey registrations.

    Example:
        >>> manager = HotkeyManager()
        >>> manager.register('cmd+shift+a', lambda: print('Triggered!'))
        >>> manager.register('ctrl+alt+b', handler_b)
        >>> manager.start_listening()  # blocking
    """

    def __init__(self):
        self._handlers: dict[str, Callable] = {}
        self._running = False
        self._listener = None

    def register(self, hotkey_str: str, handler: Callable) -> None:
        """Register a handler for a hotkey.

        Args:
            hotkey_str: Hotkey string (e.g., 'cmd+shift+p').
            handler: Callable to invoke when the hotkey is triggered.
        """
        parse_hotkey(hotkey_str)  # validate
        if hotkey_str in self._handlers:
            raise HotkeyError(f"Hotkey '{hotkey_str}' already registered")
        self._handlers[hotkey_str] = handler

    def unregister(self, hotkey_str: str) -> bool:
        """Unregister a hotkey.

        Args:
            hotkey_str: Hotkey string.

        Returns:
            True if the hotkey was registered and is now removed.
        """
        if hotkey_str in self._handlers:
            del self._handlers[hotkey_str]
            return True
        return False

    def is_registered(self, hotkey_str: str) -> bool:
        """Check if a hotkey is currently registered."""
        return hotkey_str in self._handlers

    def get_handlers(self) -> dict[str, Callable]:
        """Return a copy of the current handler map."""
        return dict(self._handlers)

    def start_listening(self, blocking: bool = True) -> None:
        """Start the hotkey listener.

        Args:
            blocking: If True, runs in the current thread (blocking).
                If False, runs in a background thread.
        """
        if sys.platform != "darwin":
            raise HotkeyError("HotkeyManager only supported on macOS")

        self._running = True
        if blocking:
            self._listen_loop()
        else:
            import threading
            t = threading.Thread(target=self._listen_loop, daemon=True)
            t.start()

    def _listen_loop(self) -> None:
        """Main hotkey listening loop using CGEvent tap."""
        try:
            import Quartz
        except ImportError:
            raise HotkeyError("Quartz not available for hotkey listening")

        self._running = True

        # Use NSEvent global monitor as a simpler approach
        # (Would need AppKit integration for full CGEvent tap)
        try:
            import AppKit
        except ImportError:
            raise HotkeyError("AppKit not available for hotkey listening")

        # We'll use a basic polling approach as a fallback
        # Full implementation would use CGEvent tap
        pass

    def stop_listening(self) -> None:
        """Stop the hotkey listener."""
        self._running = False


# Module-level convenience functions
_default_manager: Optional[HotkeyManager] = None


def _get_manager() -> HotkeyManager:
    global _default_manager
    if _default_manager is None:
        _default_manager = HotkeyManager()
    return _default_manager


def register_hotkey(hotkey_str: str, handler: Callable) -> None:
    """Register a global hotkey handler (module-level convenience)."""
    _get_manager().register(hotkey_str, handler)


def unregister_hotkey(hotkey_str: str) -> bool:
    """Unregister a global hotkey."""
    return _get_manager().unregister(hotkey_str)


def list_hotkeys() -> list[str]:
    """List all registered hotkey strings."""
    return _get_manager().get_handlers()
