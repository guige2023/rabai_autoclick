"""Keyboard shortcut execution utilities.

Provides a higher-level interface for executing keyboard shortcuts
and key combinations, with support for modifier keys,
common shortcuts, and key sequence playback.

Example:
    >>> from utils.keyboard_shortcut_utils import shortcut, key_seq, type_human
    >>> shortcut('cmd', 's')      # Cmd+S (save)
    >>> shortcut('cmd', 'shift', 'n')  # Cmd+Shift+N
    >>> key_seq('hello world')
    >>> type_human('Hello World', speed=0.1)
"""

from __future__ import annotations

import time
import random

__all__ = [
    "shortcut",
    "key_seq",
    "type_human",
    "KEYS",
]


# Common key aliases
KEYS = {
    # Modifiers
    "cmd": "command",
    "command": "command",
    "ctrl": "control",
    "control": "control",
    "opt": "option",
    "option": "option",
    "alt": "option",
    "shift": "shift",
    # Navigation
    "return": "return",
    "enter": "return",
    "tab": "tab",
    "escape": "escape",
    "esc": "escape",
    "space": "space",
    "delete": "delete",
    "backspace": "delete",
    "forward_delete": "forward delete",
    "up": "up",
    "down": "down",
    "left": "left",
    "right": "right",
    "home": "home",
    "end": "end",
    "pageup": "page up",
    "pagedown": "page down",
    # Editing
    "copy": "copy",
    "cut": "cut",
    "paste": "paste",
    "undo": "undo",
    "redo": "redo",
    "selectall": "select all",
    # F keys
    "f1": "f1",
    "f2": "f2",
    "f3": "f3",
    "f4": "f4",
    "f5": "f5",
    "f6": "f6",
    "f7": "f7",
    "f8": "f8",
    "f9": "f9",
    "f10": "f10",
    "f11": "f11",
    "f12": "f12",
    # Letters
    **{chr(c): chr(c) for c in range(ord("a"), ord("z") + 1)},
    **{chr(c): chr(c) for c in range(ord("A"), ord("Z") + 1)},
    # Numbers
    **{str(n): str(n) for n in range(10)},
}


def shortcut(*keys: str) -> bool:
    """Execute a keyboard shortcut (e.g., shortcut('cmd', 's') for Cmd+S).

    Args:
        *keys: Key combination components. Last element is the key,
            preceding elements are modifiers.

    Returns:
        True if successful.
    """
    try:
        from utils.input_simulation_utils import press_shortcut
    except ImportError:
        return False

    try:
        press_shortcut(*keys)
        return True
    except Exception:
        return False


def key_seq(keys: str, interval: float = 0.01) -> bool:
    """Type a sequence of keys without modifiers.

    Args:
        keys: String of characters to type.
        interval: Delay between keystrokes.

    Returns:
        True if successful.
    """
    try:
        from utils.input_simulation_utils import press_key
    except ImportError:
        return False

    try:
        for char in keys:
            press_key(char)
            if interval > 0:
                time.sleep(interval)
        return True
    except Exception:
        return False


def type_human(
    text: str,
    speed: float = 0.05,
    jitter: float = 0.02,
) -> bool:
    """Type text with human-like variation in timing.

    Args:
        text: Text string to type.
        speed: Base interval between keystrokes in seconds.
        jitter: Maximum random deviation in seconds.

    Returns:
        True if successful.
    """
    try:
        from utils.input_simulation_utils import type_text
    except ImportError:
        return False

    try:
        for char in text:
            type_text(char, interval=0)
            base = max(0.01, speed + random.uniform(-jitter, jitter))
            time.sleep(base)
        return True
    except Exception:
        return False


# Common shortcut presets
SHORTCUTS = {
    "copy": ("cmd", "c"),
    "cut": ("cmd", "x"),
    "paste": ("cmd", "v"),
    "undo": ("cmd", "z"),
    "redo": ("cmd", "shift", "z"),
    "select_all": ("cmd", "a"),
    "save": ("cmd", "s"),
    "close": ("cmd", "w"),
    "quit": ("cmd", "q"),
    "new": ("cmd", "n"),
    "open": ("cmd", "o"),
    "print": ("cmd", "p"),
    "find": ("cmd", "f"),
    "replace": ("cmd", "alt", "f"),
    "tab_next": ("ctrl", "tab"),
    "tab_prev": ("ctrl", "shift", "tab"),
    "spotlight": ("cmd", "space"),
    "screenshot": ("cmd", "shift", "3"),
    "screenshot_selection": ("cmd", "shift", "4"),
}


def execute_preset(name: str) -> bool:
    """Execute a named shortcut preset.

    Args:
        name: Preset name from the SHORTCUTS dictionary.

    Returns:
        True if successful.

    Example:
        >>> execute_preset('copy')
        >>> execute_preset('save')
    """
    if name not in SHORTCUTS:
        return False
    return shortcut(*SHORTCUTS[name])
