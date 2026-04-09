"""
Key tap and key press simulation utilities.

Provides utilities for simulating individual key presses, key taps,
and sequences of key events. Complements existing key_sequence_utils
with a more granular key-by-key interface.

Example:
    >>> from utils.key_tap_utils import tap_key, tap_keys, tap_with_modifier
    >>> tap_key("a")
    >>> tap_keys(["h", "e", "l", "l", "o"])
    >>> tap_with_modifier("s", modifier="cmd")
"""

from __future__ import annotations

import subprocess
import time
from typing import List, Optional, Sequence

try:
    from dataclasses import dataclass
except ImportError:
    from typing import dataclass


# ----------------------------------------------------------------------
# Data Structures
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class KeyEvent:
    """Represents a single key event."""
    key: str
    key_code: Optional[int] = None
    modifiers: Optional[List[str]] = None

    def __post_init__(self) -> None:
        if self.modifiers is None:
            object.__setattr__(self, 'modifiers', [])


@dataclass
class TapOptions:
    """Options for key tap operations."""
    delay_before: float = 0.0
    delay_after: float = 0.05
    hold_duration: float = 0.0
    repeat: int = 1


# ----------------------------------------------------------------------
# Platform Detection
# ----------------------------------------------------------------------


def get_platform() -> str:
    """Get the current operating system platform."""
    import sys
    return sys.platform


# ----------------------------------------------------------------------
# Key Code Mapping (macOS virtual key codes)
# ----------------------------------------------------------------------


MAC_VKEY_CODES: dict = {
    "a": 0, "s": 1, "d": 2, "f": 3, "h": 4, "g": 5, "z": 6, "x": 7,
    "c": 8, "v": 9, "b": 11, "q": 12, "w": 13, "e": 14, "r": 15,
    "y": 16, "t": 17, "1": 18, "2": 19, "3": 20, "4": 21, "6": 22,
    "5": 23, "=": 24, "9": 25, "7": 26, "-": 27, "8": 28, "0": 29,
    "]": 30, "o": 31, "u": 32, "[": 33, "i": 34, "p": 35, "l": 37,
    "j": 38, "'": 39, "k": 40, ";": 41, "\\": 42, ",": 43, "/": 44,
    "n": 45, "m": 46, ".": 47, " ": 49, "enter": 36, "return": 36,
    "tab": 48, "escape": 53, "esc": 53, "shift": 56, "control": 59,
    "option": 58, "alt": 58, "cmd": 55, "command": 55, "right_shift": 57,
    "right_control": 62, "right_option": 61, "right_cmd": 54,
    "delete": 51, "forward_delete": 117, "forward delete": 117,
    "up": 126, "down": 125, "left": 123, "right": 124,
    "page_up": 116, "page down": 121, "home": 115, "end": 119,
    "f1": 122, "f2": 120, "f3": 99, "f4": 118, "f5": 96, "f6": 97,
    "f7": 98, "f8": 100, "f9": 101, "f10": 109, "f11": 103, "f12": 111,
}


def get_key_code(key: str) -> Optional[int]:
    """
    Get the virtual key code for a key on macOS.

    Args:
        key: Key name (e.g., 'a', 'enter', 'cmd').

    Returns:
        Virtual key code if found, None otherwise.
    """
    return MAC_VKEY_CODES.get(key.lower())


# ----------------------------------------------------------------------
# Modifier Normalization
# ----------------------------------------------------------------------


def normalize_modifier(modifier: str) -> str:
    """
    Normalize modifier key names to a consistent format.

    Args:
        modifier: Raw modifier name.

    Returns:
        Normalized modifier name.

    Examples:
        >>> normalize_modifier("command")  # 'cmd'
        >>> normalize_modifier("option")   # 'alt'
    """
    mapping = {
        "command": "cmd",
        "ctrl": "control",
        "ctl": "control",
        "opt": "alt",
    }
    key = modifier.lower()
    return mapping.get(key, key)


# ----------------------------------------------------------------------
# Core Key Tap Functions
# ----------------------------------------------------------------------


def tap_key(
    key: str,
    options: Optional[TapOptions] = None,
) -> bool:
    """
    Tap (press and release) a single key.

    Args:
        key: The key to tap (e.g., 'a', 'enter', 'space').
        options: Optional tap configuration.

    Returns:
        True if the tap succeeded, False otherwise.

    Example:
        >>> tap_key("space")
        >>> tap_key("enter")
    """
    opts = options or TapOptions()

    if opts.delay_before > 0:
        time.sleep(opts.delay_before)

    success = _execute_key_event(key, key_down=True) and \
              _execute_key_event(key, key_down=False)

    if opts.delay_after > 0:
        time.sleep(opts.delay_after)

    return success


def tap_keys(
    keys: Sequence[str],
    delay_between: float = 0.05,
) -> bool:
    """
    Tap a sequence of keys in order.

    Args:
        keys: Sequence of key names to tap.
        delay_between: Delay in seconds between each key tap.

    Returns:
        True if all taps succeeded, False otherwise.

    Example:
        >>> tap_keys(["h", "e", "l", "l", "o"])
    """
    for key in keys:
        if not tap_key(key):
            return False
        time.sleep(delay_between)
    return True


def tap_with_modifier(
    key: str,
    modifier: str = "cmd",
    options: Optional[TapOptions] = None,
) -> bool:
    """
    Tap a key while holding a modifier key (e.g., Cmd+S).

    Args:
        key: The main key to tap.
        modifier: Modifier key ('cmd', 'shift', 'ctrl', 'alt', 'option').
        options: Optional tap configuration.

    Returns:
        True if the operation succeeded, False otherwise.

    Example:
        >>> tap_with_modifier("s", "cmd")    # Cmd+S (save)
        >>> tap_with_modifier("a", "cmd")    # Cmd+A (select all)
        >>> tap_with_modifier("v", "cmd")    # Cmd+V (paste)
    """
    norm_mod = normalize_modifier(modifier)
    opts = options or TapOptions()

    if opts.delay_before > 0:
        time.sleep(opts.delay_before)

    # Press modifier
    if not _execute_key_event(norm_mod, key_down=True):
        return False

    # Tap the key
    if not _execute_key_event(key, key_down=True) or \
       not _execute_key_event(key, key_down=False):
        _execute_key_event(norm_mod, key_down=False)
        return False

    # Release modifier
    if not _execute_key_event(norm_mod, key_down=False):
        return False

    if opts.delay_after > 0:
        time.sleep(opts.delay_after)

    return True


def tap_with_modifiers(
    key: str,
    modifiers: Sequence[str],
    options: Optional[TapOptions] = None,
) -> bool:
    """
    Tap a key while holding multiple modifier keys.

    Args:
        key: The main key to tap.
        modifiers: Sequence of modifier keys.
        options: Optional tap configuration.

    Returns:
        True if the operation succeeded, False otherwise.

    Example:
        >>> tap_with_modifiers("s", ["cmd", "shift"])  # Cmd+Shift+S
    """
    opts = options or TapOptions()
    norm_mods = [normalize_modifier(m) for m in modifiers]

    if opts.delay_before > 0:
        time.sleep(opts.delay_before)

    # Press all modifiers
    for mod in norm_mods:
        if not _execute_key_event(mod, key_down=True):
            return False

    # Tap the key
    if not _execute_key_event(key, key_down=True) or \
       not _execute_key_event(key, key_down=False):
        for mod in reversed(norm_mods):
            _execute_key_event(mod, key_down=False)
        return False

    # Release all modifiers in reverse order
    for mod in reversed(norm_mods):
        if not _execute_key_event(mod, key_down=False):
            return False

    if opts.delay_after > 0:
        time.sleep(opts.delay_after)

    return True


# ----------------------------------------------------------------------
# Internal Execution
# ----------------------------------------------------------------------


def _execute_key_event(key: str, key_down: bool) -> bool:
    """
    Execute a key event using macOS CGEvent.

    Args:
        key: Key name.
        key_down: True for key down, False for key up.

    Returns:
        True if successful, False otherwise.
    """
    if get_platform() != "darwin":
        return False

    import Quartz.CoreGraphics as CG

    key_code = get_key_code(key)
    if key_code is None:
        return False

    try:
        event_type = CG.kCGEventKeyDown if key_down else CG.kCGEventKeyUp
        event = CG.CGEventCreateKeyboardEvent(None, key_code, key_down)
        if event is None:
            return False
        CG.CGEventPost(CG.kCGHIDEventTap, event)
        return True
    except Exception:
        # Fallback to osascript for special keys
        return _execute_key_event_osascript(key, key_down)


def _execute_key_event_osascript(key: str, key_down: bool) -> bool:
    """
    Fallback key event via osascript for special keys.

    Args:
        key: Key name.
        key_down: True for key down, False for key up.

    Returns:
        True if successful, False otherwise.
    """
    action = "key down" if key_down else "key up"
    script = f'''
    tell application "System Events"
        keystroke "{key}" {action}
    end tell
    '''
    # Only works for alphanumeric keys via osascript
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            timeout=2,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


# ----------------------------------------------------------------------
# Hold Key
# ----------------------------------------------------------------------


def hold_key(key: str, duration: float = 1.0) -> bool:
    """
    Press and hold a key for a specified duration.

    Args:
        key: The key to hold.
        duration: Duration in seconds to hold the key.

    Returns:
        True if the operation succeeded, False otherwise.

    Example:
        >>> hold_key("a", 2.0)  # hold 'a' for 2 seconds
    """
    if not _execute_key_event(key, key_down=True):
        return False
    time.sleep(duration)
    if not _execute_key_event(key, key_down=False):
        return False
    return True


def type_text(text: str, delay_between_chars: float = 0.05) -> bool:
    """
    Type a string of text character by character.

    Args:
        text: The text to type.
        delay_between_chars: Delay in seconds between each character.

    Returns:
        True if all characters were typed successfully.

    Example:
        >>> type_text("Hello, World!")
    """
    for char in text:
        if char == " ":
            success = tap_key("space")
        elif char == "\n":
            success = tap_key("enter")
        else:
            # For alphanumeric chars, try using the character directly
            success = _type_character(char)
        if not success:
            return False
        time.sleep(delay_between_chars)
    return True


def _type_character(char: str) -> bool:
    """
    Type a single character using CGEvent.

    Args:
        char: Single character to type.

    Returns:
        True if successful, False otherwise.
    """
    if get_platform() != "darwin":
        return False

    import Quartz.CoreGraphics as CG

    try:
        key_down = CG.CGEventCreateKeyboardEvent(None, 0, True)
        key_up = CG.CGEventCreateKeyboardEvent(None, 0, False)
        if key_down is None or key_up is None:
            return False

        # Create Unicode event for the character
        uni_char = ord(char)
        key_down.setIntegerValueField(
            CG.kCGKeyboardEventKeycode, 0
        )
        key_down.setUnicodeString(1, [uni_char])

        CG.CGEventPost(CG.kCGHIDEventTap, key_down)
        time.sleep(0.01)
        CG.CGEventPost(CG.kCGHIDEventTap, key_up)
        return True
    except Exception:
        return False
