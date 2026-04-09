"""
Key hold and key sequence utilities.

Provides utilities for holding keys for extended periods, key combinations
with held modifiers, and releasing keys explicitly. Useful for keyboard
navigation, gaming controls, and accessibility accommodations.

Example:
    >>> from utils.key_hold_utils import hold_key, hold_with_modifier, release_key
    >>> hold_key("shift", duration=3.0)
    >>> hold_with_modifier("right", modifier="shift", duration=1.0)
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, Sequence

try:
    from dataclasses import dataclass
except ImportError:
    from typing import dataclass


# ----------------------------------------------------------------------
# Data Structures
# ----------------------------------------------------------------------


@dataclass
class HoldOptions:
    """Options for key hold operations."""
    delay_start: float = 0.0
    delay_end: float = 0.0
    with_release: bool = True
    on_release_tap: Optional[str] = None


@dataclass(frozen=True)
class KeyHoldState:
    """Immutable state of a key hold operation."""
    key: str
    key_code: int
    is_held: bool
    hold_start_time: Optional[float] = None
    hold_duration: float = 0.0


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
    "y": 16, "t": 17, "up": 126, "down": 125, "left": 123, "right": 124,
    "space": 49, "enter": 36, "return": 36, "tab": 48, "escape": 53,
    "esc": 53, "shift": 56, "control": 59, "option": 58, "alt": 58,
    "cmd": 55, "command": 55, "delete": 51, "forward_delete": 117,
    "page_up": 116, "page_down": 121, "home": 115, "end": 119,
}


def get_key_code(key: str) -> Optional[int]:
    """
    Get the virtual key code for a key.

    Args:
        key: Key name.

    Returns:
        Virtual key code if found, None otherwise.
    """
    return MAC_VKEY_CODES.get(key.lower())


# ----------------------------------------------------------------------
# Internal Key Event Execution
# ----------------------------------------------------------------------


def _press_key(key: str) -> bool:
    """Press a key down."""
    if get_platform() != "darwin":
        return False

    try:
        import Quartz.CoreGraphics as CG
        key_code = get_key_code(key)
        if key_code is None:
            return False
        event = CG.CGEventCreateKeyboardEvent(None, key_code, True)
        if event is None:
            return False
        CG.CGEventPost(CG.kCGHIDEventTap, event)
        return True
    except Exception:
        return False


def _release_key(key: str) -> bool:
    """Release a key."""
    if get_platform() != "darwin":
        return False

    try:
        import Quartz.CoreGraphics as CG
        key_code = get_key_code(key)
        if key_code is None:
            return False
        event = CG.CGEventCreateKeyboardEvent(None, key_code, False)
        if event is None:
            return False
        CG.CGEventPost(CG.kCGHIDEventTap, event)
        return True
    except Exception:
        return False


# ----------------------------------------------------------------------
# Core Hold Functions
# ----------------------------------------------------------------------


def hold_key(
    key: str,
    duration: float = 1.0,
    options: Optional[HoldOptions] = None,
) -> bool:
    """
    Press and hold a key for a specified duration.

    Args:
        key: The key to hold (e.g., 'shift', 'cmd', 'right').
        duration: Duration in seconds to hold the key.
        options: Optional hold configuration.

    Returns:
        True if the operation succeeded, False otherwise.

    Example:
        >>> hold_key("shift", duration=3.0)  # hold Shift for 3 seconds
        >>> hold_key("right", duration=0.5)  # hold right arrow for 0.5s
    """
    opts = options or HoldOptions()

    if opts.delay_start > 0:
        time.sleep(opts.delay_start)

    if not _press_key(key):
        return False

    time.sleep(duration)

    if opts.with_release:
        if not _release_key(key):
            return False

    if opts.on_release_tap:
        tap_key_simple(opts.on_release_tap)

    if opts.delay_end > 0:
        time.sleep(opts.delay_end)

    return True


def release_key(key: str) -> bool:
    """
    Explicitly release a held key.

    Args:
        key: The key to release.

    Returns:
        True if the key was released, False otherwise.

    Example:
        >>> # Start holding
        >>> _press_key("shift")
        >>> # ... do other things ...
        >>> release_key("shift")
    """
    return _release_key(key)


def hold_with_modifier(
    key: str,
    modifier: str = "shift",
    duration: float = 1.0,
    options: Optional[HoldOptions] = None,
) -> bool:
    """
    Hold a key while a modifier is active.

    Args:
        key: The main key to hold.
        modifier: The modifier key ('cmd', 'shift', 'ctrl', 'alt').
        duration: Duration in seconds to hold.
        options: Optional hold configuration.

    Returns:
        True if the operation succeeded, False otherwise.

    Example:
        >>> # Hold Shift + Right arrow for 1 second (select text)
        >>> hold_with_modifier("right", "shift", duration=1.0)
        >>> # Hold Cmd + W for 1 second (force close)
        >>> hold_with_modifier("w", "cmd", duration=1.0)
    """
    opts = options or HoldOptions()

    if opts.delay_start > 0:
        time.sleep(opts.delay_start)

    # Press modifier first
    if not _press_key(modifier):
        return False

    # Press the main key
    if not _press_key(key):
        _release_key(modifier)
        return False

    # Hold
    time.sleep(duration)

    # Release main key
    if not _release_key(key):
        _release_key(modifier)
        return False

    # Release modifier
    if not _release_key(modifier):
        return False

    if opts.delay_end > 0:
        time.sleep(opts.delay_end)

    return True


def hold_with_modifiers(
    key: str,
    modifiers: Sequence[str],
    duration: float = 1.0,
    options: Optional[HoldOptions] = None,
) -> bool:
    """
    Hold a key while multiple modifiers are active.

    Args:
        key: The main key to hold.
        modifiers: Sequence of modifier keys.
        duration: Duration in seconds to hold.
        options: Optional hold configuration.

    Returns:
        True if the operation succeeded, False otherwise.

    Example:
        >>> # Hold Cmd+Shift+4 (macOS screenshot)
        >>> hold_with_modifiers("4", ["cmd", "shift"], duration=0.5)
    """
    opts = options or HoldOptions()

    if opts.delay_start > 0:
        time.sleep(opts.delay_start)

    # Press all modifiers
    for mod in modifiers:
        if not _press_key(mod):
            # Rollback already-pressed modifiers
            for m in modifiers[:modifiers.index(mod)]:
                _release_key(m)
            return False

    # Press the main key
    if not _press_key(key):
        for mod in reversed(modifiers):
            _release_key(mod)
        return False

    time.sleep(duration)

    # Release main key
    if not _release_key(key):
        for mod in reversed(modifiers):
            _release_key(mod)
        return False

    # Release all modifiers in reverse
    for mod in reversed(modifiers):
        if not _release_key(mod):
            return False

    if opts.delay_end > 0:
        time.sleep(opts.delay_end)

    return True


# ----------------------------------------------------------------------
# Key State Tracking
# ----------------------------------------------------------------------


_active_holds: dict = {}


def start_hold(
    key: str,
    options: Optional[HoldOptions] = None,
) -> bool:
    """
    Start holding a key without releasing it. Must call end_hold()
    to release. Useful for long-running hold operations.

    Args:
        key: The key to hold.
        options: Optional configuration.

    Returns:
        True if the hold started successfully.

    Example:
        >>> start_hold("space")
        >>> # ... do other things while space is held ...
        >>> end_hold("space")
    """
    global _active_holds

    if key in _active_holds:
        return False  # Already being held

    if not _press_key(key):
        return False

    _active_holds[key] = {
        "start_time": time.time(),
        "options": options or HoldOptions(),
    }
    return True


def end_hold(key: str, tap_on_release: Optional[str] = None) -> bool:
    """
    End a key hold started by start_hold().

    Args:
        key: The key to release.
        tap_on_release: Optional key to tap immediately after releasing.

    Returns:
        True if the hold ended successfully.

    Example:
        >>> start_hold("space")
        >>> # ... long operation ...
        >>> end_hold("space", tap_on_release="return")
    """
    global _active_holds

    if key not in _active_holds:
        return False

    if not _release_key(key):
        return False

    hold_info = _active_holds.pop(key)

    if tap_on_release:
        _simple_tap(tap_on_release)

    return True


def is_key_held(key: str) -> bool:
    """
    Check if a key is currently being held by this utility.

    Args:
        key: The key to check.

    Returns:
        True if the key is currently held by this utility.
    """
    return key in _active_holds


def tap_key_simple(key: str) -> bool:
    """Simple tap (press + release) without options."""
    if not _press_key(key):
        return False
    time.sleep(0.01)
    return _release_key(key)


def _simple_tap(key: str) -> bool:
    """Alias for tap_key_simple."""
    return tap_key_simple(key)


# ----------------------------------------------------------------------
# Arrow Navigation Utilities
# ----------------------------------------------------------------------


def hold_arrow(
    direction: str,
    duration: float = 0.5,
    modifier: Optional[str] = None,
) -> bool:
    """
    Hold an arrow key with optional modifier.

    Args:
        direction: One of 'up', 'down', 'left', 'right'.
        duration: Duration to hold in seconds.
        modifier: Optional modifier ('shift', 'cmd', 'ctrl', 'alt').

    Returns:
        True if the operation succeeded.

    Example:
        >>> hold_arrow("right", 1.0)            # hold right arrow 1s
        >>> hold_arrow("right", 1.0, "shift")    # hold Shift+Right 1s
    """
    valid_directions = {"up", "down", "left", "right"}
    if direction not in valid_directions:
        return False

    if modifier:
        return hold_with_modifier(direction, modifier, duration)
    return hold_key(direction, duration)


def navigate_text(
    direction: str,
    count: int = 1,
    with_selection: bool = False,
    delay: float = 0.05,
) -> bool:
    """
    Navigate through text using arrow keys.

    Args:
        direction: One of 'up', 'down', 'left', 'right'.
        count: Number of key presses.
        with_selection: If True, hold Shift while navigating
            (for text selection).
        delay: Delay between each key press.

    Returns:
        True if all operations succeeded.

    Example:
        >>> navigate_text("right", 5)              # move right 5 chars
        >>> navigate_text("right", 10, True)        # select 10 chars right
    """
    modifier = "shift" if with_selection else None

    for _ in range(count):
        if modifier:
            if not hold_with_modifier(direction, modifier, duration=0.01):
                return False
        else:
            if not hold_key(direction, duration=0.01):
                return False
        time.sleep(delay)

    return True
