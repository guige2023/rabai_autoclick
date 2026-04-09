"""
Keyboard shortcut combination builder and executor.

Provides utilities for building, validating, and executing keyboard
shortcut combinations (e.g., Cmd+Shift+S). Works with existing key
sequence utilities to provide a higher-level combo interface.

Example:
    >>> from utils.key_combo_utils import KeyCombo, execute_combo
    >>> combo = KeyCombo(key="s", modifiers=["cmd"])
    >>> execute_combo(combo)
    >>> # or shortcut:
    >>> execute_combo("cmd+shift+4")
"""

from __future__ import annotations

import re
import subprocess
import time
from typing import List, Optional, Set

try:
    from dataclasses import dataclass, field
except ImportError:
    from typing import dataclass, field


# ----------------------------------------------------------------------
# Data Structures
# ----------------------------------------------------------------------


@dataclass(frozen=True)
class KeyCombo:
    """
    Immutable keyboard shortcut combination.

    Attributes:
        key: The main key (e.g., 's', '4', 'enter').
        modifiers: List of modifier keys ('cmd', 'shift', 'ctrl', 'alt').
        description: Human-readable description of the combo.

    Examples:
        >>> KeyCombo(key="s", modifiers=["cmd"])        # Cmd+S
        >>> KeyCombo(key="4", modifiers=["cmd", "shift"])  # Cmd+Shift+4
        >>> KeyCombo(key="a", modifiers=["ctrl"])        # Ctrl+A
    """
    key: str
    modifiers: tuple = field(default_factory=tuple)
    description: str = ""

    def __post_init__(self) -> None:
        # Ensure modifiers is a tuple for hashability
        if isinstance(self.modifiers, list):
            object.__setattr__(
                self, 'modifiers', tuple(sorted(self.modifiers))
            )

    @property
    def modifier_set(self) -> Set[str]:
        """Return modifiers as a set for fast lookup."""
        return set(self.modifiers)

    @property
    def is_empty(self) -> bool:
        """Return True if no key is set."""
        return not self.key

    def has_modifier(self, modifier: str) -> bool:
        """Check if a specific modifier is in the combo."""
        return modifier.lower() in self.modifier_set

    def __str__(self) -> str:
        """Return string representation like 'Cmd+Shift+S'."""
        parts = [m.capitalize() for m in self.modifiers]
        parts.append(self.key.upper() if len(self.key) == 1 else self.key)
        return "+".join(parts)

    def __repr__(self) -> str:
        return f"KeyCombo(key={self.key!r}, modifiers={list(self.modifiers)!r})"


@dataclass
class ComboExecutionOptions:
    """Options for combo execution."""
    delay_before: float = 0.0
    delay_after: float = 0.05
    delay_between_modifiers: float = 0.01
    error_on_failure: bool = False


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
    "page_up": 116, "page_down": 121, "home": 115, "end": 119,
    "f1": 122, "f2": 120, "f3": 99, "f4": 118, "f5": 96, "f6": 97,
    "f7": 98, "f8": 100, "f9": 101, "f10": 109, "f11": 103, "f12": 111,
}


# ----------------------------------------------------------------------
# Modifier Normalization
# ----------------------------------------------------------------------


MODIFIER_ALIASES: dict = {
    "command": "cmd",
    "ctrl": "control",
    "ctl": "control",
    "opt": "alt",
    "option": "alt",
    "⌘": "cmd",
    "⇧": "shift",
    "⌃": "control",
    "⌥": "alt",
}


def normalize_modifier(mod: str) -> str:
    """Normalize a modifier key name to canonical form."""
    key = mod.lower().strip()
    return MODIFIER_ALIASES.get(key, key)


def get_canonical_modifiers() -> Set[str]:
    """Return the set of canonical modifier names."""
    return {"cmd", "shift", "ctrl", "control", "alt", "option"}


# ----------------------------------------------------------------------
# Combo Parsing
# ----------------------------------------------------------------------


def parse_combo_string(combo_str: str) -> KeyCombo:
    """
    Parse a combo string like 'cmd+shift+4' into a KeyCombo.

    Args:
        combo_str: String representation of the combo.
            Keys separated by '+'. Order doesn't matter.

    Returns:
        KeyCombo object.

    Raises:
        ValueError: If the string cannot be parsed.

    Examples:
        >>> parse_combo_string("cmd+shift+4")
        >>> parse_combo_string("ctrl+c")
        >>> parse_combo_string("alt+tab")
    """
    parts = [p.strip().lower() for p in combo_str.split("+")]
    if not parts:
        raise ValueError(f"Empty combo string: {combo_str!r}")

    # Normalize modifiers
    modifiers: List[str] = []
    key: str = ""

    for part in parts:
        norm = normalize_modifier(part)
        if norm in get_canonical_modifiers() or norm in {
            "cmd", "shift", "control", "alt", "option"
        }:
            if norm not in modifiers:
                modifiers.append(norm)
        else:
            key = part

    if not key:
        raise ValueError(f"No main key found in combo: {combo_str!r}")

    return KeyCombo(key=key, modifiers=tuple(sorted(set(modifiers))))


def build_combo(
    key: str,
    modifiers: Optional[List[str]] = None,
) -> KeyCombo:
    """
    Build a KeyCombo from a key and modifier list.

    Args:
        key: The main key.
        modifiers: List of modifier keys (e.g., ['cmd', 'shift']).

    Returns:
        KeyCombo object.

    Example:
        >>> build_combo("s", ["cmd", "shift"])
    """
    norm_mods = [normalize_modifier(m) for m in (modifiers or [])]
    return KeyCombo(
        key=key.lower(),
        modifiers=tuple(sorted(set(norm_mods))),
    )


# ----------------------------------------------------------------------
# Validation
# ----------------------------------------------------------------------


def validate_combo(combo: KeyCombo) -> bool:
    """
    Validate that a combo has a valid key and modifiers.

    Args:
        combo: The combo to validate.

    Returns:
        True if valid, False otherwise.
    """
    if combo.is_empty:
        return False
    if not get_key_code(combo.key):
        return False
    return True


def get_key_code(key: str) -> Optional[int]:
    """Get the virtual key code for a key."""
    return MAC_VKEY_CODES.get(key.lower())


# ----------------------------------------------------------------------
# Execution
# ----------------------------------------------------------------------


def _press_key(key_code: int) -> bool:
    """Press a key down using CGEvent."""
    if get_platform() != "darwin":
        return False
    try:
        import Quartz.CoreGraphics as CG
        event = CG.CGEventCreateKeyboardEvent(None, key_code, True)
        if event is None:
            return False
        CG.CGEventPost(CG.kCGHIDEventTap, event)
        return True
    except Exception:
        return False


def _release_key(key_code: int) -> bool:
    """Release a key using CGEvent."""
    if get_platform() != "darwin":
        return False
    try:
        import Quartz.CoreGraphics as CG
        event = CG.CGEventCreateKeyboardEvent(None, key_code, False)
        if event is None:
            return False
        CG.CGEventPost(CG.kCGHIDEventTap, event)
        return True
    except Exception:
        return False


def execute_combo(
    combo: KeyCombo,
    options: Optional[ComboExecutionOptions] = None,
) -> bool:
    """
    Execute a keyboard shortcut combination.

    Args:
        combo: The KeyCombo to execute.
        options: Optional execution configuration.

    Returns:
        True if the combo was executed successfully, False otherwise.

    Example:
        >>> combo = KeyCombo(key="s", modifiers=["cmd"])
        >>> execute_combo(combo)
        >>> # Shortcut:
        >>> execute_combo(parse_combo_string("cmd+shift+4"))
    """
    opts = options or ComboExecutionOptions()

    if not validate_combo(combo):
        return False

    if opts.delay_before > 0:
        time.sleep(opts.delay_before)

    # Press modifiers in sorted order
    for mod in sorted(combo.modifiers):
        mod_code = get_key_code(mod)
        if mod_code is None:
            return False
        if not _press_key(mod_code):
            return False
        time.sleep(opts.delay_between_modifiers)

    # Press the main key
    key_code = get_key_code(combo.key)
    if key_code is None:
        for mod in sorted(combo.modifiers):
            _release_key(get_key_code(mod))
        return False

    if not _press_key(key_code):
        for mod in sorted(combo.modifiers):
            _release_key(get_key_code(mod))
        return False

    time.sleep(0.01)

    # Release main key
    if not _release_key(key_code):
        for mod in sorted(combo.modifiers):
            _release_key(get_key_code(mod))
        return False

    # Release modifiers in reverse sorted order
    for mod in sorted(combo.modifiers, reverse=True):
        if not _release_key(get_key_code(mod)):
            return False
        time.sleep(opts.delay_between_modifiers)

    if opts.delay_after > 0:
        time.sleep(opts.delay_after)

    return True


def execute_combo_string(
    combo_str: str,
    options: Optional[ComboExecutionOptions] = None,
) -> bool:
    """
    Parse and execute a combo string directly.

    Args:
        combo_str: String like 'cmd+shift+4'.
        options: Optional execution configuration.

    Returns:
        True if successful, False otherwise.

    Example:
        >>> execute_combo_string("cmd+s")      # Save
        >>> execute_combo_string("cmd+tab")   # App switch
    """
    try:
        combo = parse_combo_string(combo_str)
        return execute_combo(combo, options)
    except ValueError:
        return False


# ----------------------------------------------------------------------
# Batch Execution
# ----------------------------------------------------------------------


def execute_combo_sequence(
    combos: List[KeyCombo],
    delay_between: float = 0.1,
) -> int:
    """
    Execute a sequence of combos with delays between them.

    Args:
        combos: List of KeyCombo objects.
        delay_between: Delay in seconds between each combo.

    Returns:
        Number of combos successfully executed.

    Example:
        >>> sequence = [
        ...     parse_combo_string("cmd+n"),
        ...     parse_combo_string("cmd+v"),
        ...     parse_combo_string("cmd+w"),
        ... ]
        >>> execute_combo_sequence(sequence, delay_between=0.2)
    """
    success_count = 0
    for combo in combos:
        if execute_combo(combo):
            success_count += 1
        else:
            break
        time.sleep(delay_between)
    return success_count
