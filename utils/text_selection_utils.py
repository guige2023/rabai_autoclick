"""
Text selection and manipulation utilities.

Provides utilities for selecting text using keyboard shortcuts,
managing text selection ranges, and performing common text
manipulation operations via keyboard automation.

Example:
    >>> from utils.text_selection_utils import select_all, select_word
    >>> select_all()
    >>> select_word(cursor_position=5)
    >>> delete_selection()
"""

from __future__ import annotations

import subprocess
import time
from typing import Optional, Tuple

try:
    from dataclasses import dataclass
except ImportError:
    from typing import dataclass


# ----------------------------------------------------------------------
# Platform Detection
# ----------------------------------------------------------------------


def get_platform() -> str:
    """Get the current operating system platform."""
    import sys
    return sys.platform


# ----------------------------------------------------------------------
# Key Event Helpers (using CGEvent)
# ----------------------------------------------------------------------


MAC_VKEY_CODES: dict = {
    "a": 0, "s": 1, "d": 2, "f": 3, "h": 4, "g": 5, "z": 6, "x": 7,
    "c": 8, "v": 9, "b": 11, "q": 12, "w": 13, "e": 14, "r": 15,
    "y": 16, "t": 17, "left": 123, "right": 124, "down": 125, "up": 126,
    "shift": 56, "cmd": 55, "command": 55, "ctrl": 59, "control": 59,
    "option": 58, "alt": 58, "space": 49, "enter": 36, "return": 36,
    "delete": 51, "forward_delete": 117, "tab": 48, "escape": 53,
    "home": 115, "end": 119, "page_up": 116, "page_down": 121,
}


@dataclass
class SelectionRange:
    """Represents a text selection range."""
    start: int
    end: int

    @property
    def length(self) -> int:
        """Return the length of the selection."""
        return abs(self.end - self.start)

    @property
    def is_empty(self) -> bool:
        """Return True if start equals end (no selection)."""
        return self.start == self.end

    def normalize(self) -> Tuple[int, int]:
        """Return (start, end) with start <= end."""
        return (min(self.start, self.end), max(self.start, self.end))


# ----------------------------------------------------------------------
# Key Simulation Helpers
# ----------------------------------------------------------------------


def _send_key(key: str, key_down: bool = True) -> bool:
    """Send a key event using CGEvent."""
    if get_platform() != "darwin":
        return False
    try:
        import Quartz.CoreGraphics as CG
        key_code = MAC_VKEY_CODES.get(key.lower())
        if key_code is None:
            return False
        event = CG.CGEventCreateKeyboardEvent(None, key_code, key_down)
        if event is None:
            return False
        CG.CGEventPost(CG.kCGHIDEventTap, event)
        return True
    except Exception:
        return False


def _press_modifiers(modifiers: list, key_down: bool = True) -> bool:
    """Press or release a list of modifier keys."""
    for mod in modifiers:
        if not _send_key(mod, key_down):
            return False
    return True


def _combo_press(modifiers: list, key: str) -> bool:
    """Press modifiers, then key, then release all."""
    if not _press_modifiers(modifiers, True):
        return False
    time.sleep(0.01)
    if not _send_key(key):
        _press_modifiers(modifiers, False)
        return False
    time.sleep(0.01)
    if not _send_key(key, False):
        _press_modifiers(modifiers, False)
        return False
    _press_modifiers(modifiers, False)
    return True


# ----------------------------------------------------------------------
# Text Selection Operations
# ----------------------------------------------------------------------


def select_all() -> bool:
    """
    Select all text in the current text field.

    Returns:
        True if the operation succeeded.

    Example:
        >>> select_all()
    """
    return _combo_press(["cmd"], "a")


def select_word(cursor_position: int = 0) -> bool:
    """
    Select the word at or near the cursor position.

    This performs a double-click or equivalent word selection.
    The cursor_position parameter is a hint (0=at cursor, positive=after,
    negative=before) for applications that support it.

    Args:
        cursor_position: Position hint for word selection.

    Returns:
        True if the operation succeeded.

    Example:
        >>> select_word()           # select word at cursor
        >>> select_word(5)          # select word 5 positions after cursor
    """
    if get_platform() != "darwin":
        return False

    try:
        import Quartz.CoreGraphics as CG

        # Double-click to select word
        double_click = CG.CGEventCreateMouseEvent(
            None,
            CG.kCGEventLeftMouseDown,
            CG.CGPoint(0, 0),
            CG.kCGMouseButtonLeft,
        )
        if double_click:
            # Set click count to 2 for double-click
            double_click.setIntegerValueField(
                CG.kCGEventMouseEventClickState, 2
            )
            CG.CGEventPost(CG.kCGHIDEventTap, double_click)

            up_event = CG.CGEventCreateMouseEvent(
                None,
                CG.kCGEventLeftMouseUp,
                CG.CGPoint(0, 0),
                CG.kCGMouseButtonLeft,
            )
            if up_event:
                up_event.setIntegerValueField(
                    CG.kCGEventMouseEventClickState, 2
                )
                CG.CGEventPost(CG.kCGHIDEventTap, up_event)
        return True
    except Exception:
        # Fallback: triple-click to select line, then go back
        return _combo_press(["cmd"], "l")


def select_line() -> bool:
    """
    Select the entire current line.

    Returns:
        True if the operation succeeded.
    """
    # Triple-click to select line
    if get_platform() != "darwin":
        return False

    try:
        import Quartz.CoreGraphics as CG

        for click_state in [1, 2, 3]:
            down = CG.CGEventCreateMouseEvent(
                None, CG.kCGEventLeftMouseDown,
                CG.CGPoint(0, 0), CG.kCGMouseButtonLeft,
            )
            if down:
                down.setIntegerValueField(
                    CG.kCGEventMouseEventClickState, click_state
                )
                CG.CGEventPost(CG.kCGHIDEventTap, down)

            up = CG.CGEventCreateMouseEvent(
                None, CG.kCGEventLeftMouseUp,
                CG.CGPoint(0, 0), CG.kCGMouseButtonLeft,
            )
            if up:
                up.setIntegerValueField(
                    CG.kCGEventMouseEventClickState, click_state
                )
                CG.CGEventPost(CG.kCGHIDEventTap, up)
            time.sleep(0.05)
        return True
    except Exception:
        return False


def select_to_line_start() -> bool:
    """
    Select text from cursor to the start of the current line.

    Returns:
        True if the operation succeeded.

    Example:
        >>> select_to_line_start()  # Cmd+Shift+Left
    """
    return _combo_press(["cmd", "shift"], "left")


def select_to_line_end() -> bool:
    """
    Select text from cursor to the end of the current line.

    Returns:
        True if the operation succeeded.

    Example:
        >>> select_to_line_end()  # Cmd+Shift+Right
    """
    return _combo_press(["cmd", "shift"], "right")


def select_character(direction: str = "right", count: int = 1) -> bool:
    """
    Select characters in a direction from the cursor.

    Args:
        direction: 'left' or 'right'.
        count: Number of characters to select.

    Returns:
        True if the operation succeeded.

    Example:
        >>> select_character("right", 5)   # Shift+Right 5 times
    """
    if direction not in ("left", "right"):
        return False

    for _ in range(count):
        if not _combo_press(["shift"], direction):
            return False
        time.sleep(0.02)
    return True


def select_word_relative(offset: int = 0) -> bool:
    """
    Select a word relative to the cursor.

    Args:
        offset: 0=current word, positive=moved right, negative=moved left.

    Returns:
        True if the operation succeeded.

    Example:
        >>> select_word_relative(1)   # select next word
        >>> select_word_relative(-1)  # select previous word
    """
    if offset == 0:
        return select_word()

    # Navigate words first, then select
    arrow = "right" if offset > 0 else "left"
    for _ in range(abs(offset)):
        if not _combo_press(["option"], arrow):
            return False
        time.sleep(0.02)

    return True


def select_all_in_field() -> bool:
    """
    Select all text in the current field (same as select_all).

    Returns:
        True if the operation succeeded.
    """
    return select_all()


def deselect_all() -> bool:
    """
    Deselect any current selection by pressing Escape.

    Returns:
        True if the operation succeeded.
    """
    if not _send_key("escape"):
        return False
    time.sleep(0.02)
    return True


# ----------------------------------------------------------------------
# Selection Manipulation
# ----------------------------------------------------------------------


def delete_selection() -> bool:
    """
    Delete the currently selected text.

    Returns:
        True if the operation succeeded.

    Example:
        >>> select_all()
        >>> delete_selection()
    """
    return _combo_press([], "delete")


def cut_selection() -> bool:
    """
    Cut (copy and delete) the currently selected text.

    Returns:
        True if the operation succeeded.

    Example:
        >>> select_all()
        >>> cut_selection()
    """
    return _combo_press(["cmd"], "x")


def copy_selection() -> bool:
    """
    Copy the currently selected text to clipboard.

    Returns:
        True if the operation succeeded.

    Example:
        >>> select_all()
        >>> copy_selection()
    """
    return _combo_press(["cmd"], "c")


def extend_selection_right(count: int = 1) -> bool:
    """
    Extend selection by moving cursor right.

    Args:
        count: Number of characters to extend.

    Returns:
        True if all operations succeeded.
    """
    for _ in range(count):
        if not _combo_press(["shift"], "right"):
            return False
        time.sleep(0.02)
    return True


def extend_selection_left(count: int = 1) -> bool:
    """
    Extend selection by moving cursor left.

    Args:
        count: Number of characters to extend.

    Returns:
        True if all operations succeeded.
    """
    for _ in range(count):
        if not _combo_press(["shift"], "left"):
            return False
        time.sleep(0.02)
    return True


def select_sentence(cursor_position: int = 0) -> bool:
    """
    Select a sentence (or approximate via Option+arrows).

    Note: True sentence selection is app-dependent.
    This uses Option+arrow as a reasonable approximation.

    Args:
        cursor_position: 0=current sentence, +/- for relative.

    Returns:
        True if the operation succeeded.
    """
    if cursor_position == 0:
        # Move to sentence start, then select to end
        for _ in range(2):
            if not _combo_press(["option"], "left"):
                return False
            time.sleep(0.02)
        for _ in range(2):
            if not _combo_press(["option", "shift"], "right"):
                return False
            time.sleep(0.02)
        return True
    return False


# ----------------------------------------------------------------------
# Convenience Functions
# ----------------------------------------------------------------------


def select_and_copy() -> bool:
    """
    Select all text and copy it to clipboard.

    Returns:
        True if both operations succeeded.
    """
    if not select_all():
        return False
    time.sleep(0.05)
    return copy_selection()


def select_and_cut() -> bool:
    """
    Select all text and cut it to clipboard.

    Returns:
        True if both operations succeeded.
    """
    if not select_all():
        return False
    time.sleep(0.05)
    return cut_selection()
