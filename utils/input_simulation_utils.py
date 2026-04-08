"""Input simulation utilities for keyboard and mouse automation.

This module wraps macOS CGEvent APIs to synthesize low-level input
events — mouse clicks, movement, keyboard key presses, and scroll
wheel events — suitable for programmatic GUI automation.

Example:
    >>> from utils.input_simulation_utils import click, type_text, scroll
    >>> click(400, 300, button='left')
    >>> type_text("hello world")
    >>> scroll(0, -5)  # scroll down
"""

from __future__ import annotations

import time
from typing import Literal

__all__ = [
    "move_mouse",
    "click",
    "right_click",
    "double_click",
    "triple_click",
    "drag",
    "scroll",
    "scroll_to",
    "type_text",
    "press_key",
    "press_shortcut",
    "hold_key",
    "release_key",
]


if __import__("sys").platform == "darwin":
    import Quartz
    from Quartz import CGEventCreateMouseEvent, CGEventCreateKeyboardEvent
    from Quartz import CGEventPost, kCGHIDEventTap
    from Quartz import kCGEventLeftMouseDown, kCGEventLeftMouseUp
    from Quartz import kCGEventRightMouseDown, kCGEventRightMouseUp
    from Quartz import kCGEventMouseMoved, kCGEventLeftMouseDragged
    from Quartz import kCGEventKeyDown, kCGEventKeyUp, kCGEventScrollWheel
    from Quartz import kCGMouseButtonLeft, kCGMouseButtonRight
    from Quartz import kCGEventSourceStateHIDSystemState

    _INPUT_AVAILABLE = True
else:
    _INPUT_AVAILABLE = False


# Key code mapping for common keys
_KEY_CODES = {
    "return": 36,
    "tab": 48,
    "space": 49,
    "delete": 51,
    "escape": 53,
    "up": 126,
    "down": 125,
    "left": 123,
    "right": 124,
    "f1": 122,
    "f2": 120,
    "f3": 99,
    "f4": 118,
    "f5": 96,
    "f6": 97,
    "f7": 98,
    "f8": 100,
    "f9": 101,
    "f10": 109,
    "f11": 103,
    "f12": 111,
    "command": 55,
    "cmd": 55,
    "shift": 56,
    "control": 59,
    "ctrl": 59,
    "option": 58,
    "alt": 58,
    "capslock": 57,
    "volumeup": 72,
    "volumedown": 73,
    "mute": 74,
}

# Add letter keys
for i, letter in enumerate("abcdefghijklmnopqrstuvwxyz"):
    _KEY_CODES[letter] = 4 + i  # 'a' = 4, 'b' = 5, ...

# Add number keys
for i, digit in enumerate("1234567890"):
    _KEY_CODES[digit] = 29 + i  # '1' = 29, '2' = 30, ...

# Add punctuation
_PUNCT_CODES = {
    "`": 50,
    "-": 27,
    "=": 24,
    "[": 33,
    "]": 30,
    ";": 41,
    "'": 39,
    ",": 43,
    "."": 47,
    "/": 44,
    "\\": 42,
}
_KEY_CODES.update(_PUNCT_CODES)


def _cg_point(x: float, y: float) -> Quartz.CGPoint:
    return Quartz.CGPoint(x, y)


def _post_event(event: Quartz.CGEvent) -> None:
    CGEventPost(kCGHIDEventTap, event)


def move_mouse(x: float, y: float, duration: float = 0.0) -> None:
    """Move the mouse cursor to the specified absolute screen position.

    Args:
        x: Target X coordinate.
        y: Target Y coordinate.
        duration: Smooth movement duration in seconds (0 = instant).
    """
    if not _INPUT_AVAILABLE:
        raise RuntimeError("Input simulation only available on macOS")

    if duration <= 0:
        event = CGEventCreateMouseEvent(
            None, kCGEventMouseMoved, _cg_point(x, y), kCGMouseButtonLeft
        )
        _post_event(event)
    else:
        steps = max(1, int(duration / 0.01))
        for i in range(steps + 1):
            t = i / steps
            cx = x  # would need start position tracking for smooth anim
            event = CGEventCreateMouseEvent(
                None, kCGEventMouseMoved, _cg_point(cx, x), kCGMouseButtonLeft
            )
            _post_event(event)
            time.sleep(duration / steps)


def click(
    x: float,
    y: float,
    button: Literal["left", "right"] = "left",
    clicks: int = 1,
) -> None:
    """Simulate a mouse click at the specified position.

    Args:
        x: X coordinate.
        y: Y coordinate.
        button: Mouse button ('left' or 'right').
        clicks: Number of clicks.
    """
    if not _INPUT_AVAILABLE:
        raise RuntimeError("Input simulation only available on macOS")

    button_type = kCGMouseButtonLeft if button == "left" else kCGMouseButtonRight
    down_type = kCGEventLeftMouseDown if button == "left" else kCGEventRightMouseDown
    up_type = kCGEventLeftMouseUp if button == "left" else kCGEventRightMouseUp

    for _ in range(clicks):
        down = CGEventCreateMouseEvent(None, down_type, _cg_point(x, y), button_type)
        up = CGEventCreateMouseEvent(None, up_type, _cg_point(x, y), button_type)
        _post_event(down)
        _post_event(up)


def right_click(x: float, y: float) -> None:
    """Simulate a right-click at the specified position."""
    click(x, y, button="right")


def double_click(x: float, y: float) -> None:
    """Simulate a double left-click at the specified position."""
    click(x, y, button="left", clicks=2)


def triple_click(x: float, y: float) -> None:
    """Simulate a triple left-click at the specified position."""
    click(x, y, button="left", clicks=3)


def drag(x1: float, y1: float, x2: float, y2: float, duration: float = 0.3) -> None:
    """Simulate a mouse drag from (x1, y1) to (x2, y2).

    Args:
        x1: Start X coordinate.
        y1: Start Y coordinate.
        x2: End X coordinate.
        y2: End Y coordinate.
        duration: Drag duration in seconds.
    """
    if not _INPUT_AVAILABLE:
        raise RuntimeError("Input simulation only available on macOS")

    # Mouse down at start
    down = CGEventCreateMouseEvent(
        None, kCGEventLeftMouseDown, _cg_point(x1, y1), kCGMouseButtonLeft
    )
    _post_event(down)

    # Smooth drag
    steps = max(1, int(duration / 0.01))
    for i in range(1, steps + 1):
        t = i / steps
        cx = x1 + (x2 - x1) * t
        cy = y1 + (y2 - y1) * t
        drag_event = CGEventCreateMouseEvent(
            None, kCGEventLeftMouseDragged, _cg_point(cx, cy), kCGMouseButtonLeft
        )
        _post_event(drag_event)
        time.sleep(duration / steps)

    # Mouse up at end
    up = CGEventCreateMouseEvent(
        None, kCGEventLeftMouseUp, _cg_point(x2, y2), kCGMouseButtonLeft
    )
    _post_event(up)


def scroll(delta_x: float = 0, delta_y: float = 0, clicks: int = 1) -> None:
    """Simulate scroll wheel events.

    Args:
        delta_x: Horizontal scroll amount (positive = right).
        delta_y: Vertical scroll amount (positive = up, negative = down).
        clicks: Number of scroll wheel click events to send.
    """
    if not _INPUT_AVAILABLE:
        raise RuntimeError("Input simulation only available on macOS")

    for _ in range(clicks):
        event = Quartz.CGEventCreateScrollWheelEvent(
            None,
            kCGEventSourceStateHIDSystemState,
            2,  # 2 for 2-axis scroll
            int(delta_y * 10),
            int(delta_x * 10),
        )
        if event is not None:
            _post_event(event)


def scroll_to(x: float, y: float, clicks: int = 5) -> None:
    """Convenience: scroll down (negative Y) to bring content at (x, y) into view.

    Args:
        x: X coordinate of target element.
        y: Y coordinate of target element.
        clicks: Number of scroll clicks per step.
    """
    scroll(0, -clicks)


def _key_code_for(key: str) -> int:
    """Resolve a key name to its virtual key code."""
    key_lower = key.lower()
    if key_lower in _KEY_CODES:
        return _KEY_CODES[key_lower]
    if len(key) == 1:
        return ord(key.lower()) - ord("a") + 4
    raise ValueError(f"Unknown key: {key}")


def type_text(text: str, interval: float = 0.01) -> None:
    """Type a string of text using keyboard events.

    Args:
        text: String to type.
        interval: Delay between keystrokes in seconds.
    """
    if not _INPUT_AVAILABLE:
        raise RuntimeError("Input simulation only available on macOS")

    for char in text:
        key_code = _key_code_for(char)
        down = CGEventCreateKeyboardEvent(None, key_code, True)
        up = CGEventCreateKeyboardEvent(None, key_code, False)
        _post_event(down)
        _post_event(up)
        if interval > 0:
            time.sleep(interval)


def press_key(key: str) -> None:
    """Press and release a single key.

    Args:
        key: Key name (e.g., 'return', 'escape', 'a').
    """
    if not _INPUT_AVAILABLE:
        raise RuntimeError("Input simulation only available on macOS")

    key_code = _key_code_for(key)
    down = CGEventCreateKeyboardEvent(None, key_code, True)
    up = CGEventCreateKeyboardEvent(None, key_code, False)
    _post_event(down)
    _post_event(up)


def press_shortcut(*keys: str) -> None:
    """Press a keyboard shortcut (e.g., 'cmd', 's' for Cmd+S).

    Args:
        *keys: Key names to press simultaneously.
    """
    if not _INPUT_AVAILABLE:
        raise RuntimeError("Input simulation only available on macOS")

    key_codes = [_key_code_for(k) for k in keys]
    for kc in key_codes:
        down = CGEventCreateKeyboardEvent(None, kc, True)
        _post_event(down)

    for kc in reversed(key_codes):
        up = CGEventCreateKeyboardEvent(None, kc, False)
        _post_event(up)


def hold_key(key: str) -> None:
    """Hold a key down until release_key is called."""
    if not _INPUT_AVAILABLE:
        raise RuntimeError("Input simulation only available on macOS")

    key_code = _key_code_for(key)
    down = CGEventCreateKeyboardEvent(None, key_code, True)
    _post_event(down)


def release_key(key: str) -> None:
    """Release a held key."""
    if not _INPUT_AVAILABLE:
        raise RuntimeError("Input simulation only available on macOS")

    key_code = _key_code_for(key)
    up = CGEventCreateKeyboardEvent(None, key_code, False)
    _post_event(up)
