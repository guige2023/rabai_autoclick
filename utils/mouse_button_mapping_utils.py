"""Mouse button mapping utilities.

This module provides utilities for mapping mouse buttons across
different platforms and configurations.
"""

from __future__ import annotations

from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum, auto


class MouseButton(Enum):
    """Standard mouse buttons."""
    LEFT = auto()
    RIGHT = auto()
    MIDDLE = auto()
    X1 = auto()  # Browser back
    X2 = auto()  # Browser forward
    WHEEL_UP = auto()
    WHEEL_DOWN = auto()
    WHEEL_LEFT = auto()
    WHEEL_RIGHT = auto()


@dataclass
class ButtonMapping:
    """Mapping for a mouse button across platforms."""
    button: MouseButton
    macos_code: int
    windows_code: int
    linux_code: int
    display_name: str


BUTTON_MAPPINGS: Dict[MouseButton, ButtonMapping] = {
    MouseButton.LEFT: ButtonMapping(
        MouseButton.LEFT, 0, 0x01, 0x01, "Left Click"
    ),
    MouseButton.RIGHT: ButtonMapping(
        MouseButton.RIGHT, 1, 0x02, 0x03, "Right Click"
    ),
    MouseButton.MIDDLE: ButtonMapping(
        MouseButton.MIDDLE, 2, 0x04, 0x02, "Middle Click"
    ),
    MouseButton.X1: ButtonMapping(
        MouseButton.X1, 3, 0x05, 0x08, "X1 (Back)"
    ),
    MouseButton.X2: ButtonMapping(
        MouseButton.X2, 4, 0x06, 0x09, "X2 (Forward)"
    ),
    MouseButton.WHEEL_UP: ButtonMapping(
        MouseButton.WHEEL_UP, 4, 0x08, 0x04, "Wheel Up"
    ),
    MouseButton.WHEEL_DOWN: ButtonMapping(
        MouseButton.WHEEL_DOWN, 5, 0x10, 0x05, "Wheel Down"
    ),
    MouseButton.WHEEL_LEFT: ButtonMapping(
        MouseButton.WHEEL_LEFT, 6, 0x20, 0x06, "Wheel Left"
    ),
    MouseButton.WHEEL_RIGHT: ButtonMapping(
        MouseButton.WHEEL_RIGHT, 7, 0x40, 0x07, "Wheel Right"
    ),
}


def get_button_mapping(button: MouseButton, platform: str) -> Optional[int]:
    """Get the platform-specific code for a mouse button.

    Args:
        button: MouseButton enum value.
        platform: Platform name (macos, windows, linux).

    Returns:
        Platform-specific button code.
    """
    mapping = BUTTON_MAPPINGS.get(button)
    if not mapping:
        return None

    if platform == "macos":
        return mapping.macos_code
    elif platform == "windows":
        return mapping.windows_code
    elif platform == "linux":
        return mapping.linux_code
    return None


def swap_buttons(platform: str) -> bool:
    """Check if buttons are swapped on a platform.

    Args:
        platform: Platform name.

    Returns:
        True if left/right buttons are swapped (e.g. left-handed).
    """
    return False


@dataclass
class ClickInfo:
    """Information about a mouse click."""
    button: MouseButton
    x: int
    y: int
    click_count: int = 1
    double_click: bool = False
    triple_click: bool = False


__all__ = [
    "MouseButton",
    "ButtonMapping",
    "BUTTON_MAPPINGS",
    "get_button_mapping",
    "swap_buttons",
    "ClickInfo",
]
