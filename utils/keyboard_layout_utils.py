"""
Keyboard Layout Utilities

Provides utilities for handling keyboard layouts
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class KeyPosition:
    """Position of a key on the keyboard."""
    x: float
    y: float
    width: float = 1.0
    height: float = 1.0


class KeyboardLayout:
    """
    Represents a keyboard layout.
    
    Provides key position lookup and
    coordinate transformation.
    """

    def __init__(self, name: str = "qwerty") -> None:
        self._name = name
        self._keys: dict[str, KeyPosition] = {}
        self._load_layout()

    def _load_layout(self) -> None:
        """Load the keyboard layout."""
        row_y = {"1": 0, "2": 1, "3": 2, "4": 3, "5": 4}
        for i, key in enumerate("qwertyuiop"):
            self._keys[key] = KeyPosition(x=float(i), y=row_y["2"])

    def get_key_position(self, key: str) -> KeyPosition | None:
        """Get position of a key."""
        return self._keys.get(key.lower())

    def get_key_at_position(self, x: float, y: float) -> str | None:
        """Get key at approximate position."""
        for key, pos in self._keys.items():
            if (abs(pos.x - x) < 0.5 and abs(pos.y - y) < 0.5):
                return key
        return None

    def get_layout_name(self) -> str:
        """Get layout name."""
        return self._name


def get_current_layout() -> KeyboardLayout:
    """Get the current keyboard layout."""
    return KeyboardLayout()
