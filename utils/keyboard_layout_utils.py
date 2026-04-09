"""
Keyboard layout and key mapping utilities.

This module provides utilities for working with keyboard layouts,
key codes, modifiers, and text input across different keyboard types.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple, Callable
from enum import Enum, auto


class KeyCode(Enum):
    """Virtual key codes for keyboard input."""
    # Letters
    A = 0x00
    B = 0x0B
    C = 0x08
    D = 0x02
    E = 0x0E
    F = 0x03
    G = 0x05
    H = 0x04
    I = 0x22
    J = 0x26
    K = 0x28
    L = 0x25
    M = 0x2E
    N = 0x2D
    O = 0x1F
    P = 0x23
    Q = 0x0C
    R = 0x0F
    S = 0x01
    T = 0x11
    U = 0x20
    V = 0x09
    W = 0x0D
    X = 0x07
    Y = 0x10
    Z = 0x06

    # Numbers
    ZERO = 0x1D
    ONE = 0x12
    TWO = 0x13
    THREE = 0x14
    FOUR = 0x15
    FIVE = 0x17
    SIX = 0x16
    SEVEN = 0x1A
    EIGHT = 0x1C
    NINE = 0x19

    # Special keys
    RETURN = 0x24
    TAB = 0x30
    SPACE = 0x31
    DELETE = 0x33
    ESCAPE = 0x35
    COMMAND = 0x37
    SHIFT = 0x38
    CAPS_LOCK = 0x39
    OPTION = 0x3A
    CONTROL = 0x3B
    RIGHT_SHIFT = 0x3C
    RIGHT_OPTION = 0x3D
    RIGHT_CONTROL = 0x3E
    FUNCTION = 0x3F

    # Arrow keys
    UP_ARROW = 0x7E
    DOWN_ARROW = 0x7D
    LEFT_ARROW = 0x7B
    RIGHT_ARROW = 0x7C

    # Function keys
    F1 = 0x7A
    F2 = 0x78
    F3 = 0x63
    F4 = 0x76
    F5 = 0x60
    F6 = 0x61
    F7 = 0x62
    F8 = 0x64
    F9 = 0x65
    F10 = 0x6D
    F11 = 0x67
    F12 = 0x6F

    # Punctuation
    PERIOD = 0x2F
    COMMA = 0x2B
    SLASH = 0x2C
    SEMICOLON = 0x29
    QUOTE = 0x27
    BRACKET_LEFT = 0x21
    BRACKET_RIGHT = 0x1E
    BACKSLASH = 0x2A
    MINUS = 0x1B
    EQUAL = 0x18
    GRAVE = 0x32


class ModifierKey(Enum):
    """Keyboard modifier keys."""
    COMMAND = "command"
    SHIFT = "shift"
    OPTION = "option"
    CONTROL = "control"
    CAPS_LOCK = "caps_lock"
    FUNCTION = "function"


@dataclass
class KeyEvent:
    """
    Represents a keyboard event.

    Attributes:
        key_code: The virtual key code.
        modifiers: Set of active modifier keys.
        character: Character representation if applicable.
        is_key_down: True for key down, False for key up.
    """
    key_code: KeyCode
    modifiers: Set[ModifierKey] = field(default_factory=set)
    character: Optional[str] = None
    is_key_down: bool = True

    def with_modifier(self, modifier: ModifierKey) -> KeyEvent:
        """Return new KeyEvent with added modifier."""
        new_mods = self.modifiers | {modifier}
        return KeyEvent(
            key_code=self.key_code,
            modifiers=new_mods,
            character=self.character,
            is_key_down=self.is_key_down,
        )


@dataclass
class HotkeyBinding:
    """
    Represents a keyboard shortcut binding.

    Attributes:
        keys: Set of key codes that must all be pressed.
        modifiers: Required modifier keys.
        action: Callback when hotkey is triggered.
        description: Human-readable description.
    """
    keys: Set[KeyCode]
    modifiers: Set[ModifierKey] = field(default_factory=set)
    action: Callable[[], None]
    description: str = ""

    def matches(self, event: KeyEvent) -> bool:
        """Check if this binding matches the given key event."""
        if event.key_code not in self.keys:
            return False
        required_mods = self.modifiers - {ModifierKey.CAPS_LOCK, ModifierKey.FUNCTION}
        event_mods = event.modifiers - {ModifierKey.CAPS_LOCK, ModifierKey.FUNCTION}
        return required_mods == event_mods


class KeyboardLayout:
    """
    Represents a keyboard layout and provides mapping utilities.

    Supports QWERTY (US) layout with extensibility for others.
    """

    # QWERTY key positions (row, column)
    QWERTY_MAP: Dict[str, Tuple[int, int]] = {
        "q": (0, 0), "w": (0, 1), "e": (0, 2), "r": (0, 3), "t": (0, 4),
        "y": (0, 5), "u": (0, 6), "i": (0, 7), "o": (0, 8), "p": (0, 9),
        "a": (1, 0), "s": (1, 1), "d": (1, 2), "f": (1, 3), "g": (1, 4),
        "h": (1, 5), "j": (1, 6), "k": (1, 7), "l": (1, 8),
        "z": (2, 0), "x": (2, 1), "c": (2, 2), "v": (2, 3), "b": (2, 4),
        "n": (2, 5), "m": (2, 6),
    }

    # Character to KeyCode mapping
    CHAR_TO_KEYCODE: Dict[str, KeyCode] = {
        **{chr(ord("a") + i): KeyCode(ord("a") + i) for i in range(26)},
        **{chr(ord("0") + i): KeyCode(ord("0") + i) for i in range(10)},
        " ": KeyCode.SPACE,
        "\n": KeyCode.RETURN,
        "\t": KeyCode.TAB,
    }

    def __init__(self, layout_name: str = "QWERTY") -> None:
        self._layout_name = layout_name

    @property
    def name(self) -> str:
        """Get layout name."""
        return self._layout_name

    def key_for_char(self, char: str) -> Optional[KeyCode]:
        """Get KeyCode for a character."""
        return self.CHAR_TO_KEYCODE.get(char.lower())

    def position_for_key(self, key: str) -> Optional[Tuple[int, int]]:
        """Get (row, col) position for a key."""
        return self.QWERTY_MAP.get(key.lower())

    def adjacent_keys(self, key: str) -> List[str]:
        """Get list of adjacent keys."""
        pos = self.position_for_key(key)
        if not pos:
            return []
        row, col = pos
        adjacent: List[str] = []
        for k, (r, c) in self.QWERTY_MAP.items():
            if abs(r - row) <= 1 and abs(c - col) <= 1 and k != key:
                adjacent.append(k)
        return adjacent

    def shift_char(self, char: str) -> str:
        """Get shifted version of character."""
        shift_map = {
            "`": "~", "1": "!", "2": "@", "3": "#", "4": "$", "5": "%",
            "6": "^", "7": "&", "8": "*", "9": "(", "0": ")", "-": "_",
            "=": "+", "[": "{", "]": "}", ";": ":", "'": '"', ",": "<",
            ".": ">", "/": "?", "\\": "|",
        }
        return shift_map.get(char, char.upper() if char.isalpha() else char)


class HotkeyManager:
    """
    Manages global hotkey bindings.

    Allows registration of callbacks for keyboard shortcuts.
    """

    def __init__(self) -> None:
        self._bindings: Dict[str, HotkeyBinding] = {}
        self._next_id: int = 0

    def register(
        self,
        keys: List[str],
        modifiers: Optional[List[ModifierKey]] = None,
        action: Callable[[], None] = None,
        description: str = "",
    ) -> str:
        """
        Register a hotkey binding.

        Returns a unique binding ID.
        """
        key_codes = set()
        for key in keys:
            code = KeyboardLayout.CHAR_TO_KEYCODE.get(key.lower())
            if code:
                key_codes.add(code)

        mod_set = set(modifiers) if modifiers else set()

        binding = HotkeyBinding(
            keys=key_codes,
            modifiers=mod_set,
            action=action or (lambda: None),
            description=description,
        )

        binding_id = f"hotkey_{self._next_id}"
        self._next_id += 1
        self._bindings[binding_id] = binding
        return binding_id

    def unregister(self, binding_id: str) -> bool:
        """Unregister a hotkey binding."""
        return self._bindings.pop(binding_id, None) is not None

    def handle_key_event(self, event: KeyEvent) -> bool:
        """
        Handle a key event and trigger matching bindings.

        Returns True if a binding was triggered.
        """
        triggered = False
        for binding in self._bindings.values():
            if binding.matches(event):
                binding.action()
                triggered = True
        return triggered

    def get_bindings(self) -> List[Tuple[str, HotkeyBinding]]:
        """Get all registered bindings."""
        return list(self._bindings.items())


# Modifier key symbols for display
MODIFIER_SYMBOLS: Dict[ModifierKey, str] = {
    ModifierKey.COMMAND: "⌘",
    ModifierKey.SHIFT: "⇧",
    ModifierKey.OPTION: "⌥",
    ModifierKey.CONTROL: "⌃",
    ModifierKey.CAPS_LOCK: "⇪",
    ModifierKey.FUNCTION: "fn",
}


def format_hotkey(keys: List[str], modifiers: List[ModifierKey] = None) -> str:
    """Format a hotkey for display (e.g., '⌘C', '⇧⌘V')."""
    parts = []
    if modifiers:
        for mod in sorted(modifiers, key=lambda m: m.value):
            if mod in MODIFIER_SYMBOLS:
                parts.append(MODIFIER_SYMBOLS[mod])
    parts.extend(keys)
    return "".join(parts)
