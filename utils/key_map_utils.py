"""
Key Map Utilities

Provides utilities for mapping and translating
keyboard keys in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from typing import Any


class KeyMap:
    """
    Maps key names to codes and vice versa.
    
    Provides consistent key handling across
    different platforms.
    """

    def __init__(self) -> None:
        self._key_to_code: dict[str, int] = {}
        self._code_to_key: dict[int, str] = {}
        self._modifier_keys = {"ctrl", "alt", "shift", "meta", "cmd"}

    def register_key(self, name: str, code: int) -> None:
        """Register a key mapping."""
        self._key_to_code[name.lower()] = code
        self._code_to_key[code] = name.lower()

    def get_code(self, key: str) -> int | None:
        """Get code for a key name."""
        return self._key_to_code.get(key.lower())

    def get_name(self, code: int) -> str | None:
        """Get key name for a code."""
        return self._code_to_key.get(code)

    def is_modifier(self, key: str) -> bool:
        """Check if key is a modifier."""
        return key.lower() in self._modifier_keys

    def normalize_key_combo(self, combo: str) -> list[str]:
        """Normalize a key combination string."""
        parts = combo.lower().replace("+", " ").split()
        return [p.strip() for p in parts if p]


# Common key mappings
KEY_MAP_MAC = KeyMap()
KEY_MAP_MAC.register_key("a", 0x00)
KEY_MAP_MAC.register_key("b", 0x0B)
KEY_MAP_MAC.register_key("c", 0x08)
KEY_MAP_MAC.register_key("enter", 0x24)
KEY_MAP_MAC.register_key("escape", 0x35)
KEY_MAP_MAC.register_key("space", 0x31)
KEY_MAP_MAC.register_key("tab", 0x30)


def create_platform_keymap() -> KeyMap:
    """Create a keymap for the current platform."""
    return KEY_MAP_MAC
