"""Keyboard layout utilities.

This module provides utilities for working with different
keyboard layouts and mapping keys across layouts.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum, auto


class KeyModifier(Enum):
    """Keyboard modifier keys."""
    SHIFT = auto()
    CTRL = auto()
    ALT = auto()
    META = auto()
    CAPS_LOCK = auto()


@dataclass
class KeyEvent:
    """A keyboard event."""
    key_code: int
    key_name: str
    modifiers: Set[KeyModifier]
    is_pressed: bool
    scan_code: int = 0


@dataclass
class KeyMapping:
    """A key mapping between virtual key and character."""
    vk_code: int
    scan_code: int
    char_lower: str
    char_upper: str
    key_name: str


class KeyboardLayout:
    """Represents a keyboard layout."""

    def __init__(
        self,
        name: str,
        mappings: Optional[List[KeyMapping]] = None,
    ) -> None:
        self.name = name
        self._vk_to_mapping: Dict[int, KeyMapping] = {}
        self._char_to_lower: Dict[str, KeyMapping] = {}
        self._char_to_upper: Dict[str, KeyMapping] = {}
        if mappings:
            for m in mappings:
                self.add_mapping(m)

    def add_mapping(self, mapping: KeyMapping) -> None:
        self._vk_to_mapping[mapping.vk_code] = mapping
        self._char_to_lower[mapping.char_lower.lower()] = mapping
        if mapping.char_upper:
            self._char_to_upper[mapping.char_upper] = mapping

    def get_mapping_by_vk(self, vk_code: int) -> Optional[KeyMapping]:
        return self._vk_to_mapping.get(vk_code)

    def get_mapping_by_char(self, char: str) -> Optional[KeyMapping]:
        return self._char_to_lower.get(char.lower())

    def char_for_vk(self, vk_code: int, shift: bool = False) -> Optional[str]:
        mapping = self.get_mapping_by_vk(vk_code)
        if not mapping:
            return None
        return mapping.char_upper if shift else mapping.char_lower


QWERTY_MAPPING = [
    KeyMapping(0x30, 0x10, "0", ")", "0"),
    KeyMapping(0x31, 0x11, "1", "!", "1"),
    KeyMapping(0x32, 0x12, "2", "@", "2"),
    KeyMapping(0x33, 0x13, "3", "#", "3"),
    KeyMapping(0x34, 0x14, "4", "$", "4"),
    KeyMapping(0x35, 0x15, "5", "%", "5"),
    KeyMapping(0x36, 0x16, "6", "^", "6"),
    KeyMapping(0x37, 0x17, "7", "&", "7"),
    KeyMapping(0x38, 0x18, "8", "*", "8"),
    KeyMapping(0x39, 0x19, "9", "(", "9"),
    KeyMapping(0x41, 0x1E, "a", "A", "A"),
    KeyMapping(0x42, 0x1F, "b", "B", "B"),
    KeyMapping(0x43, 0x20, "c", "C", "C"),
    KeyMapping(0x44, 0x21, "d", "D", "D"),
    KeyMapping(0x45, 0x22, "e", "E", "E"),
    KeyMapping(0x46, 0x23, "f", "F", "F"),
    KeyMapping(0x47, 0x24, "g", "G", "G"),
    KeyMapping(0x48, 0x25, "h", "H", "H"),
    KeyMapping(0x49, 0x26, "i", "I", "I"),
    KeyMapping(0x4A, 0x27, "j", "J", "J"),
    KeyMapping(0x4B, 0x28, "k", "K", "K"),
    KeyMapping(0x4C, 0x29, "l", "L", "L"),
    KeyMapping(0x4D, 0x2A, "m", "M", "M"),
    KeyMapping(0x4E, 0x2B, "n", "N", "N"),
    KeyMapping(0x4F, 0x2C, "o", "O", "O"),
    KeyMapping(0x50, 0x2D, "p", "P", "P"),
    KeyMapping(0x51, 0x2E, "q", "Q", "Q"),
    KeyMapping(0x52, 0x2F, "r", "R", "R"),
    KeyMapping(0x53, 0x30, "s", "S", "S"),
    KeyMapping(0x54, 0x31, "t", "T", "T"),
    KeyMapping(0x55, 0x32, "u", "U", "U"),
    KeyMapping(0x56, 0x33, "v", "V", "V"),
    KeyMapping(0x57, 0x34, "w", "W", "W"),
    KeyMapping(0x58, 0x35, "x", "X", "X"),
    KeyMapping(0x59, 0x36, "y", "Y", "Y"),
    KeyMapping(0x5A, 0x37, "z", "Z", "Z"),
]

QWERTY_US = KeyboardLayout("QWERTY-US", QWERTY_MAPPING)


def apply_modifiers(char: str, modifiers: Set[KeyModifier]) -> str:
    """Apply modifiers to a character.

    Args:
        char: Input character.
        modifiers: Active modifiers.

    Returns:
        Modified character.
    """
    if KeyModifier.SHIFT in modifiers:
        return char.upper()
    return char


__all__ = [
    "KeyModifier",
    "KeyEvent",
    "KeyMapping",
    "KeyboardLayout",
    "QWERTY_US",
    "apply_modifiers",
]
