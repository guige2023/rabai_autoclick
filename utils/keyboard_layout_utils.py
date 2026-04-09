"""
Keyboard Layout Utilities for UI Automation

Handles keyboard layout detection, key mapping, and
layout-aware key input simulation.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class KeyboardLayout(Enum):
    """Supported keyboard layouts."""
    US_ENGLISH = auto()
    US_INTERNATIONAL = auto()
    UK_ENGLISH = auto()
    GERMAN = auto()
    FRENCH = auto()
    SPANISH = auto()
    ITALIAN = auto()
    JAPANESE_JIS = auto()
    CHINESE_SIMPLIFIED = auto()
    UNKNOWN = auto()


@dataclass
class KeyMapping:
    """Mapping for a single key."""
    scan_code: int
    vk_code: int
    primary: str
    shifted: Optional[str] = None
    alt_gr: Optional[str] = None


# US English key mappings (scan_code -> KeyMapping)
US_ENGLISH_MAPPINGS: dict[int, KeyMapping] = {
    0x1E: KeyMapping(0x1E, 0x41, 'a', 'A'),
    0x1F: KeyMapping(0x1F, 0x53, 's', 'S'),
    0x20: KeyMapping(0x20, 0x44, 'd', 'D'),
    0x21: KeyMapping(0x21, 0x46, 'f', 'F'),
    0x22: KeyMapping(0x22, 0x48, 'g', 'G'),
    0x23: KeyMapping(0x23, 0x48, 'h', 'H'),
    0x24: KeyMapping(0x24, 0x4A, 'j', 'J'),
    0x25: KeyMapping(0x25, 0x4B, 'k', 'K'),
    0x26: KeyMapping(0x26, 0x4C, 'l', 'L'),
    0x2C: KeyMapping(0x2C, 0x5A, 'z', 'Z'),
    0x2D: KeyMapping(0x2D, 0x58, 'x', 'X'),
    0x2E: KeyMapping(0x2E, 0x43, 'c', 'C'),
    0x2F: KeyMapping(0x2F, 0x56, 'v', 'V'),
    0x30: KeyMapping(0x30, 0x42, 'b', 'B'),
    0x31: KeyMapping(0x31, 0x4E, 'n', 'N'),
    0x32: KeyMapping(0x32, 0x4D, 'm', 'M'),
}


@dataclass
class KeyboardLayoutDetector:
    """Detects and manages keyboard layout information."""

    _current_layout: KeyboardLayout = KeyboardLayout.US_ENGLISH
    _layout_mappings: dict[KeyboardLayout, dict[int, KeyMapping]] = {
        KeyboardLayout.US_ENGLISH: US_ENGLISH_MAPPINGS,
    }

    def detect_current_layout(self) -> KeyboardLayout:
        """
        Detect the current system keyboard layout.

        Returns:
            Detected KeyboardLayout enum value
        """
        if sys.platform == "darwin":
            return self._detect_macos_layout()
        elif sys.platform == "win32":
            return self._detect_windows_layout()
        else:
            return self._detect_linux_layout()

    def _detect_macos_layout(self) -> KeyboardLayout:
        """Detect keyboard layout on macOS."""
        # On macOS, we would use Carbon or Objective-C APIs
        # For now, default to US English
        return KeyboardLayout.US_ENGLISH

    def _detect_windows_layout(self) -> KeyboardLayout:
        """Detect keyboard layout on Windows."""
        # On Windows, we would use GetKeyboardLayout API
        return KeyboardLayout.US_ENGLISH

    def _detect_linux_layout(self) -> KeyboardLayout:
        """Detect keyboard layout on Linux."""
        # On Linux, check XKB settings
        return KeyboardLayout.US_ENGLISH

    def set_layout(self, layout: KeyboardLayout) -> None:
        """Explicitly set the current keyboard layout."""
        self._current_layout = layout

    def get_current_layout(self) -> KeyboardLayout:
        """Get the current keyboard layout."""
        return self._current_layout

    def get_key_mapping(self, scan_code: int) -> Optional[KeyMapping]:
        """
        Get key mapping for a scan code in the current layout.

        Args:
            scan_code: Hardware scan code

        Returns:
            KeyMapping if found, None otherwise
        """
        mappings = self._layout_mappings.get(self._current_layout, {})
        return mappings.get(scan_code)

    def char_to_key_events(
        self,
        char: str,
        shift: bool = False,
        alt_gr: bool = False,
    ) -> list[tuple[int, int]]:
        """
        Convert a character to a sequence of (scan_code, vk_code) tuples.

        Args:
            char: Character to convert
            shift: Whether shift modifier is needed
            alt_gr: Whether AltGr modifier is needed

        Returns:
            List of (scan_code, vk_code) tuples for key events
        """
        # Find the key that produces this character
        for mappings in self._layout_mappings.values():
            for scan_code, mapping in mappings.items():
                if char == mapping.primary:
                    return [(scan_code, mapping.vk_code)]
                if shift and mapping.shifted and char == mapping.shifted:
                    return [(scan_code, mapping.vk_code)]

        # Special character handling
        return self._handle_special_character(char, shift)

    def _handle_special_character(
        self,
        char: str,
        shift: bool,
    ) -> list[tuple[int, int]]:
        """Handle special characters like punctuation and numbers."""
        special_map = {
            '1': (0x02, 0x31), '!': (0x02, 0x31),
            '2': (0x03, 0x32), '@': (0x03, 0x32),
            '3': (0x04, 0x33), '#': (0x04, 0x33),
            '4': (0x05, 0x34), '$': (0x05, 0x34),
            '5': (0x06, 0x35), '%': (0x06, 0x35),
            '6': (0x07, 0x36), '^': (0x07, 0x36),
            '7': (0x08, 0x37), '&': (0x08, 0x37),
            '8': (0x09, 0x38), '*': (0x09, 0x38),
            '9': (0x0A, 0x39), '(': (0x0A, 0x39),
            '0': (0x0B, 0x30), ')': (0x0B, 0x30),
            ' ': (0x39, 0x20),
        }
        result = special_map.get(char) or special_map.get(char.upper() if shift else char)
        if result:
            return [result]
        return []

    def get_dead_key_sequence(self, char: str) -> list[str]:
        """
        Get the sequence of keys to produce a dead key character.

        Args:
            char: Dead key character (e.g., '´', '`', '¨')

        Returns:
            List of key names to press in sequence
        """
        dead_keys: dict[str, list[str]] = {
            '´': ['dead_acute'],
            '`': ['dead_grave'],
            '¨': ['dead_diaeresis'],
            '^': ['dead_circumflex'],
            '~': ['dead_tilde'],
        }
        return dead_keys.get(char, [])


def get_key_name(vk_code: int) -> str:
    """
    Get the name for a virtual key code.

    Args:
        vk_code: Windows virtual key code

    Returns:
        Human-readable key name
    """
    key_names: dict[int, str] = {
        0x08: "Backspace",
        0x09: "Tab",
        0x0D: "Enter",
        0x1B: "Escape",
        0x20: "Space",
        0x2E: "Delete",
        0x2B: "Insert",
        0x2D: "End",
        0x2F: "Help",
        0x70: "F1",
        0x71: "F2",
        0x72: "F3",
        0x73: "F4",
        0x74: "F5",
        0x75: "F6",
        0x76: "F7",
        0x77: "F8",
        0x78: "F9",
        0x79: "F10",
        0x7A: "F11",
        0x7B: "F12",
        0x25: "Left",
        0x26: "Up",
        0x27: "Right",
        0x28: "Down",
    }
    return key_names.get(vk_code, f"Unknown({vk_code})")
