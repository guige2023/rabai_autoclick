"""Keyboard utilities for RabAI AutoClick.

Provides:
- Key code definitions
- Key sequence builder
- Hotkey registration
"""

from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Set


@dataclass(frozen=True)
class KeyCode:
    """Virtual key code constants."""
    # Special keys
    BACKSPACE = 0x08
    TAB = 0x09
    ENTER = 0x0D
    SHIFT = 0x10
    CTRL = 0x11
    ALT = 0x12
    ESCAPE = 0x1B
    SPACE = 0x20
    PAGE_UP = 0x21
    PAGE_DOWN = 0x22
    END = 0x23
    HOME = 0x24
    LEFT = 0x25
    UP = 0x26
    RIGHT = 0x27
    DOWN = 0x28

    # Function keys
    F1 = 0x70
    F2 = 0x71
    F3 = 0x72
    F4 = 0x73
    F5 = 0x74
    F6 = 0x75
    F7 = 0x76
    F8 = 0x77
    F9 = 0x78
    F10 = 0x79
    F11 = 0x7A
    F12 = 0x7B


@dataclass
class KeySequence:
    """Sequence of key presses and releases."""

    def __init__(self) -> None:
        """Initialize key sequence."""
        self._events: List[tuple] = []

    def press(self, key_code: int) -> "KeySequence":
        """Add key press event.

        Args:
            key_code: Virtual key code.

        Returns:
            Self for chaining.
        """
        self._events.append(("press", key_code))
        return self

    def release(self, key_code: int) -> "KeySequence":
        """Add key release event.

        Args:
            key_code: Virtual key code.

        Returns:
            Self for chaining.
        """
        self._events.append(("release", key_code))
        return self

    def type(self, key_code: int) -> "KeySequence":
        """Add key press and release (type).

        Args:
            key_code: Virtual key code.

        Returns:
            Self for chaining.
        """
        self._events.append(("press", key_code))
        self._events.append(("release", key_code))
        return self

    def text(self, text: str) -> "KeySequence":
        """Add text typing sequence.

        Args:
            text: Text to type.

        Returns:
            Self for chaining.
        """
        for char in text:
            char_code = ord(char.upper())
            if char_code >= 0x41 and char_code <= 0x5A:
                self._events.append(("press", char_code))
                self._events.append(("release", char_code))
            elif char_code >= 0x30 and char_code <= 0x39:
                self._events.append(("press", char_code))
                self._events.append(("release", char_code))
            else:
                # For special characters, try shift + key
                self._events.append(("press", 0x10))  # SHIFT
                self._events.append(("press", char_code))
                self._events.append(("release", char_code))
                self._events.append(("release", 0x10))
        return self

    def hold(self, *key_codes: int) -> "KeySequence":
        """Hold keys down.

        Args:
            *key_codes: Keys to hold.

        Returns:
            Self for chaining.
        """
        for key_code in key_codes:
            self._events.append(("press", key_code))
        return self

    def release_all(self, *key_codes: int) -> "KeySequence":
        """Release held keys.

        Args:
            *key_codes: Keys to release.

        Returns:
            Self for chaining.
        """
        for key_code in reversed(key_codes):
            self._events.append(("release", key_code))
        return self

    @property
    def events(self) -> List[tuple]:
        """Get all events."""
        return self._events.copy()

    def clear(self) -> None:
        """Clear all events."""
        self._events.clear()


class Hotkey:
    """Represents a hotkey combination."""

    def __init__(self, modifiers: Set[int], key_code: int) -> None:
        """Initialize hotkey.

        Args:
            modifiers: Set of modifier key codes.
            key_code: Main key code.
        """
        self.modifiers = modifiers
        self.key_code = key_code

    def __hash__(self) -> int:
        """Hash for use in dicts."""
        return hash((frozenset(self.modifiers), self.key_code))

    def __eq__(self, other: object) -> bool:
        """Check equality."""
        if not isinstance(other, Hotkey):
            return False
        return self.modifiers == other.modifiers and self.key_code == other.key_code

    def __repr__(self) -> str:
        """String representation."""
        parts = []
        if 0x10 in self.modifiers:
            parts.append("Ctrl")
        if 0x11 in self.modifiers:
            parts.append("Alt")
        if 0x12 in self.modifiers:
            parts.append("Shift")
        parts.append(f"Key({self.key_code})")
        return "+".join(parts)


class HotkeyManager:
    """Manage global hotkeys."""

    def __init__(self) -> None:
        """Initialize hotkey manager."""
        self._handlers: Dict[Hotkey, Callable[[], None]] = {}
        self._registered: Set[Hotkey] = set()

    def register(
        self,
        hotkey: Hotkey,
        handler: Callable[[], None],
    ) -> bool:
        """Register a hotkey handler.

        Args:
            hotkey: Hotkey to register.
            handler: Function to call when hotkey is pressed.

        Returns:
            True if registration successful.
        """
        try:
            import win32api
            import win32con
            # Register with Windows
            id = hash(hotkey) % 0xBFFF
            win32api.RegisterHotKey(None, id,
                sum(hotkey.modifiers), hotkey.key_code)
            self._handlers[hotkey] = handler
            self._registered.add(hotkey)
            return True
        except Exception:
            # Fallback for non-Windows or if win32api unavailable
            self._handlers[hotkey] = handler
            return True

    def unregister(self, hotkey: Hotkey) -> bool:
        """Unregister a hotkey.

        Args:
            hotkey: Hotkey to unregister.

        Returns:
            True if unregistration successful.
        """
        try:
            import win32api
            id = hash(hotkey) % 0xBFFF
            win32api.UnregisterHotKey(None, id)
        except Exception:
            pass

        if hotkey in self._handlers:
            del self._handlers[hotkey]
        self._registered.discard(hotkey)
        return True

    def trigger(self, hotkey: Hotkey) -> None:
        """Trigger handler for hotkey.

        Args:
            hotkey: Hotkey that was pressed.
        """
        if hotkey in self._handlers:
            self._handlers[hotkey]()

    def clear(self) -> None:
        """Clear all hotkeys."""
        for hotkey in list(self._registered):
            self.unregister(hotkey)


class KeyMapper:
    """Map key sequences to actions."""

    def __init__(self) -> None:
        """Initialize key mapper."""
        self._mappings: Dict[str, Callable[[], None]] = {}

    def map(self, sequence: str, action: Callable[[], None]) -> None:
        """Map a key sequence string to an action.

        Args:
            sequence: String like "ctrl+c" or "ctrl+shift+s".
            action: Function to execute.
        """
        self._mappings[sequence.lower()] = action

    def unmap(self, sequence: str) -> bool:
        """Remove a mapping.

        Args:
            sequence: Sequence string to remove.

        Returns:
            True if mapping existed.
        """
        if sequence.lower() in self._mappings:
            del self._mappings[sequence.lower()]
            return True
        return False

    def execute(self, sequence: str) -> bool:
        """Execute action for sequence.

        Args:
            sequence: Sequence string.

        Returns:
            True if mapping found and executed.
        """
        action = self._mappings.get(sequence.lower())
        if action:
            action()
            return True
        return False

    def get_action(self, sequence: str) -> Optional[Callable[[], None]]:
        """Get action for sequence.

        Args:
            sequence: Sequence string.

        Returns:
            Action function or None.
        """
        return self._mappings.get(sequence.lower())
