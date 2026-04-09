"""
Keyboard Simulator Action Module.

Simulates keyboard events including key presses, text input,
and key combinations with proper event ordering.
"""

import time
from dataclasses import dataclass
from typing import Callable, Optional, Set


@dataclass
class KeyEvent:
    """Represents a keyboard event."""
    key: str
    event_type: str
    modifiers: Set[str]
    timestamp: float


class KeyboardSimulator:
    """Simulates keyboard events for automation."""

    def __init__(self, sender: Optional[Callable[[KeyEvent], None]] = None):
        """
        Initialize keyboard simulator.

        Args:
            sender: Optional function to send key events.
        """
        self._sender = sender
        self._active_modifiers: Set[str] = set()

    def press_key(self, key: str) -> None:
        """
        Press and release a key.

        Args:
            key: Key name (e.g., 'a', 'Enter', 'Escape').
        """
        self.key_down(key)
        self.key_up(key)

    def key_down(self, key: str) -> KeyEvent:
        """
        Press a key down.

        Args:
            key: Key name.

        Returns:
            Generated KeyEvent.
        """
        event = KeyEvent(
            key=key,
            event_type="down",
            modifiers=set(self._active_modifiers),
            timestamp=time.time(),
        )
        self._update_modifiers(key, True)
        self._send(event)
        return event

    def key_up(self, key: str) -> KeyEvent:
        """
        Release a key.

        Args:
            key: Key name.

        Returns:
            Generated KeyEvent.
        """
        event = KeyEvent(
            key=key,
            event_type="up",
            modifiers=set(self._active_modifiers),
            timestamp=time.time(),
        )
        self._update_modifiers(key, False)
        self._send(event)
        return event

    def type_text(self, text: str, interval: float = 0.05) -> list[KeyEvent]:
        """
        Type a string of text.

        Args:
            text: Text to type.
            interval: Delay between keystrokes.

        Returns:
            List of generated KeyEvents.
        """
        events = []

        for char in text:
            if char == '\n':
                self.press_key('Enter')
            elif char == '\t':
                self.press_key('Tab')
            else:
                events.extend(self._type_char(char))

            time.sleep(interval)

        return events

    def combo(self, *keys: str) -> list[KeyEvent]:
        """
        Press a key combination.

        Args:
            *keys: Keys to press simultaneously.

        Returns:
            List of generated KeyEvents.
        """
        events = []

        for key in keys[:-1]:
            self.key_down(key)

        self.key_down(keys[-1])
        events.append(KeyEvent(
            key=keys[-1],
            event_type="down",
            modifiers=set(self._active_modifiers),
            timestamp=time.time(),
        ))

        for key in reversed(keys):
            self.key_up(key)

        return events

    def select_all(self) -> list[KeyEvent]:
        """Simulate Ctrl+A (select all)."""
        return self.combo("Control", "a")

    def copy(self) -> list[KeyEvent]:
        """Simulate Ctrl+C (copy)."""
        return self.combo("Control", "c")

    def paste(self) -> list[KeyEvent]:
        """Simulate Ctrl+V (paste)."""
        return self.combo("Control", "v")

    def cut(self) -> list[KeyEvent]:
        """Simulate Ctrl+X (cut)."""
        return self.combo("Control", "x")

    def undo(self) -> list[KeyEvent]:
        """Simulate Ctrl+Z (undo)."""
        return self.combo("Control", "z")

    def redo(self) -> list[KeyEvent]:
        """Simulate Ctrl+Shift+Z (redo)."""
        return self.combo("Control", "Shift", "z")

    def _type_char(self, char: str) -> list[KeyEvent]:
        """Type a single character."""
        events = []
        upper = char.isupper()
        modifier = set()

        if upper:
            modifier.add("Shift")

        for key in modifier:
            self.key_down(key)

        key_name = char.upper()

        events.append(KeyEvent(
            key=key_name,
            event_type="down",
            modifiers=set(self._active_modifiers),
            timestamp=time.time(),
        ))
        self._send(events[-1])

        events.append(KeyEvent(
            key=key_name,
            event_type="up",
            modifiers=set(self._active_modifiers),
            timestamp=time.time(),
        ))
        self._send(events[-1])

        for key in modifier:
            self.key_up(key)

        return events

    def _update_modifiers(self, key: str, is_press: bool) -> None:
        """Track active modifier keys."""
        modifier_keys = {"Control", "Shift", "Alt", "Meta", "Command"}
        if key in modifier_keys:
            if is_press:
                self._active_modifiers.add(key)
            else:
                self._active_modifiers.discard(key)

    def _send(self, event: KeyEvent) -> None:
        """Send a key event."""
        if self._sender:
            self._sender(event)
