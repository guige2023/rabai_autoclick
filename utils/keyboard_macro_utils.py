"""
Keyboard macro utilities for recording and replaying keyboard macros.

Provides keyboard macro recording, storage, and playback
with support for key combinations and timing.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Optional


@dataclass
class KeyEvent:
    """A single key event."""
    key: str
    event_type: str  # "down", "up", "press"
    timestamp_ms: float
    modifiers: list[str] = field(default_factory=list)  # "ctrl", "shift", "alt", "cmd"


@dataclass
class KeyboardMacro:
    """A recorded keyboard macro."""
    name: str
    events: list[KeyEvent] = field(default_factory=list)
    description: str = ""
    created_at: float = field(default_factory=time.time)

    def total_duration_ms(self) -> float:
        if not self.events:
            return 0.0
        return self.events[-1].timestamp_ms - self.events[0].timestamp_ms


class KeyboardMacroRecorder:
    """Records keyboard macros."""

    def __init__(self, name: str = ""):
        self.name = name
        self._events: list[KeyEvent] = []
        self._start_time: Optional[float] = None
        self._is_recording = False

    def start(self) -> None:
        """Start recording."""
        self._events.clear()
        self._start_time = time.time() * 1000
        self._is_recording = True

    def stop(self) -> KeyboardMacro:
        """Stop recording and return the macro."""
        self._is_recording = False
        return KeyboardMacro(name=self.name, events=self._events[:])

    def record_key_down(self, key: str, modifiers: list[str] = None) -> None:
        """Record a key down event."""
        if not self._is_recording:
            return
        ts = (time.time() * 1000) - (self._start_time or 0)
        self._events.append(KeyEvent(
            key=key, event_type="down",
            timestamp_ms=ts, modifiers=modifiers or []
        ))

    def record_key_up(self, key: str, modifiers: list[str] = None) -> None:
        """Record a key up event."""
        if not self._is_recording:
            return
        ts = (time.time() * 1000) - (self._start_time or 0)
        self._events.append(KeyEvent(
            key=key, event_type="up",
            timestamp_ms=ts, modifiers=modifiers or []
        ))

    def record_key_press(self, key: str, modifiers: list[str] = None) -> None:
        """Record a complete key press (down + up)."""
        if not self._is_recording:
            return
        ts = (time.time() * 1000) - (self._start_time or 0)
        mods = modifiers or []
        self._events.append(KeyEvent(key=key, event_type="down", timestamp_ms=ts, modifiers=mods))
        self._events.append(KeyEvent(key=key, event_type="up", timestamp_ms=ts + 10, modifiers=mods))


class KeyboardMacroPlayer:
    """Plays back keyboard macros."""

    def __init__(
        self,
        key_executor: Optional[Callable[[KeyEvent], bool]] = None,
    ):
        self._key_executor = key_executor or self._default_executor
        self._is_playing = False

    def play(
        self,
        macro: KeyboardMacro,
        speed: float = 1.0,
        on_event: Optional[Callable[[KeyEvent], None]] = None,
    ) -> bool:
        """Play a keyboard macro.

        Args:
            macro: The macro to play
            speed: Playback speed multiplier
            on_event: Optional callback for each event

        Returns:
            True if successful, False otherwise
        """
        if not macro.events:
            return False

        self._is_playing = True
        start_time = time.time() * 1000
        first_ts = macro.events[0].timestamp_ms

        try:
            for event in macro.events:
                if not self._is_playing:
                    return False

                event_start = (event.timestamp_ms - first_ts) / speed
                elapsed = (time.time() * 1000) - start_time
                wait_time = (event_start - elapsed) / 1000.0

                if wait_time > 0:
                    time.sleep(wait_time)

                if on_event:
                    on_event(event)

                if not self._key_executor(event):
                    return False

            return True
        finally:
            self._is_playing = False

    def stop(self) -> None:
        """Stop playback."""
        self._is_playing = False

    def _default_executor(self, event: KeyEvent) -> bool:
        """Default key executor."""
        return True


# Utility function for creating key combos
def make_key_combo(keys: list[str]) -> str:
    """Create a key combo string like 'ctrl+shift+a'."""
    return "+".join(keys)


__all__ = ["KeyboardMacroRecorder", "KeyboardMacroPlayer", "KeyboardMacro", "KeyEvent", "make_key_combo"]
