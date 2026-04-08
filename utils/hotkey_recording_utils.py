"""Hotkey recording, parsing, and playback utilities.

Provides tools for capturing keyboard shortcut combinations,
parsing human-readable hotkey strings (e.g., "Ctrl+Shift+A"),
and generating the corresponding key events for playback.

Example:
    >>> from utils.hotkey_recording_utils import Hotkey, HotkeyRecorder
    >>> hk = Hotkey.parse("Cmd+Shift+P")
    >>> print(hk.is_chord)  # True for multi-key sequences
    >>> recorder = HotkeyRecorder()
    >>> recorder.start()
    >>> recorder.record(Key.A, modifiers=["Cmd", "Shift"])
    >>> combo = recorder.stop()
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Literal

__all__ = [
    "Hotkey",
    "HotkeyRecorder",
    "HotkeyParser",
    "MODIFIER_KEYS",
    "ALL_KEYS",
]


MODIFIER_KEYS = frozenset({
    "ctrl", "control", "cmd", "command", "alt", "option",
    "shift", "fn", "hyper", "super",
})

# Canonical modifier names
CANONICAL_MODIFIERS = {
    "ctrl": "CmdOrCtrl",
    "control": "CmdOrCtrl",
    "cmd": "CmdOrCtrl",
    "command": "CmdOrCtrl",
    "alt": "Alt",
    "option": "Alt",
    "shift": "Shift",
    "fn": "Fn",
    "hyper": "Hyper",
    "super": "Super",
}

ALL_KEYS = frozenset({
    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m",
    "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z",
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9",
    "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11", "f12",
    "return", "enter", "tab", "space", "backspace", "delete",
    "escape", "esc", "left", "right", "up", "down",
    "home", "end", "pageup", "pagedown",
})


@dataclass
class Hotkey:
    """Represents a keyboard shortcut combination.

    Attributes:
        key: The main key (e.g., "p", "return", "f5").
        modifiers: Ordered list of modifier keys (e.g., ["Cmd", "Shift"]).
        is_chord: True if this is a key chord (multiple keys pressed together).
        label: Human-readable string representation.

    Example:
        >>> hk = Hotkey(key="p", modifiers=["Cmd", "Shift"])
        >>> print(hk.label)
        Cmd+Shift+P
        >>> hk.to_pyautogui()
        ('p', ['cmd', 'shift'])
    """

    key: str
    modifiers: list[str] = field(default_factory=list)
    _label: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        self.key = self.key.lower()
        self.modifiers = [self._canonical(m) for m in self.modifiers]

    @staticmethod
    def _canonical(mod: str) -> str:
        """Return canonical modifier name."""
        return CANONICAL_MODIFIERS.get(mod.lower(), mod.title())

    @property
    def label(self) -> str:
        """Human-readable hotkey string."""
        if self._label:
            return self._label
        mod_str = "+".join(m.title() for m in self.modifiers)
        key_str = self.key.upper()
        if mod_str:
            return f"{mod_str}+{key_str}"
        return key_str

    @property
    def is_chord(self) -> bool:
        """Return True if this has modifiers (is a chord)."""
        return len(self.modifiers) > 0

    def to_pyautogui(self) -> tuple[str, list[str]]:
        """Return a pyautogui-compatible (key, modifiers) tuple."""
        return (self.key, [m.lower() for m in self.modifiers])

    def __repr__(self) -> str:
        return f"Hotkey({self.label})"

    @classmethod
    def parse(cls, s: str) -> "Hotkey":
        """Parse a hotkey string like 'Ctrl+Shift+P'.

        Args:
            s: A hotkey string with '+' separated keys.

        Returns:
            A Hotkey instance.
        """
        parts = s.replace(" ", "+").split("+")
        parts = [p for p in parts if p]
        if not parts:
            raise ValueError(f"Empty hotkey string: {s!r}")

        key = parts[-1].lower()
        mods = parts[:-1]
        label = "+".join(p.title() for p in parts)
        return cls(key=key, modifiers=mods, _label=label)


class HotkeyRecorder:
    """Records hotkey sequences for later playback.

    Example:
        >>> recorder = HotkeyRecorder()
        >>> recorder.start()
        >>> # simulate key presses ...
        >>> recorder.record_key_down("p", ["Cmd", "Shift"])
        >>> recorder.record_key_up("p")
        >>> combo = recorder.stop()
    """

    def __init__(self) -> None:
        self._active = False
        self._start_time: float = 0.0
        self._events: list[dict] = []

    def start(self) -> None:
        """Start recording."""
        self._active = True
        self._start_time = time.time()
        self._events = []

    def stop(self) -> dict:
        """Stop recording and return the captured sequence."""
        self._active = False
        return self._build_sequence()

    def record_key_down(
        self,
        key: str,
        modifiers: list[str] | None = None,
    ) -> None:
        """Record a key down event.

        Args:
            key: The key being pressed.
            modifiers: List of active modifiers.
        """
        if not self._active:
            return
        self._events.append({
            "type": "down",
            "key": key.lower(),
            "modifiers": modifiers or [],
            "time": time.time() - self._start_time,
        })

    def record_key_up(self, key: str) -> None:
        """Record a key up event."""
        if not self._active:
            return
        self._events.append({
            "type": "up",
            "key": key.lower(),
            "time": time.time() - self._start_time,
        })

    def record_chord(
        self,
        key: str,
        modifiers: list[str],
    ) -> None:
        """Record a complete chord (press and release).

        Args:
            key: The main key.
            modifiers: List of modifiers.
        """
        if not self._active:
            return
        t = time.time() - self._start_time
        self._events.append({
            "type": "chord",
            "key": key.lower(),
            "modifiers": modifiers,
            "time": t,
        })

    def _build_sequence(self) -> dict:
        """Build a sequence dict from recorded events."""
        return {
            "events": list(self._events),
            "duration": self._events[-1]["time"] if self._events else 0.0,
            "key_count": sum(1 for e in self._events if e["type"] in ("down", "chord")),
        }


class HotkeyParser:
    """Parse and validate hotkey strings.

    Provides validation, normalization, and conversion utilities
    for hotkey strings from various formats.
    """

    @staticmethod
    def is_valid(s: str) -> bool:
        """Check if a hotkey string is syntactically valid.

        Args:
            s: Hotkey string like 'Ctrl+Shift+P'.

        Returns:
            True if valid, False otherwise.
        """
        try:
            Hotkey.parse(s)
            return True
        except (ValueError, KeyError):
            return False

    @staticmethod
    def normalize(s: str) -> str:
        """Normalize a hotkey string to canonical form.

        Args:
            s: Input hotkey string.

        Returns:
            Normalized string like 'CmdOrCtrl+Shift+P'.
        """
        return str(Hotkey.parse(s))

    @staticmethod
    def parse_platform(
        s: str,
        platform: Literal["mac", "windows", "linux"],
    ) -> str:
        """Parse a hotkey and return a platform-specific string.

        Args:
            s: Input hotkey string.
            platform: Target platform.

        Returns:
            Platform-specific hotkey string.
        """
        hk = Hotkey.parse(s)
        parts = [p.lower() for p in hk.modifiers]
        if platform == "mac":
            parts = [p.replace("ctrl", "⌘").replace("cmd", "⌘")
                     .replace("alt", "⌥").replace("option", "⌥")
                     .replace("shift", "⇧") for p in parts]
        elif platform == "windows":
            parts = [p.replace("cmd", "Ctrl").replace("cmdorctrl", "Ctrl")
                     for p in parts]
        key_part = hk.key.upper()
        return "+".join(parts + [key_part])
