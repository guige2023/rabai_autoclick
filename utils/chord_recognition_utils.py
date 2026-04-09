"""Chord recognition utilities for keyboard shortcut and gesture chord detection.

This module provides utilities for recognizing multi-key chords,
gesture combinations, and complex input patterns.
"""

from __future__ import annotations

from typing import Set, FrozenSet, Callable
from dataclasses import dataclass, field
from enum import Enum
import time


class InputType(Enum):
    """Type of input event."""
    KEY = "key"
    MOUSE_BUTTON = "mouse_button"
    TOUCH = "touch"
    GESTURE = "gesture"


@dataclass
class InputEvent:
    """Single input event.

    Attributes:
        input_type: Type of input.
        code: Input code (key code, button index, etc.).
        modifiers: Set of active modifiers.
        timestamp: Event timestamp in seconds.
    """
    input_type: InputType
    code: str
    modifiers: FrozenSet[str] = field(default_factory=frozenset)
    timestamp: float = field(default_factory=time.time)


@dataclass
class Chord:
    """Represents a chord (combination of inputs pressed together).

    Attributes:
        inputs: Set of input codes in this chord.
        modifiers: Required modifiers.
        name: Optional human-readable name.
    """
    inputs: FrozenSet[str]
    modifiers: FrozenSet[str] = field(default_factory=frozenset)
    name: str | None = None

    def matches(self, other: Chord) -> bool:
        """Check if this chord matches another.

        Args:
            other: Chord to compare against.

        Returns:
            True if chords match.
        """
        return self.inputs == other.inputs and self.modifiers == other.modifiers

    def contains(self, code: str) -> bool:
        """Check if chord contains a specific input code.

        Args:
            code: Input code to check.

        Returns:
            True if code is in this chord.
        """
        return code in self.inputs


@dataclass
class ChordPattern:
    """Pattern of chords in sequence.

    Attributes:
        name: Pattern name.
        chords: Sequence of chords.
        timeout_ms: Max time between chords.
        repeat_allowed: Whether pattern can repeat.
    """
    name: str
    chords: list[Chord]
    timeout_ms: float = 500.0
    repeat_allowed: bool = False


class ChordRecognizer:
    """Recognizer for input chords and chord patterns.

    Tracks currently pressed inputs and matches against
    registered chord patterns.
    """

    def __init__(self) -> None:
        """Initialize chord recognizer."""
        self._active_keys: Set[str] = set()
        self._active_modifiers: Set[str] = set()
        self._last_chord_time: float = 0.0
        self._chord_history: list[Chord] = []
        self._patterns: list[ChordPattern] = []

    @property
    def active_chord(self) -> Chord:
        """Get current active chord."""
        return Chord(
            inputs=frozenset(self._active_keys),
            modifiers=frozenset(self._active_modifiers)
        )

    def press(self, code: str, is_modifier: bool = False) -> None:
        """Register a key/button press.

        Args:
            code: Input code.
            is_modifier: Whether this is a modifier key.
        """
        self._active_keys.add(code)

        if is_modifier:
            self._active_modifiers.add(code)

    def release(self, code: str) -> None:
        """Register a key/button release.

        Args:
            code: Input code.
        """
        self._active_keys.discard(code)
        self._active_modifiers.discard(code)

    def get_current_chord(self) -> Chord:
        """Get current held chord.

        Returns:
            Chord representing all currently pressed inputs.
        """
        return Chord(
            inputs=frozenset(self._active_keys),
            modifiers=frozenset(self._active_modifiers)
        )

    def is_chord_pressed(self, chord: Chord) -> bool:
        """Check if a specific chord is currently pressed.

        Args:
            chord: Chord to check.

        Returns:
            True if exactly all inputs in chord are pressed.
        """
        current = self.get_current_chord()
        return current.matches(chord)

    def register_pattern(self, pattern: ChordPattern) -> None:
        """Register a chord pattern to recognize.

        Args:
            pattern: ChordPattern to register.
        """
        self._patterns.append(pattern)

    def recognize(self) -> ChordPattern | None:
        """Attempt to recognize a registered pattern.

        Returns:
            Matched ChordPattern or None.
        """
        current_time = time.time()

        for pattern in self._patterns:
            if self._match_pattern(pattern, current_time):
                return pattern

        return None

    def _match_pattern(self, pattern: ChordPattern, current_time: float) -> bool:
        """Check if current input matches a pattern.

        Args:
            pattern: Pattern to match against.
            current_time: Current timestamp.

        Returns:
            True if pattern matches.
        """
        if len(self._chord_history) > len(pattern.chords):
            self._chord_history = self._chord_history[-len(pattern.chords):]

        for i, chord in enumerate(pattern.chords):
            if i >= len(self._chord_history):
                return False

            history_chord = self._chord_history[i]

            if not history_chord.matches(chord):
                return False

            if i > 0:
                time_diff = (current_time - self._last_chord_time) * 1000
                if time_diff > pattern.timeout_ms:
                    return False

        return True

    def commit_current_chord(self) -> Chord:
        """Commit current chord to history.

        Returns:
            The committed chord.
        """
        chord = self.get_current_chord()
        self._chord_history.append(chord)
        self._last_chord_time = time.time()
        return chord

    def clear(self) -> None:
        """Clear all active inputs and history."""
        self._active_keys.clear()
        self._active_modifiers.clear()
        self._chord_history.clear()


def normalize_modifiers(modifiers: Set[str] | FrozenSet[str]) -> FrozenSet[str]:
    """Normalize modifier key names.

    Handles platform differences (cmd vs meta, etc.).

    Args:
        modifiers: Set of modifier names.

    Returns:
        Normalized frozenset of modifiers.
    """
    normalize_map = {
        "cmd": "meta",
        "command": "meta",
        "control": "ctrl",
        "option": "alt",
    }

    result: Set[str] = set()

    for mod in modifiers:
        lower_mod = mod.lower()
        normalized = normalize_map.get(lower_mod, lower_mod)
        result.add(normalized)

    return frozenset(result)


def parse_chord_string(chord_str: str) -> Chord:
    """Parse a chord string like "Cmd+Shift+P" or "Ctrl+Alt+Del".

    Args:
        chord_str: String representation of chord.

    Returns:
        Parsed Chord object.
    """
    parts = chord_str.replace(" ", "").split("+")
    inputs: Set[str] = set()
    modifiers: Set[str] = set()

    modifier_keys = {"cmd", "command", "control", "ctrl", "option", "alt", "shift", "meta", "super"}

    for part in parts:
        lower = part.lower()

        if lower in modifier_keys:
            modifiers.add(lower)
        else:
            inputs.add(part)

    return Chord(
        inputs=frozenset(inputs),
        modifiers=frozenset(modifiers),
        name=chord_str
    )


def chord_to_string(chord: Chord, separator: str = "+") -> str:
    """Convert chord to human-readable string.

    Args:
        chord: Chord to convert.
        separator: Separator between inputs.

    Returns:
        String like "Cmd+Shift+P".
    """
    parts = list(chord.modifiers) + list(chord.inputs)
    return separator.join(parts)


def chords_equal(c1: Chord | None, c2: Chord | None) -> bool:
    """Check if two chords are equal (handling None).

    Args:
        c1: First chord.
        c2: Second chord.

    Returns:
        True if both None or both equal.
    """
    if c1 is None and c2 is None:
        return True

    if c1 is None or c2 is None:
        return False

    return c1.matches(c2)


def get_chord_conflicts(chord1: Chord, chord2: Chord) -> Set[str]:
    """Find conflicting input codes between two chords.

    Args:
        chord1: First chord.
        chord2: Second chord.

    Returns:
        Set of conflicting input codes.
    """
    return chord1.inputs & chord2.inputs


def is_subchord(small: Chord, large: Chord) -> bool:
    """Check if small is a sub-chord of large.

    Args:
        small: Potential sub-chord.
        large: Potential containing chord.

    Returns:
        True if small is subset of large.
    """
    return small.inputs <= large.inputs and small.modifiers <= large.modifiers


# Common shortcut chords
MODIFIER_CODES = frozenset({
    "shift", "ctrl", "alt", "meta", "super", "cmd",
    "left_shift", "right_shift",
    "left_control", "right_control",
    "left_alt", "right_alt",
    "left_meta", "right_meta",
})

SPECIAL_CODES = frozenset({
    "escape", "enter", "return", "tab", "space",
    "backspace", "delete", "forward_delete",
    "up", "down", "left", "right",
    "home", "end", "page_up", "page_down",
    "f1", "f2", "f3", "f4", "f5", "f6",
    "f7", "f8", "f9", "f10", "f11", "f12",
})
