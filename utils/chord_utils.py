"""
Chord Recognition Utilities for Keyboard Input.

This module provides utilities for recognizing and processing
keyboard chords (multiple simultaneous key presses) in UI automation.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Set, FrozenSet, Optional, Dict, List, Tuple
from enum import Enum


class ChordState(Enum):
    """States of a chord recognition."""
    INCOMPLETE = "incomplete"
    RECOGNIZED = "recognized"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class ChordMatch:
    """Result of chord matching."""
    is_match: bool
    chord_name: Optional[str] = None
    confidence: float = 0.0
    state: ChordState = ChordState.INCOMPLETE
    details: Dict = field(default_factory=dict)


@dataclass
class ChordPattern:
    """Pattern defining a keyboard chord."""
    name: str
    keys: FrozenSet[str]
    description: str = ""
    modifiers: FrozenSet[str] = field(default_factory=frozenset)

    def matches(self, pressed_keys: Set[str]) -> bool:
        """Check if pressed keys match this pattern."""
        return self.keys == pressed_keys

    def is_subset(self, pressed_keys: Set[str]) -> bool:
        """Check if this chord's keys are a subset of pressed keys."""
        return self.keys.issubset(pressed_keys)

    @property
    def key_count(self) -> int:
        return len(self.keys)

    @property
    def is_modifier_chord(self) -> bool:
        return bool(self.modifiers)


class ChordRecognizer:
    """
    Recognize keyboard chords from key press events.
    """

    def __init__(self, timeout_ms: float = 500.0):
        """
        Initialize chord recognizer.

        Args:
            timeout_ms: Maximum time between key presses for chord
        """
        self.timeout_ms = timeout_ms
        self._patterns: Dict[str, ChordPattern] = {}
        self._pressed_keys: Set[str] = set()
        self._press_times: Dict[str, float] = {}

    def register_chord(self, pattern: ChordPattern) -> None:
        """Register a chord pattern."""
        self._patterns[pattern.name] = pattern

    def register_chords(self, patterns: List[ChordPattern]) -> None:
        """Register multiple chord patterns."""
        for pattern in patterns:
            self.register_chord(pattern)

    def key_pressed(self, key: str, timestamp: float) -> ChordMatch:
        """
        Record a key press.

        Args:
            key: Key identifier
            timestamp: Press timestamp in seconds

        Returns:
            ChordMatch result
        """
        self._pressed_keys.add(key)
        self._press_times[key] = timestamp
        return self._recognize()

    def key_released(self, key: str) -> None:
        """
        Record a key release.

        Args:
            key: Key identifier
        """
        self._pressed_keys.discard(key)
        self._press_times.pop(key, None)

    def _recognize(self) -> ChordMatch:
        """Attempt to recognize a chord from current state."""
        if not self._pressed_keys:
            return ChordMatch(is_match=False, state=ChordState.INCOMPLETE)

        for name, pattern in self._patterns.items():
            if pattern.matches(self._pressed_keys):
                return ChordMatch(
                    is_match=True,
                    chord_name=name,
                    confidence=1.0,
                    state=ChordState.RECOGNIZED,
                    details={"pattern": pattern.description}
                )

        partial_matches = [
            (name, p) for name, p in self._patterns.items()
            if p.is_subset(self._pressed_keys)
        ]
        if partial_matches:
            return ChordMatch(
                is_match=False,
                state=ChordState.INCOMPLETE,
                details={"partial_matches": [n for n, _ in partial_matches]}
            )

        return ChordMatch(is_match=False, state=ChordState.INCOMPLETE)

    def reset(self) -> None:
        """Reset current chord state."""
        self._pressed_keys.clear()
        self._press_times.clear()

    def get_current_keys(self) -> Set[str]:
        """Get currently pressed keys."""
        return self._pressed_keys.copy()


def common_chords() -> List[ChordPattern]:
    """Get list of common keyboard chords."""
    return [
        ChordPattern(
            name="ctrl_a",
            keys=frozenset({"ctrl", "a"}),
            description="Select all",
            modifiers=frozenset({"ctrl"})
        ),
        ChordPattern(
            name="ctrl_c",
            keys=frozenset({"ctrl", "c"}),
            description="Copy",
            modifiers=frozenset({"ctrl"})
        ),
        ChordPattern(
            name="ctrl_v",
            keys=frozenset({"ctrl", "v"}),
            description="Paste",
            modifiers=frozenset({"ctrl"})
        ),
        ChordPattern(
            name="ctrl_z",
            keys=frozenset({"ctrl", "z"}),
            description="Undo",
            modifiers=frozenset({"ctrl"})
        ),
        ChordPattern(
            name="ctrl_shift_z",
            keys=frozenset({"ctrl", "shift", "z"}),
            description="Redo",
            modifiers=frozenset({"ctrl", "shift"})
        ),
        ChordPattern(
            name="alt_f4",
            keys=frozenset({"alt", "f4"}),
            description="Close window",
            modifiers=frozenset({"alt"})
        ),
        ChordPattern(
            name="cmd_space",
            keys=frozenset({"cmd", "space"}),
            description="Spotlight search",
            modifiers=frozenset({"cmd"})
        ),
        ChordPattern(
            name="ctrl_tab",
            keys=frozenset({"ctrl", "tab"}),
            description="Next tab",
            modifiers=frozenset({"ctrl"})
        ),
    ]


def chord_to_human_readable(chord: ChordPattern) -> str:
    """
    Convert chord to human-readable string.

    Args:
        chord: Chord pattern

    Returns:
        Human-readable representation
    """
    key_parts = []
    for key in sorted(chord.keys):
        if key == "ctrl":
            key_parts.append("Ctrl")
        elif key == "shift":
            key_parts.append("Shift")
        elif key == "alt":
            key_parts.append("Alt")
        elif key == "cmd":
            key_parts.append("Cmd")
        else:
            key_parts.append(key.upper())

    return " + ".join(key_parts)
