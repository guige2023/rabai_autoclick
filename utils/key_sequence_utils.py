"""
Key Sequence Utilities

Provides utilities for managing key sequences
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class KeyStroke:
    """Represents a single key stroke."""
    key: str
    modifiers: list[str]
    timestamp: float = 0.0


class KeySequence:
    """
    Manages sequences of key strokes.
    
    Provides recording, playback, and
    modification of key sequences.
    """

    def __init__(self) -> None:
        self._strokes: list[KeyStroke] = []

    def add_stroke(
        self,
        key: str,
        modifiers: list[str] | None = None,
    ) -> None:
        """Add a key stroke to the sequence."""
        import time
        self._strokes.append(KeyStroke(
            key=key,
            modifiers=modifiers or [],
            timestamp=time.time(),
        ))

    def get_strokes(self) -> list[KeyStroke]:
        """Get all strokes in sequence."""
        return list(self._strokes)

    def clear(self) -> None:
        """Clear all strokes."""
        self._strokes.clear()

    def append(self, other: KeySequence) -> None:
        """Append another sequence."""
        self._strokes.extend(other.get_strokes())

    def to_string(self) -> str:
        """Convert sequence to readable string."""
        parts = []
        for stroke in self._strokes:
            combo = ""
            if stroke.modifiers:
                combo = "+".join(stroke.modifiers) + "+"
            combo += stroke.key
            parts.append(combo)
        return " -> ".join(parts)

    def get_length(self) -> int:
        """Get number of strokes."""
        return len(self._strokes)
