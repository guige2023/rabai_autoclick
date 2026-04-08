"""Key sequence recorder for UI automation.

Records and replays keyboard input sequences for automation workflows.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class KeyEvent(Enum):
    """Keyboard event types."""
    DOWN = auto()
    UP = auto()
    PRESS = auto()


@dataclass
class KeyStroke:
    """A single key event.

    Attributes:
        key: The key name (e.g., 'a', 'enter', 'ctrl').
        event: The type of key event.
        modifiers: Set of active modifiers when key was pressed.
        timestamp: Time in seconds from sequence start.
    """
    key: str
    event: KeyEvent = KeyEvent.PRESS
    modifiers: set[str] = field(default_factory=set)
    timestamp: float = 0.0

    def is_modifier(self) -> bool:
        """Return True if this key is a modifier."""
        return self.key.lower() in {
            "ctrl", "control", "alt", "shift", "meta",
            "command", "cmd", "option",
        }

    def is_combo(self) -> bool:
        """Return True if this is a key combo."""
        return len(self.modifiers) > 0 and self.event == KeyEvent.DOWN

    def get_combo_name(self) -> str:
        """Return a string representation of this combo."""
        parts = sorted(self.modifiers) + [self.key]
        return "+".join(parts)


@dataclass
class KeySequence:
    """A recorded sequence of key strokes.

    Attributes:
        name: Human-readable name for this sequence.
        strokes: Ordered list of key strokes.
        description: Description of what this sequence does.
    """
    name: str
    strokes: list[KeyStroke] = field(default_factory=list)
    description: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def add_stroke(
        self,
        key: str,
        event: KeyEvent = KeyEvent.PRESS,
        modifiers: Optional[set[str]] = None,
        timestamp: float = 0.0,
    ) -> None:
        """Add a stroke to this sequence."""
        self.strokes.append(KeyStroke(
            key=key,
            event=event,
            modifiers=modifiers or set(),
            timestamp=timestamp,
        ))

    @property
    def duration(self) -> float:
        """Total duration from first to last stroke."""
        if len(self.strokes) < 2:
            return 0.0
        return self.strokes[-1].timestamp - self.strokes[0].timestamp

    @property
    def combo_keys(self) -> list[str]:
        """Return all unique combo key combinations."""
        combos = set()
        for stroke in self.strokes:
            if stroke.is_combo():
                combos.add(stroke.get_combo_name())
        return sorted(combos)


class KeySequenceRecorder:
    """Records keyboard input sequences."""

    def __init__(self) -> None:
        """Initialize recorder."""
        self._current: list[KeyStroke] = []
        self._start_time: float = 0.0
        self._is_recording: bool = False

    def start_recording(self) -> None:
        """Start recording a new sequence."""
        import time
        self._current = []
        self._start_time = time.time()
        self._is_recording = True

    def stop_recording(self) -> list[KeyStroke]:
        """Stop recording and return the recorded strokes."""
        self._is_recording = False
        return list(self._current)

    def record_stroke(
        self,
        key: str,
        event: KeyEvent = KeyEvent.PRESS,
        modifiers: Optional[set[str]] = None,
    ) -> None:
        """Record a single key stroke."""
        if not self._is_recording:
            return

        import time
        timestamp = time.time() - self._start_time
        self._current.append(KeyStroke(
            key=key,
            event=event,
            modifiers=modifiers or set(),
            timestamp=timestamp,
        ))

    def is_recording(self) -> bool:
        """Return True if currently recording."""
        return self._is_recording


class KeySequenceReplayer:
    """Replays recorded key sequences."""

    def __init__(self) -> None:
        """Initialize replayer."""
        self._key_press_handler: Optional[Callable[[str, set[str]], None]] = None
        self._key_release_handler: Optional[Callable[[str, set[str]], None]] = None

    def set_handlers(
        self,
        on_press: Optional[Callable[[str, set[str]], None]] = None,
        on_release: Optional[Callable[[str, set[str]], None]] = None,
    ) -> None:
        """Set keyboard event handlers."""
        self._key_press_handler = on_press
        self._key_release_handler = on_release

    def replay(self, sequence: KeySequence) -> bool:
        """Replay a key sequence.

        Returns True if replay completed successfully.
        """
        try:
            for stroke in sequence.strokes:
                if stroke.event == KeyEvent.DOWN:
                    if self._key_press_handler:
                        self._key_press_handler(stroke.key, stroke.modifiers)
                elif stroke.event == KeyEvent.UP:
                    if self._key_release_handler:
                        self._key_release_handler(stroke.key, stroke.modifiers)
                else:
                    if self._key_press_handler:
                        self._key_press_handler(stroke.key, stroke.modifiers)
            return True
        except Exception:
            return False
