"""Input sequence mapper for UI automation.

Maps sequences of input events (keyboard, mouse, touch) to
named actions or automation tasks.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, Sequence


class InputEventType(Enum):
    """Types of input events."""
    KEY_DOWN = auto()
    KEY_UP = auto()
    MOUSE_MOVE = auto()
    MOUSE_DOWN = auto()
    MOUSE_UP = auto()
    MOUSE_CLICK = auto()
    MOUSE_WHEEL = auto()
    TOUCH_START = auto()
    TOUCH_MOVE = auto()
    TOUCH_END = auto()
    GESTURE = auto()


@dataclass
class InputEvent:
    """A single input event.

    Attributes:
        event_type: The type of input event.
        x: X coordinate (for mouse/touch events).
        y: Y coordinate (for mouse/touch events).
        key_code: Key code (for keyboard events).
        key_name: Named key (e.g., 'ctrl', 'enter').
        button: Mouse button (for mouse events).
        delta: Wheel delta (for wheel events).
        timestamp: Event timestamp in seconds.
        modifiers: Set of active modifier keys.
    """
    event_type: InputEventType
    x: float = 0.0
    y: float = 0.0
    key_code: Optional[int] = None
    key_name: str = ""
    button: int = 0
    delta: float = 0.0
    timestamp: float = 0.0
    modifiers: set[str] = field(default_factory=set)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def is_key(self) -> bool:
        """Return True if this is a keyboard event."""
        return self.event_type in (InputEventType.KEY_DOWN, InputEventType.KEY_UP)

    def is_mouse(self) -> bool:
        """Return True if this is a mouse event."""
        return self.event_type in (
            InputEventType.MOUSE_MOVE,
            InputEventType.MOUSE_DOWN,
            InputEventType.MOUSE_UP,
            InputEventType.MOUSE_CLICK,
        )

    def is_touch(self) -> bool:
        """Return True if this is a touch event."""
        return self.event_type in (
            InputEventType.TOUCH_START,
            InputEventType.TOUCH_MOVE,
            InputEventType.TOUCH_END,
        )

    def is_modifier_key(self) -> bool:
        """Return True if this is a modifier key."""
        return self.key_name.lower() in {
            "ctrl", "control", "alt", "shift", "meta",
            "command", "cmd", "option", "super",
        }

    def get_modifier_state(self) -> dict[str, bool]:
        """Return dict of modifier name -> pressed state."""
        return {mod: True for mod in self.modifiers}


@dataclass
class InputSequence:
    """A sequence of input events representing a complete action.

    Attributes:
        name: Human-readable name for this sequence.
        events: The ordered list of events in this sequence.
        timeout: Maximum time allowed between events (seconds).
        description: Human-readable description.
        metadata: Additional metadata.
    """
    name: str
    events: list[InputEvent] = field(default_factory=list)
    timeout: float = 2.0
    description: str = ""
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    metadata: dict = field(default_factory=dict)

    def add_event(self, event: InputEvent) -> None:
        """Append an event to this sequence."""
        self.events.append(event)

    @property
    def duration(self) -> float:
        """Total duration from first to last event."""
        if len(self.events) < 2:
            return 0.0
        return self.events[-1].timestamp - self.events[0].timestamp

    @property
    def is_empty(self) -> bool:
        """Return True if sequence has no events."""
        return len(self.events) == 0

    def get_keys_pressed(self) -> list[str]:
        """Return all key names that were pressed in this sequence."""
        return [
            e.key_name for e in self.events
            if e.event_type == InputEventType.KEY_DOWN and e.key_name
        ]

    def get_key_combo(self) -> frozenset[str]:
        """Return the set of keys held simultaneously (for combo detection)."""
        key_combo: set[str] = set()
        for event in self.events:
            if event.event_type == InputEventType.KEY_DOWN:
                key_combo.add(event.key_name.lower())
            elif event.event_type == InputEventType.KEY_UP:
                key_combo.discard(event.key_name.lower())
        return frozenset(key_combo)

    def get_mouse_positions(self) -> list[tuple[float, float]]:
        """Return list of (x, y) positions from mouse/touch events."""
        return [
            (e.x, e.y) for e in self.events
            if e.event_type in (InputEventType.MOUSE_MOVE, InputEventType.TOUCH_MOVE)
            or (e.is_mouse() and e.x != 0 or e.y != 0)
        ]

    def match_key_combo(self, combo: set[str]) -> bool:
        """Check if this sequence matches a key combo."""
        return self.get_key_combo() == frozenset(k.lower() for k in combo)


class InputSequenceMapper:
    """Maps input sequences to automation actions.

    Maintains a registry of sequences and their associated actions,
    supports fuzzy matching with time tolerance.
    """

    def __init__(self) -> None:
        """Initialize with an empty registry."""
        self._sequences: dict[str, InputSequence] = {}
        self._action_handlers: dict[str, Callable[[], Any]] = {}
        self._on_match_callbacks: list[
            Callable[[InputSequence, InputSequence], Any]
        ] = []

    def register_sequence(self, sequence: InputSequence) -> str:
        """Register a named input sequence."""
        self._sequences[sequence.id] = sequence
        return sequence.id

    def register_action(
        self,
        sequence_id: str,
        handler: Callable[[], Any],
    ) -> None:
        """Register an action handler for a sequence."""
        self._action_handlers[sequence_id] = handler

    def unregister_sequence(self, sequence_id: str) -> bool:
        """Remove a sequence. Returns True if found."""
        if sequence_id in self._sequences:
            del self._sequences[sequence_id]
            self._action_handlers.pop(sequence_id, None)
            return True
        return False

    def get_sequence(self, sequence_id: str) -> Optional[InputSequence]:
        """Retrieve a registered sequence."""
        return self._sequences.get(sequence_id)

    def find_by_name(self, name: str) -> Optional[InputSequence]:
        """Find first sequence with matching name."""
        for seq in self._sequences.values():
            if seq.name.lower() == name.lower():
                return seq
        return None

    def match(
        self,
        input_seq: InputSequence,
        tolerance: float = 0.3,
    ) -> Optional[tuple[InputSequence, float]]:
        """Match an input sequence against registered sequences.

        Returns (matched_sequence, confidence) or None.
        Confidence is 0.0-1.0.
        """
        best_match: Optional[InputSequence] = None
        best_confidence = 0.0

        for registered in self._sequences.values():
            confidence = self._compute_match(input_seq, registered, tolerance)
            if confidence > best_confidence:
                best_confidence = confidence
                best_match = registered

        if best_match and best_confidence >= 0.7:
            return (best_match, best_confidence)
        return None

    def _compute_match(
        self,
        input_seq: InputSequence,
        registered: InputSequence,
        tolerance: float,
    ) -> float:
        """Compute match confidence between two sequences."""
        if input_seq.is_empty or registered.is_empty:
            return 0.0

        input_keys = input_seq.get_key_combo()
        reg_keys = registered.get_key_combo()
        key_match = len(input_keys & reg_keys) / max(len(input_keys | reg_keys), 1)
        if not reg_keys:
            key_match = 0.0 if input_keys else 1.0

        duration_ratio = (
            input_seq.duration / max(registered.duration, 0.001)
            if registered.duration > 0
            else 1.0 if input_seq.duration == 0 else 0.0
        )
        duration_score = min(duration_ratio, 1.0 / max(duration_ratio, 0.001))
        duration_score = min(duration_score, 1.0)

        score = (key_match * 0.7) + (duration_score * 0.3)
        return max(0.0, min(1.0, score))

    def execute_match(self, input_seq: InputSequence) -> Any:
        """Match and execute the corresponding action."""
        result = self.match(input_seq)
        if result is None:
            return None
        sequence, _ = result
        handler = self._action_handlers.get(sequence.id)
        if handler:
            return handler()
        return None

    def on_match(
        self,
        callback: Callable[[InputSequence, InputSequence], Any],
    ) -> None:
        """Register a callback for sequence matches."""
        self._on_match_callbacks.append(callback)

    @property
    def count(self) -> int:
        """Return number of registered sequences."""
        return len(self._sequences)

    @property
    def all_sequences(self) -> list[InputSequence]:
        """Return all registered sequences."""
        return list(self._sequences.values())


# Convenience: build a sequence from a list of event dicts
def build_sequence_from_dicts(
    name: str,
    event_dicts: list[dict[str, Any]],
) -> InputSequence:
    """Build an InputSequence from a list of event dictionaries."""
    sequence = InputSequence(name=name)
    for ed in event_dicts:
        event_type_str = ed.get("type", "")
        try:
            event_type = InputEventType[event_type_str.upper()]
        except KeyError:
            event_type = InputEventType.MOUSE_MOVE
        event = InputEvent(
            event_type=event_type,
            x=ed.get("x", 0.0),
            y=ed.get("y", 0.0),
            key_code=ed.get("key_code"),
            key_name=ed.get("key_name", ""),
            button=ed.get("button", 0),
            delta=ed.get("delta", 0.0),
            timestamp=ed.get("timestamp", 0.0),
            modifiers=set(ed.get("modifiers", [])),
        )
        sequence.add_event(event)
    return sequence
