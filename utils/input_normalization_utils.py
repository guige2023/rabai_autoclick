"""Input normalization utilities.

This module provides utilities for normalizing input events across
different input sources and platforms.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, TypeVar
from dataclasses import dataclass, field
from enum import Enum, auto

T = TypeVar("T")


class InputType(Enum):
    """Types of input events."""
    KEYBOARD = auto()
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_SCROLL = auto()
    TOUCH = auto()
    GESTURE = auto()


@dataclass
class NormalizedInput:
    """A normalized input event."""
    input_type: InputType
    timestamp: float
    data: Dict[str, Any] = field(default_factory=dict)
    source: str = "unknown"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "input_type": self.input_type.name,
            "timestamp": self.timestamp,
            "data": self.data,
            "source": self.source,
        }


@dataclass
class InputSequence:
    """A sequence of normalized input events."""
    inputs: List[NormalizedInput] = field(default_factory=list)
    label: Optional[str] = None

    def add(self, inp: NormalizedInput) -> None:
        """Add an input to the sequence."""
        self.inputs.append(inp)

    def duration(self) -> float:
        """Get total duration of the sequence."""
        if len(self.inputs) < 2:
            return 0.0
        return self.inputs[-1].timestamp - self.inputs[0].timestamp

    def is_empty(self) -> bool:
        """Check if sequence is empty."""
        return len(self.inputs) == 0

    def clear(self) -> None:
        """Clear all inputs."""
        self.inputs.clear()


def normalize_key_event(
    key_code: int,
    key_name: str,
    timestamp: float,
    modifiers: Optional[List[str]] = None,
) -> NormalizedInput:
    """Normalize a keyboard event.

    Args:
        key_code: Virtual key code.
        key_name: Human-readable key name.
        timestamp: Event timestamp.
        modifiers: List of active modifiers.

    Returns:
        NormalizedInput for keyboard.
    """
    return NormalizedInput(
        input_type=InputType.KEYBOARD,
        timestamp=timestamp,
        data={
            "key_code": key_code,
            "key_name": key_name,
            "modifiers": modifiers or [],
        },
    )


def normalize_mouse_event(
    x: int,
    y: int,
    button: int,
    event_type: str,
    timestamp: float,
) -> NormalizedInput:
    """Normalize a mouse event.

    Args:
        x: X coordinate.
        y: Y coordinate.
        button: Button number (0=left, 1=middle, 2=right).
        event_type: Type of event (move, click, release, scroll).
        timestamp: Event timestamp.

    Returns:
        NormalizedInput for mouse.
    """
    return NormalizedInput(
        input_type=InputType.MOUSE_CLICK if "click" in event_type else InputType.MOUSE_MOVE,
        timestamp=timestamp,
        data={
            "x": x,
            "y": y,
            "button": button,
            "event_type": event_type,
        },
    )


def normalize_touch_event(
    x: int,
    y: int,
    touch_id: int,
    phase: str,
    timestamp: float,
) -> NormalizedInput:
    """Normalize a touch event.

    Args:
        x: X coordinate.
        y: Y coordinate.
        touch_id: Touch identifier.
        phase: Touch phase (began, moved, ended, cancelled).
        timestamp: Event timestamp.

    Returns:
        NormalizedInput for touch.
    """
    return NormalizedInput(
        input_type=InputType.TOUCH,
        timestamp=timestamp,
        data={
            "x": x,
            "y": y,
            "touch_id": touch_id,
            "phase": phase,
        },
    )


def merge_sequences(*sequences: InputSequence) -> InputSequence:
    """Merge multiple input sequences.

    Args:
        *sequences: Input sequences to merge.

    Returns:
        Merged InputSequence.
    """
    merged = InputSequence()
    for seq in sequences:
        merged.inputs.extend(seq.inputs)
    return merged


def deduplicate_sequence(seq: InputSequence, max_delta_ms: float = 50.0) -> InputSequence:
    """Remove duplicate events within time window.

    Args:
        seq: Input sequence to deduplicate.
        max_delta_ms: Maximum time delta in milliseconds.

    Returns:
        Deduplicated InputSequence.
    """
    if not seq.inputs:
        return InputSequence()

    result = InputSequence()
    result.add(seq.inputs[0])

    for inp in seq.inputs[1:]:
        last = result.inputs[-1]
        delta_ms = (inp.timestamp - last.timestamp) * 1000
        if delta_ms > max_delta_ms or inp.input_type != last.input_type:
            result.add(inp)

    return result


__all__ = [
    "InputType",
    "NormalizedInput",
    "InputSequence",
    "normalize_key_event",
    "normalize_mouse_event",
    "normalize_touch_event",
    "merge_sequences",
    "deduplicate_sequence",
]
