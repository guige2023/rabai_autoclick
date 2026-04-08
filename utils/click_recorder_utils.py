"""
Click Recorder Utilities

Records and manages click events for UI automation,
including click patterns and sequences.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime
from enum import Enum, auto


class ClickType(Enum):
    """Types of click events."""
    LEFT = auto()
    RIGHT = auto()
    MIDDLE = auto()
    DOUBLE = auto()
    TRIPLE = auto()


@dataclass
class ClickEvent:
    """Represents a recorded click event."""
    x: int
    y: int
    click_type: ClickType
    timestamp: datetime
    target: str | None = None
    element_id: str | None = None
    modifiers: list[str] = field(default_factory=list)


class ClickRecorder:
    """
    Records click events for playback or analysis.
    
    Supports recording various click types and
    provides methods for playback and analysis.
    """

    def __init__(self) -> None:
        self._events: list[ClickEvent] = []
        self._is_recording = False

    def start_recording(self) -> None:
        """Start recording click events."""
        self._is_recording = True

    def stop_recording(self) -> None:
        """Stop recording click events."""
        self._is_recording = False

    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._is_recording

    def record(
        self,
        x: int,
        y: int,
        click_type: ClickType,
        target: str | None = None,
        element_id: str | None = None,
        modifiers: list[str] | None = None,
    ) -> ClickEvent:
        """
        Record a click event.
        
        Args:
            x: X coordinate.
            y: Y coordinate.
            click_type: Type of click.
            target: Target element description.
            element_id: Target element ID.
            modifiers: Active modifier keys.
            
        Returns:
            Created ClickEvent.
        """
        event = ClickEvent(
            x=x,
            y=y,
            click_type=click_type,
            timestamp=datetime.now(),
            target=target,
            element_id=element_id,
            modifiers=modifiers or [],
        )
        if self._is_recording:
            self._events.append(event)
        return event

    def get_events(self) -> list[ClickEvent]:
        """Get all recorded events."""
        return list(self._events)

    def clear(self) -> None:
        """Clear all recorded events."""
        self._events.clear()

    def get_click_count(self) -> dict[ClickType, int]:
        """Get count of each click type."""
        counts: dict[ClickType, int] = {}
        for event in self._events:
            counts[event.click_type] = counts.get(event.click_type, 0) + 1
        return counts

    def get_bounds(self) -> tuple[int, int, int, int] | None:
        """
        Get bounding box of all clicks.
        
        Returns:
            (min_x, min_y, max_x, max_y) or None if no events.
        """
        if not self._events:
            return None
        xs = [e.x for e in self._events]
        ys = [e.y for e in self._events]
        return (min(xs), min(ys), max(xs), max(ys))

    def export_sequence(self) -> list[dict[str, Any]]:
        """Export events as dictionaries."""
        return [
            {
                "x": e.x,
                "y": e.y,
                "type": e.click_type.name,
                "timestamp": e.timestamp.isoformat(),
                "target": e.target,
            }
            for e in self._events
        ]
