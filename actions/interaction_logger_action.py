"""
Interaction Logger Action Module.

Logs all UI interactions for debugging, playback, and audit
purposes with structured event capture.
"""

import json
import time
from collections import deque
from datetime import datetime
from typing import Any, Optional


class InteractionEvent:
    """Represents a single interaction event."""

    def __init__(
        self,
        event_type: str,
        target: Optional[str] = None,
        coordinates: Optional[tuple] = None,
        metadata: Optional[dict] = None,
    ):
        """
        Initialize interaction event.

        Args:
            event_type: Type of event (click, type, scroll, etc.).
            target: Element selector or identifier.
            coordinates: (x, y) tuple for click/scroll events.
            metadata: Additional event metadata.
        """
        self.event_type = event_type
        self.target = target
        self.coordinates = coordinates
        self.metadata = metadata or {}
        self.timestamp = time.time()
        self.iso_timestamp = datetime.now().isoformat()

    def to_dict(self) -> dict:
        """Convert to dictionary representation."""
        return {
            "event_type": self.event_type,
            "target": self.target,
            "coordinates": self.coordinates,
            "metadata": self.metadata,
            "timestamp": self.timestamp,
            "iso_timestamp": self.iso_timestamp,
        }

    def __repr__(self) -> str:
        coords_str = f"@{self.coordinates}" if self.coordinates else ""
        return f"InteractionEvent({self.event_type} {self.target or ''} {coords_str})"


class InteractionLogger:
    """Logs and manages UI interaction events."""

    def __init__(self, max_events: int = 10000):
        """
        Initialize interaction logger.

        Args:
            max_events: Maximum number of events to retain in memory.
        """
        self.max_events = max_events
        self._events: deque[InteractionEvent] = deque(maxlen=max_events)
        self._session_id = self._generate_session_id()
        self._enabled = True

    def log(
        self,
        event_type: str,
        target: Optional[str] = None,
        coordinates: Optional[tuple] = None,
        **metadata,
    ) -> InteractionEvent:
        """
        Log an interaction event.

        Args:
            event_type: Type of interaction.
            target: Element target.
            coordinates: Click/scroll coordinates.
            **metadata: Additional metadata.

        Returns:
            The created InteractionEvent.
        """
        if not self._enabled:
            return InteractionEvent(event_type, target, coordinates, metadata)

        event = InteractionEvent(
            event_type=event_type,
            target=target,
            coordinates=coordinates,
            metadata=metadata,
        )
        self._events.append(event)
        return event

    def log_click(
        self,
        target: str,
        coordinates: tuple,
        button: str = "left",
        **metadata,
    ) -> InteractionEvent:
        """
        Log a click event.

        Args:
            target: Element target.
            coordinates: (x, y) click position.
            button: Mouse button (left, right, middle).
            **metadata: Additional metadata.

        Returns:
            The created event.
        """
        return self.log("click", target, coordinates, button=button, **metadata)

    def log_type(
        self,
        target: str,
        text: str,
        **metadata,
    ) -> InteractionEvent:
        """
        Log a type event.

        Args:
            target: Input element target.
            text: Text being typed.
            **metadata: Additional metadata.

        Returns:
            The created event.
        """
        return self.log("type", target, text=text, **metadata)

    def log_scroll(
        self,
        direction: str,
        amount: int,
        coordinates: Optional[tuple] = None,
        **metadata,
    ) -> InteractionEvent:
        """
        Log a scroll event.

        Args:
            direction: Scroll direction (up, down, left, right).
            amount: Scroll amount in pixels.
            coordinates: Optional (x, y) position.
            **metadata: Additional metadata.

        Returns:
            The created event.
        """
        return self.log(
            "scroll", None, coordinates,
            direction=direction, amount=amount, **metadata
        )

    def log_screenshot(
        self,
        label: Optional[str] = None,
        **metadata,
    ) -> InteractionEvent:
        """
        Log a screenshot capture event.

        Args:
            label: Optional label for the screenshot.
            **metadata: Additional metadata.

        Returns:
            The created event.
        """
        return self.log("screenshot", target=label, **metadata)

    def get_events(
        self,
        event_type: Optional[str] = None,
        since: Optional[float] = None,
        limit: Optional[int] = None,
    ) -> list[dict]:
        """
        Retrieve logged events.

        Args:
            event_type: Filter by event type.
            since: Filter events after this timestamp.
            limit: Maximum number of events to return.

        Returns:
            List of event dictionaries.
        """
        events = [e.to_dict() for e in self._events]

        if event_type:
            events = [e for e in events if e["event_type"] == event_type]

        if since is not None:
            events = [e for e in events if e["timestamp"] >= since]

        if limit:
            events = events[-limit:]

        return events

    def get_last(self, count: int = 1) -> list[InteractionEvent]:
        """
        Get the last N events.

        Args:
            count: Number of events to retrieve.

        Returns:
            List of last InteractionEvents.
        """
        return list(self._events)[-count:]

    def export_json(self, filepath: str) -> None:
        """
        Export events to JSON file.

        Args:
            filepath: Path to output file.
        """
        data = {
            "session_id": self._session_id,
            "exported_at": datetime.now().isoformat(),
            "event_count": len(self._events),
            "events": [e.to_dict() for e in self._events],
        }
        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

    def clear(self) -> None:
        """Clear all logged events."""
        self._events.clear()

    def enable(self) -> None:
        """Enable logging."""
        self._enabled = True

    def disable(self) -> None:
        """Disable logging."""
        self._enabled = False

    @staticmethod
    def _generate_session_id() -> str:
        """Generate a unique session ID."""
        return f"session_{int(time.time() * 1000)}"
