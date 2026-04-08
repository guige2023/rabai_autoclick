"""
Input Session Recorder.

Record and replay user input sessions including mouse movements,
keyboard input, and timing information for automation playback.

Usage:
    from utils.input_session_recorder import InputSessionRecorder

    recorder = InputSessionRecorder()
    recorder.start_recording()
    # ... perform actions ...
    session = recorder.stop_recording()
    session.save("session.json")
"""

from __future__ import annotations

from typing import Optional, List, Dict, Any, Callable, TYPE_CHECKING
from dataclasses import dataclass, field
from dataclasses import asdict
from datetime import datetime
import time
import json

if TYPE_CHECKING:
    pass


@dataclass
class InputEvent:
    """A single input event in a session."""
    event_type: str  # "mouse_move", "mouse_click", "key_press", etc.
    timestamp: float
    x: Optional[int] = None
    y: Optional[int] = None
    button: Optional[str] = None
    key: Optional[str] = None
    modifiers: List[str] = field(default_factory=list)
    duration: float = 0.0


@dataclass
class InputSession:
    """A complete recorded input session."""
    session_id: str
    start_time: float
    end_time: Optional[float] = None
    events: List[InputEvent] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> Optional[float]:
        """Session duration in seconds."""
        if self.end_time is None:
            return None
        return self.end_time - self.start_time

    @property
    def event_count(self) -> int:
        """Number of events in the session."""
        return len(self.events)

    def save(self, path: str) -> bool:
        """
        Save session to a JSON file.

        Args:
            path: File path to save to.

        Returns:
            True if successful.
        """
        try:
            data = {
                "session_id": self.session_id,
                "start_time": self.start_time,
                "end_time": self.end_time,
                "events": [asdict(e) for e in self.events],
                "metadata": self.metadata,
            }
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
            return True
        except Exception:
            return False

    @classmethod
    def load(cls, path: str) -> Optional["InputSession"]:
        """
        Load a session from a JSON file.

        Args:
            path: File path to load from.

        Returns:
            InputSession object or None.
        """
        try:
            with open(path) as f:
                data = json.load(f)

            events = [InputEvent(**e) for e in data.get("events", [])]
            return cls(
                session_id=data["session_id"],
                start_time=data["start_time"],
                end_time=data.get("end_time"),
                events=events,
                metadata=data.get("metadata", {}),
            )
        except Exception:
            return None


class InputSessionRecorder:
    """
    Record user input sessions.

    Records mouse movements, clicks, keyboard input, and timing
    for later replay. Useful for creating automation scripts
    from demonstrated actions.

    Example:
        recorder = InputSessionRecorder()
        recorder.start_recording()

        # ... perform actions ...

        session = recorder.stop_recording()
        session.save("recorded_session.json")
    """

    def __init__(
        self,
        record_mouse_moves: bool = True,
        record_keyboard: bool = True,
        min_move_distance: int = 5,
    ) -> None:
        """
        Initialize the recorder.

        Args:
            record_mouse_moves: Whether to record mouse movements.
            record_keyboard: Whether to record keyboard input.
            min_move_distance: Minimum distance to record a move event.
        """
        self._record_moves = record_mouse_moves
        self._record_keyboard = record_keyboard
        self._min_distance = min_move_distance
        self._session: Optional[InputSession] = None
        self._running = False
        self._last_x = 0
        self._last_y = 0
        self._on_event_callbacks: List[Callable[[InputEvent], None]] = []

    def start_recording(self) -> InputSession:
        """
        Start recording a new session.

        Returns:
            The created InputSession object.
        """
        import uuid
        self._session = InputSession(
            session_id=str(uuid.uuid4())[:8],
            start_time=time.time(),
        )
        self._running = True
        return self._session

    def stop_recording(self) -> Optional[InputSession]:
        """
        Stop recording and return the session.

        Returns:
            The recorded InputSession or None if not recording.
        """
        if self._session is None:
            return None

        self._running = False
        self._session.end_time = time.time()
        session = self._session
        self._session = None
        return session

    def is_recording(self) -> bool:
        """Return True if currently recording."""
        return self._running

    def record_mouse_move(
        self,
        x: int,
        y: int,
    ) -> None:
        """
        Record a mouse move event.

        Args:
            x: X coordinate.
            y: Y coordinate.
        """
        if not self._running or not self._record_moves:
            return

        dx = abs(x - self._last_x)
        dy = abs(y - self._last_y)
        if dx < self._min_distance and dy < self._min_distance:
            return

        event = InputEvent(
            event_type="mouse_move",
            timestamp=time.time(),
            x=x,
            y=y,
        )
        self._add_event(event)
        self._last_x = x
        self._last_y = y

    def record_mouse_click(
        self,
        x: int,
        y: int,
        button: str = "left",
    ) -> None:
        """
        Record a mouse click event.

        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Button name ("left", "right", "middle").
        """
        if not self._running:
            return

        event = InputEvent(
            event_type="mouse_click",
            timestamp=time.time(),
            x=x,
            y=y,
            button=button,
        )
        self._add_event(event)

    def record_key_press(
        self,
        key: str,
        modifiers: Optional[List[str]] = None,
    ) -> None:
        """
        Record a key press event.

        Args:
            key: Key name.
            modifiers: List of modifier keys.
        """
        if not self._running or not self._record_keyboard:
            return

        event = InputEvent(
            event_type="key_press",
            timestamp=time.time(),
            key=key,
            modifiers=modifiers or [],
        )
        self._add_event(event)

    def record_scroll(
        self,
        dx: int,
        dy: int,
        x: Optional[int] = None,
        y: Optional[int] = None,
    ) -> None:
        """
        Record a scroll event.

        Args:
            dx: Horizontal scroll amount.
            dy: Vertical scroll amount.
            x: Optional X coordinate.
            y: Optional Y coordinate.
        """
        if not self._running:
            return

        event = InputEvent(
            event_type="scroll",
            timestamp=time.time(),
            x=x,
            y=y,
            modifiers=[],  # repurposed for dx/dy
            duration=float(dy * 1000 + dx),  # repurposed
        )
        self._add_event(event)

    def _add_event(self, event: InputEvent) -> None:
        """Add an event to the current session."""
        if self._session:
            self._session.events.append(event)

        for cb in self._on_event_callbacks:
            try:
                cb(event)
            except Exception:
                pass

    def on_event(
        self,
        callback: Callable[[InputEvent], None],
    ) -> None:
        """
        Register a callback for each recorded event.

        Args:
            callback: Function called with each InputEvent.
        """
        if callback not in self._on_event_callbacks:
            self._on_event_callbacks.append(callback)


def replay_session(
    session: InputSession,
    bridge: Any,
    speed: float = 1.0,
) -> bool:
    """
    Replay a recorded session.

    Args:
        session: InputSession to replay.
        bridge: AccessibilityBridge for executing actions.
        speed: Playback speed multiplier (1.0 = real-time).

    Returns:
        True if replay completed successfully.
    """
    if not session.events:
        return False

    prev_time = session.events[0].timestamp if session.events else 0

    for event in session.events:
        delay = (event.timestamp - prev_time) / speed
        if delay > 0:
            time.sleep(delay)

        prev_time = event.timestamp

        if event.event_type == "mouse_move" and event.x is not None:
            bridge.move_mouse_to(event.x, event.y)
        elif event.event_type == "mouse_click" and event.x is not None:
            bridge.click_at(event.x, event.y)

    return True
