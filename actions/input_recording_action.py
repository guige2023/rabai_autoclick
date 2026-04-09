"""Input recording and playback action for UI automation.

Records and replays user inputs:
- Touch/mouse event recording
- Keyboard input recording
- Playback with variable speed
- Input file format support
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable


class InputEventType(Enum):
    """Input event types."""
    TOUCH_DOWN = auto()
    TOUCH_UP = auto()
    TOUCH_MOVE = auto()
    MOUSE_DOWN = auto()
    MOUSE_UP = auto()
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    KEY_DOWN = auto()
    KEY_UP = auto()
    KEY_TYPE = auto()
    SCROLL = auto()
    PINCH = auto()
    ROTATE = auto()


@dataclass
class InputEvent:
    """Single input event."""
    event_type: InputEventType
    timestamp: float
    x: float = 0
    y: float = 0
    target_x: float = 0  # For drag end position
    target_y: float = 0
    button: int = 0
    key_code: int = 0
    key_char: str = ""
    scroll_dx: float = 0
    scroll_dy: float = 0
    scale: float = 1.0
    rotation: float = 0
    duration: float = 0  # For hold events
    metadata: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "event_type": self.event_type.name,
            "timestamp": self.timestamp,
            "x": self.x,
            "y": self.y,
            "target_x": self.target_x,
            "target_y": self.target_y,
            "button": self.button,
            "key_code": self.key_code,
            "key_char": self.key_char,
            "scroll_dx": self.scroll_dx,
            "scroll_dy": self.scroll_dy,
            "scale": self.scale,
            "rotation": self.rotation,
            "duration": self.duration,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> InputEvent:
        """Create from dictionary."""
        return cls(
            event_type=InputEventType[data["event_type"]],
            timestamp=data["timestamp"],
            x=data.get("x", 0),
            y=data.get("y", 0),
            target_x=data.get("target_x", 0),
            target_y=data.get("target_y", 0),
            button=data.get("button", 0),
            key_code=data.get("key_code", 0),
            key_char=data.get("key_char", ""),
            scroll_dx=data.get("scroll_dx", 0),
            scroll_dy=data.get("scroll_dy", 0),
            scale=data.get("scale", 1.0),
            rotation=data.get("rotation", 0),
            duration=data.get("duration", 0),
            metadata=data.get("metadata", {}),
        )


@dataclass
class RecordingSession:
    """Recording session."""
    id: str
    name: str
    start_time: float
    end_time: float = 0
    events: list[InputEvent] = field(default_factory=list)
    display_size: tuple[int, int] = (0, 0)
    metadata: dict = field(default_factory=dict)

    @property
    def duration(self) -> float:
        return self.end_time - self.start_time if self.end_time > 0 else time.time() - self.start_time

    @property
    def event_count(self) -> int:
        return len(self.events)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "events": [e.to_dict() for e in self.events],
            "display_size": self.display_size,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict) -> RecordingSession:
        """Create from dictionary."""
        return cls(
            id=data["id"],
            name=data["name"],
            start_time=data["start_time"],
            end_time=data.get("end_time", 0),
            events=[InputEvent.from_dict(e) for e in data.get("events", [])],
            display_size=tuple(data.get("display_size", [0, 0])),
            metadata=data.get("metadata", {}),
        )


class InputRecorder:
    """Records and plays back user inputs.

    Features:
    - Touch and mouse event recording
    - Keyboard input recording
    - Variable speed playback
    - Recording file export/import
    """

    def __init__(self):
        self._current_session: RecordingSession | None = None
        self._is_recording = False
        self._is_playing = False
        self._playback_callback: Callable | None = None
        self._playback_position = 0

    def start_recording(self, name: str, display_size: tuple[int, int] = (0, 0)) -> str:
        """Start recording input session.

        Args:
            name: Session name
            display_size: Display dimensions

        Returns:
            Session ID
        """
        if self._is_recording:
            self.stop_recording()

        session_id = f"rec_{int(time.time() * 1000)}"
        self._current_session = RecordingSession(
            id=session_id,
            name=name,
            start_time=time.time(),
            display_size=display_size,
        )
        self._is_recording = True
        return session_id

    def stop_recording(self) -> RecordingSession | None:
        """Stop recording.

        Returns:
            Completed recording session
        """
        if not self._is_recording or not self._current_session:
            return None

        self._current_session.end_time = time.time()
        self._is_recording = False
        session = self._current_session
        self._current_session = None
        return session

    @property
    def is_recording(self) -> bool:
        """Check if recording."""
        return self._is_recording

    def record_event(self, event: InputEvent) -> None:
        """Record an input event.

        Args:
            event: Input event to record
        """
        if not self._is_recording or not self._current_session:
            return

        # Adjust timestamp relative to session start
        event.timestamp = time.time() - self._current_session.start_time
        self._current_session.events.append(event)

    def record_touch(self, event_type: InputEventType, x: float, y: float) -> None:
        """Record touch event."""
        self.record_event(InputEvent(
            event_type=event_type,
            timestamp=time.time() - (self._current_session.start_time if self._current_session else 0),
            x=x,
            y=y,
        ))

    def record_mouse(
        self,
        event_type: InputEventType,
        x: float,
        y: float,
        button: int = 0,
    ) -> None:
        """Record mouse event."""
        self.record_event(InputEvent(
            event_type=event_type,
            timestamp=time.time() - (self._current_session.start_time if self._current_session else 0),
            x=x,
            y=y,
            button=button,
        ))

    def record_key(self, event_type: InputEventType, key_code: int, key_char: str = "") -> None:
        """Record keyboard event."""
        self.record_event(InputEvent(
            event_type=event_type,
            timestamp=time.time() - (self._current_session.start_time if self._current_session else 0),
            key_code=key_code,
            key_char=key_char,
        ))

    def record_scroll(self, dx: float, dy: float) -> None:
        """Record scroll event."""
        self.record_event(InputEvent(
            event_type=InputEventType.SCROLL,
            timestamp=time.time() - (self._current_session.start_time if self._current_session else 0),
            scroll_dx=dx,
            scroll_dy=dy,
        ))

    def set_playback_callback(self, callback: Callable[[InputEvent], None]) -> None:
        """Set playback callback.

        Args:
            callback: Function(InputEvent) called during playback
        """
        self._playback_callback = callback

    def play(
        self,
        session: RecordingSession,
        speed: float = 1.0,
        loop: bool = False,
    ) -> None:
        """Play back recording.

        Args:
            session: Recording to play
            speed: Playback speed multiplier
            loop: Whether to loop playback
        """
        if self._is_playing:
            self.stop_playback()

        self._is_playing = True
        self._playback_position = 0

        while self._is_playing:
            for event in session.events:
                if not self._is_playing:
                    break

                # Wait for event time
                time.sleep(event.timestamp / speed)

                # Execute event
                if self._playback_callback:
                    self._playback_callback(event)

                self._playback_position += 1

            if not loop:
                break

        self._is_playing = False

    def stop_playback(self) -> None:
        """Stop playback."""
        self._is_playing = False

    @property
    def is_playing(self) -> bool:
        """Check if playing."""
        return self._is_playing

    def export_session(self, session: RecordingSession, path: str) -> None:
        """Export session to file.

        Args:
            session: Session to export
            path: Output file path
        """
        with open(path, "w") as f:
            json.dump(session.to_dict(), f, indent=2)

    def import_session(self, path: str) -> RecordingSession:
        """Import session from file.

        Args:
            path: Input file path

        Returns:
            Imported session
        """
        with open(path) as f:
            data = json.load(f)
        return RecordingSession.from_dict(data)

    def get_current_session(self) -> RecordingSession | None:
        """Get current recording session."""
        return self._current_session


def create_input_recorder() -> InputRecorder:
    """Create input recorder."""
    return InputRecorder()
