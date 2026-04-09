"""
Input Recorder Action Module

Records and replays mouse/keyboard input sequences for
macro automation and workflow playback.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)


class InputType(Enum):
    """Types of recorded inputs."""

    MOUSE_MOVE = "mouse_move"
    MOUSE_CLICK = "mouse_click"
    MOUSE_DOWN = "mouse_down"
    MOUSE_UP = "mouse_up"
    MOUSE_WHEEL = "mouse_wheel"
    KEYBOARD_KEY = "keyboard_key"
    KEYBOARD_TYPE = "keyboard_type"
    DELAY = "delay"


class MouseButton(Enum):
    """Mouse button identifiers."""

    LEFT = "left"
    RIGHT = "right"
    MIDDLE = "middle"


@dataclass
class InputEvent:
    """Represents a single input event."""

    type: InputType
    timestamp: float = field(default_factory=time.time)
    x: Optional[float] = None
    y: Optional[float] = None
    button: Optional[MouseButton] = None
    key: Optional[str] = None
    text: Optional[str] = None
    delta: Optional[int] = None
    duration: Optional[float] = None
    modifiers: List[str] = field(default_factory=list)


@dataclass
class RecordedSession:
    """A complete recorded input session."""

    name: str
    events: List[InputEvent] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def duration_seconds(self) -> float:
        """Get total session duration."""
        if not self.events:
            return 0.0
        return self.events[-1].timestamp - self.events[0].timestamp

    def event_count(self) -> int:
        """Get total event count."""
        return len(self.events)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "name": self.name,
            "events": [
                {
                    "type": e.type.value,
                    "timestamp": e.timestamp,
                    "x": e.x,
                    "y": e.y,
                    "button": e.button.value if e.button else None,
                    "key": e.key,
                    "text": e.text,
                    "delta": e.delta,
                    "duration": e.duration,
                    "modifiers": e.modifiers,
                }
                for e in self.events
            ],
            "created_at": self.created_at,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RecordedSession":
        """Deserialize from dictionary."""
        events = [
            InputEvent(
                type=InputType(e["type"]),
                timestamp=e.get("timestamp", 0),
                x=e.get("x"),
                y=e.get("y"),
                button=MouseButton(e["button"]) if e.get("button") else None,
                key=e.get("key"),
                text=e.get("text"),
                delta=e.get("delta"),
                duration=e.get("duration"),
                modifiers=e.get("modifiers", []),
            )
            for e in data.get("events", [])
        ]
        return cls(
            name=data["name"],
            events=events,
            created_at=data.get("created_at", 0),
            metadata=data.get("metadata", {}),
        )


@dataclass
class RecorderConfig:
    """Configuration for input recorder."""

    record_mouse: bool = True
    record_keyboard: bool = True
    record_delay: bool = True
    min_move_distance: float = 2.0
    compress_delays: bool = True
    delay_threshold: float = 0.05
    include_metadata: bool = True


class InputRecorder:
    """
    Records mouse and keyboard inputs for replay.

    Supports pause/resume, event filtering, compression,
    and various replay options including speed control.
    """

    def __init__(
        self,
        config: Optional[RecorderConfig] = None,
        mouse_handler: Optional[Callable[[InputEvent], None]] = None,
        keyboard_handler: Optional[Callable[[InputEvent], None]] = None,
    ):
        self.config = config or RecorderConfig()
        self.mouse_handler = mouse_handler
        self.keyboard_handler = keyboard_handler
        self._sessions: Dict[str, RecordedSession] = {}
        self._is_recording: bool = False
        self._current_session: Optional[RecordedSession] = None
        self._last_position: Optional[Tuple[float, float]] = None
        self._start_time: Optional[float] = None

    def start_recording(self, session_name: str) -> bool:
        """
        Start recording a new session.

        Args:
            session_name: Name for the new session

        Returns:
            True if recording started successfully
        """
        if self._is_recording:
            logger.warning("Already recording, stop current session first")
            return False

        self._current_session = RecordedSession(name=session_name)
        self._is_recording = True
        self._start_time = time.time()
        self._last_position = None

        logger.info(f"Started recording session: {session_name}")
        return True

    def stop_recording(self) -> Optional[RecordedSession]:
        """
        Stop the current recording session.

        Returns:
            The recorded session or None if not recording
        """
        if not self._is_recording or self._current_session is None:
            return None

        if self.config.compress_delays:
            self._compress_delays()

        self._is_recording = False
        session = self._current_session

        if self.config.include_metadata:
            session.metadata = {
                "duration": session.duration_seconds(),
                "event_count": session.event_count(),
                "recorded_at": time.time(),
            }

        self._sessions[session.name] = session
        self._current_session = None
        self._start_time = None

        logger.info(f"Stopped recording: {session.name} ({session.event_count()} events)")
        return session

    def _compress_delays(self) -> None:
        """Compress consecutive delay events."""
        if not self._current_session:
            return

        compressed: List[InputEvent] = []
        current_delay = 0.0

        for event in self._current_session.events:
            if event.type == InputType.DELAY:
                current_delay += event.duration or 0
            else:
                if current_delay >= self.config.delay_threshold:
                    compressed.append(
                        InputEvent(type=InputType.DELAY, duration=current_delay)
                    )
                current_delay = 0.0
                compressed.append(event)

        if current_delay >= self.config.delay_threshold:
            compressed.append(InputEvent(type=InputType.DELAY, duration=current_delay))

        self._current_session.events = compressed

    def record_mouse_move(self, x: float, y: float) -> None:
        """
        Record a mouse move event.

        Args:
            x: X coordinate
            y: Y coordinate
        """
        if not self._is_recording or not self.config.record_mouse:
            return

        if self._last_position:
            dx = x - self._last_position[0]
            dy = y - self._last_position[1]
            distance = (dx ** 2 + dy ** 2) ** 0.5
            if distance < self.config.min_move_distance:
                return

        event = InputEvent(type=InputType.MOUSE_MOVE, x=x, y=y)
        self._current_session.events.append(event)
        self._last_position = (x, y)

        if self.mouse_handler:
            self.mouse_handler(event)

    def record_mouse_click(
        self,
        x: float,
        y: float,
        button: MouseButton = MouseButton.LEFT,
        modifiers: Optional[List[str]] = None,
    ) -> None:
        """
        Record a mouse click event.

        Args:
            x: X coordinate
            y: Y coordinate
            button: Mouse button
            modifiers: Modifier keys held (ctrl, alt, shift, etc.)
        """
        if not self._is_recording or not self.config.record_mouse:
            return

        event = InputEvent(
            type=InputType.MOUSE_CLICK,
            x=x,
            y=y,
            button=button,
            modifiers=modifiers or [],
        )
        self._current_session.events.append(event)
        self._last_position = (x, y)

        if self.mouse_handler:
            self.mouse_handler(event)

    def record_keyboard_key(
        self,
        key: str,
        modifiers: Optional[List[str]] = None,
    ) -> None:
        """
        Record a keyboard key press event.

        Args:
            key: Key identifier
            modifiers: Modifier keys held
        """
        if not self._is_recording or not self.config.record_keyboard:
            return

        event = InputEvent(
            type=InputType.KEYBOARD_KEY,
            key=key,
            modifiers=modifiers or [],
        )
        self._current_session.events.append(event)

        if self.keyboard_handler:
            self.keyboard_handler(event)

    def record_text(self, text: str) -> None:
        """
        Record a text input event.

        Args:
            text: Typed text
        """
        if not self._is_recording or not self.config.record_keyboard:
            return

        event = InputEvent(type=InputType.KEYBOARD_TYPE, text=text)
        self._current_session.events.append(event)

        if self.keyboard_handler:
            self.keyboard_handler(event)

    def record_delay(self, duration: float) -> None:
        """
        Record a delay period.

        Args:
            duration: Delay duration in seconds
        """
        if not self._is_recording or not self.config.record_delay:
            return

        event = InputEvent(type=InputType.DELAY, duration=duration)
        self._current_session.events.append(event)

    def pause_recording(self) -> bool:
        """Pause the current recording."""
        if not self._is_recording:
            return False
        logger.info("Recording paused")
        return True

    def resume_recording(self) -> bool:
        """Resume a paused recording."""
        if self._is_recording:
            return False
        self._is_recording = True
        logger.info("Recording resumed")
        return True

    def get_session(self, name: str) -> Optional[RecordedSession]:
        """Get a recorded session by name."""
        return self._sessions.get(name)

    def list_sessions(self) -> List[str]:
        """List all recorded session names."""
        return list(self._sessions.keys())

    def delete_session(self, name: str) -> bool:
        """Delete a recorded session."""
        if name in self._sessions:
            del self._sessions[name]
            return True
        return False

    def save_session(
        self,
        name: str,
        path: str,
    ) -> bool:
        """
        Save a session to a JSON file.

        Args:
            name: Session name
            path: Output file path

        Returns:
            True if save succeeded
        """
        session = self._sessions.get(name)
        if not session:
            return False

        try:
            with open(path, "w") as f:
                json.dump(session.to_dict(), f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to save session: {e}")
            return False

    def load_session(self, path: str) -> Optional[RecordedSession]:
        """
        Load a session from a JSON file.

        Args:
            path: Input file path

        Returns:
            Loaded session or None
        """
        try:
            with open(path, "r") as f:
                data = json.load(f)
            session = RecordedSession.from_dict(data)
            self._sessions[session.name] = session
            return session
        except Exception as e:
            logger.error(f"Failed to load session: {e}")
            return None


class InputReplayer:
    """
    Replays recorded input sessions.

    Supports variable playback speed, looping, and
    coordinate offset/scaling for different displays.
    """

    def __init__(
        self,
        mouse_executor: Optional[Callable[[InputEvent], None]] = None,
        keyboard_executor: Optional[Callable[[InputEvent], None]] = None,
    ):
        self.mouse_executor = mouse_executor
        self.keyboard_executor = keyboard_executor
        self._is_playing: bool = False
        self._abort_requested: bool = False

    def replay(
        self,
        session: RecordedSession,
        speed: float = 1.0,
        loop: bool = False,
        offset_x: float = 0.0,
        offset_y: float = 0.0,
        scale_x: float = 1.0,
        scale_y: float = 1.0,
    ) -> bool:
        """
        Replay a recorded session.

        Args:
            session: Session to replay
            speed: Playback speed multiplier
            loop: Whether to loop playback
            offset_x: X coordinate offset
            offset_y: Y coordinate offset
            scale_x: X coordinate scale
            scale_y: Y coordinate scale

        Returns:
            True if replay completed successfully
        """
        self._is_playing = True
        self._abort_requested = False

        while True:
            start_time = session.events[0].timestamp if session.events else 0

            for event in session.events:
                if self._abort_requested:
                    self._is_playing = False
                    return False

                adjusted_event = self._adjust_event(
                    event, offset_x, offset_y, scale_x, scale_y
                )

                self._execute_event(adjusted_event)

                if event.type == InputType.DELAY and event.duration:
                    time.sleep(event.duration / speed)

            if not loop:
                break

        self._is_playing = False
        return True

    def _adjust_event(
        self,
        event: InputEvent,
        offset_x: float,
        offset_y: float,
        scale_x: float,
        scale_y: float,
    ) -> InputEvent:
        """Adjust event coordinates."""
        if event.x is not None:
            event.x = event.x * scale_x + offset_x
        if event.y is not None:
            event.y = event.y * scale_y + offset_y
        return event

    def _execute_event(self, event: InputEvent) -> None:
        """Execute a single input event."""
        if event.type in (InputType.MOUSE_MOVE, InputType.MOUSE_CLICK):
            if self.mouse_executor:
                self.mouse_executor(event)
        elif event.type in (InputType.KEYBOARD_KEY, InputType.KEYBOARD_TYPE):
            if self.keyboard_executor:
                self.keyboard_executor(event)

    def abort(self) -> None:
        """Request abort of current playback."""
        self._abort_requested = True

    @property
    def is_playing(self) -> bool:
        """Check if replay is in progress."""
        return self._is_playing


def create_input_recorder(config: Optional[RecorderConfig] = None) -> InputRecorder:
    """Factory function to create an InputRecorder."""
    return InputRecorder(config=config)
