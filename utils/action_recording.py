"""Action recording and playback for UI automation.

Provides a framework for recording user interactions and
playing them back with timing control.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class ActionType(Enum):
    """Types of recorded actions."""
    MOUSE_MOVE = auto()
    MOUSE_DOWN = auto()
    MOUSE_UP = auto()
    MOUSE_CLICK = auto()
    MOUSE_DOUBLE_CLICK = auto()
    MOUSE_RIGHT_CLICK = auto()
    MOUSE_DRAG = auto()
    KEY_DOWN = auto()
    KEY_UP = auto()
    KEY_PRESS = auto()
    SCROLL = auto()
    WAIT = auto()
    CUSTOM = auto()


@dataclass
class RecordedAction:
    """A single recorded action.

    Attributes:
        action_type: Type of the action.
        x: X coordinate (for mouse actions).
        y: Y coordinate (for mouse actions).
        key: Key name (for keyboard actions).
        key_code: Key code (for keyboard actions).
        scroll_dx: Horizontal scroll delta.
        scroll_dy: Vertical scroll delta.
        timestamp: Time offset from recording start.
        duration: Duration for timed actions.
        modifiers: Active modifier keys.
    """
    action_type: ActionType
    x: float = 0.0
    y: float = 0.0
    key: str = ""
    key_code: Optional[int] = None
    scroll_dx: float = 0.0
    scroll_dy: float = 0.0
    timestamp: float = 0.0
    duration: float = 0.0
    modifiers: set[str] = field(default_factory=set)
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    def get_position(self) -> tuple[float, float]:
        """Return (x, y) position."""
        return (self.x, self.y)


@dataclass
class Recording:
    """A complete recording session.

    Attributes:
        name: Recording name.
        description: Description of what this recording does.
        actions: List of recorded actions in order.
        start_time: When recording started.
        end_time: When recording ended.
        playback_speed: Playback speed multiplier.
    """
    name: str
    description: str = ""
    actions: list[RecordedAction] = field(default_factory=list)
    start_time: float = field(default_factory=time.time)
    end_time: float = 0.0
    playback_speed: float = 1.0
    id: str = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def duration(self) -> float:
        """Total duration of the recording."""
        if not self.actions:
            return 0.0
        return self.actions[-1].timestamp

    @property
    def action_count(self) -> int:
        """Number of actions in this recording."""
        return len(self.actions)


class ActionRecorder:
    """Records user interactions as actions."""

    def __init__(self) -> None:
        """Initialize recorder."""
        self._is_recording = False
        self._recording_start = 0.0
        self._recording: Optional[Recording] = None
        self._action_handlers: dict[
            ActionType, Callable[[RecordedAction], None]
        ] = {}

    def start_recording(
        self,
        name: str,
        description: str = "",
    ) -> Recording:
        """Start a new recording session."""
        self._is_recording = True
        self._recording_start = time.time()
        self._recording = Recording(
            name=name,
            description=description,
            start_time=self._recording_start,
        )
        return self._recording

    def record(
        self,
        action_type: ActionType,
        x: float = 0.0,
        y: float = 0.0,
        key: str = "",
        key_code: Optional[int] = None,
        scroll_dx: float = 0.0,
        scroll_dy: float = 0.0,
        duration: float = 0.0,
        modifiers: Optional[set[str]] = None,
    ) -> Optional[RecordedAction]:
        """Record a single action."""
        if not self._is_recording or not self._recording:
            return None

        timestamp = time.time() - self._recording_start
        action = RecordedAction(
            action_type=action_type,
            x=x, y=y,
            key=key, key_code=key_code,
            scroll_dx=scroll_dx, scroll_dy=scroll_dy,
            timestamp=timestamp, duration=duration,
            modifiers=modifiers or set(),
        )
        self._recording.actions.append(action)
        return action

    def stop_recording(self) -> Optional[Recording]:
        """Stop recording and return the completed recording."""
        if not self._is_recording or not self._recording:
            return None

        self._is_recording = False
        self._recording.end_time = time.time()
        recording = self._recording
        self._recording = None
        return recording

    def is_recording(self) -> bool:
        """Return True if currently recording."""
        return self._is_recording


class ActionPlayer:
    """Plays back recorded actions."""

    def __init__(self) -> None:
        """Initialize player."""
        self._action_handlers: dict[
            ActionType, Callable[[RecordedAction], bool]
        ] = {}
        self._is_playing = False
        self._on_start_callbacks: list[Callable[[], None]] = []
        self._on_stop_callbacks: list[Callable[[], None]] = []
        self._on_action_callbacks: list[Callable[[RecordedAction], None]] = []

    def register_handler(
        self,
        action_type: ActionType,
        handler: Callable[[RecordedAction], bool],
    ) -> None:
        """Register a handler for an action type."""
        self._action_handlers[action_type] = handler

    def play(
        self,
        recording: Recording,
        loop: bool = False,
        loop_count: int = 1,
    ) -> bool:
        """Play a recording.

        Returns True if playback completed.
        """
        self._is_playing = True
        self._notify_start()

        for loop_idx in range(loop_count if loop else 1):
            if not self._is_playing:
                break

            prev_timestamp = 0.0
            for action in recording.actions:
                if not self._is_playing:
                    break

                delay = (action.timestamp - prev_timestamp) / recording.playback_speed
                if delay > 0:
                    time.sleep(delay)

                handler = self._action_handlers.get(action.action_type)
                if handler:
                    handler(action)

                self._notify_action(action)
                prev_timestamp = action.timestamp

        self._is_playing = False
        self._notify_stop()
        return True

    def stop(self) -> None:
        """Stop current playback."""
        self._is_playing = False

    def is_playing(self) -> bool:
        """Return True if currently playing."""
        return self._is_playing

    def on_start(self, callback: Callable[[], None]) -> None:
        """Register callback for playback start."""
        self._on_start_callbacks.append(callback)

    def on_stop(self, callback: Callable[[], None]) -> None:
        """Register callback for playback stop."""
        self._on_stop_callbacks.append(callback)

    def on_action(self, callback: Callable[[RecordedAction], None]) -> None:
        """Register callback for each action."""
        self._on_action_callbacks.append(callback)

    def _notify_start(self) -> None:
        for cb in self._on_start_callbacks:
            try:
                cb()
            except Exception:
                pass

    def _notify_stop(self) -> None:
        for cb in self._on_stop_callbacks:
            try:
                cb()
            except Exception:
                pass

    def _notify_action(self, action: RecordedAction) -> None:
        for cb in self._on_action_callbacks:
            try:
                cb(action)
            except Exception:
                pass
