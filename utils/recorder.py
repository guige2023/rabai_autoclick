"""Action recording utilities for RabAI AutoClick.

Provides:
- Action recording
- Recording playback
- Recording management
"""

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional


class ActionType(Enum):
    """Recorded action types."""
    MOUSE_MOVE = "mouse_move"
    MOUSE_CLICK = "mouse_click"
    MOUSE_DOUBLE_CLICK = "mouse_double_click"
    MOUSE_RIGHT_CLICK = "mouse_right_click"
    MOUSE_DRAG = "mouse_drag"
    KEYBOARD_PRESS = "keyboard_press"
    KEYBOARD_TYPE = "keyboard_type"
    WAIT = "wait"
    SCREENSHOT = "screenshot"


@dataclass
class RecordedAction:
    """A single recorded action."""
    type: ActionType
    timestamp: float
    data: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "type": self.type.value,
            "timestamp": self.timestamp,
            "data": self.data,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "RecordedAction":
        """Create from dictionary."""
        return cls(
            type=ActionType(data["type"]),
            timestamp=data["timestamp"],
            data=data.get("data", {}),
        )


class Recording:
    """A recording of actions."""

    def __init__(self, name: str) -> None:
        """Initialize recording.

        Args:
            name: Recording name.
        """
        self.name = name
        self._actions: List[RecordedAction] = []
        self._start_time: Optional[float] = None
        self._end_time: Optional[float] = None

    def add_action(self, action: RecordedAction) -> None:
        """Add action to recording.

        Args:
            action: Action to add.
        """
        if self._start_time is None:
            self._start_time = action.timestamp
        self._end_time = action.timestamp
        self._actions.append(action)

    @property
    def actions(self) -> List[RecordedAction]:
        """Get all actions."""
        return self._actions.copy()

    @property
    def duration(self) -> float:
        """Get recording duration."""
        if self._start_time is None or self._end_time is None:
            return 0
        return self._end_time - self._start_time

    @property
    def action_count(self) -> int:
        """Get number of actions."""
        return len(self._actions)

    def clear(self) -> None:
        """Clear all actions."""
        self._actions.clear()
        self._start_time = None
        self._end_time = None

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "start_time": self._start_time,
            "end_time": self._end_time,
            "actions": [a.to_dict() for a in self._actions],
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Recording":
        """Create from dictionary."""
        recording = cls(name=data["name"])
        for action_data in data.get("actions", []):
            recording.add_action(RecordedAction.from_dict(action_data))
        return recording

    def save(self, path: str) -> bool:
        """Save recording to file.

        Args:
            path: Output path.

        Returns:
            True if successful.
        """
        try:
            with open(path, "w") as f:
                json.dump(self.to_dict(), f, indent=2)
            return True
        except Exception:
            return False

    @classmethod
    def load(cls, path: str) -> Optional["Recording"]:
        """Load recording from file.

        Args:
            path: Input path.

        Returns:
            Recording or None.
        """
        try:
            with open(path, "r") as f:
                data = json.load(f)
            return cls.from_dict(data)
        except Exception:
            return None


class ActionRecorder:
    """Record user actions."""

    def __init__(self) -> None:
        """Initialize recorder."""
        self._recording: Optional[Recording] = None
        self._recording_name: Optional[str] = None
        self._start_timestamp: Optional[float] = None

    def start_recording(self, name: str) -> None:
        """Start recording.

        Args:
            name: Recording name.
        """
        self._recording_name = name
        self._recording = Recording(name)
        self._start_timestamp = time.time()

    def stop_recording(self) -> Optional[Recording]:
        """Stop recording.

        Returns:
            Recorded actions or None.
        """
        recording = self._recording
        self._recording = None
        self._recording_name = None
        self._start_timestamp = None
        return recording

    def is_recording(self) -> bool:
        """Check if recording.

        Returns:
            True if recording.
        """
        return self._recording is not None

    def record_mouse_move(self, x: int, y: int) -> None:
        """Record mouse move.

        Args:
            x: X coordinate.
            y: Y coordinate.
        """
        if self._recording:
            self._recording.add_action(RecordedAction(
                type=ActionType.MOUSE_MOVE,
                timestamp=time.time() - self._start_timestamp,
                data={"x": x, "y": y},
            ))

    def record_mouse_click(
        self,
        x: int,
        y: int,
        button: str = "left",
    ) -> None:
        """Record mouse click.

        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Mouse button.
        """
        if self._recording:
            action_type = ActionType.MOUSE_CLICK
            if button == "right":
                action_type = ActionType.MOUSE_RIGHT_CLICK
            self._recording.add_action(RecordedAction(
                type=action_type,
                timestamp=time.time() - self._start_timestamp,
                data={"x": x, "y": y, "button": button},
            ))

    def record_mouse_double_click(
        self,
        x: int,
        y: int,
        button: str = "left",
    ) -> None:
        """Record mouse double click.

        Args:
            x: X coordinate.
            y: Y coordinate.
            button: Mouse button.
        """
        if self._recording:
            self._recording.add_action(RecordedAction(
                type=ActionType.MOUSE_DOUBLE_CLICK,
                timestamp=time.time() - self._start_timestamp,
                data={"x": x, "y": y, "button": button},
            ))

    def record_mouse_drag(
        self,
        start_x: int,
        start_y: int,
        end_x: int,
        end_y: int,
    ) -> None:
        """Record mouse drag.

        Args:
            start_x: Start X coordinate.
            start_y: Start Y coordinate.
            end_x: End X coordinate.
            end_y: End Y coordinate.
        """
        if self._recording:
            self._recording.add_action(RecordedAction(
                type=ActionType.MOUSE_DRAG,
                timestamp=time.time() - self._start_timestamp,
                data={
                    "start_x": start_x,
                    "start_y": start_y,
                    "end_x": end_x,
                    "end_y": end_y,
                },
            ))

    def record_keyboard_press(self, key_code: int) -> None:
        """Record keyboard press.

        Args:
            key_code: Virtual key code.
        """
        if self._recording:
            self._recording.add_action(RecordedAction(
                type=ActionType.KEYBOARD_PRESS,
                timestamp=time.time() - self._start_timestamp,
                data={"key_code": key_code},
            ))

    def record_keyboard_type(self, text: str) -> None:
        """Record keyboard type.

        Args:
            text: Typed text.
        """
        if self._recording:
            self._recording.add_action(RecordedAction(
                type=ActionType.KEYBOARD_TYPE,
                timestamp=time.time() - self._start_timestamp,
                data={"text": text},
            ))

    def record_wait(self, duration: float) -> None:
        """Record wait.

        Args:
            duration: Wait duration.
        """
        if self._recording:
            self._recording.add_action(RecordedAction(
                type=ActionType.WAIT,
                timestamp=time.time() - self._start_timestamp,
                data={"duration": duration},
            ))

    def record_screenshot(self, path: str) -> None:
        """Record screenshot.

        Args:
            path: Screenshot path.
        """
        if self._recording:
            self._recording.add_action(RecordedAction(
                type=ActionType.SCREENSHOT,
                timestamp=time.time() - self._start_timestamp,
                data={"path": path},
            ))


class RecordingPlayer:
    """Play back recordings."""

    def __init__(self) -> None:
        """Initialize player."""
        self._running = False
        self._paused = False

    def play(
        self,
        recording: Recording,
        speed: float = 1.0,
        on_action: Optional[Callable[[RecordedAction], None]] = None,
    ) -> bool:
        """Play recording.

        Args:
            recording: Recording to play.
            speed: Playback speed multiplier.
            on_action: Optional callback for each action.

        Returns:
            True if completed successfully.
        """
        self._running = True
        self._paused = False

        last_timestamp = 0

        for action in recording.actions:
            if not self._running:
                return False

            while self._paused:
                if not self._running:
                    return False

            # Wait for action timing
            wait_time = (action.timestamp - last_timestamp) / speed
            if wait_time > 0:
                time.sleep(wait_time)
            last_timestamp = action.timestamp

            # Execute action
            if not self._execute_action(action):
                return False

            if on_action:
                on_action(action)

        self._running = False
        return True

    def _execute_action(self, action: RecordedAction) -> bool:
        """Execute a single action.

        Args:
            action: Action to execute.

        Returns:
            True if successful.
        """
        try:
            if action.type == ActionType.MOUSE_MOVE:
                from utils.mouse import MouseSimulator
                MouseSimulator.move(action.data["x"], action.data["y"])

            elif action.type == ActionType.MOUSE_CLICK:
                from utils.mouse import MouseSimulator, MouseButton
                MouseSimulator.click(
                    action.data["x"],
                    action.data["y"],
                    MouseButton.LEFT if action.data.get("button") != "right" else MouseButton.RIGHT,
                )

            elif action.type == ActionType.MOUSE_DOUBLE_CLICK:
                from utils.mouse import MouseSimulator, MouseButton
                MouseSimulator.double_click(
                    action.data["x"],
                    action.data["y"],
                )

            elif action.type == ActionType.MOUSE_DRAG:
                from utils.mouse import MouseSimulator
                MouseSimulator.drag(
                    action.data["start_x"],
                    action.data["start_y"],
                    action.data["end_x"],
                    action.data["end_y"],
                )

            elif action.type == ActionType.KEYBOARD_PRESS:
                from utils.keyboard import KeySequence
                seq = KeySequence()
                seq.type(action.data["key_code"])

            elif action.type == ActionType.KEYBOARD_TYPE:
                from utils.clipboard import Clipboard
                Clipboard.set_text(action.data["text"])

            elif action.type == ActionType.WAIT:
                time.sleep(action.data["duration"])

            elif action.type == ActionType.SCREENSHOT:
                from utils.image import Screenshot
                Screenshot.save(action.data["path"])

            return True

        except Exception:
            return False

    def pause(self) -> None:
        """Pause playback."""
        self._paused = True

    def resume(self) -> None:
        """Resume playback."""
        self._paused = False

    def stop(self) -> None:
        """Stop playback."""
        self._running = False
        self._paused = False

    @property
    def is_running(self) -> bool:
        """Check if running."""
        return self._running

    @property
    def is_paused(self) -> bool:
        """Check if paused."""
        return self._paused


class RecordingManager:
    """Manage multiple recordings."""

    def __init__(self) -> None:
        """Initialize manager."""
        self._recordings: Dict[str, Recording] = {}

    def add(self, recording: Recording) -> None:
        """Add recording.

        Args:
            recording: Recording to add.
        """
        self._recordings[recording.name] = recording

    def get(self, name: str) -> Optional[Recording]:
        """Get recording by name.

        Args:
            name: Recording name.

        Returns:
            Recording or None.
        """
        return self._recordings.get(name)

    def remove(self, name: str) -> bool:
        """Remove recording.

        Args:
            name: Recording name.

        Returns:
            True if removed.
        """
        if name in self._recordings:
            del self._recordings[name]
            return True
        return False

    def list_recordings(self) -> List[str]:
        """List recording names.

        Returns:
            List of names.
        """
        return list(self._recordings.keys())

    def save_all(self, directory: str) -> int:
        """Save all recordings to directory.

        Args:
            directory: Output directory.

        Returns:
            Number saved successfully.
        """
        import os
        saved = 0
        for name, recording in self._recordings.items():
            path = os.path.join(directory, f"{name}.json")
            if recording.save(path):
                saved += 1
        return saved

    def load_all(self, directory: str) -> int:
        """Load all recordings from directory.

        Args:
            directory: Input directory.

        Returns:
            Number loaded successfully.
        """
        import os
        import glob
        loaded = 0
        pattern = os.path.join(directory, "*.json")
        for path in glob.glob(pattern):
            recording = Recording.load(path)
            if recording:
                self.add(recording)
                loaded += 1
        return loaded
