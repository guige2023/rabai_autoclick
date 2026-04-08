"""Action recording and replay engine.

Captures sequences of automation actions (mouse moves, clicks, keystrokes)
and provides a replay engine with variable speed, pause, and seeking.
Designed to record user demonstrations and replay them faithfully.

Example:
    >>> from utils.action_replay_utils import ActionRecorder, ActionReplayer
    >>> recorder = ActionRecorder()
    >>> recorder.start()
    >>> # ... perform actions ...
    >>> actions = recorder.stop()
    >>> replayer = ActionReplayer(actions)
    >>> replayer.replay(speed=2.0)  # replay at 2x speed
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable

__all__ = [
    "ActionType",
    "Action",
    "ActionRecorder",
    "ActionReplayer",
]


class ActionType(Enum):
    """Types of recorded actions."""

    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_RIGHT_CLICK = auto()
    MOUSE_DOUBLE_CLICK = auto()
    MOUSE_DRAG = auto()
    KEY_PRESS = auto()
    KEY_RELEASE = auto()
    WAIT = auto()
    SCREENSHOT = auto()


@dataclass
class Action:
    """A single recorded action with timestamp and metadata.

    Attributes:
        action_type: The type of action.
        timestamp: Absolute time when the action occurred (from time.time()).
        data: Action-specific data dict (e.g., coordinates, keycode).
        duration: Optional duration for drag/wait actions.
    """

    action_type: ActionType
    timestamp: float
    data: dict = field(default_factory=dict)
    duration: float = 0.0

    def relative_time(self, start: float) -> float:
        """Return the time offset from the recording start."""
        return self.timestamp - start

    def __repr__(self) -> str:
        return f"Action({self.action_type.name}, t={self.timestamp:.3f}, {self.data})"


class ActionRecorder:
    """Records automation actions for later replay.

    Example:
        >>> recorder = ActionRecorder()
        >>> recorder.start()
        >>> recorder.record(ActionType.MOUSE_CLICK, {"x": 100, "y": 200})
        >>> recorder.record(ActionType.KEY_PRESS, {"key": "a"})
        >>> actions = recorder.stop()
    """

    def __init__(self) -> None:
        self._actions: list[Action] = []
        self._recording = False
        self._start_time: float = 0.0

    def start(self) -> None:
        """Begin recording actions."""
        self._actions = []
        self._recording = True
        self._start_time = time.time()

    def stop(self) -> list[Action]:
        """Stop recording and return the captured actions."""
        self._recording = False
        return list(self._actions)

    def record(
        self,
        action_type: ActionType,
        data: dict | None = None,
        duration: float = 0.0,
    ) -> None:
        """Record a single action.

        Args:
            action_type: Type of action performed.
            data: Action-specific data (coordinates, keycode, etc.).
            duration: Optional duration for drag/wait actions.
        """
        if not self._recording:
            return
        action = Action(
            action_type=action_type,
            timestamp=time.time(),
            data=data or {},
            duration=duration,
        )
        self._actions.append(action)

    def record_mouse_move(self, x: int, y: int) -> None:
        """Convenience method to record a mouse move."""
        self.record(ActionType.MOUSE_MOVE, {"x": x, "y": y})

    def record_mouse_click(
        self,
        x: int,
        y: int,
        button: str = "left",
        double: bool = False,
    ) -> None:
        """Convenience method to record a mouse click."""
        at = ActionType.MOUSE_DOUBLE_CLICK if double else ActionType.MOUSE_CLICK
        if button == "right":
            at = ActionType.MOUSE_RIGHT_CLICK
        self.record(at, {"x": x, "y": y, "button": button})

    def record_key(
        self,
        key: str,
        pressed: bool = True,
    ) -> None:
        """Convenience method to record a key press/release."""
        at = ActionType.KEY_PRESS if pressed else ActionType.KEY_RELEASE
        self.record(at, {"key": key})

    @property
    def duration(self) -> float:
        """Total recording duration in seconds."""
        if len(self._actions) < 2:
            return 0.0
        return self._actions[-1].timestamp - self._start_time

    @property
    def count(self) -> int:
        """Number of recorded actions."""
        return len(self._actions)


class ActionReplayer:
    """Replays recorded actions with speed control and seeking.

    Example:
        >>> actions = recorder.stop()
        >>> replayer = ActionReplayer(actions)
        >>> for action in replayer.seek(offset=5.0):
        ...     execute_action(action)
    """

    def __init__(self, actions: list[Action]) -> None:
        self._actions = actions
        self._current_index = 0
        self._paused = False

    @property
    def total_duration(self) -> float:
        """Total replay duration in seconds."""
        if not self._actions:
            return 0.0
        return self._actions[-1].relative_time(self._actions[0].timestamp)

    def seek(self, offset: float) -> list[Action]:
        """Return actions starting from the given time offset.

        Args:
            offset: Time offset in seconds from the start.

        Returns:
            List of actions from the offset to the end.
        """
        if not self._actions:
            return []
        start_ts = self._actions[0].timestamp + offset
        return [a for a in self._actions if a.timestamp >= start_ts]

    def replay(
        self,
        speed: float = 1.0,
        on_action: Callable[[Action], None] | None = None,
    ) -> None:
        """Replay all recorded actions.

        Args:
            speed: Playback speed multiplier (2.0 = 2x faster).
            on_action: Optional callback invoked for each action.
        """
        if not self._actions:
            return
        if speed <= 0:
            raise ValueError(f"speed must be positive, got {speed}")

        start_ts = self._actions[0].timestamp
        prev_ts = start_ts

        for action in self._actions:
            elapsed = action.relative_time(start_ts)
            wait_time = elapsed / speed - (prev_ts - start_ts) / speed
            if wait_time > 0:
                time.sleep(wait_time)
            prev_ts = action.timestamp
            if on_action:
                on_action(action)

    def actions(self) -> list[Action]:
        """Return all recorded actions."""
        return list(self._actions)

    def actions_of_type(
        self,
        action_type: ActionType,
    ) -> list[Action]:
        """Return only actions of the given type."""
        return [a for a in self._actions if a.action_type == action_type]

    def summary(self) -> dict[str, int]:
        """Return a summary count of each action type."""
        counts: dict[str, int] = {}
        for a in self._actions:
            name = a.action_type.name
            counts[name] = counts.get(name, 0) + 1
        return counts
