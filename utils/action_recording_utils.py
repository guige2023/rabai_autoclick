"""Action recording and replay utilities.

Records sequences of mouse and keyboard events as structured action
objects, then serializes them to JSON for storage and later replay.
Supports parameterized playback with speed control and conditional
event filtering.

Example:
    >>> from utils.action_recording_utils import ActionRecorder, ActionReplayer
    >>> recorder = ActionRecorder()
    >>> recorder.start()
    >>> # ... perform actions ...
    >>> actions = recorder.stop()
    >>> replayer = ActionReplayer(actions)
    >>> replayer.play(speed=2.0)
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from typing import Optional, Literal, Any

__all__ = [
    "ActionType",
    "Action",
    "ActionRecorder",
    "ActionReplayer",
    "serialize_actions",
    "deserialize_actions",
    "PlaybackError",
]


@dataclass
class Action:
    """A single recorded action.

    Attributes:
        id: Unique identifier for this action.
        action_type: Type of action (click, move, type, scroll, wait, etc.).
        timestamp: Absolute timestamp when the action occurred.
        x: X coordinate for mouse actions.
        y: Y coordinate for mouse actions.
        key: Key name for keyboard actions.
        text: Text content for type actions.
        button: Mouse button ('left', 'right', 'middle').
        delta: Scroll delta for scroll actions.
        duration: Duration for wait/drag actions.
        metadata: Additional action-specific data.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    action_type: Literal[
        "move",
        "click",
        "right_click",
        "double_click",
        "drag",
        "scroll",
        "key_press",
        "type",
        "wait",
        "custom",
    ] = "custom"
    timestamp: float = field(default_factory=time.time)
    x: Optional[float] = None
    y: Optional[float] = None
    key: Optional[str] = None
    text: Optional[str] = None
    button: Optional[str] = None
    delta: Optional[tuple[float, float]] = None
    duration: Optional[float] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def relative_time(self, start: float) -> float:
        """Get time offset from recording start."""
        return self.timestamp - start

    def to_dict(self) -> dict:
        """Serialize to a plain dictionary."""
        return {
            "id": self.id,
            "action_type": self.action_type,
            "timestamp": self.timestamp,
            "x": self.x,
            "y": self.y,
            "key": self.key,
            "text": self.text,
            "button": self.button,
            "delta": self.delta,
            "duration": self.duration,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Action":
        """Deserialize from a plain dictionary."""
        return cls(
            id=d.get("id", str(uuid.uuid4())),
            action_type=d.get("action_type", "custom"),
            timestamp=d.get("timestamp", 0.0),
            x=d.get("x"),
            y=d.get("y"),
            key=d.get("key"),
            text=d.get("text"),
            button=d.get("button"),
            delta=d.get("delta"),
            duration=d.get("duration"),
            metadata=d.get("metadata", {}),
        )


class ActionRecorder:
    """Records a sequence of actions for later replay.

    Example:
        >>> recorder = ActionRecorder()
        >>> recorder.start()
        >>> recorder.record_click(100, 200, button='left')
        >>> recorder.record_type("hello")
        >>> actions = recorder.stop()
    """

    def __init__(self):
        self._actions: list[Action] = []
        self._start_time: Optional[float] = None
        self._running = False

    def start(self) -> None:
        """Start recording actions."""
        self._actions = []
        self._start_time = time.time()
        self._running = True

    def stop(self) -> list[Action]:
        """Stop recording and return the list of recorded actions."""
        self._running = False
        return list(self._actions)

    @property
    def is_recording(self) -> bool:
        return self._running

    def _record(self, action: Action) -> None:
        """Internal: add an action if recording is active."""
        if self._running:
            self._actions.append(action)

    def record_move(self, x: float, y: float) -> None:
        self._record(Action(action_type="move", x=x, y=y))

    def record_click(
        self,
        x: float,
        y: float,
        button: Literal["left", "right"] = "left",
    ) -> None:
        self._record(
            Action(action_type="click", x=x, y=y, button=button)
        )

    def record_right_click(self, x: float, y: float) -> None:
        self._record(Action(action_type="right_click", x=x, y=y))

    def record_double_click(self, x: float, y: float) -> None:
        self._record(Action(action_type="double_click", x=x, y=y))

    def record_drag(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        duration: float = 0.3,
    ) -> None:
        self._record(
            Action(
                action_type="drag",
                x=x1,
                y=y1,
                delta=(x2, y2),
                duration=duration,
            )
        )

    def record_scroll(self, dx: float = 0, dy: float = 0) -> None:
        self._record(Action(action_type="scroll", delta=(dx, dy)))

    def record_key_press(self, key: str) -> None:
        self._record(Action(action_type="key_press", key=key))

    def record_type(self, text: str) -> None:
        self._record(Action(action_type="type", text=text))

    def record_wait(self, duration: float) -> None:
        self._record(Action(action_type="wait", duration=duration))

    def record_custom(
        self,
        action_type: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> None:
        self._record(
            Action(action_type=action_type, metadata=metadata or {})
        )

    def get_actions(self) -> list[Action]:
        """Return a copy of recorded actions so far."""
        return list(self._actions)

    def clear(self) -> None:
        """Clear all recorded actions."""
        self._actions = []


class PlaybackError(Exception):
    """Raised when playback encounters an error."""
    pass


class ActionReplayer:
    """Replays a sequence of recorded actions.

    Args:
        actions: List of Action objects to replay.

    Example:
        >>> replayer = ActionReplayer(actions)
        >>> replayer.play(speed=2.0)  # 2x speed
    """

    def __init__(self, actions: list[Action]):
        self.actions = actions
        self._aborted = False

    def abort(self) -> None:
        """Abort the currently playing playback."""
        self._aborted = True

    def play(
        self,
        speed: float = 1.0,
        start_time: Optional[float] = None,
        filter_fn: Optional[callable] = None,
    ) -> None:
        """Replay all actions.

        Args:
            speed: Playback speed multiplier (2.0 = 2x faster).
            start_time: Override the recording start time.
            filter_fn: Optional callable(Action) -> bool to skip actions.

        Raises:
            PlaybackError: If replay fails.
        """
        if not self.actions:
            return

        if start_time is None:
            start_time = self.actions[0].timestamp

        self._aborted = False

        # Import here to avoid circular imports
        try:
            from utils.input_simulation_utils import (
                click,
                double_click,
                right_click,
                drag,
                scroll,
                type_text,
                press_key,
                move_mouse,
            )
        except ImportError:
            raise PlaybackError("Input simulation module not available")

        for action in self.actions:
            if self._aborted:
                break

            if filter_fn is not None and not filter_fn(action):
                continue

            delay = (action.timestamp - start_time) / speed
            if delay > 0:
                time.sleep(delay)

            try:
                self._dispatch_action(action)
            except Exception as e:
                raise PlaybackError(f"Action {action.id} failed: {e}")

    def _dispatch_action(self, action: Action) -> None:
        """Dispatch a single action to the input simulation layer."""
        from utils.input_simulation_utils import (
            click,
            double_click,
            right_click,
            drag,
            scroll,
            type_text,
            press_key,
            move_mouse,
        )

        at = action.action_type
        x, y = action.x, action.y

        if at == "move" and x is not None and y is not None:
            move_mouse(x, y)
        elif at == "click" and x is not None and y is not None:
            click(x, y, button=action.button or "left")
        elif at == "right_click" and x is not None and y is not None:
            right_click(x, y)
        elif at == "double_click" and x is not None and y is not None:
            double_click(x, y)
        elif at == "drag" and x is not None and y is not None and action.delta:
            dx, dy = action.delta
            duration = action.duration or 0.3
            drag(x, y, x + dx, y + dy, duration=duration)
        elif at == "scroll" and action.delta:
            dx, dy = action.delta
            scroll(dx=dx, delta_y=dy)
        elif at == "key_press" and action.key:
            press_key(action.key)
        elif at == "type" and action.text:
            type_text(action.text)
        elif at == "wait" and action.duration:
            time.sleep(action.duration)


def serialize_actions(actions: list[Action]) -> str:
    """Serialize a list of actions to JSON."""
    import json

    return json.dumps([a.to_dict() for a in actions], indent=2)


def deserialize_actions(data: str) -> list[Action]:
    """Deserialize a JSON string to a list of actions."""
    import json

    return [Action.from_dict(d) for d in json.loads(data)]
