"""
Action Replay Utilities for UI Automation

Records and replays user interaction sequences,
supporting playback with variable speed and conditions.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Callable, Optional


class ActionType(Enum):
    """Types of recorded actions."""
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_DOUBLE_CLICK = auto()
    MOUSE_RIGHT_CLICK = auto()
    MOUSE_DRAG = auto()
    KEYBOARD_TYPE = auto()
    KEYBOARD_PRESS = auto()
    SCROLL = auto()
    TOUCH_TAP = auto()
    TOUCH_SWIPE = auto()
    WAIT = auto()


@dataclass
class ActionRecord:
    """Single recorded action."""
    action_id: str
    action_type: ActionType
    timestamp: float
    data: dict
    element_locator: Optional[str] = None


@dataclass
class Recording:
    """Complete recording session."""
    recording_id: str
    name: str
    created_at: float
    actions: list[ActionRecord] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def duration(self) -> float:
        """Calculate total duration of recording."""
        if not self.actions:
            return 0.0
        return self.actions[-1].timestamp - self.actions[0].timestamp


@dataclass
class ReplayConfig:
    """Configuration for replay behavior."""
    speed: float = 1.0
    loop: bool = False
    loop_count: int = 1
    stop_on_error: bool = True
    skip_actions: set[ActionType] = field(default_factory=set)
    pre_action_delay: float = 0.0
    post_action_delay: float = 0.0


@dataclass
class ReplayResult:
    """Result of a replay operation."""
    success: bool
    actions_completed: int
    actions_failed: int
    duration: float
    errors: list[dict] = field(default_factory=list)


class ActionRecorder:
    """
    Records user actions for later replay.

    Captures mouse movements, clicks, keyboard input,
    and other interactions with timestamps.
    """

    def __init__(self) -> None:
        self._current_recording: Optional[Recording] = None
        self._action_callbacks: list[Callable[[ActionRecord], None]] = []

    def start_recording(self, name: str = "") -> Recording:
        """
        Start a new recording session.

        Args:
            name: Optional name for the recording

        Returns:
            Recording object
        """
        self._current_recording = Recording(
            recording_id=str(uuid.uuid4()),
            name=name or f"Recording_{int(time.time())}",
            created_at=time.time(),
        )
        return self._current_recording

    def stop_recording(self) -> Optional[Recording]:
        """Stop the current recording session."""
        recording = self._current_recording
        self._current_recording = None
        return recording

    def record_action(
        self,
        action_type: ActionType,
        data: dict,
        element_locator: Optional[str] = None,
    ) -> ActionRecord:
        """
        Record a single action.

        Args:
            action_type: Type of action
            data: Action-specific data
            element_locator: Optional element locator

        Returns:
            ActionRecord that was created
        """
        if self._current_recording is None:
            raise RuntimeError("No active recording session")

        record = ActionRecord(
            action_id=str(uuid.uuid4()),
            action_type=action_type,
            timestamp=time.time(),
            data=data,
            element_locator=element_locator,
        )

        self._current_recording.actions.append(record)

        for callback in self._action_callbacks:
            callback(record)

        return record

    def record_mouse_move(
        self,
        x: float,
        y: float,
        button: int = 0,
    ) -> ActionRecord:
        """Record a mouse move action."""
        return self.record_action(
            ActionType.MOUSE_MOVE,
            {"x": x, "y": y, "button": button},
        )

    def record_mouse_click(
        self,
        x: float,
        y: float,
        button: str = "left",
        element: Optional[str] = None,
    ) -> ActionRecord:
        """Record a mouse click action."""
        return self.record_action(
            ActionType.MOUSE_CLICK,
            {"x": x, "y": y, "button": button},
            element_locator=element,
        )

    def record_keyboard_type(
        self,
        text: str,
        modifiers: Optional[dict] = None,
    ) -> ActionRecord:
        """Record a keyboard type action."""
        return self.record_action(
            ActionType.KEYBOARD_TYPE,
            {"text": text, "modifiers": modifiers or {}},
        )

    def record_scroll(
        self,
        delta_x: float,
        delta_y: float,
    ) -> ActionRecord:
        """Record a scroll action."""
        return self.record_action(
            ActionType.SCROLL,
            {"delta_x": delta_x, "delta_y": delta_y},
        )

    def register_action_callback(
        self,
        callback: Callable[[ActionRecord], None],
    ) -> None:
        """Register a callback for action events."""
        self._action_callbacks.append(callback)

    def save_recording(self, recording: Recording, filepath: str) -> None:
        """
        Save a recording to a JSON file.

        Args:
            recording: Recording to save
            filepath: Output file path
        """
        data = {
            "recording_id": recording.recording_id,
            "name": recording.name,
            "created_at": recording.created_at,
            "metadata": recording.metadata,
            "actions": [
                {
                    "action_id": a.action_id,
                    "action_type": a.action_type.name,
                    "timestamp": a.timestamp,
                    "data": a.data,
                    "element_locator": a.element_locator,
                }
                for a in recording.actions
            ],
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

    def load_recording(self, filepath: str) -> Recording:
        """
        Load a recording from a JSON file.

        Args:
            filepath: Input file path

        Returns:
            Loaded Recording object
        """
        with open(filepath, "r") as f:
            data = json.load(f)

        return Recording(
            recording_id=data["recording_id"],
            name=data["name"],
            created_at=data["created_at"],
            metadata=data.get("metadata", {}),
            actions=[
                ActionRecord(
                    action_id=a["action_id"],
                    action_type=ActionType[a["action_type"]],
                    timestamp=a["timestamp"],
                    data=a["data"],
                    element_locator=a.get("element_locator"),
                )
                for a in data["actions"]
            ],
        )


class ActionReplayer:
    """
    Replays recorded action sequences.

    Supports variable speed, looping, and conditional playback.
    """

    def __init__(self, executor: Callable[[ActionRecord], bool]) -> None:
        self._executor = executor
        self._condition_checkers: dict[str, Callable[[], bool]] = {}

    def register_condition(
        self,
        name: str,
        checker: Callable[[], bool],
    ) -> None:
        """Register a condition checker for conditional replay."""
        self._condition_checkers[name] = checker

    def replay(
        self,
        recording: Recording,
        config: ReplayConfig | None = None,
    ) -> ReplayResult:
        """
        Replay a recording with the given configuration.

        Args:
            recording: Recording to replay
            config: Replay configuration

        Returns:
            ReplayResult with statistics
        """
        config = config or ReplayConfig()
        start_time = time.time()
        actions_completed = 0
        actions_failed = 0
        errors: list[dict] = []

        loop_iterations = config.loop_count if config.loop else 1

        for loop in range(loop_iterations):
            for action in recording.actions:
                if action.action_type in config.skip_actions:
                    continue

                # Check conditions
                if not self._check_conditions(action):
                    continue

                # Calculate delay based on speed
                adjusted_delay = self._calculate_delay(action, config)

                if config.pre_action_delay > 0:
                    time.sleep(config.pre_action_delay)

                # Execute action
                try:
                    success = self._executor(action)
                    if success:
                        actions_completed += 1
                    else:
                        actions_failed += 1
                        if config.stop_on_error:
                            break
                except Exception as e:
                    actions_failed += 1
                    errors.append({
                        "action_id": action.action_id,
                        "error": str(e),
                    })
                    if config.stop_on_error:
                        break

                if config.post_action_delay > 0:
                    time.sleep(config.post_action_delay)

                if adjusted_delay > 0:
                    time.sleep(adjusted_delay)

            if not config.loop:
                break

        return ReplayResult(
            success=actions_failed == 0,
            actions_completed=actions_completed,
            actions_failed=actions_failed,
            duration=time.time() - start_time,
            errors=errors,
        )

    def _check_conditions(self, action: ActionRecord) -> bool:
        """Check if all conditions for an action are met."""
        # Placeholder for condition checking logic
        return True

    def _calculate_delay(
        self,
        action: ActionRecord,
        config: ReplayConfig,
    ) -> float:
        """Calculate delay between actions based on speed."""
        if not action.data.get("duration"):
            return 0.0
        return action.data["duration"] / config.speed
