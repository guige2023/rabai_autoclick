"""
Automation Recorder Action Module.

Records automation workflows by capturing user interactions
 and replaying them as executable automation scripts.
"""

from __future__ import annotations

import time
import json
from typing import Any, Callable, Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import deque
import logging

logger = logging.getLogger(__name__)


class ActionType(Enum):
    """Type of recorded action."""
    CLICK = "click"
    TYPE = "type"
    PRESS = "press"
    SCROLL = "scroll"
    WAIT = "wait"
    SWIPE = "swipe"
    SCREENSHOT = "screenshot"
    CUSTOM = "custom"


@dataclass
class RecordedAction:
    """A single recorded action."""
    action_type: ActionType
    timestamp: float
    data: dict[str, Any]
    duration_ms: float = 0.0
    selector: Optional[str] = None
    screenshot: Optional[str] = None


@dataclass
class RecordedSession:
    """A complete recorded session."""
    session_id: str
    started_at: float
    ended_at: Optional[float] = None
    actions: list[RecordedAction] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReplayResult:
    """Result of replaying a recorded session."""
    success: bool
    actions_replayed: int = 0
    actions_failed: int = 0
    failed_actions: list[int] = field(default_factory=list)
    duration_ms: float = 0.0


class AutomationRecorderAction:
    """
    UI automation recorder and playback system.

    Records user interactions and converts them to replayable
    automation scripts with timing preservation.

    Example:
        recorder = AutomationRecorderAction()
        recorder.start_recording("session_001")
        # ... user performs actions ...
        session = recorder.stop_recording()
        recorder.save_session(session, "session.json")
    """

    def __init__(
        self,
        action_handlers: Optional[dict[ActionType, Callable[[RecordedAction], None]]] = None,
        screenshot_dir: Optional[str] = None,
    ) -> None:
        self.action_handlers = action_handlers or {}
        self.screenshot_dir = screenshot_dir
        self._current_session: Optional[RecordedSession] = None
        self._recording = False
        self._action_queue: deque = deque(maxlen=10000)

    def start_recording(
        self,
        session_id: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> RecordedSession:
        """Start recording a new automation session."""
        if self._recording:
            self.stop_recording()

        self._current_session = RecordedSession(
            session_id=session_id,
            started_at=time.monotonic(),
            metadata=metadata or {},
        )
        self._recording = True
        logger.info(f"Started recording session: {session_id}")
        return self._current_session

    def stop_recording(self) -> Optional[RecordedSession]:
        """Stop the current recording session."""
        if not self._recording or not self._current_session:
            return None

        self._current_session.ended_at = time.monotonic()
        self._recording = False
        logger.info(f"Stopped recording session: {self._current_session.session_id}")
        return self._current_session

    def record_action(
        self,
        action_type: ActionType,
        data: dict[str, Any],
        selector: Optional[str] = None,
        screenshot: Optional[str] = None,
        duration_ms: float = 0.0,
    ) -> RecordedAction:
        """Record a single action."""
        action = RecordedAction(
            action_type=action_type,
            timestamp=time.monotonic(),
            data=data,
            duration_ms=duration_ms,
            selector=selector,
            screenshot=screenshot,
        )

        if self._recording and self._current_session:
            self._current_session.actions.append(action)

        self._action_queue.append(action)
        return action

    def record_click(
        self,
        x: int,
        y: int,
        button: str = "left",
        selector: Optional[str] = None,
    ) -> RecordedAction:
        """Record a click action."""
        return self.record_action(
            ActionType.CLICK,
            {"x": x, "y": y, "button": button},
            selector=selector,
        )

    def record_type(
        self,
        text: str,
        selector: Optional[str] = None,
    ) -> RecordedAction:
        """Record a type action."""
        return self.record_action(
            ActionType.TYPE,
            {"text": text},
            selector=selector,
        )

    def record_press(
        self,
        key: str,
        modifiers: Optional[list[str]] = None,
    ) -> RecordedAction:
        """Record a key press action."""
        return self.record_action(
            ActionType.PRESS,
            {"key": key, "modifiers": modifiers or []},
        )

    def record_scroll(
        self,
        dx: int,
        dy: int,
        selector: Optional[str] = None,
    ) -> RecordedAction:
        """Record a scroll action."""
        return self.record_action(
            ActionType.SCROLL,
            {"dx": dx, "dy": dy},
            selector=selector,
        )

    def record_wait(
        self,
        duration_ms: float,
    ) -> RecordedAction:
        """Record a wait action."""
        return self.record_action(
            ActionType.WAIT,
            {"duration_ms": duration_ms},
            duration_ms=duration_ms,
        )

    def save_session(
        self,
        session: RecordedSession,
        filepath: str,
    ) -> None:
        """Save a recorded session to a file."""
        data = {
            "session_id": session.session_id,
            "started_at": session.started_at,
            "ended_at": session.ended_at,
            "metadata": session.metadata,
            "actions": [
                {
                    "type": a.action_type.value,
                    "timestamp": a.timestamp,
                    "data": a.data,
                    "duration_ms": a.duration_ms,
                    "selector": a.selector,
                }
                for a in session.actions
            ],
        }

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info(f"Saved session to {filepath}")

    def load_session(self, filepath: str) -> RecordedSession:
        """Load a recorded session from a file."""
        with open(filepath, "r") as f:
            data = json.load(f)

        actions = [
            RecordedAction(
                action_type=ActionType(a["type"]),
                timestamp=a["timestamp"],
                data=a["data"],
                duration_ms=a.get("duration_ms", 0),
                selector=a.get("selector"),
            )
            for a in data["actions"]
        ]

        return RecordedSession(
            session_id=data["session_id"],
            started_at=data["started_at"],
            ended_at=data.get("ended_at"),
            metadata=data.get("metadata", {}),
            actions=actions,
        )

    async def replay_session(
        self,
        session: RecordedSession,
        action_executor: Callable[[RecordedAction], Any],
        delay_between_actions: float = 0.1,
    ) -> ReplayResult:
        """Replay a recorded session."""
        import asyncio
        start_time = time.monotonic()
        actions_replayed = 0
        actions_failed = 0
        failed_actions: list[int] = []

        for idx, action in enumerate(session.actions):
            try:
                if action.duration_ms > 0:
                    await asyncio.sleep(max(0, action.duration_ms / 1000 - delay_between_actions))

                await action_executor(action)
                actions_replayed += 1

            except Exception as e:
                logger.warning(f"Action {idx} failed: {e}")
                actions_failed += 1
                failed_actions.append(idx)

        return ReplayResult(
            success=actions_failed == 0,
            actions_replayed=actions_replayed,
            actions_failed=actions_failed,
            failed_actions=failed_actions,
            duration_ms=(time.monotonic() - start_time) * 1000,
        )

    def get_current_session(self) -> Optional[RecordedSession]:
        """Get the currently recording session."""
        return self._current_session if self._recording else None

    def is_recording(self) -> bool:
        """Check if currently recording."""
        return self._recording
