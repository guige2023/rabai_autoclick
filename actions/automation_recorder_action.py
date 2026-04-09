"""
Automation Recording and Playback Module.

Records user interactions (clicks, keystrokes, waits) and replays them
with variable speed, error handling, and checkpoint recovery.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

logger = logging.getLogger(__name__)


class ActionType(Enum):
    CLICK = auto()
    RIGHT_CLICK = auto()
    DOUBLE_CLICK = auto()
    TYPE = auto()
    PRESS = auto()
    SCROLL = auto()
    WAIT = auto()
    HOVER = auto()
    DRAG = auto()
    SWIPE = auto()
    SCREENSHOT = auto()
    CUSTOM = auto()


@dataclass(frozen=True)
class RecordedAction:
    """Immutable record of a single user action."""
    timestamp: float
    action_type: ActionType
    target: str
    params: Tuple[Tuple[str, Any], ...] = field(default_factory=tuple)
    screen_hash: Optional[str] = None
    window_title: Optional[str] = None
    duration_ms: float = 0.0
    result: Optional[str] = None

    def param_dict(self) -> Dict[str, Any]:
        return dict(self.params)


@dataclass
class RecordingSession:
    """A complete recording session with metadata."""
    session_id: str
    name: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    actions: List[RecordedAction] = field(default_factory=list)
    duration_seconds: float = 0.0
    app_bundle: Optional[str] = None
    screen_resolution: Optional[str] = None
    tags: FrozenSet[str] = field(default_factory=frozenset)
    version: str = "1.0"


@dataclass
class PlaybackConfig:
    """Configuration for playback."""
    speed: float = 1.0
    fail_fast: bool = False
    retry_count: int = 0
    retry_delay: float = 1.0
    checkpoint_interval: int = 10
    screenshot_on_error: bool = True
    interactive: bool = False


class ActionSerializer:
    """Serializes and deserializes recorded actions."""

    @staticmethod
    def serialize_action(action: RecordedAction) -> Dict[str, Any]:
        return {
            "timestamp": action.timestamp,
            "action_type": action.action_type.name,
            "target": action.target,
            "params": dict(action.params),
            "screen_hash": action.screen_hash,
            "window_title": action.window_title,
            "duration_ms": action.duration_ms,
            "result": action.result,
        }

    @staticmethod
    def deserialize_action(data: Dict[str, Any]) -> RecordedAction:
        return RecordedAction(
            timestamp=data["timestamp"],
            action_type=ActionType[data["action_type"]],
            target=data["target"],
            params=tuple((k, v) for k, v in data.get("params", {}).items()),
            screen_hash=data.get("screen_hash"),
            window_title=data.get("window_title"),
            duration_ms=data.get("duration_ms", 0.0),
            result=data.get("result"),
        )

    def serialize_session(self, session: RecordingSession) -> str:
        return json.dumps(
            {
                "session_id": session.session_id,
                "name": session.name,
                "created_at": session.created_at.isoformat(),
                "duration_seconds": session.duration_seconds,
                "app_bundle": session.app_bundle,
                "screen_resolution": session.screen_resolution,
                "tags": list(session.tags),
                "version": session.version,
                "actions": [
                    self.serialize_action(a) for a in session.actions
                ],
            },
            indent=2,
        )

    def deserialize_session(self, data: str) -> RecordingSession:
        obj = json.loads(data)
        return RecordingSession(
            session_id=obj["session_id"],
            name=obj["name"],
            created_at=datetime.fromisoformat(obj["created_at"]),
            duration_seconds=obj.get("duration_seconds", 0.0),
            app_bundle=obj.get("app_bundle"),
            screen_resolution=obj.get("screen_resolution"),
            tags=frozenset(obj.get("tags", [])),
            version=obj.get("version", "1.0"),
            actions=[self.deserialize_action(a) for a in obj.get("actions", [])],
        )


class ScreenHasher:
    """Computes screen hashes for visual change detection."""

    @staticmethod
    def compute_hash(image_data: bytes) -> str:
        return hashlib.sha256(image_data).hexdigest()[:16]

    @staticmethod
    def compare(h1: str, h2: str) -> float:
        """Compare two hashes, returning similarity ratio 0-1."""
        if h1 == h2:
            return 1.0
        return 0.0


class AutomationRecorder:
    """
    Records user interactions for later playback.
    """

    def __init__(self, session_name: str, app_bundle: Optional[str] = None):
        import uuid
        self.session = RecordingSession(
            session_id=str(uuid.uuid4())[:8],
            name=session_name,
            app_bundle=app_bundle,
        )
        self._recording = False
        self._start_time: Optional[float] = None
        self.serializer = ActionSerializer()

    def start_recording(self) -> None:
        self._recording = True
        self._start_time = time.time()
        self.session = RecordingSession(
            session_id=self.session.session_id,
            name=self.session.name,
            created_at=datetime.utcnow(),
            app_bundle=self.session.app_bundle,
        )
        logger.info("Recording started: %s", self.session.session_id)

    def stop_recording(self) -> RecordingSession:
        self._recording = False
        if self._start_time:
            self.session.duration_seconds = time.time() - self._start_time
        logger.info(
            "Recording stopped: %d actions, %.1f seconds",
            len(self.session.actions),
            self.session.duration_seconds,
        )
        return self.session

    def is_recording(self) -> bool:
        return self._recording

    def record_action(
        self,
        action_type: ActionType,
        target: str,
        params: Optional[Dict[str, Any]] = None,
        duration_ms: float = 0.0,
        result: Optional[str] = None,
    ) -> None:
        if not self._recording:
            return

        ts = time.time() - (self._start_time or time.time())

        action = RecordedAction(
            timestamp=ts,
            action_type=action_type,
            target=target,
            params=tuple((k, v) for k, v in (params or {}).items()),
            duration_ms=duration_ms,
            result=result,
        )
        self.session.actions.append(action)

    def record_click(
        self,
        x: int,
        y: int,
        button: str = "left",
        target_desc: str = "",
    ) -> None:
        self.record_action(
            ActionType.CLICK,
            f"({x},{y})",
            {"x": x, "y": y, "button": button, "target": target_desc},
        )

    def record_type(
        self,
        text: str,
        target: str = "",
        modifiers: Optional[Dict[str, bool]] = None,
    ) -> None:
        self.record_action(
            ActionType.TYPE,
            target,
            {"text": text, "modifiers": modifiers or {}},
        )

    def record_wait(self, duration_seconds: float, reason: str = "") -> None:
        self.record_action(
            ActionType.WAIT,
            f"wait_{duration_seconds}s",
            {"duration": duration_seconds, "reason": reason},
        )

    def save_session(self, path: str) -> None:
        with open(path, "w") as f:
            f.write(self.serializer.serialize_session(self.session))
        logger.info("Session saved to %s", path)

    def load_session(self, path: str) -> RecordingSession:
        with open(path) as f:
            self.session = self.serializer.deserialize_session(f.read())
        return self.session


class AutomationPlayer:
    """
    Plays back recorded automation sessions with error handling.
    """

    def __init__(self, action_executor: Callable[[RecordedAction], bool]):
        self.action_executor = action_executor
        self.config = PlaybackConfig()
        self._checkpoints: Dict[int, List[RecordedAction]] = {}
        self._current_index: int = 0
        self._paused: bool = False
        self._cancelled: bool = False

    def set_config(self, **kwargs) -> None:
        for key, value in kwargs.items():
            if hasattr(self.config, key):
                setattr(self.config, key, value)

    async def play(
        self,
        session: RecordingSession,
        from_checkpoint: int = 0,
    ) -> Tuple[bool, int, List[str]]:
        """
        Play back a recording session.

        Returns (success, completed_count, errors).
        """
        self._current_index = from_checkpoint
        self._cancelled = False
        errors: List[str] = []
        actions = session.actions[from_checkpoint:]

        logger.info(
            "Starting playback: %d actions from index %d",
            len(actions), from_checkpoint
        )

        for i, action in enumerate(actions):
            if self._cancelled:
                logger.info("Playback cancelled at action %d", self._current_index)
                break

            while self._paused:
                await asyncio.sleep(0.1)

            checkpoint_key = self._current_index
            if (
                self.config.checkpoint_interval > 0
                and self._current_index > 0
                and self._current_index % self.config.checkpoint_interval == 0
            ):
                self._checkpoints[checkpoint_key] = list(session.actions[: self._current_index])

            success = await self._execute_action(action)

            if not success:
                error_msg = f"Action {self._current_index} failed: {action.action_type.name} on {action.target}"
                errors.append(error_msg)
                logger.error(error_msg)

                if self.config.fail_fast:
                    return (False, self._current_index, errors)

                for retry in range(self.config.retry_count):
                    await asyncio.sleep(self.config.retry_delay)
                    logger.info("Retry %d/%d for action %d", retry + 1, self.config.retry_count, self._current_index)
                    if await self._execute_action(action):
                        break

            self._current_index += 1

            adjusted_delay = (action.params_dict().get("duration", 0) / 1000) / self.config.speed
            if action.action_type == ActionType.WAIT:
                wait_duration = action.params_dict().get("duration", 1)
                await asyncio.sleep(wait_duration / self.config.speed)
            elif adjusted_delay > 0:
                await asyncio.sleep(adjusted_delay)

        return (len(errors) == 0, self._current_index, errors)

    async def _execute_action(self, action: RecordedAction) -> bool:
        try:
            if asyncio.iscoroutinefunction(self.action_executor):
                return await self.action_executor(action)
            else:
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(None, self.action_executor, action)
        except Exception as exc:
            logger.error("Action execution error: %s", exc)
            return False

    def pause(self) -> None:
        self._paused = True
        logger.info("Playback paused at action %d", self._current_index)

    def resume(self) -> None:
        self._paused = False
        logger.info("Playback resumed")

    def cancel(self) -> None:
        self._cancelled = True
        self._paused = False

    def get_checkpoint(self, index: int) -> Optional[List[RecordedAction]]:
        return self._checkpoints.get(index)

    def list_checkpoints(self) -> List[int]:
        return sorted(self._checkpoints.keys())
