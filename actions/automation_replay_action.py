"""
Automation Replay Action Module.

Provides action replay functionality for debugging and testing,
recording and replaying automation workflows.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
import json
import logging

logger = logging.getLogger(__name__)


class ActionType(Enum):
    """Types of recorded actions."""
    MOUSE_MOVE = auto()
    MOUSE_CLICK = auto()
    MOUSE_DRAG = auto()
    KEY_PRESS = auto()
    KEY_TYPE = auto()
    WAIT = auto()
    SCREENSHOT = auto()
    CUSTOM = auto()


@dataclass
class RecordedAction:
    """A single recorded action."""
    action_id: str
    action_type: ActionType
    timestamp: datetime
    x: Optional[int] = None
    y: Optional[int] = None
    target: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    duration_ms: float = 0.0
    result: Optional[Any] = None
    error: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "action_id": self.action_id,
            "action_type": self.action_type.name,
            "timestamp": self.timestamp.isoformat(),
            "x": self.x,
            "y": self.y,
            "target": self.target,
            "data": self.data,
            "duration_ms": self.duration_ms,
            "result": str(self.result) if self.result else None,
            "error": self.error,
        }


@dataclass
class RecordingSession:
    """A recording session."""
    session_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    actions: List[RecordedAction] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

    def duration_ms(self) -> float:
        """Get session duration in milliseconds."""
        end = self.end_time or datetime.now(timezone.utc)
        return (end - self.start_time).total_seconds() * 1000

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "session_id": self.session_id,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "action_count": len(self.actions),
            "duration_ms": self.duration_ms(),
            "metadata": self.metadata,
            "tags": self.tags,
        }


@dataclass
class ReplayResult:
    """Result of replaying actions."""
    success: bool
    actions_replayed: int
    actions_failed: int
    duration_ms: float
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "actions_replayed": self.actions_replayed,
            "actions_failed": self.actions_failed,
            "duration_ms": self.duration_ms,
            "error_count": len(self.errors),
        }


class AutomationReplayAction:
    """
    Records and replays automation actions.

    This action provides recording and replay functionality for
    automation workflows, useful for debugging, testing, and
    creating repeatable automation sequences.

    Example:
        >>> recorder = AutomationReplayAction()
        >>> recorder.start_recording("test_session")
        >>> recorder.record_mouse_click(100, 200, "button")
        >>> recorder.stop_recording()
        >>> # Later:
        >>> result = await recorder.replay("test_session")
    """

    def __init__(
        self,
        replay_delay_ms: float = 100,
        stop_on_error: bool = False,
    ):
        """
        Initialize the Automation Replay Action.

        Args:
            replay_delay_ms: Delay between replayed actions.
            stop_on_error: Whether to stop replay on first error.
        """
        self.replay_delay_ms = replay_delay_ms
        self.stop_on_error = stop_on_error

        self._sessions: Dict[str, RecordingSession] = {}
        self._current_session: Optional[RecordingSession] = None
        self._is_recording = False

        self._action_handlers: Dict[ActionType, Callable] = {}
        self._register_default_handlers()

    def _register_default_handlers(self) -> None:
        """Register default action handlers."""
        pass

    def start_recording(
        self,
        session_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        tags: Optional[List[str]] = None,
    ) -> RecordingSession:
        """
        Start a new recording session.

        Args:
            session_id: Unique session identifier.
            metadata: Optional metadata.
            tags: Optional tags.

        Returns:
            Created RecordingSession.
        """
        if self._is_recording:
            self.stop_recording()

        session = RecordingSession(
            session_id=session_id,
            start_time=datetime.now(timezone.utc),
            metadata=metadata or {},
            tags=tags or [],
        )

        self._sessions[session_id] = session
        self._current_session = session
        self._is_recording = True

        logger.info(f"Started recording session: {session_id}")
        return session

    def stop_recording(self) -> Optional[RecordingSession]:
        """
        Stop the current recording session.

        Returns:
            The completed RecordingSession.
        """
        if not self._is_recording or not self._current_session:
            return None

        self._current_session.end_time = datetime.now(timezone.utc)
        self._is_recording = False

        logger.info(
            f"Stopped recording session: {self._current_session.session_id}, "
            f"actions: {len(self._current_session.actions)}"
        )

        session = self._current_session
        self._current_session = None
        return session

    def record_action(
        self,
        action_type: ActionType,
        x: Optional[int] = None,
        y: Optional[int] = None,
        target: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        duration_ms: float = 0.0,
        result: Optional[Any] = None,
        error: Optional[str] = None,
    ) -> Optional[str]:
        """
        Record an action.

        Args:
            action_type: Type of action.
            x: X coordinate.
            y: Y coordinate.
            target: Target element.
            data: Additional data.
            duration_ms: Action duration.
            result: Action result.
            error: Error if failed.

        Returns:
            Action ID.
        """
        if not self._is_recording or not self._current_session:
            return None

        import uuid

        action_id = str(uuid.uuid4())[:8]

        action = RecordedAction(
            action_id=action_id,
            action_type=action_type,
            timestamp=datetime.now(timezone.utc),
            x=x,
            y=y,
            target=target,
            data=data,
            duration_ms=duration_ms,
            result=result,
            error=error,
        )

        self._current_session.actions.append(action)
        return action_id

    def record_mouse_move(
        self,
        x: int,
        y: int,
        target: Optional[str] = None,
    ) -> Optional[str]:
        """Record a mouse move action."""
        return self.record_action(
            ActionType.MOUSE_MOVE,
            x=x,
            y=y,
            target=target,
        )

    def record_mouse_click(
        self,
        x: int,
        y: int,
        button: str = "left",
        target: Optional[str] = None,
    ) -> Optional[str]:
        """Record a mouse click action."""
        return self.record_action(
            ActionType.MOUSE_CLICK,
            x=x,
            y=y,
            target=target,
            data={"button": button},
        )

    def record_key_press(
        self,
        key: str,
        modifiers: Optional[List[str]] = None,
    ) -> Optional[str]:
        """Record a key press action."""
        return self.record_action(
            ActionType.KEY_PRESS,
            target=key,
            data={"modifiers": modifiers or []},
        )

    def record_key_type(
        self,
        text: str,
    ) -> Optional[str]:
        """Record a key type action."""
        return self.record_action(
            ActionType.KEY_TYPE,
            data={"text": text},
        )

    def record_wait(
        self,
        duration_ms: float,
    ) -> Optional[str]:
        """Record a wait action."""
        return self.record_action(
            ActionType.WAIT,
            duration_ms=duration_ms,
        )

    async def replay(
        self,
        session_id: str,
        speed_multiplier: float = 1.0,
        start_index: int = 0,
        end_index: Optional[int] = None,
        action_filter: Optional[Callable[[ActionType], bool]] = None,
    ) -> ReplayResult:
        """
        Replay a recorded session.

        Args:
            session_id: Session to replay.
            speed_multiplier: Replay speed multiplier.
            start_index: Starting action index.
            end_index: Ending action index.
            action_filter: Optional filter for action types.

        Returns:
            ReplayResult with replay outcome.
        """
        session = self._sessions.get(session_id)
        if not session:
            return ReplayResult(
                success=False,
                actions_replayed=0,
                actions_failed=0,
                duration_ms=0,
                errors=[f"Session not found: {session_id}"],
            )

        import time

        start = time.time()
        actions_replayed = 0
        actions_failed = 0
        errors = []

        actions_to_replay = session.actions[start_index:end_index]

        for i, action in enumerate(actions_to_replay):
            if action_filter and not action_filter(action.action_type):
                continue

            try:
                await self._execute_action(action, speed_multiplier)
                actions_replayed += 1
            except Exception as e:
                actions_failed += 1
                error_msg = f"Action {i} ({action.action_type.name}): {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)

                if self.stop_on_error:
                    break

        duration = (time.time() - start) * 1000

        return ReplayResult(
            success=actions_failed == 0,
            actions_replayed=actions_replayed,
            actions_failed=actions_failed,
            duration_ms=duration,
            errors=errors,
        )

    async def _execute_action(
        self,
        action: RecordedAction,
        speed_multiplier: float,
    ) -> None:
        """Execute a single recorded action."""
        handler = self._action_handlers.get(action.action_type)

        if handler:
            await handler(action)
        else:
            logger.debug(f"No handler for action type: {action.action_type.name}")

        adjusted_delay = self.replay_delay_ms / speed_multiplier
        if adjusted_delay > 0:
            await asyncio.sleep(adjusted_delay / 1000)

    def register_handler(
        self,
        action_type: ActionType,
        handler: Callable[[RecordedAction], Any],
    ) -> None:
        """
        Register a handler for an action type.

        Args:
            action_type: Action type to handle.
            handler: Handler function.
        """
        self._action_handlers[action_type] = handler

    def get_session(self, session_id: str) -> Optional[RecordingSession]:
        """Get a recording session."""
        return self._sessions.get(session_id)

    def list_sessions(self) -> List[str]:
        """List all session IDs."""
        return list(self._sessions.keys())

    def delete_session(self, session_id: str) -> bool:
        """Delete a recording session."""
        if session_id in self._sessions:
            del self._sessions[session_id]
            return True
        return False

    def export_session(
        self,
        session_id: str,
        format: str = "json",
    ) -> Optional[str]:
        """
        Export a session.

        Args:
            session_id: Session to export.
            format: Export format (json).

        Returns:
            Exported data as string.
        """
        session = self._sessions.get(session_id)
        if not session:
            return None

        if format == "json":
            return json.dumps(session.to_dict(), indent=2)

        return None

    def import_session(
        self,
        data: str,
        format: str = "json",
    ) -> Optional[RecordingSession]:
        """
        Import a session.

        Args:
            data: Session data.
            format: Import format.

        Returns:
            Imported RecordingSession.
        """
        if format == "json":
            parsed = json.loads(data)

            actions = [
                RecordedAction(
                    action_id=a["action_id"],
                    action_type=ActionType[a["action_type"]],
                    timestamp=datetime.fromisoformat(a["timestamp"]),
                    x=a.get("x"),
                    y=a.get("y"),
                    target=a.get("target"),
                    data=a.get("data"),
                    duration_ms=a.get("duration_ms", 0),
                )
                for a in parsed.get("actions", [])
            ]

            session = RecordingSession(
                session_id=parsed["session_id"],
                start_time=datetime.fromisoformat(parsed["start_time"]),
                end_time=datetime.fromisoformat(parsed["end_time"]) if parsed.get("end_time") else None,
                actions=actions,
                metadata=parsed.get("metadata", {}),
                tags=parsed.get("tags", []),
            )

            self._sessions[session.session_id] = session
            return session

        return None

    def clear_sessions(self) -> int:
        """Clear all sessions."""
        count = len(self._sessions)
        self._sessions.clear()
        return count


def create_replay_action(
    replay_delay_ms: float = 100,
    **kwargs,
) -> AutomationReplayAction:
    """Factory function to create an AutomationReplayAction."""
    return AutomationReplayAction(
        replay_delay_ms=replay_delay_ms,
        **kwargs,
    )
