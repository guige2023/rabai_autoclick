"""Automation recording and playback action."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class StepType(str, Enum):
    """Type of automation step."""

    CLICK = "click"
    TYPE = "type"
    PRESS = "press"
    WAIT = "wait"
    SCROLL = "scroll"
    HOVER = "hover"
    SCREENSHOT = "screenshot"
    CUSTOM = "custom"


@dataclass
class AutomationStep:
    """A single step in an automation sequence."""

    step_type: StepType
    timestamp: float
    data: dict[str, Any]
    duration_ms: float = 0
    result: Optional[Any] = None
    error: Optional[str] = None


@dataclass
class RecordedSession:
    """A recorded automation session."""

    session_id: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    steps: list[AutomationStep] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class PlaybackResult:
    """Result of playing back a session."""

    session_id: str
    total_steps: int
    successful_steps: int
    failed_steps: int
    duration_ms: float
    errors: list[str] = field(default_factory=list)


class AutomationRecorderAction:
    """Records and replays automation sequences."""

    def __init__(
        self,
        on_step: Optional[Callable[[AutomationStep], None]] = None,
        on_error: Optional[Callable[[AutomationStep, Exception], None]] = None,
    ):
        """Initialize recorder.

        Args:
            on_step: Callback for each step executed.
            on_error: Callback when step fails.
        """
        self._on_step = on_step
        self._on_error = on_error
        self._sessions: dict[str, RecordedSession] = {}
        self._current_session: Optional[RecordedSession] = None
        self._handlers: dict[StepType, Callable[[dict[str, Any]], Any]] = {}

    def register_handler(
        self,
        step_type: StepType,
        handler: Callable[[dict[str, Any]], Any],
    ) -> None:
        """Register a handler for a step type."""
        self._handlers[step_type] = handler

    def _get_default_handler(self, step_type: StepType) -> Callable[[dict[str, Any]], Any]:
        """Get default handler for step type."""
        def default_handler(data: dict[str, Any]) -> Any:
            return {"handled": True, "type": step_type.value, "data": data}

        return default_handler

    async def start_recording(self, session_id: Optional[str] = None) -> str:
        """Start a new recording session."""
        import uuid

        session_id = session_id or str(uuid.uuid4())[:8]
        session = RecordedSession(
            session_id=session_id,
            started_at=datetime.now(),
        )
        self._sessions[session_id] = session
        self._current_session = session
        return session_id

    async def stop_recording(self) -> Optional[RecordedSession]:
        """Stop the current recording session."""
        if self._current_session:
            self._current_session.completed_at = datetime.now()
            session = self._current_session
            self._current_session = None
            return session
        return None

    def add_step(self, step: AutomationStep) -> None:
        """Add a step to the current recording."""
        if self._current_session:
            self._current_session.steps.append(step)

    async def record_step(
        self,
        step_type: StepType,
        data: dict[str, Any],
        duration_ms: float = 0,
    ) -> AutomationStep:
        """Record a single step."""
        step = AutomationStep(
            step_type=step_type,
            timestamp=time.time(),
            data=data,
            duration_ms=duration_ms,
        )

        handler = self._handlers.get(step_type) or self._get_default_handler(step_type)

        try:
            if asyncio.iscoroutinefunction(handler):
                step.result = await handler(data)
            else:
                step.result = handler(data)
        except Exception as e:
            step.error = str(e)
            if self._on_error:
                self._on_error(step, e)

        self.add_step(step)

        if self._on_step:
            self._on_step(step)

        return step

    async def playback(
        self,
        session_id: str,
        speed_factor: float = 1.0,
        stop_on_error: bool = True,
    ) -> PlaybackResult:
        """Play back a recorded session.

        Args:
            session_id: Session to play back.
            speed_factor: Playback speed (1.0 = normal).
            stop_on_error: Stop playback on first error.

        Returns:
            PlaybackResult with execution statistics.
        """
        session = self._sessions.get(session_id)
        if not session:
            raise ValueError(f"Session not found: {session_id}")

        start_time = time.time()
        successful = 0
        failed = 0
        errors = []

        for step in session.steps:
            wait_time = step.duration_ms / 1000 / speed_factor
            if wait_time > 0:
                await asyncio.sleep(wait_time)

            handler = self._handlers.get(step.step_type) or self._get_default_handler(
                step.step_type
            )

            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(step.data)
                else:
                    handler(step.data)
                successful += 1
            except Exception as e:
                failed += 1
                error_msg = f"Step {step.step_type.value} failed: {e}"
                errors.append(error_msg)
                if stop_on_error:
                    break

        duration_ms = (time.time() - start_time) * 1000

        return PlaybackResult(
            session_id=session_id,
            total_steps=len(session.steps),
            successful_steps=successful,
            failed_steps=failed,
            duration_ms=duration_ms,
            errors=errors,
        )

    def get_session(self, session_id: str) -> Optional[RecordedSession]:
        """Get a recorded session."""
        return self._sessions.get(session_id)

    def list_sessions(self) -> list[str]:
        """List all recorded session IDs."""
        return list(self._sessions.keys())

    def delete_session(self, session_id: str) -> bool:
        """Delete a recorded session."""
        return self._sessions.pop(session_id, None) is not None

    def export_session(self, session_id: str) -> dict[str, Any]:
        """Export session as dict."""
        session = self._sessions.get(session_id)
        if not session:
            raise ValueError(f"Session not found: {session_id}")

        return {
            "session_id": session.session_id,
            "started_at": session.started_at.isoformat(),
            "completed_at": session.completed_at.isoformat() if session.completed_at else None,
            "steps": [
                {
                    "type": s.step_type.value,
                    "timestamp": s.timestamp,
                    "data": s.data,
                    "duration_ms": s.duration_ms,
                    "error": s.error,
                }
                for s in session.steps
            ],
            "metadata": session.metadata,
        }
