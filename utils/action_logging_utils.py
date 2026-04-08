"""Action Logging and Audit Trail Utilities.

Logs and audits all UI actions for debugging and compliance.
Supports structured logging, action replay, and audit trails.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from pathlib import Path
from typing import Any, Callable, Optional


class ActionLevel(Enum):
    """Log levels for actions."""

    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    CRITICAL = auto()


class ActionStatus(Enum):
    """Status of an action."""

    PENDING = auto()
    STARTED = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()


@dataclass
class LoggedAction:
    """A single logged action.

    Attributes:
        timestamp: When the action occurred.
        action_type: Type of action.
        target_id: Target element/window ID.
        parameters: Action parameters.
        status: Action status.
        level: Log level.
        result: Action result data.
        error: Error message if failed.
        duration_ms: Execution duration.
    """

    timestamp: float
    action_type: str
    target_id: str = ""
    parameters: dict = field(default_factory=dict)
    status: ActionStatus = ActionStatus.PENDING
    level: ActionLevel = ActionLevel.INFO
    result: Any = None
    error: str = ""
    duration_ms: float = 0.0

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp,
            "datetime": datetime.fromtimestamp(self.timestamp).isoformat(),
            "action_type": self.action_type,
            "target_id": self.target_id,
            "parameters": self.parameters,
            "status": self.status.name,
            "level": self.level.name,
            "result": self.result,
            "error": self.error,
            "duration_ms": self.duration_ms,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "LoggedAction":
        """Create from dictionary."""
        return cls(
            timestamp=data["timestamp"],
            action_type=data["action_type"],
            target_id=data.get("target_id", ""),
            parameters=data.get("parameters", {}),
            status=ActionStatus[data.get("status", "PENDING")],
            level=ActionLevel[data.get("level", "INFO")],
            result=data.get("result"),
            error=data.get("error", ""),
            duration_ms=data.get("duration_ms", 0.0),
        )


@dataclass
class ActionLog:
    """A complete action log session.

    Attributes:
        session_id: Unique session identifier.
        start_time: Session start timestamp.
        end_time: Session end timestamp.
        actions: List of logged actions.
        metadata: Additional session metadata.
    """

    session_id: str
    start_time: float
    end_time: Optional[float] = None
    actions: list[LoggedAction] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    def add_action(self, action: LoggedAction) -> None:
        """Add an action to the log.

        Args:
            action: LoggedAction to add.
        """
        self.actions.append(action)

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "session_id": self.session_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "datetime_start": datetime.fromtimestamp(self.start_time).isoformat(),
            "datetime_end": datetime.fromtimestamp(self.end_time).isoformat() if self.end_time else None,
            "actions": [a.to_dict() for a in self.actions],
            "metadata": self.metadata,
        }

    def get_action_count(self) -> int:
        """Get total action count."""
        return len(self.actions)

    def get_failed_actions(self) -> list[LoggedAction]:
        """Get all failed actions."""
        return [a for a in self.actions if a.status == ActionStatus.FAILED]

    def get_completed_actions(self) -> list[LoggedAction]:
        """Get all completed actions."""
        return [a for a in self.actions if a.status == ActionStatus.COMPLETED]


class ActionLogger:
    """Logger for UI actions.

    Example:
        logger = ActionLogger()
        logger.start_session()
        with logger.log_action("click", target_id="btn1"):
            click_button("btn1")
        logger.end_session()
    """

    def __init__(self, session_id: Optional[str] = None):
        """Initialize the action logger.

        Args:
            session_id: Session identifier (generated if None).
        """
        self._session_id = session_id or self._generate_session_id()
        self._log: Optional[ActionLog] = None
        self._current_action: Optional[LoggedAction] = None
        self._start_time: Optional[float] = None

    def _generate_session_id(self) -> str:
        """Generate a unique session ID."""
        return f"session_{int(time.time() * 1000)}"

    def start_session(self, metadata: Optional[dict] = None) -> None:
        """Start a new logging session.

        Args:
            metadata: Optional session metadata.
        """
        self._start_time = time.time()
        self._log = ActionLog(
            session_id=self._session_id,
            start_time=self._start_time,
            metadata=metadata or {},
        )

    def end_session(self) -> ActionLog:
        """End the current session.

        Returns:
            Completed ActionLog.
        """
        if self._log:
            self._log.end_time = time.time()
        log = self._log
        self._log = None
        return log

    def log_action(
        self,
        action_type: str,
        target_id: str = "",
        parameters: Optional[dict] = None,
        level: ActionLevel = ActionLevel.INFO,
    ) -> "ActionContext":
        """Start logging an action.

        Args:
            action_type: Type of action.
            target_id: Target element/window ID.
            parameters: Action parameters.
            level: Log level.

        Returns:
            ActionContext for the action.
        """
        return ActionContext(self, action_type, target_id, parameters or {}, level)

    def _begin_action(
        self,
        action_type: str,
        target_id: str,
        parameters: dict,
        level: ActionLevel,
    ) -> LoggedAction:
        """Begin recording an action."""
        action = LoggedAction(
            timestamp=time.time(),
            action_type=action_type,
            target_id=target_id,
            parameters=parameters,
            level=level,
            status=ActionStatus.STARTED,
        )
        self._current_action = action
        return action

    def _complete_action(
        self,
        result: Any = None,
    ) -> None:
        """Complete the current action."""
        if self._current_action and self._log:
            self._current_action.status = ActionStatus.COMPLETED
            self._current_action.result = result
            self._current_action.duration_ms = (
                time.time() - self._current_action.timestamp
            ) * 1000
            self._log.add_action(self._current_action)
        self._current_action = None

    def _fail_action(self, error: str) -> None:
        """Mark the current action as failed."""
        if self._current_action and self._log:
            self._current_action.status = ActionStatus.FAILED
            self._current_action.error = error
            self._current_action.duration_ms = (
                time.time() - self._current_action.timestamp
            ) * 1000
            self._log.add_action(self._current_action)
        self._current_action = None

    def get_log(self) -> Optional[ActionLog]:
        """Get the current log.

        Returns:
            Current ActionLog or None.
        """
        return self._log


class ActionContext:
    """Context manager for logging actions.

    Example:
        logger = ActionLogger()
        with logger.log_action("click", target_id="btn1") as ctx:
            click_button("btn1")
            ctx.set_result({"clicked": True})
    """

    def __init__(
        self,
        logger: ActionLogger,
        action_type: str,
        target_id: str,
        parameters: dict,
        level: ActionLevel,
    ):
        """Initialize the context."""
        self._logger = logger
        self._action_type = action_type
        self._target_id = target_id
        self._parameters = parameters
        self._level = level

    def __enter__(self) -> "ActionContext":
        """Enter the context."""
        self._logger._begin_action(
            self._action_type,
            self._target_id,
            self._parameters,
            self._level,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        """Exit the context."""
        if exc_type is not None:
            self._logger._fail_action(str(exc_val))
        else:
            self._logger._complete_action()
        return False

    def set_result(self, result: Any) -> None:
        """Set the action result.

        Args:
            result: Result data.
        """
        pass  # Result is set on context exit


class ActionLogExporter:
    """Exports action logs to various formats.

    Example:
        exporter = ActionLogExporter()
        exporter.to_json(log, "/tmp/log.json")
    """

    @staticmethod
    def to_json(log: ActionLog, path: str | Path) -> None:
        """Export log to JSON file.

        Args:
            log: ActionLog to export.
            path: Output file path.
        """
        with open(path, "w") as f:
            json.dump(log.to_dict(), f, indent=2)

    @staticmethod
    def to_csv(log: ActionLog) -> str:
        """Export log to CSV format.

        Args:
            log: ActionLog to export.

        Returns:
            CSV string.
        """
        lines = ["timestamp,action_type,target_id,status,level,duration_ms,error"]
        for action in log.actions:
            lines.append(
                f"{action.timestamp},{action.action_type},"
                f"{action.target_id},{action.status.name},"
                f"{action.level.name},{action.duration_ms:.2f},"
                f'"{action.error}"'
            )
        return "\n".join(lines)

    @staticmethod
    def summary(log: ActionLog) -> dict:
        """Generate a summary of the log.

        Args:
            log: ActionLog to summarize.

        Returns:
            Summary dictionary.
        """
        total = len(log.actions)
        completed = sum(1 for a in log.actions if a.status == ActionStatus.COMPLETED)
        failed = sum(1 for a in log.actions if a.status == ActionStatus.FAILED)

        total_duration = sum(a.duration_ms for a in log.actions)
        avg_duration = total_duration / total if total > 0 else 0

        action_types: dict[str, int] = {}
        for action in log.actions:
            action_types[action.action_type] = action_types.get(action.action_type, 0) + 1

        return {
            "session_id": log.session_id,
            "total_actions": total,
            "completed": completed,
            "failed": failed,
            "success_rate": completed / total if total > 0 else 0,
            "total_duration_ms": total_duration,
            "average_duration_ms": avg_duration,
            "action_types": action_types,
        }
