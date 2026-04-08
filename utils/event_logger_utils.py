"""
Event Logger Utilities

Provides utilities for logging events
in automation workflows.

Author: Agent3
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from datetime import datetime
from enum import Enum, auto
import json


class LogLevel(Enum):
    """Log levels."""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


@dataclass
class LogEntry:
    """Represents a log entry."""
    timestamp: datetime
    level: LogLevel
    message: str
    event_type: str | None = None
    data: dict[str, Any] | None = None


class EventLogger:
    """
    Logger for automation events.
    
    Provides structured logging with
    filtering and export capabilities.
    """

    def __init__(self, name: str = "automation") -> None:
        self._name = name
        self._entries: list[LogEntry] = []
        self._max_entries = 10000
        self._min_level = LogLevel.INFO

    def set_level(self, level: LogLevel) -> None:
        """Set minimum log level."""
        self._min_level = level

    def debug(self, message: str, event_type: str | None = None, **data: Any) -> None:
        """Log debug message."""
        self._log(LogLevel.DEBUG, message, event_type, data)

    def info(self, message: str, event_type: str | None = None, **data: Any) -> None:
        """Log info message."""
        self._log(LogLevel.INFO, message, event_type, data)

    def warning(self, message: str, event_type: str | None = None, **data: Any) -> None:
        """Log warning message."""
        self._log(LogLevel.WARNING, message, event_type, data)

    def error(self, message: str, event_type: str | None = None, **data: Any) -> None:
        """Log error message."""
        self._log(LogLevel.ERROR, message, event_type, data)

    def critical(self, message: str, event_type: str | None = None, **data: Any) -> None:
        """Log critical message."""
        self._log(LogLevel.CRITICAL, message, event_type, data)

    def _log(
        self,
        level: LogLevel,
        message: str,
        event_type: str | None,
        data: dict[str, Any],
    ) -> None:
        """Internal log method."""
        if level.value < self._min_level.value:
            return
        entry = LogEntry(
            timestamp=datetime.now(),
            level=level,
            message=message,
            event_type=event_type,
            data=data if data else None,
        )
        self._entries.append(entry)
        if len(self._entries) > self._max_entries:
            self._entries.pop(0)

    def get_entries(
        self,
        level: LogLevel | None = None,
        event_type: str | None = None,
        limit: int | None = None,
    ) -> list[LogEntry]:
        """Get log entries with optional filtering."""
        entries = self._entries
        if level:
            entries = [e for e in entries if e.level == level]
        if event_type:
            entries = [e for e in entries if e.event_type == event_type]
        if limit:
            entries = entries[-limit:]
        return entries

    def export_json(self) -> str:
        """Export logs as JSON."""
        return json.dumps(
            [
                {
                    "timestamp": e.timestamp.isoformat(),
                    "level": e.level.name,
                    "message": e.message,
                    "event_type": e.event_type,
                    "data": e.data,
                }
                for e in self._entries
            ],
            indent=2,
        )

    def clear(self) -> None:
        """Clear all log entries."""
        self._entries.clear()
