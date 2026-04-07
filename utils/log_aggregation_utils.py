"""
Log aggregation utilities for centralized logging.

Provides structured log formatting, multi-destination routing,
log level filtering, and aggregation pipeline.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    TRACE = 5
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


@dataclass
class LogEntry:
    """Structured log entry."""
    timestamp: str
    level: str
    logger: str
    message: str
    module: Optional[str] = None
    function: Optional[str] = None
    line_number: Optional[int] = None
    process_id: Optional[int] = None
    thread_id: Optional[int] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    user_id: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class LogFormatter:
    """Formats log entries for various output destinations."""

    @staticmethod
    def format_json(entry: LogEntry) -> str:
        """Format as JSON."""
        return entry.to_json()

    @staticmethod
    def format_text(entry: LogEntry) -> str:
        """Format as readable text."""
        parts = [
            f"[{entry.timestamp}]",
            f"{entry.level}",
            f"[{entry.logger}]",
            entry.message,
        ]
        if entry.module:
            parts.append(f"({entry.module}:{entry.function or '?'}:{entry.line_number or '?'})")
        if entry.trace_id:
            parts.append(f"trace_id={entry.trace_id}")
        if entry.metadata:
            parts.append(json.dumps(entry.metadata))
        return " ".join(parts)

    @staticmethod
    def format_ecs(entry: LogEntry) -> dict[str, Any]:
        """Format as Elasticsearch Common Schema."""
        return {
            "@timestamp": entry.timestamp,
            "log.level": entry.level.lower(),
            "log.logger": entry.logger,
            "message": entry.message,
            "service.name": entry.logger,
            "trace.id": entry.trace_id,
            "span.id": entry.span_id,
            "user.id": entry.user_id,
            "event.module": entry.module,
            "code.function": entry.function,
            "code.lineno": entry.line_number,
            **entry.metadata,
        }


class LogRouter:
    """Routes logs to multiple destinations."""

    def __init__(self) -> None:
        self._handlers: list[LogHandler] = []
        self._filters: list[LogFilter] = []
        self._level = LogLevel.INFO

    def add_handler(self, handler: "LogHandler") -> "LogRouter":
        """Add a log handler."""
        self._handlers.append(handler)
        return self

    def add_filter(self, filter_obj: "LogFilter") -> "LogRouter":
        """Add a log filter."""
        self._filters.append(filter_obj)
        return self

    def set_level(self, level: LogLevel) -> "LogRouter":
        """Set minimum log level."""
        self._level = level
        return self

    def log(self, entry: LogEntry) -> None:
        """Route a log entry to all handlers."""
        if not self._should_process(entry):
            return

        for handler in self._handlers:
            try:
                handler.emit(entry)
            except Exception as e:
                logger.error("Handler %s failed: %s", handler, e)

    def _should_process(self, entry: LogEntry) -> bool:
        """Check if entry passes all filters and level."""
        try:
            entry_level = LogLevel[entry.level.upper()]
        except KeyError:
            entry_level = LogLevel.INFO

        if entry_level.value < self._level.value:
            return False

        for filter_obj in self._filters:
            if not filter_obj.accepts(entry):
                return False

        return True


class LogHandler:
    """Base class for log handlers."""

    def emit(self, entry: LogEntry) -> None:
        """Emit a log entry. Override in subclass."""
        raise NotImplementedError


class ConsoleHandler(LogHandler):
    """Handler that writes to stdout/stderr."""

    def __init__(self, formatter: Optional[LogFormatter] = None, pretty: bool = False) -> None:
        self.formatter = formatter or LogFormatter()
        self.pretty = pretty

    def emit(self, entry: LogEntry) -> None:
        output = self.formatter.format_text(entry)
        if entry.level in ("ERROR", "CRITICAL"):
            print(output, file=__import__("sys").stderr)
        else:
            print(output)


class FileHandler(LogHandler):
    """Handler that writes to a file."""

    def __init__(self, filepath: str, formatter: Optional[LogFormatter] = None, max_bytes: int = 10 * 1024 * 1024, backup_count: int = 5) -> None:
        self.filepath = filepath
        self.formatter = formatter or LogFormatter()
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self._current_bytes = 0

    def emit(self, entry: LogEntry) -> None:
        line = self.formatter.format_json(entry) + "\n"
        self._current_bytes += len(line.encode())
        with open(self.filepath, "a") as f:
            f.write(line)
        if self._current_bytes >= self.max_bytes:
            self._rotate()

    def _rotate(self) -> None:
        """Rotate log file."""
        import os
        for i in range(self.backup_count - 1, 0, -1):
            src = f"{self.filepath}.{i}"
            dst = f"{self.filepath}.{i + 1}"
            if os.path.exists(src):
                os.rename(src, dst)
        if os.path.exists(self.filepath):
            os.rename(self.filepath, f"{self.filepath}.1")
        self._current_bytes = 0


class SyslogHandler(LogHandler):
    """Handler that sends logs to syslog."""

    def __init__(self, host: str = "localhost", port: int = 514, facility: int = 16) -> None:
        self.host = host
        self.port = port
        self.facility = facility

    def emit(self, entry: LogEntry) -> None:
        import socket
        level_map = {
            "DEBUG": 7, "INFO": 6, "WARNING": 4, "ERROR": 3, "CRITICAL": 2
        }
        severity = level_map.get(entry.level.upper(), 6)
        message = f"<{self.facility * 8 + severity}>{entry.timestamp} {entry.logger}: {entry.message}"
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(message.encode(), (self.host, self.port))
            sock.close()
        except Exception as e:
            logger.error("Syslog emit failed: %s", e)


class LogFilter:
    """Filter for log entries."""

    def accepts(self, entry: LogEntry) -> bool:
        """Check if entry should be accepted."""
        return True


class TraceIdFilter(LogFilter):
    """Filter that adds trace IDs to entries."""

    def __init__(self, trace_id: Optional[str] = None) -> None:
        self.trace_id = trace_id

    def accepts(self, entry: LogEntry) -> bool:
        if self.trace_id:
            entry.trace_id = self.trace_id
        return True
