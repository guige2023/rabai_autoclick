"""Structured logging utilities for RabAI AutoClick.

Provides:
- Structured log messages
- Log levels
- Log formatting
"""

import json
import logging
import os
import sys
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class LogLevel(Enum):
    """Log levels."""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


@dataclass
class LogRecord:
    """A structured log record."""
    timestamp: float
    level: LogLevel
    message: str
    logger: str
    fields: Dict[str, Any] = field(default_factory=dict)
    exception: Optional[str] = None


class StructuredFormatter:
    """Format log records."""

    def __init__(self, include_fields: bool = True) -> None:
        """Initialize formatter.

        Args:
            include_fields: Include extra fields in output.
        """
        self._include_fields = include_fields

    def format(self, record: LogRecord) -> str:
        """Format log record.

        Args:
            record: Log record.

        Returns:
            Formatted string.
        """
        parts = [
            f"[{self._format_time(record.timestamp)}]",
            f"[{record.level.name}]",
            f"[{record.logger}]",
            record.message,
        ]

        if record.fields and self._include_fields:
            parts.append(json.dumps(record.fields))

        if record.exception:
            parts.append(record.exception)

        return " ".join(parts)

    def _format_time(self, timestamp: float) -> str:
        """Format timestamp.

        Args:
            timestamp: Unix timestamp.

        Returns:
            Formatted time string.
        """
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(timestamp))


class JSONFormatter:
    """Format log records as JSON."""

    def format(self, record: LogRecord) -> str:
        """Format log record as JSON.

        Args:
            record: Log record.

        Returns:
            JSON string.
        """
        data = {
            "timestamp": record.timestamp,
            "level": record.level.name,
            "logger": record.logger,
            "message": record.message,
            "fields": record.fields,
        }
        if record.exception:
            data["exception"] = record.exception
        return json.dumps(data)


class LogHandler:
    """Handle log records."""

    def __init__(self) -> None:
        """Initialize handler."""
        self._filters: List[callable] = []

    def handle(self, record: LogRecord) -> None:
        """Handle a log record.

        Args:
            record: Log record to handle.
        """
        raise NotImplementedError

    def add_filter(self, filter_func: callable) -> None:
        """Add a filter function.

        Args:
            filter_func: Function that returns True if should handle.
        """
        self._filters.append(filter_func)

    def _should_handle(self, record: LogRecord) -> bool:
        """Check if should handle record.

        Args:
            record: Log record.

        Returns:
            True if should handle.
        """
        for filter_func in self._filters:
            if not filter_func(record):
                return False
        return True


class ConsoleHandler(LogHandler):
    """Write logs to console."""

    def __init__(self, formatter: Optional[StructuredFormatter] = None) -> None:
        """Initialize console handler.

        Args:
            formatter: Log formatter to use.
        """
        super().__init__()
        self._formatter = formatter or StructuredFormatter()

    def handle(self, record: LogRecord) -> None:
        """Handle log record.

        Args:
            record: Log record.
        """
        if not self._should_handle(record):
            return

        formatted = self._formatter.format(record)
        print(formatted, file=sys.stderr)


class FileHandler(LogHandler):
    """Write logs to file."""

    def __init__(
        self,
        path: str,
        formatter: Optional[StructuredFormatter] = None,
        max_size: int = 10 * 1024 * 1024,
        backup_count: int = 5,
    ) -> None:
        """Initialize file handler.

        Args:
            path: Log file path.
            formatter: Log formatter to use.
            max_size: Maximum file size before rotation.
            backup_count: Number of backup files to keep.
        """
        super().__init__()
        self._path = path
        self._formatter = formatter or StructuredFormatter()
        self._max_size = max_size
        self._backup_count = backup_count
        self._lock = threading.Lock()

    def handle(self, record: LogRecord) -> None:
        """Handle log record.

        Args:
            record: Log record.
        """
        if not self._should_handle(record):
            return

        with self._lock:
            try:
                # Check file size
                if os.path.exists(self._path):
                    size = os.path.getsize(self._path)
                    if size >= self._max_size:
                        self._rotate()

                formatted = self._formatter.format(record) + "\n"
                with open(self._path, "a") as f:
                    f.write(formatted)
            except Exception:
                pass

    def _rotate(self) -> None:
        """Rotate log files."""
        import glob

        # Remove oldest backup
        oldest = f"{self._path}.{self._backup_count}"
        if os.path.exists(oldest):
            os.remove(oldest)

        # Rotate existing backups
        for i in range(self._backup_count - 1, 0, -1):
            src = f"{self._path}.{i}"
            dst = f"{self._path}.{i + 1}"
            if os.path.exists(src):
                os.rename(src, dst)

        # Rename current to .1
        os.rename(self._path, f"{self._path}.1")


class Logger:
    """Structured logger."""

    def __init__(self, name: str) -> None:
        """Initialize logger.

        Args:
            name: Logger name.
        """
        self._name = name
        self._handlers: List[LogHandler] = []
        self._level = LogLevel.DEBUG

    def add_handler(self, handler: LogHandler) -> None:
        """Add log handler.

        Args:
            handler: Handler to add.
        """
        self._handlers.append(handler)

    def set_level(self, level: LogLevel) -> None:
        """Set minimum log level.

        Args:
            level: Minimum level to log.
        """
        self._level = level

    def _log(
        self,
        level: LogLevel,
        message: str,
        fields: Optional[Dict[str, Any]] = None,
        exception: Optional[str] = None,
    ) -> None:
        """Log a message.

        Args:
            level: Log level.
            message: Log message.
            fields: Extra fields.
            exception: Exception info.
        """
        if level.value < self._level.value:
            return

        record = LogRecord(
            timestamp=time.time(),
            level=level,
            message=message,
            logger=self._name,
            fields=fields or {},
            exception=exception,
        )

        for handler in self._handlers:
            try:
                handler.handle(record)
            except Exception:
                pass

    def debug(self, message: str, **fields: Any) -> None:
        """Log debug message."""
        self._log(LogLevel.DEBUG, message, fields)

    def info(self, message: str, **fields: Any) -> None:
        """Log info message."""
        self._log(LogLevel.INFO, message, fields)

    def warning(self, message: str, **fields: Any) -> None:
        """Log warning message."""
        self._log(LogLevel.WARNING, message, fields)

    def error(self, message: str, **fields: Any) -> None:
        """Log error message."""
        self._log(LogLevel.ERROR, message, fields)

    def critical(self, message: str, **fields: Any) -> None:
        """Log critical message."""
        self._log(LogLevel.CRITICAL, message, fields)


class LogManager:
    """Manage multiple loggers."""

    def __init__(self) -> None:
        """Initialize manager."""
        self._loggers: Dict[str, Logger] = {}
        self._handlers: List[LogHandler] = []

    def get_logger(self, name: str) -> Logger:
        """Get or create logger.

        Args:
            name: Logger name.

        Returns:
            Logger instance.
        """
        if name not in self._loggers:
            logger = Logger(name)
            for handler in self._handlers:
                logger.add_handler(handler)
            self._loggers[name] = logger
        return self._loggers[name]

    def add_handler(self, handler: LogHandler) -> None:
        """Add handler to all loggers.

        Args:
            handler: Handler to add.
        """
        self._handlers.append(handler)
        for logger in self._loggers.values():
            logger.add_handler(handler)

    def set_level_all(self, level: LogLevel) -> None:
        """Set level for all loggers.

        Args:
            level: Minimum level.
        """
        for logger in self._loggers.values():
            logger.set_level(level)


# Global log manager
_log_manager = LogManager()


def get_logger(name: str) -> Logger:
    """Get a logger.

    Args:
        name: Logger name.

    Returns:
        Logger instance.
    """
    return _log_manager.get_logger(name)


def add_handler(handler: LogHandler) -> None:
    """Add handler to global manager.

    Args:
        handler: Handler to add.
    """
    _log_manager.add_handler(handler)


def set_level_all(level: LogLevel) -> None:
    """Set level for all loggers.

    Args:
        level: Minimum level.
    """
    _log_manager.set_level_all(level)
