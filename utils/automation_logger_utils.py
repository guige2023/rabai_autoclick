"""
Automation logging utilities for debugging and audit trails.

This module provides structured logging capabilities for
automation workflows with levels, categories, and output formats.
"""

from __future__ import annotations

import time
import json
import threading
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Callable
from enum import Enum, auto
from contextlib import contextmanager


class LogLevel(Enum):
    """Log severity levels."""
    DEBUG = 10
    INFO = 20
    WARNING = 30
    ERROR = 40
    CRITICAL = 50


class LogCategory(Enum):
    """Log categories for filtering."""
    GENERAL = auto()
    UI = auto()
    ACCESSIBILITY = auto()
    INPUT = auto()
    ACTION = auto()
    WORKFLOW = auto()
    PERFORMANCE = auto()
    ERROR = auto()


@dataclass
class LogEntry:
    """
    Single log entry.

    Attributes:
        timestamp: When the log was created.
        level: Severity level.
        category: Log category.
        message: Log message.
        details: Additional context data.
        source: Source module/class name.
        thread_id: ID of logging thread.
    """
    timestamp: float = field(default_factory=time.time)
    level: LogLevel = LogLevel.INFO
    category: LogCategory = LogCategory.GENERAL
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    source: str = ""
    thread_id: int = field(default_factory=lambda: threading.get_ident)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp,
            "level": self.level.name,
            "category": self.category.name,
            "message": self.message,
            "details": self.details,
            "source": self.source,
            "thread_id": self.thread_id,
        }

    @property
    def formatted_time(self) -> str:
        """Get formatted timestamp."""
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.timestamp))

    @property
    def formatted_ms_time(self) -> str:
        """Get formatted timestamp with milliseconds."""
        ms = int((self.timestamp % 1) * 1000)
        return f"{self.formatted_time}.{ms:03d}"


class LogHandler(Callable[[LogEntry], None]):
    """
    Base log handler.

    Subclass and implement __call__ to create custom handlers.
    """

    def __init__(self, min_level: LogLevel = LogLevel.DEBUG) -> None:
        self._min_level = min_level

    def __call__(self, entry: LogEntry) -> None:
        """Handle a log entry."""
        if entry.level.value >= self._min_level.value:
            self.emit(entry)

    def emit(self, entry: LogEntry) -> None:
        """Emit a log entry - override in subclass."""
        pass


class ConsoleHandler(LogHandler):
    """Logs to stdout/stderr with color support."""

    def __init__(
        self,
        min_level: LogLevel = LogLevel.DEBUG,
        use_colors: bool = True,
    ) -> None:
        super().__init__(min_level)
        self._use_colors = use_colors

    def emit(self, entry: LogEntry) -> None:
        """Print log entry to console."""
        if self._use_colors:
            color = self._get_color(entry.level)
            prefix = f"\033[{color}m"
            suffix = "\033[0m"
        else:
            prefix = suffix = ""

        details_str = ""
        if entry.details:
            details_str = f" | {json.dumps(entry.details)}"

        print(f"{prefix}[{entry.formatted_ms_time}] {entry.level.name:8} [{entry.category.name}] {entry.message}{details_str}{suffix}")


class FileHandler(LogHandler):
    """Logs to a file in JSON Lines format."""

    def __init__(self, filepath: str, min_level: LogLevel = LogLevel.DEBUG) -> None:
        super().__init__(min_level)
        self._filepath = filepath
        self._lock = threading.Lock()

    def emit(self, entry: LogEntry) -> None:
        """Write log entry to file."""
        with self._lock:
            try:
                with open(self._filepath, "a") as f:
                    f.write(json.dumps(entry.to_dict()) + "\n")
            except Exception:
                pass


class MemoryHandler(LogHandler):
    """Stores log entries in memory for later retrieval."""

    def __init__(
        self,
        max_entries: int = 1000,
        min_level: LogLevel = LogLevel.DEBUG,
    ) -> None:
        super().__init__(min_level)
        self._max_entries = max_entries
        self._entries: List[LogEntry] = []
        self._lock = threading.Lock()

    def emit(self, entry: LogEntry) -> None:
        """Store log entry in memory."""
        with self._lock:
            self._entries.append(entry)
            if len(self._entries) > self._max_entries:
                self._entries.pop(0)

    def get_entries(
        self,
        level: Optional[LogLevel] = None,
        category: Optional[LogCategory] = None,
        since: Optional[float] = None,
    ) -> List[LogEntry]:
        """Get filtered log entries."""
        with self._lock:
            entries = self._entries

            if level:
                entries = [e for e in entries if e.level == level]

            if category:
                entries = [e for e in entries if e.category == category]

            if since:
                entries = [e for e in entries if e.timestamp >= since]

            return entries

    def clear(self) -> None:
        """Clear all stored entries."""
        with self._lock:
            self._entries.clear()


class AutomationLogger:
    """
    Structured logger for automation workflows.

    Supports multiple handlers, categories, and levels.
    """

    def __init__(self, name: str = "automation") -> None:
        self._name = name
        self._handlers: List[LogHandler] = []
        self._min_level = LogLevel.DEBUG
        self._categories_enabled: Dict[LogCategory, bool] = {
            cat: True for cat in LogCategory
        }
        self._lock = threading.Lock()

    def add_handler(self, handler: LogHandler) -> AutomationLogger:
        """Add a log handler."""
        with self._lock:
            self._handlers.append(handler)
        return self

    def remove_handler(self, handler: LogHandler) -> bool:
        """Remove a log handler."""
        with self._lock:
            if handler in self._handlers:
                self._handlers.remove(handler)
                return True
            return False

    def set_min_level(self, level: LogLevel) -> AutomationLogger:
        """Set minimum log level."""
        self._min_level = level
        return self

    def enable_category(self, category: LogCategory) -> AutomationLogger:
        """Enable a log category."""
        self._categories_enabled[category] = True
        return self

    def disable_category(self, category: LogCategory) -> AutomationLogger:
        """Disable a log category."""
        self._categories_enabled[category] = False
        return self

    def log(
        self,
        message: str,
        level: LogLevel = LogLevel.INFO,
        category: LogCategory = LogCategory.GENERAL,
        details: Optional[Dict[str, Any]] = None,
        source: str = "",
    ) -> None:
        """Log a message."""
        if level.value < self._min_level.value:
            return

        if not self._categories_enabled.get(category, True):
            return

        entry = LogEntry(
            level=level,
            category=category,
            message=message,
            details=details or {},
            source=source or self._name,
        )

        with self._lock:
            for handler in self._handlers:
                handler(entry)

    def debug(self, message: str, **kwargs: Any) -> None:
        """Log debug message."""
        self.log(message, LogLevel.DEBUG, **kwargs)

    def info(self, message: str, **kwargs: Any) -> None:
        """Log info message."""
        self.log(message, LogLevel.INFO, **kwargs)

    def warning(self, message: str, **kwargs: Any) -> None:
        """Log warning message."""
        self.log(message, LogLevel.WARNING, **kwargs)

    def error(self, message: str, **kwargs: Any) -> None:
        """Log error message."""
        self.log(message, LogLevel.ERROR, **kwargs)

    def critical(self, message: str, **kwargs: Any) -> None:
        """Log critical message."""
        self.log(message, LogLevel.CRITICAL, **kwargs)

    @contextmanager
    def measure(self, operation: str, category: LogCategory = LogCategory.PERFORMANCE) -> Any:
        """Context manager to measure and log operation duration."""
        start_time = time.time()
        result = None
        error = None

        try:
            yield result
        except Exception as e:
            error = e
            raise
        finally:
            duration = time.time() - start_time
            level = LogLevel.WARNING if error else LogLevel.INFO
            self.log(
                f"{operation} completed in {duration:.3f}s",
                level=level,
                category=category,
                details={"duration": duration, "error": str(error) if error else None},
            )


# Global default logger
_default_logger: Optional[AutomationLogger] = None


def get_logger(name: str = "automation") -> AutomationLogger:
    """Get or create the default logger."""
    global _default_logger
    if _default_logger is None:
        _default_logger = AutomationLogger(name)
        _default_logger.add_handler(ConsoleHandler())
    return _default_logger
