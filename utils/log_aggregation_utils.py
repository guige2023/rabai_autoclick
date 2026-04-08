"""Log aggregation utilities for centralized logging."""

from typing import Optional, Dict, Any, List
from enum import Enum
import time
import threading
import json


class LogLevel(Enum):
    """Log levels."""
    TRACE = 0
    DEBUG = 1
    INFO = 2
    WARNING = 3
    ERROR = 4
    FATAL = 5


class LogEntry:
    """Single log entry."""

    def __init__(
        self,
        level: LogLevel,
        message: str,
        logger: str = "root",
        context: Optional[Dict[str, Any]] = None
    ):
        """Initialize log entry."""
        self.timestamp = time.time()
        self.level = level
        self.message = message
        self.logger = logger
        self.context = context or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp,
            "level": self.level.name,
            "logger": self.logger,
            "message": self.message,
            "context": self.context,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())


class LogAggregator:
    """Centralized log aggregator with filtering."""

    def __init__(self, max_entries: int = 10000):
        """Initialize log aggregator.
        
        Args:
            max_entries: Maximum entries to retain.
        """
        self.max_entries = max_entries
        self._entries: List[LogEntry] = []
        self._handlers: List[callable] = []
        self._min_level = LogLevel.DEBUG
        self._lock = threading.RLock()
        self._filters: Dict[str, LogLevel] = {}

    def add_handler(self, handler: callable) -> None:
        """Add a log handler."""
        with self._lock:
            self._handlers.append(handler)

    def remove_handler(self, handler: callable) -> bool:
        """Remove a log handler."""
        with self._lock:
            for h in self._handlers:
                if h == handler:
                    self._handlers.remove(h)
                    return True
            return False

    def set_min_level(self, level: LogLevel) -> None:
        """Set minimum log level."""
        self._min_level = level

    def add_filter(self, logger: str, level: LogLevel) -> None:
        """Add filter for specific logger."""
        self._filters[logger] = level

    def log(
        self,
        level: LogLevel,
        message: str,
        logger: str = "root",
        context: Optional[Dict[str, Any]] = None
    ) -> None:
        """Log an entry.
        
        Args:
            level: Log level.
            message: Log message.
            logger: Logger name.
            context: Additional context.
        """
        min_level = self._filters.get(logger, self._min_level)
        if level.value < min_level.value:
            return
        entry = LogEntry(level, message, logger, context)
        with self._lock:
            self._entries.append(entry)
            if len(self._entries) > self.max_entries:
                self._entries = self._entries[-self.max_entries:]
            handlers = list(self._handlers)
        for handler in handlers:
            try:
                handler(entry)
            except Exception:
                pass

    def get_entries(
        self,
        level: Optional[LogLevel] = None,
        logger: Optional[str] = None,
        limit: int = 100
    ) -> List[LogEntry]:
        """Get log entries with optional filtering.
        
        Args:
            level: Filter by level.
            logger: Filter by logger name.
            limit: Maximum entries to return.
        
        Returns:
            List of log entries.
        """
        with self._lock:
            entries = list(self._entries)
        if level is not None:
            entries = [e for e in entries if e.level == level]
        if logger is not None:
            entries = [e for e in entries if e.logger == logger]
        return entries[-limit:]

    def clear(self) -> None:
        """Clear all entries."""
        with self._lock:
            self._entries.clear()

    def get_stats(self) -> Dict[str, Any]:
        """Get log statistics."""
        with self._lock:
            entries = list(self._entries)
        level_counts = {}
        for e in entries:
            level_counts[e.level.name] = level_counts.get(e.level.name, 0) + 1
        return {
            "total": len(entries),
            "level_counts": level_counts,
            "oldest": entries[0].timestamp if entries else None,
            "newest": entries[-1].timestamp if entries else None,
        }


def console_handler(entry: LogEntry) -> None:
    """Simple console log handler."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(entry.timestamp))
    print(f"[{ts}] {entry.level.name:7s} [{entry.logger}] {entry.message}")
