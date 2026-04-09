"""Log aggregator action for collecting and analyzing logs.

Aggregates logs from multiple sources with filtering,
search, and real-time streaming support.
"""

import logging
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Iterator, Optional

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogEntry:
    timestamp: float
    level: LogLevel
    message: str
    source: str
    metadata: dict[str, Any] = field(default_factory=dict)
    line_number: Optional[int] = None


@dataclass
class LogQuery:
    pattern: Optional[str] = None
    level_filter: Optional[list[LogLevel]] = None
    source_filter: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    limit: int = 1000


class LogAggregatorAction:
    """Aggregate and search logs from multiple sources.

    Args:
        max_entries: Maximum log entries to retain in memory.
        auto_flush_count: Flush to storage after this many entries.
    """

    def __init__(
        self,
        max_entries: int = 10000,
        auto_flush_count: int = 1000,
    ) -> None:
        self._entries: list[LogEntry] = []
        self._max_entries = max_entries
        self._auto_flush_count = auto_flush_count
        self._sources: set[str] = set()
        self._stream_handlers: list[Callable[[LogEntry], None]] = []
        self._compiled_patterns: dict[str, re.Pattern] = {}

    def add_entry(
        self,
        level: LogLevel,
        message: str,
        source: str,
        metadata: Optional[dict[str, Any]] = None,
        timestamp: Optional[float] = None,
        line_number: Optional[int] = None,
    ) -> LogEntry:
        """Add a log entry.

        Args:
            level: Log level.
            message: Log message.
            source: Log source identifier.
            metadata: Optional additional metadata.
            timestamp: Optional timestamp (uses current time if not provided).
            line_number: Optional source line number.

        Returns:
            The created log entry.
        """
        entry = LogEntry(
            timestamp=timestamp or time.time(),
            level=level,
            message=message,
            source=source,
            metadata=metadata or {},
            line_number=line_number,
        )
        self._entries.append(entry)
        self._sources.add(source)

        if len(self._entries) > self._max_entries:
            self._entries.pop(0)

        for handler in self._stream_handlers:
            try:
                handler(entry)
            except Exception as e:
                logger.error(f"Stream handler error: {e}")

        return entry

    def log_debug(self, message: str, source: str, **metadata: Any) -> LogEntry:
        """Add a DEBUG level log entry."""
        return self.add_entry(LogLevel.DEBUG, message, source, metadata)

    def log_info(self, message: str, source: str, **metadata: Any) -> LogEntry:
        """Add an INFO level log entry."""
        return self.add_entry(LogLevel.INFO, message, source, metadata)

    def log_warning(self, message: str, source: str, **metadata: Any) -> LogEntry:
        """Add a WARNING level log entry."""
        return self.add_entry(LogLevel.WARNING, message, source, metadata)

    def log_error(self, message: str, source: str, **metadata: Any) -> LogEntry:
        """Add an ERROR level log entry."""
        return self.add_entry(LogLevel.ERROR, message, source, metadata)

    def log_critical(self, message: str, source: str, **metadata: Any) -> LogEntry:
        """Add a CRITICAL level log entry."""
        return self.add_entry(LogLevel.CRITICAL, message, source, metadata)

    def register_stream_handler(self, handler: Callable[[LogEntry], None]) -> None:
        """Register a handler for real-time log streaming.

        Args:
            handler: Callback function for new log entries.
        """
        self._stream_handlers.append(handler)

    def unregister_stream_handler(self, handler: Callable[[LogEntry], None]) -> bool:
        """Unregister a stream handler.

        Args:
            handler: Handler to remove.

        Returns:
            True if handler was found and removed.
        """
        try:
            self._stream_handlers.remove(handler)
            return True
        except ValueError:
            return False

    def query(self, query: LogQuery) -> list[LogEntry]:
        """Query log entries with filters.

        Args:
            query: Query parameters.

        Returns:
            Matching log entries (newest first).
        """
        results = self._entries

        if query.source_filter:
            results = [e for e in results if query.source_filter in e.source]

        if query.level_filter:
            results = [e for e in results if e.level in query.level_filter]

        if query.start_time:
            results = [e for e in results if e.timestamp >= query.start_time]

        if query.end_time:
            results = [e for e in results if e.timestamp <= query.end_time]

        if query.pattern:
            pattern = self._get_compiled_pattern(query.pattern)
            results = [e for e in results if pattern.search(e.message)]

        return sorted(results, key=lambda e: e.timestamp, reverse=True)[:query.limit]

    def _get_compiled_pattern(self, pattern: str) -> re.Pattern:
        """Get or create a compiled regex pattern.

        Args:
            pattern: Regex pattern string.

        Returns:
            Compiled regex pattern.
        """
        if pattern not in self._compiled_patterns:
            self._compiled_patterns[pattern] = re.compile(pattern, re.IGNORECASE)
        return self._compiled_patterns[pattern]

    def search(self, text: str, case_sensitive: bool = False) -> list[LogEntry]:
        """Search logs by text content.

        Args:
            text: Text to search for.
            case_sensitive: Whether search is case sensitive.

        Returns:
            Matching entries (newest first).
        """
        if case_sensitive:
            pattern = text
        else:
            text = text.lower()
            pattern = text
        return [
            e for e in sorted(self._entries, key=lambda x: x.timestamp, reverse=True)
            if (text in e.message if case_sensitive else text in e.message.lower())
        ][:1000]

    def get_entries_for_source(self, source: str, limit: int = 100) -> list[LogEntry]:
        """Get recent entries for a specific source.

        Args:
            source: Source identifier.
            limit: Maximum entries to return.

        Returns:
            Entries for the source (newest first).
        """
        return [
            e for e in self._entries if e.source == source
        ][-limit:][::-1]

    def get_stats(self) -> dict[str, Any]:
        """Get log statistics.

        Returns:
            Dictionary with log stats.
        """
        by_level: dict[str, int] = {}
        for entry in self._entries:
            level_name = entry.level.value
            by_level[level_name] = by_level.get(level_name, 0) + 1

        return {
            "total_entries": len(self._entries),
            "max_entries": self._max_entries,
            "unique_sources": len(self._sources),
            "by_level": by_level,
            "oldest_timestamp": self._entries[0].timestamp if self._entries else None,
            "newest_timestamp": self._entries[-1].timestamp if self._entries else None,
        }

    def clear(self) -> int:
        """Clear all log entries.

        Returns:
            Number of entries cleared.
        """
        count = len(self._entries)
        self._entries.clear()
        self._sources.clear()
        return count

    def stream(self, query: Optional[LogQuery] = None) -> Iterator[LogEntry]:
        """Stream log entries in real-time.

        Args:
            query: Optional query to filter streamed entries.

        Yields:
            Log entries as they arrive.
        """
        seen_count = len(self._entries)
        while True:
            current_entries = self._entries
            if len(current_entries) > seen_count:
                new_entries = current_entries[seen_count:]
                for entry in new_entries:
                    if query:
                        if query.level_filter and entry.level not in query.level_filter:
                            continue
                        if query.source_filter and query.source_filter not in entry.source:
                            continue
                    yield entry
                seen_count = len(current_entries)
            time.sleep(0.1)
