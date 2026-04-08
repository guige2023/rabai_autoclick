"""Data Logging Action.

Logs data operations with structured logging and log levels.
"""
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from datetime import datetime
import time
import json


@dataclass
class LogEntry:
    timestamp: float
    level: str
    message: str
    entity_type: Optional[str] = None
    entity_id: Optional[str] = None
    operation: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
    user: Optional[str] = None
    duration_ms: Optional[float] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "timestamp_iso": datetime.fromtimestamp(self.timestamp).isoformat(),
            "level": self.level,
            "message": self.message,
            "entity_type": self.entity_type,
            "entity_id": self.entity_id,
            "operation": self.operation,
            "data": self.data,
            "user": self.user,
            "duration_ms": self.duration_ms,
            "error": self.error,
            "metadata": self.metadata,
        }


class DataLoggingAction:
    """Structured logging for data operations."""

    LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

    def __init__(
        self,
        min_level: str = "INFO",
        max_entries: int = 10000,
        entity_types: Optional[List[str]] = None,
    ) -> None:
        self.min_level = min_level
        self.max_entries = max_entries
        self.entity_types = set(entity_types or [])
        self.entries: List[LogEntry] = []
        self._count_by_level: Dict[str, int] = {l: 0 for l in self.LEVELS}

    def _should_log(self, level: str) -> bool:
        level_order = list(self.LEVELS)
        return level_order.index(level) >= level_order.index(self.min_level)

    def log(
        self,
        level: str,
        message: str,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        operation: Optional[str] = None,
        data: Optional[Dict[str, Any]] = None,
        user: Optional[str] = None,
        duration_ms: Optional[float] = None,
        error: Optional[str] = None,
        **metadata,
    ) -> LogEntry:
        entry = LogEntry(
            timestamp=time.time(),
            level=level.upper(),
            message=message,
            entity_type=entity_type,
            entity_id=entity_id,
            operation=operation,
            data=data,
            user=user,
            duration_ms=duration_ms,
            error=error,
            metadata=metadata,
        )
        self.entries.append(entry)
        self._count_by_level[entry.level] = self._count_by_level.get(entry.level, 0) + 1
        if len(self.entries) > self.max_entries:
            self.entries.pop(0)
        return entry

    def debug(self, message: str, **kwargs) -> LogEntry:
        return self.log("DEBUG", message, **kwargs)

    def info(self, message: str, **kwargs) -> LogEntry:
        return self.log("INFO", message, **kwargs)

    def warning(self, message: str, **kwargs) -> LogEntry:
        return self.log("WARNING", message, **kwargs)

    def error(self, message: str, **kwargs) -> LogEntry:
        return self.log("ERROR", message, **kwargs)

    def get_entries(
        self,
        level: Optional[str] = None,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        operation: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 100,
    ) -> List[LogEntry]:
        result = self.entries
        if level:
            result = [e for e in result if e.level == level.upper()]
        if entity_type:
            result = [e for e in result if e.entity_type == entity_type]
        if entity_id:
            result = [e for e in result if e.entity_id == entity_id]
        if operation:
            result = [e for e in result if e.operation == operation]
        if since:
            result = [e for e in result if e.timestamp >= since]
        return result[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        return {
            "total": len(self.entries),
            "by_level": dict(self._count_by_level),
            "min_level": self.min_level,
            "max_entries": self.max_entries,
        }

    def export_json(self, path: str) -> None:
        with open(path, "w") as f:
            json.dump([e.to_dict() for e in self.entries], f, indent=2)
