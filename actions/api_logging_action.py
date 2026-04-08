"""
API Logging Action Module.

Provides structured API logging with
request/response tracking and log levels.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import json
import logging
import time

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    """Log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class APILogEntry:
    """API log entry."""
    log_id: str
    timestamp: datetime
    level: LogLevel
    method: str
    path: str
    status_code: Optional[int] = None
    duration_ms: Optional[float] = None
    request_size: Optional[int] = None
    response_size: Optional[int] = None
    user_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LogFilter:
    """Log filter configuration."""
    min_level: LogLevel = LogLevel.INFO
    methods: Optional[Set[str]] = None
    paths: Optional[List[str]] = None
    status_codes: Optional[Set[int]] = None


class LogFormatter:
    """Formats log entries."""

    def format_json(self, entry: APILogEntry) -> str:
        """Format entry as JSON."""
        return json.dumps({
            "log_id": entry.log_id,
            "timestamp": entry.timestamp.isoformat(),
            "level": entry.level.value,
            "method": entry.method,
            "path": entry.path,
            "status_code": entry.status_code,
            "duration_ms": entry.duration_ms,
            "request_size": entry.request_size,
            "response_size": entry.response_size,
            "user_id": entry.user_id,
            "ip_address": entry.ip_address,
            "metadata": entry.metadata
        }, default=str)

    def format_text(self, entry: APILogEntry) -> str:
        """Format entry as text."""
        parts = [
            f"[{entry.timestamp.isoformat()}]",
            f"[{entry.level.value.upper()}]",
            f"{entry.method} {entry.path}"
        ]

        if entry.status_code:
            parts.append(f"-> {entry.status_code}")

        if entry.duration_ms:
            parts.append(f"({entry.duration_ms:.2f}ms)")

        if entry.user_id:
            parts.append(f"[user={entry.user_id}]")

        return " ".join(parts)


class LogBuffer:
    """Buffers log entries for batch processing."""

    def __init__(self, max_size: int = 1000, flush_interval: float = 5.0):
        self.max_size = max_size
        self.flush_interval = flush_interval
        self.buffer: List[APILogEntry] = []
        self._lock = asyncio.Lock()

    async def add(self, entry: APILogEntry):
        """Add entry to buffer."""
        async with self._lock:
            self.buffer.append(entry)

            if len(self.buffer) >= self.max_size:
                await self.flush()

    async def flush(self) -> List[APILogEntry]:
        """Flush buffer and return entries."""
        async with self._lock:
            entries = self.buffer.copy()
            self.buffer.clear()
            return entries


class APILogger:
    """Main API logger."""

    def __init__(self, filter_config: Optional[LogFilter] = None):
        self.filter = filter_config or LogFilter()
        self.formatter = LogFormatter()
        self.buffer = LogBuffer()
        self.handlers: List[Callable] = []
        self.entries: List[APILogEntry] = []

    def add_handler(self, handler: Callable):
        """Add log handler."""
        self.handlers.append(handler)

    def log(
        self,
        method: str,
        path: str,
        level: LogLevel = LogLevel.INFO,
        **kwargs
    ) -> APILogEntry:
        """Create and process log entry."""
        if not self._should_log(method, path, level):
            return None

        import uuid
        entry = APILogEntry(
            log_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            level=level,
            method=method,
            path=path,
            **kwargs
        )

        self.entries.append(entry)

        for handler in self.handlers:
            try:
                handler(entry)
            except Exception as e:
                logger.error(f"Log handler error: {e}")

        return entry

    def _should_log(self, method: str, path: str, level: LogLevel) -> bool:
        """Check if entry should be logged."""
        level_order = [
            LogLevel.DEBUG,
            LogLevel.INFO,
            LogLevel.WARNING,
            LogLevel.ERROR,
            LogLevel.CRITICAL
        ]

        if level_order.index(level) < level_order.index(self.filter.min_level):
            return False

        if self.filter.methods and method not in self.filter.methods:
            return False

        if self.filter.paths:
            path_matches = any(p in path for p in self.filter.paths)
            if not path_matches:
                return False

        return True

    def log_request(
        self,
        method: str,
        path: str,
        request_size: int = 0,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None
    ) -> APILogEntry:
        """Log incoming request."""
        return self.log(
            method=method,
            path=path,
            level=LogLevel.INFO,
            request_size=request_size,
            user_id=user_id,
            ip_address=ip_address
        )

    def log_response(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        response_size: int = 0,
        user_id: Optional[str] = None
    ) -> APILogEntry:
        """Log outgoing response."""
        if status_code >= 500:
            level = LogLevel.ERROR
        elif status_code >= 400:
            level = LogLevel.WARNING
        else:
            level = LogLevel.INFO

        return self.log(
            method=method,
            path=path,
            level=level,
            status_code=status_code,
            duration_ms=duration_ms,
            response_size=response_size,
            user_id=user_id
        )

    def get_entries(
        self,
        level: Optional[LogLevel] = None,
        method: Optional[str] = None,
        path: Optional[str] = None,
        limit: int = 100
    ) -> List[APILogEntry]:
        """Get log entries with filters."""
        entries = self.entries

        if level:
            entries = [e for e in entries if e.level == level]

        if method:
            entries = [e for e in entries if e.method == method]

        if path:
            entries = [e for e in entries if path in e.path]

        return entries[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Get log statistics."""
        if not self.entries:
            return {}

        level_counts: Dict[str, int] = {}
        method_counts: Dict[str, int] = {}
        status_counts: Dict[int, int] = {}

        total_duration = 0.0
        count_with_duration = 0

        for entry in self.entries:
            level_counts[entry.level.value] = level_counts.get(entry.level.value, 0) + 1
            method_counts[entry.method] = method_counts.get(entry.method, 0) + 1

            if entry.status_code:
                status_counts[entry.status_code] = status_counts.get(entry.status_code, 0) + 1

            if entry.duration_ms:
                total_duration += entry.duration_ms
                count_with_duration += 1

        return {
            "total_entries": len(self.entries),
            "level_counts": level_counts,
            "method_counts": method_counts,
            "status_counts": status_counts,
            "avg_duration_ms": total_duration / count_with_duration if count_with_duration > 0 else 0
        }


class RequestLogger:
    """Middleware-style request logger."""

    def __init__(self, logger: APILogger):
        self.logger = logger

    async def log_request_start(
        self,
        method: str,
        path: str,
        request_size: int = 0,
        user_id: Optional[str] = None,
        ip_address: Optional[str] = None
    ) -> str:
        """Start request logging."""
        import uuid
        request_id = str(uuid.uuid4())

        self.logger.log_request(
            method=method,
            path=path,
            request_size=request_size,
            user_id=user_id,
            ip_address=ip_address
        )

        return request_id

    async def log_request_end(
        self,
        method: str,
        path: str,
        status_code: int,
        duration_ms: float,
        response_size: int = 0,
        user_id: Optional[str] = None
    ):
        """Log request completion."""
        self.logger.log_response(
            method=method,
            path=path,
            status_code=status_code,
            duration_ms=duration_ms,
            response_size=response_size,
            user_id=user_id
        )


def main():
    """Demonstrate API logging."""
    api_logger = APILogger()

    api_logger.log_request("GET", "/api/users")
    api_logger.log_response("GET", "/api/users", 200, 45.2)

    api_logger.log_request("POST", "/api/users", request_size=500)
    api_logger.log_response("POST", "/api/users", 201, 120.5, response_size=100)

    stats = api_logger.get_stats()
    print(f"Total entries: {stats['total_entries']}")
    print(f"Avg duration: {stats.get('avg_duration_ms', 0):.2f}ms")


if __name__ == "__main__":
    main()
