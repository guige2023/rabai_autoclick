"""API Logger Action Module.

Provides structured API logging with request/response capture,
log levels, and export capabilities.
"""
from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    """Log level."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class APILogEntry:
    """API log entry."""
    timestamp: float
    level: LogLevel
    method: str
    endpoint: str
    request_body: Optional[Any] = None
    request_headers: Optional[Dict] = None
    response_body: Optional[Any] = None
    response_status: Optional[int] = None
    latency_ms: Optional[float] = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class APILoggerAction:
    """Structured API logger.

    Example:
        logger = APILoggerAction()

        logger.log_request("GET", "/api/users", {"id": 1})
        logger.log_response(200, {"user": "Alice"}, latency_ms=45.0)

        logs = logger.get_recent_logs()
    """

    def __init__(
        self,
        max_entries: int = 10000,
        log_file: Optional[str] = None,
        level: LogLevel = LogLevel.INFO,
    ) -> None:
        self.max_entries = max_entries
        self.log_file = log_file
        self.level = level
        self._logs: deque = deque(maxlen=max_entries)
        self._current_request: Optional[APILogEntry] = None
        self._filters: List[Callable] = []

    def log_request(
        self,
        method: str,
        endpoint: str,
        body: Optional[Any] = None,
        headers: Optional[Dict] = None,
        metadata: Optional[Dict] = None,
    ) -> None:
        """Log API request.

        Args:
            method: HTTP method
            endpoint: API endpoint
            body: Request body
            headers: Request headers
            metadata: Additional metadata
        """
        self._current_request = APILogEntry(
            timestamp=time.time(),
            level=LogLevel.INFO,
            method=method,
            endpoint=endpoint,
            request_body=body,
            request_headers=headers,
            metadata=metadata or {},
        )

    def log_response(
        self,
        status: int,
        body: Optional[Any] = None,
        latency_ms: Optional[float] = None,
        error: Optional[str] = None,
        level: Optional[LogLevel] = None,
    ) -> None:
        """Log API response.

        Args:
            status: Response status code
            body: Response body
            latency_ms: Request latency
            error: Error message
            level: Log level override
        """
        if not self._current_request:
            logger.warning("Response logged without request")
            return

        self._current_request.response_status = status
        self._current_request.response_body = body
        self._current_request.latency_ms = latency_ms
        self._current_request.error = error

        if status >= 500:
            self._current_request.level = LogLevel.ERROR
        elif status >= 400:
            self._current_request.level = LogLevel.WARNING
        elif error:
            self._current_request.level = LogLevel.ERROR

        if level:
            self._current_request.level = level

        if self._should_log(self._current_request):
            self._logs.append(self._current_request)
            self._write_to_file(self._current_request)

        self._current_request = None

    def log_error(
        self,
        method: str,
        endpoint: str,
        error: str,
        metadata: Optional[Dict] = None,
    ) -> None:
        """Log error directly.

        Args:
            method: HTTP method
            endpoint: API endpoint
            error: Error message
            metadata: Additional metadata
        """
        entry = APILogEntry(
            timestamp=time.time(),
            level=LogLevel.ERROR,
            method=method,
            endpoint=endpoint,
            error=error,
            metadata=metadata or {},
        )

        if self._should_log(entry):
            self._logs.append(entry)
            self._write_to_file(entry)

    def add_filter(self, filter_fn: Callable[[APILogEntry], bool]) -> None:
        """Add log filter function."""
        self._filters.append(filter_fn)

    def _should_log(self, entry: APILogEntry) -> bool:
        """Check if entry should be logged."""
        if self._filters:
            return all(f(entry) for f in self._filters)

        level_values = {
            LogLevel.DEBUG: 0,
            LogLevel.INFO: 1,
            LogLevel.WARNING: 2,
            LogLevel.ERROR: 3,
            LogLevel.CRITICAL: 4,
        }

        return level_values.get(entry.level, 0) >= level_values.get(self.level, 0)

    def _write_to_file(self, entry: APILogEntry) -> None:
        """Write entry to log file."""
        if not self.log_file:
            return

        try:
            line = json.dumps({
                "timestamp": datetime.fromtimestamp(entry.timestamp).isoformat(),
                "level": entry.level.value,
                "method": entry.method,
                "endpoint": entry.endpoint,
                "status": entry.response_status,
                "latency_ms": entry.latency_ms,
                "error": entry.error,
            })
            Path(self.log_file).open("a").write(line + "\n")
        except Exception as e:
            logger.error(f"Failed to write log: {e}")

    def get_recent_logs(
        self,
        count: int = 100,
        level: Optional[LogLevel] = None,
    ) -> List[APILogEntry]:
        """Get recent log entries.

        Args:
            count: Number of entries
            level: Filter by level

        Returns:
            List of log entries
        """
        logs = list(self._logs)

        if level:
            logs = [l for l in logs if l.level == level]

        return logs[-count:]

    def get_stats(self) -> Dict[str, Any]:
        """Get log statistics."""
        if not self._logs:
            return {"total": 0}

        logs = list(self._logs)
        levels = {}
        methods = {}
        endpoints = {}

        for entry in logs:
            level_name = entry.level.value
            levels[level_name] = levels.get(level_name, 0) + 1
            methods[entry.method] = methods.get(entry.method, 0) + 1
            endpoints[entry.endpoint] = endpoints.get(entry.endpoint, 0) + 1

        return {
            "total": len(logs),
            "by_level": levels,
            "by_method": methods,
            "top_endpoints": sorted(
                endpoints.items(),
                key=lambda x: x[1],
                reverse=True
            )[:10],
        }

    def clear(self) -> None:
        """Clear all logs."""
        self._logs.clear()
