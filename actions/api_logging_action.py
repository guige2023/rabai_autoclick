"""
API Logging Action - Logs API requests and responses.

This module provides API logging capabilities including
request/response capture, log formatting, and log storage.
"""

from __future__ import annotations

import time
import json
from dataclasses import dataclass, field
from typing import Any, Callable
from enum import Enum


class LogLevel(Enum):
    """Log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class APILogEntry:
    """A single API log entry."""
    timestamp: float
    level: LogLevel
    method: str
    url: str
    status_code: int | None = None
    request_body: Any = None
    response_body: Any = None
    duration_ms: float | None = None
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class APILogger:
    """Logs API requests and responses."""
    
    def __init__(self, max_entries: int = 10000) -> None:
        self.max_entries = max_entries
        self._logs: list[APILogEntry] = []
        self._log_handlers: list[Callable[[APILogEntry], None]] = []
    
    def log_request(
        self,
        method: str,
        url: str,
        request_body: Any = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Log an API request."""
        entry = APILogEntry(
            timestamp=time.time(),
            level=LogLevel.INFO,
            method=method,
            url=url,
            request_body=request_body,
            metadata=metadata or {},
        )
        self._add_entry(entry)
    
    def log_response(
        self,
        method: str,
        url: str,
        status_code: int,
        response_body: Any = None,
        duration_ms: float | None = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """Log an API response."""
        level = LogLevel.INFO if status_code < 400 else LogLevel.ERROR
        
        entry = APILogEntry(
            timestamp=time.time(),
            level=level,
            method=method,
            url=url,
            status_code=status_code,
            response_body=response_body,
            duration_ms=duration_ms,
            error=error,
            metadata=metadata or {},
        )
        self._add_entry(entry)
    
    def _add_entry(self, entry: APILogEntry) -> None:
        """Add log entry."""
        self._logs.append(entry)
        
        if len(self._logs) > self.max_entries:
            self._logs = self._logs[-self.max_entries:]
        
        for handler in self._log_handlers:
            handler(entry)
    
    def add_handler(self, handler: Callable[[APILogEntry], None]) -> None:
        """Add a log handler."""
        self._log_handlers.append(handler)
    
    def get_logs(
        self,
        level: LogLevel | None = None,
        method: str | None = None,
        since: float | None = None,
    ) -> list[APILogEntry]:
        """Get filtered logs."""
        logs = self._logs
        
        if level:
            logs = [l for l in logs if l.level == level]
        
        if method:
            logs = [l for l in logs if l.method == method]
        
        if since:
            logs = [l for l in logs if l.timestamp >= since]
        
        return logs
    
    def clear(self) -> None:
        """Clear all logs."""
        self._logs.clear()


class APILoggingAction:
    """API logging action for automation workflows."""
    
    def __init__(self) -> None:
        self.logger = APILogger()
    
    def log_request(self, method: str, url: str, **kwargs) -> None:
        """Log an API request."""
        self.logger.log_request(method, url, **kwargs)
    
    def log_response(self, method: str, url: str, status_code: int, **kwargs) -> None:
        """Log an API response."""
        self.logger.log_response(method, url, status_code, **kwargs)
    
    def get_logs(self, **kwargs) -> list[APILogEntry]:
        """Get filtered logs."""
        return self.logger.get_logs(**kwargs)
    
    def clear_logs(self) -> None:
        """Clear all logs."""
        self.logger.clear()


__all__ = ["LogLevel", "APILogEntry", "APILogger", "APILoggingAction"]
