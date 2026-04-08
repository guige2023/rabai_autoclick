# Copyright (c) 2024. coded by claude
"""API Logging Action Module.

Provides comprehensive API request/response logging with support for
log levels, formatting, and external log aggregation.
"""
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import logging
import json
import asyncio

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class APILogEntry:
    timestamp: datetime
    level: LogLevel
    request_id: str
    method: str
    path: str
    status_code: Optional[int]
    duration_ms: Optional[float]
    request_body: Optional[Any] = None
    response_body: Optional[Any] = None
    headers: Optional[Dict[str, str]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LoggingConfig:
    level: LogLevel = LogLevel.INFO
    include_request_body: bool = True
    include_response_body: bool = False
    max_body_size: int = 1024
    log_to_console: bool = True
    log_to_file: bool = False
    file_path: Optional[str] = None


class APILogging:
    def __init__(self, config: Optional[LoggingConfig] = None):
        self.config = config or LoggingConfig()
        self._logs: List[APILogEntry] = []
        self._handlers: List[callable] = []
        self._log_lock = asyncio.Lock()

    def add_handler(self, handler: callable) -> None:
        self._handlers.append(handler)

    async def log_request(
        self,
        request_id: str,
        method: str,
        path: str,
        headers: Optional[Dict[str, str]] = None,
        body: Optional[Any] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        entry = APILogEntry(
            timestamp=datetime.now(),
            level=LogLevel.INFO,
            request_id=request_id,
            method=method,
            path=path,
            status_code=None,
            duration_ms=None,
            request_body=self._truncate_body(body) if self.config.include_request_body else None,
            headers=self._sanitize_headers(headers) if headers else None,
            metadata=metadata or {},
        )
        await self._store_log(entry)

    async def log_response(
        self,
        request_id: str,
        status_code: int,
        duration_ms: float,
        response_body: Optional[Any] = None,
    ) -> None:
        entry = APILogEntry(
            timestamp=datetime.now(),
            level=self._get_level_for_status(status_code),
            request_id=request_id,
            method="",
            path="",
            status_code=status_code,
            duration_ms=duration_ms,
            response_body=self._truncate_body(response_body) if self.config.include_response_body else None,
        )
        await self._store_log(entry)

    async def log_error(
        self,
        request_id: str,
        error: str,
        method: str,
        path: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        entry = APILogEntry(
            timestamp=datetime.now(),
            level=LogLevel.ERROR,
            request_id=request_id,
            method=method,
            path=path,
            status_code=None,
            duration_ms=None,
            metadata={**(metadata or {}), "error": error},
        )
        await self._store_log(entry)

    async def _store_log(self, entry: APILogEntry) -> None:
        async with self._log_lock:
            self._logs.append(entry)
            if len(self._logs) > 10000:
                self._logs = self._logs[-5000:]
        for handler in self._handlers:
            try:
                result = handler(entry)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error(f"Log handler failed: {e}")
        if self.config.log_to_console:
            self._write_to_console(entry)

    def _write_to_console(self, entry: APILogEntry) -> None:
        log_message = f"[{entry.timestamp.isoformat()}] {entry.level.value.upper()} - {entry.request_id} - {entry.method} {entry.path}"
        if entry.status_code:
            log_message += f" - {entry.status_code}"
        if entry.duration_ms:
            log_message += f" - {entry.duration_ms:.2f}ms"
        if entry.level == LogLevel.ERROR:
            logger.error(log_message)
        elif entry.level == LogLevel.WARNING:
            logger.warning(log_message)
        else:
            logger.info(log_message)

    def _get_level_for_status(self, status_code: int) -> LogLevel:
        if status_code >= 500:
            return LogLevel.CRITICAL
        elif status_code >= 400:
            return LogLevel.ERROR
        elif status_code >= 300:
            return LogLevel.WARNING
        return LogLevel.INFO

    def _truncate_body(self, body: Any, max_size: Optional[int] = None) -> Any:
        max_size = max_size or self.config.max_body_size
        if body is None:
            return None
        body_str = str(body)
        if len(body_str) > max_size:
            return body_str[:max_size] + "...[truncated]"
        return body

    def _sanitize_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        sensitive = {"authorization", "cookie", "x-api-key", "x-auth-token"}
        return {k: ("[REDACTED]" if k.lower() in sensitive else v) for k, v in headers.items()}

    def get_logs(self, limit: Optional[int] = None, level: Optional[LogLevel] = None) -> List[APILogEntry]:
        logs = self._logs
        if level:
            logs = [l for l in logs if l.level == level]
        if limit:
            return logs[-limit:]
        return logs
