"""API request/response logging utilities.

This module provides API logging:
- Request/response logging
- Log sanitization
- Log formatting
- Log levels by status code

Example:
    >>> from actions.api_logging_action import APILogger
    >>> logger = APILogger()
    >>> logger.log_request(request)
"""

from __future__ import annotations

import time
import json
import logging
import re
from typing import Any, Optional
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class LogEntry:
    """A log entry for an API request/response."""
    timestamp: str
    method: str
    path: str
    status_code: Optional[int] = None
    latency: Optional[float] = None
    request_headers: dict[str, str] = field(default_factory=dict)
    response_headers: dict[str, str] = field(default_factory=dict)
    request_body: Optional[Any] = None
    response_body: Optional[Any] = None
    error: Optional[str] = None
    client_ip: Optional[str] = None
    user_agent: Optional[str] = None


class APILogger:
    """Logger for API requests and responses.

    Example:
        >>> api_logger = APILogger(sanitize_keys=["password", "token"])
        >>> api_logger.log_request(request)
    """

    SENSITIVE_KEYS = {
        "password", "token", "secret", "api_key", "apikey",
        "authorization", "access_token", "refresh_token",
        "session", "cookie", "credit_card",
    }

    def __init__(
        self,
        sanitize_keys: Optional[set[str]] = None,
        log_bodies: bool = True,
        max_body_size: int = 10000,
        log_levels_by_status: Optional[dict[int, str]] = None,
    ) -> None:
        self.sanitize_keys = sanitize_keys or self.SENSITIVE_KEYS
        self.log_bodies = log_bodies
        self.max_body_size = max_body_size
        self.log_levels_by_status = log_levels_by_status or {
            2: "INFO",
            3: "INFO",
            4: "WARNING",
            5: "ERROR",
        }

    def log_request(
        self,
        method: str,
        path: str,
        headers: Optional[dict[str, str]] = None,
        body: Optional[Any] = None,
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> LogEntry:
        """Log an incoming request.

        Args:
            method: HTTP method.
            path: Request path.
            headers: Request headers.
            body: Request body.
            client_ip: Client IP address.
            user_agent: User agent string.

        Returns:
            LogEntry for this request.
        """
        entry = LogEntry(
            timestamp=datetime.utcnow().isoformat(),
            method=method,
            path=path,
            request_headers=self._sanitize_dict(headers or {}),
            request_body=self._sanitize_body(body) if self.log_bodies else None,
            client_ip=client_ip,
            user_agent=user_agent,
        )
        self._log_entry(entry, "INFO")
        return entry

    def log_response(
        self,
        entry: LogEntry,
        status_code: int,
        headers: Optional[dict[str, str]] = None,
        body: Optional[Any] = None,
        error: Optional[str] = None,
    ) -> None:
        """Log a response.

        Args:
            entry: LogEntry from log_request.
            status_code: Response status code.
            headers: Response headers.
            body: Response body.
            error: Error message if any.
        """
        entry.status_code = status_code
        entry.response_headers = headers or {}
        entry.response_body = self._sanitize_body(body) if self.log_bodies else None
        entry.error = error
        level = self._get_log_level(status_code)
        self._log_entry(entry, level)

    def log_error(
        self,
        entry: LogEntry,
        error: Exception,
    ) -> None:
        """Log an error.

        Args:
            entry: LogEntry from log_request.
            error: Exception that occurred.
        """
        entry.error = str(error)
        self._log_entry(entry, "ERROR")

    def _sanitize_dict(self, data: dict[str, str]) -> dict[str, str]:
        """Sanitize a dictionary by redacting sensitive values."""
        result = {}
        for key, value in data.items():
            lower_key = key.lower().replace("-", "_")
            if any(sk in lower_key for sk in self.sanitize_keys):
                result[key] = "***REDACTED***"
            else:
                result[key] = str(value)[:500]
        return result

    def _sanitize_body(self, body: Any) -> Any:
        """Sanitize a request/response body."""
        if body is None:
            return None
        if isinstance(body, dict):
            return {k: self._sanitize_body(v) for k, v in body.items()}
        if isinstance(body, list):
            return [self._sanitize_body(item) for item in body]
        if isinstance(body, str):
            body_str = body[:self.max_body_size]
            for key in self.sanitize_keys:
                pattern = rf'({key}["\s:=]+)[^&\s"]+'
                body_str = re.sub(pattern, r"\1***REDACTED***", body_str, flags=re.IGNORECASE)
            return body_str
        return body

    def _get_log_level(self, status_code: int) -> str:
        """Get log level based on status code."""
        for code, level in sorted(self.log_levels_by_status.items()):
            if status_code >= code:
                return level
        return "INFO"

    def _log_entry(self, entry: LogEntry, level: str) -> None:
        """Log an entry at the specified level."""
        log_data = {
            "timestamp": entry.timestamp,
            "method": entry.method,
            "path": entry.path,
            "status_code": entry.status_code,
            "latency": entry.latency,
            "client_ip": entry.client_ip,
        }
        log_func = getattr(logger, level.lower(), logger.info)
        log_func(json.dumps(log_data))


class RequestLogger:
    """Context manager for logging request lifecycle."""

    def __init__(
        self,
        api_logger: APILogger,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> None:
        self.api_logger = api_logger
        self.method = method
        self.path = path
        self.kwargs = kwargs
        self.entry: Optional[LogEntry] = None
        self.start_time: float = 0.0

    def __enter__(self) -> RequestLogger:
        self.start_time = time.time()
        self.entry = self.api_logger.log_request(
            self.method,
            self.path,
            **self.kwargs,
        )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if self.entry:
            self.entry.latency = time.time() - self.start_time
            if exc_val:
                self.api_logger.log_error(self.entry, exc_val)
