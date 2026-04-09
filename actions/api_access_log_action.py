"""API Access Logger and Analytics.

This module provides API access logging and analytics:
- Request/response logging
- Response time tracking
- Error rate analysis
- Endpoint popularity stats

Example:
    >>> from actions.api_access_log_action import AccessLogger
    >>> logger = AccessLogger()
    >>> logger.log_request("/api/users", method="GET", status=200, duration_ms=45)
"""

from __future__ import annotations

import time
import logging
import threading
import json
from dataclasses import dataclass, field
from typing import Any, Optional
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class AccessLogEntry:
    """A single API access log entry."""
    timestamp: float
    path: str
    method: str
    status: int
    duration_ms: float
    ip: str = ""
    user_agent: str = ""
    request_id: str = ""
    error: Optional[str] = None
    request_size: int = 0
    response_size: int = 0


class AccessLogger:
    """API access logger with analytics capabilities."""

    def __init__(
        self,
        max_entries: int = 100000,
        slow_request_threshold_ms: float = 1000.0,
    ) -> None:
        """Initialize the access logger.

        Args:
            max_entries: Maximum log entries to retain.
            slow_request_threshold_ms: Threshold for slow requests.
        """
        self._entries: list[AccessLogEntry] = []
        self._max_entries = max_entries
        self._slow_threshold = slow_request_threshold_ms
        self._lock = threading.RLock()
        self._stats: dict[str, Any] = defaultdict(int)
        self._path_stats: dict[str, dict[str, Any]] = defaultdict(
            lambda: {"count": 0, "total_ms": 0.0, "errors": 0}
        )
        self._status_codes: dict[int, int] = defaultdict(int)

    def log_request(
        self,
        path: str,
        method: str,
        status: int,
        duration_ms: float,
        ip: str = "",
        user_agent: str = "",
        request_id: str = "",
        error: Optional[str] = None,
        request_size: int = 0,
        response_size: int = 0,
    ) -> None:
        """Log an API request.

        Args:
            path: Request path.
            method: HTTP method.
            status: Response status code.
            duration_ms: Request duration in milliseconds.
            ip: Client IP address.
            user_agent: Client user agent.
            request_id: Unique request ID.
            error: Error message if applicable.
            request_size: Request body size in bytes.
            response_size: Response body size in bytes.
        """
        entry = AccessLogEntry(
            timestamp=time.time(),
            path=path,
            method=method,
            status=status,
            duration_ms=duration_ms,
            ip=ip,
            user_agent=user_agent,
            request_id=request_id,
            error=error,
            request_size=request_size,
            response_size=response_size,
        )

        with self._lock:
            self._entries.append(entry)
            if len(self._entries) > self._max_entries:
                self._entries = self._entries[-self._max_entries // 2:]

            self._stats["total_requests"] += 1
            self._stats["total_duration_ms"] += duration_ms
            self._status_codes[status] += 1

            if status >= 400:
                self._stats["errors"] += 1
            if error:
                self._stats["error_count"] += 1
            if duration_ms >= self._slow_threshold:
                self._stats["slow_requests"] += 1

            ps = self._path_stats[path]
            ps["count"] += 1
            ps["total_ms"] += duration_ms
            if status >= 400:
                ps["errors"] += 1

    def get_recent_logs(
        self,
        limit: int = 100,
        path_filter: Optional[str] = None,
        status_filter: Optional[int] = None,
    ) -> list[AccessLogEntry]:
        """Get recent log entries.

        Args:
            limit: Maximum number of entries.
            path_filter: Only return entries with this path prefix.
            status_filter: Only return entries with this status code.

        Returns:
            List of log entries, newest first.
        """
        with self._lock:
            entries = list(reversed(self._entries))

            if path_filter:
                entries = [e for e in entries if e.path.startswith(path_filter)]
            if status_filter is not None:
                entries = [e for e in entries if e.status == status_filter]

            return entries[:limit]

    def get_stats(self) -> dict[str, Any]:
        """Get overall access statistics.

        Returns:
            Dict with total requests, avg duration, error rate, etc.
        """
        with self._lock:
            total = self._stats["total_requests"]
            if total == 0:
                return dict(self._stats)

            avg_duration = self._stats["total_duration_ms"] / total
            error_rate = (self._stats["errors"] / total) * 100

            return {
                **self._stats,
                "avg_duration_ms": round(avg_duration, 2),
                "error_rate_percent": round(error_rate, 2),
                "slow_request_rate_percent": round(
                    (self._stats["slow_requests"] / total) * 100, 2
                ),
            }

    def get_path_stats(self, path: Optional[str] = None) -> dict[str, dict[str, Any]]:
        """Get per-path statistics.

        Args:
            path: Specific path. None = all paths.

        Returns:
            Dict mapping path to stats.
        """
        with self._lock:
            if path:
                return {path: self._path_stats.get(path, {})}
            return dict(self._path_stats)

    def get_status_distribution(self) -> dict[int, int]:
        """Get distribution of HTTP status codes."""
        with self._lock:
            return dict(self._status_codes)

    def get_slow_requests(self, limit: int = 50) -> list[AccessLogEntry]:
        """Get the slowest requests.

        Args:
            limit: Maximum number of entries.

        Returns:
            List of slowest entries.
        """
        with self._lock:
            sorted_entries = sorted(
                self._entries, key=lambda e: e.duration_ms, reverse=True
            )
            return sorted_entries[:limit]

    def get_error_logs(self, limit: int = 100) -> list[AccessLogEntry]:
        """Get recent error entries.

        Args:
            limit: Maximum number of entries.

        Returns:
            List of error entries, newest first.
        """
        with self._lock:
            errors = [e for e in reversed(self._entries) if e.status >= 400 or e.error]
            return errors[:limit]

    def export_logs_json(self, filepath: str, limit: Optional[int] = None) -> int:
        """Export logs to a JSON file.

        Args:
            filepath: Output file path.
            limit: Maximum entries to export. None = all.

        Returns:
            Number of entries exported.
        """
        with self._lock:
            entries = self._entries[-limit:] if limit else self._entries

        data = [
            {
                "timestamp": e.timestamp,
                "path": e.path,
                "method": e.method,
                "status": e.status,
                "duration_ms": e.duration_ms,
                "ip": e.ip,
                "error": e.error,
            }
            for e in entries
        ]

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.info("Exported %d log entries to %s", len(data), filepath)
        return len(data)
