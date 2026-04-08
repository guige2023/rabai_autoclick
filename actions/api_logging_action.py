"""API Logging Action Module.

Handles API request/response logging with filtering,
masking, and log storage management.
"""

from __future__ import annotations

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class LogLevel(Enum):
    """Log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class APILogEntry:
    """An API log entry."""
    timestamp: float
    request_id: str
    method: str
    path: str
    status_code: int
    duration: float
    request_headers: Dict[str, str]
    response_headers: Dict[str, str]
    masked_fields: List[str] = field(default_factory=list)


class APILoggingAction(BaseAction):
    """
    API request/response logging.

    Logs API requests and responses with field masking,
    filtering, and log storage management.

    Example:
        logger = APILoggingAction()
        result = logger.execute(ctx, {"action": "log", "method": "POST", "path": "/api/users"})
    """
    action_type = "api_logging"
    display_name = "API日志记录"
    description = "API请求/响应日志记录"

    def __init__(self) -> None:
        super().__init__()
        self._logs: List[APILogEntry] = []
        self._masked_fields: List[str] = ["password", "token", "secret", "api_key"]
        self._log_level = LogLevel.INFO

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        action = params.get("action", "")
        try:
            if action == "log":
                return self._log_request(params)
            elif action == "query":
                return self._query_logs(params)
            elif action == "add_mask":
                return self._add_masked_field(params)
            elif action == "clear":
                return self._clear_logs(params)
            elif action == "get_stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Logging error: {str(e)}")

    def _log_request(self, params: Dict[str, Any]) -> ActionResult:
        request_id = params.get("request_id", "")
        method = params.get("method", "GET")
        path = params.get("path", "/")
        status_code = params.get("status_code", 200)
        duration = params.get("duration", 0.0)
        request_headers = params.get("request_headers", {})
        response_headers = params.get("response_headers", {})

        masked_request_headers = self._mask_fields(dict(request_headers))
        masked_response_headers = self._mask_fields(dict(response_headers))

        entry = APILogEntry(timestamp=time.time(), request_id=request_id, method=method, path=path, status_code=status_code, duration=duration, request_headers=masked_request_headers, response_headers=masked_response_headers, masked_fields=self._masked_fields.copy())

        self._logs.append(entry)

        return ActionResult(success=True, message=f"Logged: {method} {path}")

    def _query_logs(self, params: Dict[str, Any]) -> ActionResult:
        method = params.get("method")
        path_pattern = params.get("path_pattern")
        status_code = params.get("status_code")
        limit = params.get("limit", 100)

        filtered = self._logs

        if method:
            filtered = [l for l in filtered if l.method == method]
        if path_pattern:
            filtered = [l for l in filtered if path_pattern in l.path]
        if status_code:
            filtered = [l for l in filtered if l.status_code == status_code]

        filtered = filtered[-limit:]

        return ActionResult(success=True, data={"count": len(filtered), "logs": [{"method": l.method, "path": l.path, "status": l.status_code} for l in filtered]})

    def _add_masked_field(self, params: Dict[str, Any]) -> ActionResult:
        field_name = params.get("field_name", "")
        if field_name and field_name not in self._masked_fields:
            self._masked_fields.append(field_name)
        return ActionResult(success=True, message=f"Added mask: {field_name}")

    def _clear_logs(self, params: Dict[str, Any]) -> ActionResult:
        count = len(self._logs)
        self._logs.clear()
        return ActionResult(success=True, message=f"Cleared {count} log entries")

    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        if not self._logs:
            return ActionResult(success=True, data={"total_logs": 0})

        status_counts: Dict[int, int] = {}
        method_counts: Dict[str, int] = {}
        total_duration = 0.0

        for log in self._logs:
            status_counts[log.status_code] = status_counts.get(log.status_code, 0) + 1
            method_counts[log.method] = method_counts.get(log.method, 0) + 1
            total_duration += log.duration

        return ActionResult(success=True, data={"total_logs": len(self._logs), "status_counts": status_counts, "method_counts": method_counts, "avg_duration_ms": (total_duration / len(self._logs)) * 1000})

    def _mask_fields(self, headers: Dict[str, str]) -> Dict[str, str]:
        for field_name in self._masked_fields:
            for key in headers:
                if field_name.lower() in key.lower():
                    headers[key] = "***MASKED***"
        return headers
