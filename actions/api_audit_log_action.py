"""API Audit Log Action Module.

Audit logging for API operations and access tracking.
"""

from __future__ import annotations

import asyncio
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from .id_generator_action import IDGenerator


class AuditEventType(Enum):
    """Audit event types."""
    REQUEST = "request"
    RESPONSE = "response"
    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    RATE_LIMIT = "rate_limit"
    ERROR = "error"


@dataclass
class AuditLogEntry:
    """Audit log entry."""
    entry_id: str
    event_type: AuditEventType
    timestamp: float
    user_id: str | None = None
    api_key_id: str | None = None
    method: str | None = None
    path: str | None = None
    status_code: int | None = None
    duration_ms: float | None = None
    ip_address: str | None = None
    user_agent: str | None = None
    request_id: str | None = None
    metadata: dict = field(default_factory=dict)


class AuditLogger:
    """Audit logger for API operations."""

    def __init__(self, max_entries: int = 100000) -> None:
        self.max_entries = max_entries
        self._entries: deque[AuditLogEntry] = deque(maxlen=max_entries)
        self._lock = asyncio.Lock()
        self._id_gen = IDGenerator()
        self._handlers: list[callable] = []

    async def log(
        self,
        event_type: AuditEventType,
        **kwargs
    ) -> AuditLogEntry:
        """Log an audit event."""
        entry = AuditLogEntry(
            entry_id=self._id_gen.generate_custom(prefix="audit"),
            event_type=event_type,
            timestamp=time.time(),
            **kwargs
        )
        async with self._lock:
            self._entries.append(entry)
        for handler in self._handlers:
            if asyncio.iscoroutinefunction(handler):
                asyncio.create_task(handler(entry))
            else:
                handler(entry)
        return entry

    async def log_request(
        self,
        method: str,
        path: str,
        user_id: str | None = None,
        api_key_id: str | None = None,
        request_id: str | None = None,
        ip_address: str | None = None
    ) -> AuditLogEntry:
        """Log API request."""
        return await self.log(
            AuditEventType.REQUEST,
            method=method,
            path=path,
            user_id=user_id,
            api_key_id=api_key_id,
            request_id=request_id,
            ip_address=ip_address
        )

    async def log_response(
        self,
        request_id: str,
        status_code: int,
        duration_ms: float,
        **kwargs
    ) -> AuditLogEntry:
        """Log API response."""
        return await self.log(
            AuditEventType.RESPONSE,
            request_id=request_id,
            status_code=status_code,
            duration_ms=duration_ms,
            **kwargs
        )

    def on_log(self, handler: callable) -> None:
        """Register log handler."""
        self._handlers.append(handler)

    async def get_entries(
        self,
        event_type: AuditEventType | None = None,
        user_id: str | None = None,
        limit: int = 100
    ) -> list[AuditLogEntry]:
        """Get audit entries with filters."""
        async with self._lock:
            entries = list(self._entries)
        if event_type:
            entries = [e for e in entries if e.event_type == event_type]
        if user_id:
            entries = [e for e in entries if e.user_id == user_id]
        return entries[-limit:]

    async def get_stats(self) -> dict[str, Any]:
        """Get audit statistics."""
        async with self._lock:
            total = len(self._entries)
            by_type = {}
            for entry in self._entries:
                type_name = entry.event_type.value
                by_type[type_name] = by_type.get(type_name, 0) + 1
            return {
                "total_entries": total,
                "by_type": by_type,
            }
