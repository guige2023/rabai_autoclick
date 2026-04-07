"""Audit logging utilities.

Comprehensive audit trail for tracking user actions, system changes,
and data modifications with structured logging and query support.

Example:
    audit = AuditLogger(conn, service="order_service")
    audit.log(action="CREATE_ORDER", user_id=123, resource="orders", metadata={"amount": 99.99})
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable
from uuid import uuid4

logger = logging.getLogger(__name__)


class AuditLevel(Enum):
    """Audit log severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AuditCategory(Enum):
    """Categories of auditable events."""
    USER_ACTION = "user_action"
    DATA_CHANGE = "data_change"
    SYSTEM_EVENT = "system_event"
    SECURITY = "security"
    ACCESS = "access"
    CONFIG_CHANGE = "config_change"
    BUSINESS = "business"


@dataclass
class AuditEntry:
    """A single audit log entry."""
    id: str
    timestamp: datetime
    level: AuditLevel
    category: AuditCategory
    action: str
    actor_id: str | None
    actor_type: str | None
    resource_type: str
    resource_id: str | None
    service: str
    metadata: dict[str, Any]
    ip_address: str | None = None
    user_agent: str | None = None
    correlation_id: str | None = None
    duration_ms: float | None = None
    success: bool = True
    error_message: str | None = None


class AuditLogger:
    """Structured audit logger with database persistence.

    Tracks user actions, data changes, and system events with
    full query capabilities for compliance and debugging.
    """

    TABLE_NAME = "audit_logs"

    def __init__(
        self,
        connection: Any,
        service: str = "unknown",
        table_name: str | None = None,
        async_mode: bool = False,
    ) -> None:
        """Initialize audit logger.

        Args:
            connection: Database connection.
            service: Name of the service generating audit logs.
            table_name: Optional custom table name.
            async_mode: If True, logs are queued and flushed asynchronously.
        """
        self.connection = connection
        self.service = service
        self.table_name = table_name or self.TABLE_NAME
        self._ensure_table()
        self._queue: list[AuditEntry] = []
        self._async_mode = async_mode

    def _ensure_table(self) -> None:
        """Create audit log table if not exists."""
        cursor = self.connection.cursor()
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id VARCHAR(64) PRIMARY KEY,
                timestamp TIMESTAMP NOT NULL,
                level VARCHAR(20) NOT NULL,
                category VARCHAR(40) NOT NULL,
                action VARCHAR(128) NOT NULL,
                actor_id VARCHAR(128),
                actor_type VARCHAR(40),
                resource_type VARCHAR(80) NOT NULL,
                resource_id VARCHAR(256),
                service VARCHAR(80) NOT NULL,
                metadata JSONB,
                ip_address VARCHAR(45),
                user_agent TEXT,
                correlation_id VARCHAR(64),
                duration_ms FLOAT,
                success BOOLEAN DEFAULT TRUE,
                error_message TEXT
            )
        """)
        self.connection.commit()

    def log(
        self,
        action: str,
        resource_type: str,
        resource_id: str | None = None,
        *,
        level: AuditLevel = AuditLevel.INFO,
        category: AuditCategory = AuditCategory.USER_ACTION,
        actor_id: str | None = None,
        actor_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        ip_address: str | None = None,
        user_agent: str | None = None,
        correlation_id: str | None = None,
        duration_ms: float | None = None,
        success: bool = True,
        error_message: str | None = None,
    ) -> str:
        """Log an audit event.

        Args:
            action: Action performed (e.g., "CREATE_ORDER", "LOGIN").
            resource_type: Type of resource affected (e.g., "orders", "users").
            resource_id: ID of the affected resource.
            level: Severity level.
            category: Event category.
            actor_id: ID of user/service performing action.
            actor_type: Type of actor ("user", "system", "service").
            metadata: Additional structured data.
            ip_address: Client IP address.
            user_agent: Client user agent.
            correlation_id: Request correlation ID for tracing.
            duration_ms: Operation duration in milliseconds.
            success: Whether the operation succeeded.
            error_message: Error message if failed.

        Returns:
            Audit entry ID.
        """
        entry = AuditEntry(
            id=str(uuid4()),
            timestamp=datetime.utcnow(),
            level=level,
            category=category,
            action=action,
            actor_id=actor_id,
            actor_type=actor_type,
            resource_type=resource_type,
            resource_id=resource_id,
            service=self.service,
            metadata=metadata or {},
            ip_address=ip_address,
            user_agent=user_agent,
            correlation_id=correlation_id,
            duration_ms=duration_ms,
            success=success,
            error_message=error_message,
        )

        if self._async_mode:
            self._queue.append(entry)
            return entry.id

        self._write_entry(entry)
        return entry.id

    def _write_entry(self, entry: AuditEntry) -> None:
        """Write a single audit entry to the database."""
        cursor = self.connection.cursor()
        cursor.execute(
            f"""
            INSERT INTO {self.table_name} (
                id, timestamp, level, category, action, actor_id, actor_type,
                resource_type, resource_id, service, metadata, ip_address,
                user_agent, correlation_id, duration_ms, success, error_message
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            """,
            (
                entry.id,
                entry.timestamp,
                entry.level.value,
                entry.category.value,
                entry.action,
                entry.actor_id,
                entry.actor_type,
                entry.resource_type,
                entry.resource_id,
                entry.service,
                json.dumps(entry.metadata),
                entry.ip_address,
                entry.user_agent,
                entry.correlation_id,
                entry.duration_ms,
                entry.success,
                entry.error_message,
            ),
        )
        self.connection.commit()

    def flush(self) -> int:
        """Flush queued entries to database (async mode).

        Returns:
            Number of entries flushed.
        """
        count = len(self._queue)
        for entry in self._queue:
            self._write_entry(entry)
        self._queue.clear()
        return count

    def query(
        self,
        *,
        actor_id: str | None = None,
        resource_type: str | None = None,
        resource_id: str | None = None,
        action: str | None = None,
        category: AuditCategory | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[AuditEntry]:
        """Query audit logs with filters.

        Returns:
            List of matching AuditEntry objects.
        """
        conditions = ["service = %s"]
        params: list[Any] = [self.service]

        if actor_id:
            conditions.append("actor_id = %s")
            params.append(actor_id)
        if resource_type:
            conditions.append("resource_type = %s")
            params.append(resource_type)
        if resource_id:
            conditions.append("resource_id = %s")
            params.append(resource_id)
        if action:
            conditions.append("action = %s")
            params.append(action)
        if category:
            conditions.append("category = %s")
            params.append(category.value)
        if start_time:
            conditions.append("timestamp >= %s")
            params.append(start_time)
        if end_time:
            conditions.append("timestamp <= %s")
            params.append(end_time)

        where_clause = " AND ".join(conditions)

        cursor = self.connection.cursor()
        cursor.execute(
            f"""
            SELECT * FROM {self.table_name}
            WHERE {where_clause}
            ORDER BY timestamp DESC
            LIMIT %s OFFSET %s
            """,
            [*params, limit, offset],
        )

        return [self._row_to_entry(row, cursor.description) for row in cursor.fetchall()]

    def _row_to_entry(self, row: tuple, columns: list) -> AuditEntry:
        """Convert database row to AuditEntry."""
        col_map = {desc[0]: i for i, desc in enumerate(columns)}
        return AuditEntry(
            id=row[col_map["id"]],
            timestamp=row[col_map["timestamp"]],
            level=AuditLevel(row[col_map["level"]]),
            category=AuditCategory(row[col_map["category"]]),
            action=row[col_map["action"]],
            actor_id=row[col_map["actor_id"]],
            actor_type=row[col_map["actor_type"]],
            resource_type=row[col_map["resource_type"]],
            resource_id=row[col_map["resource_id"]],
            service=row[col_map["service"]],
            metadata=json.loads(row[col_map["metadata"]] or "{}"),
            ip_address=row[col_map["ip_address"]],
            user_agent=row[col_map["user_agent"]],
            correlation_id=row[col_map["correlation_id"]],
            duration_ms=row[col_map["duration_ms"]],
            success=row[col_map["success"]],
            error_message=row[col_map["error_message"]],
        )


class AuditContext:
    """Context manager for wrapping operations with automatic audit logging.

    Example:
        with AuditContext(logger, action="PROCESS_ORDER", resource_type="orders") as ctx:
            ctx.metadata["order_id"] = 123
            process_order()
            ctx.success = True
    """

    def __init__(
        self,
        audit_logger: AuditLogger,
        action: str,
        resource_type: str,
        resource_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        self.audit_logger = audit_logger
        self.action = action
        self.resource_type = resource_type
        self.resource_id = resource_id
        self.kwargs = kwargs
        self.metadata: dict[str, Any] = {}
        self.success = False
        self.error: Exception | None = None
        self.start_time = time.perf_counter()

    def __enter__(self) -> "AuditContext":
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        duration_ms = (time.perf_counter() - self.start_time) * 1000

        if exc_val is not None:
            self.error = exc_val
            self.success = False

        self.audit_logger.log(
            action=self.action,
            resource_type=self.resource_type,
            resource_id=self.resource_id,
            metadata=self.metadata,
            duration_ms=duration_ms,
            success=self.success,
            error_message=str(self.error) if self.error else None,
            **self.kwargs,
        )
        return False


def mask_sensitive(data: dict[str, Any], fields: list[str]) -> dict[str, Any]:
    """Mask sensitive fields in audit metadata.

    Args:
        data: Dictionary to mask.
        fields: Field names to mask.

    Returns:
        New dictionary with masked fields.
    """
    result = dict(data)
    for field_name in fields:
        if field_name in result:
            val = str(result[field_name])
            if len(val) > 4:
                result[field_name] = val[:2] + "*" * (len(val) - 4) + val[-2:]
            else:
                result[field_name] = "****"
    return result
