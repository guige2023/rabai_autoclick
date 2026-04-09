"""Automation audit trail action for tracking operation history.

Records and manages audit trails of automation operations
with searchable history and compliance reporting.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

logger = logging.getLogger(__name__)


class AuditLevel(Enum):
    """Audit log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEntry:
    """A single audit log entry."""
    id: str
    timestamp: float
    level: AuditLevel
    operation: str
    user: str
    resource: str
    action: str
    result: str
    metadata: dict[str, Any] = field(default_factory=dict)
    duration_ms: float = 0.0


@dataclass
class AuditReport:
    """Generated audit report."""
    period_start: float
    period_end: float
    total_entries: int
    entries_by_level: dict[str, int]
    entries_by_user: dict[str, int]
    entries_by_resource: dict[str, int]
    generated_at: float


class AutomationAuditTrailAction:
    """Track and audit automation operations.

    Example:
        >>> audit = AutomationAuditTrailAction()
        >>> audit.log_operation("deploy", "user1", "prod-server", "start")
        >>> entries = audit.query(user="user1", limit=100)
    """

    def __init__(self, max_entries: int = 10000) -> None:
        self.max_entries = max_entries
        self._entries: list[AuditEntry] = []
        self._id_counter = 0

    def log_operation(
        self,
        operation: str,
        user: str,
        resource: str,
        action: str,
        result: str = "success",
        level: AuditLevel = AuditLevel.INFO,
        duration_ms: float = 0.0,
        **metadata: Any,
    ) -> str:
        """Log an operation to the audit trail.

        Args:
            operation: Operation name.
            user: User performing operation.
            resource: Resource being acted upon.
            action: Action being performed.
            result: Result of operation.
            level: Audit level.
            duration_ms: Operation duration.
            **metadata: Additional metadata.

        Returns:
            Audit entry ID.
        """
        self._id_counter += 1
        entry_id = f"AUD-{self._id_counter:08d}"

        entry = AuditEntry(
            id=entry_id,
            timestamp=time.time(),
            level=level,
            operation=operation,
            user=user,
            resource=resource,
            action=action,
            result=result,
            metadata=metadata,
            duration_ms=duration_ms,
        )

        self._entries.append(entry)

        if len(self._entries) > self.max_entries:
            self._entries.pop(0)

        logger.info(
            f"AUDIT: {level.value} {operation} by {user} on {resource}: {result}"
        )

        return entry_id

    def query(
        self,
        user: Optional[str] = None,
        resource: Optional[str] = None,
        operation: Optional[str] = None,
        level: Optional[AuditLevel] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        limit: int = 100,
    ) -> list[AuditEntry]:
        """Query audit entries with filters.

        Args:
            user: Filter by user.
            resource: Filter by resource.
            operation: Filter by operation.
            level: Filter by level.
            start_time: Filter by start time.
            end_time: Filter by end time.
            limit: Maximum entries to return.

        Returns:
            List of matching entries.
        """
        results = self._entries

        if user:
            results = [e for e in results if e.user == user]
        if resource:
            results = [e for e in results if e.resource == resource]
        if operation:
            results = [e for e in results if e.operation == operation]
        if level:
            results = [e for e in results if e.level == level]
        if start_time:
            results = [e for e in results if e.timestamp >= start_time]
        if end_time:
            results = [e for e in results if e.timestamp <= end_time]

        return results[-limit:]

    def generate_report(
        self,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> AuditReport:
        """Generate an audit report.

        Args:
            start_time: Report period start.
            end_time: Report period end.

        Returns:
            Audit report.
        """
        entries = self._entries

        if start_time:
            entries = [e for e in entries if e.timestamp >= start_time]
        if end_time:
            entries = [e for e in entries if e.timestamp <= end_time]

        period_start = entries[0].timestamp if entries else time.time()
        period_end = entries[-1].timestamp if entries else time.time()

        entries_by_level: dict[str, int] = {}
        entries_by_user: dict[str, int] = {}
        entries_by_resource: dict[str, int] = {}

        for entry in entries:
            entries_by_level[entry.level.value] = (
                entries_by_level.get(entry.level.value, 0) + 1
            )
            entries_by_user[entry.user] = entries_by_user.get(entry.user, 0) + 1
            entries_by_resource[entry.resource] = (
                entries_by_resource.get(entry.resource, 0) + 1
            )

        return AuditReport(
            period_start=period_start,
            period_end=period_end,
            total_entries=len(entries),
            entries_by_level=entries_by_level,
            entries_by_user=entries_by_user,
            entries_by_resource=entries_by_resource,
            generated_at=time.time(),
        )

    def export_json(
        self,
        path: Optional[str] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> str:
        """Export audit entries as JSON.

        Args:
            path: Optional file path to write.
            start_time: Filter by start time.
            end_time: Filter by end time.

        Returns:
            JSON string of entries.
        """
        entries = self._entries

        if start_time:
            entries = [e for e in entries if e.timestamp >= start_time]
        if end_time:
            entries = [e for e in entries if e.timestamp <= end_time]

        data = [
            {
                "id": e.id,
                "timestamp": e.timestamp,
                "level": e.level.value,
                "operation": e.operation,
                "user": e.user,
                "resource": e.resource,
                "action": e.action,
                "result": e.result,
                "duration_ms": e.duration_ms,
                "metadata": e.metadata,
            }
            for e in entries
        ]

        json_str = json.dumps(data, indent=2)

        if path:
            with open(path, "w") as f:
                f.write(json_str)

        return json_str

    def clear_older_than(self, timestamp: float) -> int:
        """Remove entries older than timestamp.

        Args:
            timestamp: Cutoff timestamp.

        Returns:
            Number of entries removed.
        """
        original_count = len(self._entries)
        self._entries = [e for e in self._entries if e.timestamp >= timestamp]
        removed = original_count - len(self._entries)
        logger.info(f"Cleared {removed} audit entries older than {timestamp}")
        return removed

    def get_stats(self) -> dict[str, Any]:
        """Get audit trail statistics.

        Returns:
            Statistics dictionary.
        """
        return {
            "total_entries": len(self._entries),
            "max_entries": self.max_entries,
            "entries_by_level": {
                level.value: len([e for e in self._entries if e.level == level])
                for level in AuditLevel
            },
        }
