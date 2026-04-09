"""Error tracker action for tracking and analyzing errors.

Provides error aggregation, grouping, alerting, and
root cause analysis support.
"""

import logging
import traceback
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ErrorSeverity(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ErrorRecord:
    error_id: str
    error_type: str
    message: str
    severity: ErrorSeverity
    timestamp: float
    traceback: str
    context: dict[str, Any] = field(default_factory=dict)
    frequency: int = 1
    last_occurrence: float = field(default_factory=time.time)
    resolved: bool = False


import time

class ErrorTrackerAction:
    """Track, group, and analyze errors with alerting.

    Args:
        max_records: Maximum error records to retain.
        auto_group: Automatically group similar errors.
        alert_threshold: Trigger alert after this many occurrences.
    """

    def __init__(
        self,
        max_records: int = 1000,
        auto_group: bool = True,
        alert_threshold: int = 10,
    ) -> None:
        self._errors: dict[str, ErrorRecord] = {}
        self._max_records = max_records
        self._auto_group = auto_group
        self._alert_threshold = alert_threshold
        self._alert_handlers: list[Callable[[ErrorRecord], None]] = []
        self._grouped_errors: dict[str, list[str]] = {}

    def track(
        self,
        error_type: str,
        message: str,
        severity: ErrorSeverity = ErrorSeverity.ERROR,
        context: Optional[dict[str, Any]] = None,
        exc_info: Optional[Exception] = None,
    ) -> str:
        """Track an error occurrence.

        Args:
            error_type: Type/class of error.
            message: Error message.
            severity: Error severity level.
            context: Additional context data.
            exc_info: Exception object for traceback.

        Returns:
            Error ID.
        """
        error_id = str(uuid.uuid4())[:8]
        tb = ""
        if exc_info:
            tb = "".join(traceback.format_exception(type(exc_info), exc_info, exc_info.__traceback__))
        elif len(traceback.extract_stack()) > 1:
            tb = "".join(traceback.format_stack())

        record = ErrorRecord(
            error_id=error_id,
            error_type=error_type,
            message=message,
            severity=severity,
            timestamp=time.time(),
            traceback=tb,
            context=context or {},
        )

        if self._auto_group:
            group_key = self._make_group_key(error_type, message)
            if group_key in self._grouped_errors:
                existing_id = self._grouped_errors[group_key][0]
                existing = self._errors.get(existing_id)
                if existing:
                    existing.frequency += 1
                    existing.last_occurrence = time.time()
                    return existing_id

        self._errors[error_id] = record
        if self._auto_group:
            group_key = self._make_group_key(error_type, message)
            if group_key not in self._grouped_errors:
                self._grouped_errors[group_key] = []
            self._grouped_errors[group_key].append(error_id)

        if len(self._errors) > self._max_records:
            self._evict_oldest()

        if record.frequency >= self._alert_threshold:
            self._trigger_alert(record)

        logger.debug(f"Tracked error: {error_type} ({error_id})")
        return error_id

    def _make_group_key(self, error_type: str, message: str) -> str:
        """Create a grouping key for similar errors.

        Args:
            error_type: Error type.
            message: Error message.

        Returns:
            Grouping key.
        """
        return f"{error_type}:{message[:100]}"

    def _evict_oldest(self) -> None:
        """Evict the oldest error record."""
        if not self._errors:
            return
        oldest_id = min(self._errors.items(), key=lambda x: x[1].timestamp)[0]
        del self._errors[oldest_id]

    def _trigger_alert(self, record: ErrorRecord) -> None:
        """Trigger alert for high-frequency error.

        Args:
            record: Error record.
        """
        for handler in self._alert_handlers:
            try:
                handler(record)
            except Exception as e:
                logger.error(f"Alert handler error: {e}")
        logger.warning(f"Error alert triggered: {record.error_type} (freq={record.frequency})")

    def register_alert_handler(self, handler: Callable[[ErrorRecord], None]) -> None:
        """Register a handler for error alerts.

        Args:
            handler: Callback function.
        """
        self._alert_handlers.append(handler)

    def get_error(self, error_id: str) -> Optional[ErrorRecord]:
        """Get an error record by ID.

        Args:
            error_id: Error ID.

        Returns:
            Error record or None.
        """
        return self._errors.get(error_id)

    def get_errors(
        self,
        severity_filter: Optional[ErrorSeverity] = None,
        resolved_filter: Optional[bool] = None,
        limit: int = 100,
    ) -> list[ErrorRecord]:
        """Get error records with filters.

        Args:
            severity_filter: Filter by severity.
            resolved_filter: Filter by resolved status.
            limit: Maximum results.

        Returns:
            List of error records (newest first).
        """
        errors = list(self._errors.values())

        if severity_filter:
            errors = [e for e in errors if e.severity == severity_filter]
        if resolved_filter is not None:
            errors = [e for e in errors if e.resolved == resolved_filter]

        return sorted(errors, key=lambda e: e.timestamp, reverse=True)[:limit]

    def resolve(self, error_id: str) -> bool:
        """Mark an error as resolved.

        Args:
            error_id: Error ID.

        Returns:
            True if error was found and resolved.
        """
        record = self._errors.get(error_id)
        if record:
            record.resolved = True
            return True
        return False

    def unresolve(self, error_id: str) -> bool:
        """Mark an error as unresolved.

        Args:
            error_id: Error ID.

        Returns:
            True if error was found and unmarked.
        """
        record = self._errors.get(error_id)
        if record:
            record.resolved = False
            return True
        return False

    def get_stats(self) -> dict[str, Any]:
        """Get error tracking statistics.

        Returns:
            Dictionary with error stats.
        """
        total = len(self._errors)
        by_severity: dict[str, int] = {}
        for record in self._errors.values():
            key = record.severity.value
            by_severity[key] = by_severity.get(key, 0) + 1

        resolved = sum(1 for r in self._errors.values() if r.resolved)
        unresolved = total - resolved
        high_freq = sum(1 for r in self._errors.values() if r.frequency >= self._alert_threshold)

        return {
            "total_errors": total,
            "resolved": resolved,
            "unresolved": unresolved,
            "by_severity": by_severity,
            "high_frequency": high_freq,
            "max_records": self._max_records,
            "alert_threshold": self._alert_threshold,
        }

    def clear_resolved(self) -> int:
        """Clear all resolved errors.

        Returns:
            Number of errors cleared.
        """
        resolved_ids = [eid for eid, e in self._errors.items() if e.resolved]
        for eid in resolved_ids:
            del self._errors[eid]
        return len(resolved_ids)

    def clear_all(self) -> int:
        """Clear all error records.

        Returns:
            Number of errors cleared.
        """
        count = len(self._errors)
        self._errors.clear()
        self._grouped_errors.clear()
        return count
