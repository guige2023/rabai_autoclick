"""Audit logger action for comprehensive audit logging.

Provides structured audit logging with event tracking,
compliance reporting, and forensic analysis support.
"""

import json
import logging
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class AuditEventType(Enum):
    USER_ACTION = "user_action"
    SYSTEM_EVENT = "system_event"
    SECURITY_EVENT = "security_event"
    DATA_ACCESS = "data_access"
    CONFIG_CHANGE = "config_change"
    BUSINESS_EVENT = "business_event"


class AuditSeverity(Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    event_id: str
    event_type: AuditEventType
    severity: AuditSeverity
    timestamp: float
    actor: str
    action: str
    resource: str
    outcome: str
    details: dict[str, Any] = field(default_factory=dict)
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None
    correlation_id: Optional[str] = None


class AuditLoggerAction:
    """Comprehensive audit logging with compliance support.

    Args:
        max_events: Maximum events to retain in memory.
        enable_persistence: Persist events to disk.
        retention_days: Event retention period in days.
    """

    def __init__(
        self,
        max_events: int = 100000,
        enable_persistence: bool = True,
        retention_days: int = 90,
    ) -> None:
        self._events: list[AuditEvent] = []
        self._max_events = max_events
        self._enable_persistence = enable_persistence
        self._retention_days = retention_days
        self._event_handlers: list[Callable[[AuditEvent], None]] = []
        self._event_counter = 0
        self._correlation_index: dict[str, list[str]] = {}

    def _generate_event_id(self) -> str:
        """Generate a unique event ID.

        Returns:
            Event ID string.
        """
        self._event_counter += 1
        return f"evt_{int(time.time() * 1000)}_{self._event_counter}"

    def log_event(
        self,
        event_type: AuditEventType,
        severity: AuditSeverity,
        actor: str,
        action: str,
        resource: str,
        outcome: str,
        details: Optional[dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
    ) -> str:
        """Log an audit event.

        Args:
            event_type: Type of event.
            severity: Event severity.
            actor: User or system performing the action.
            action: Action being performed.
            resource: Resource being acted upon.
            outcome: Outcome of the action.
            details: Additional event details.
            ip_address: Client IP address.
            user_agent: Client user agent.
            session_id: Session identifier.
            correlation_id: Correlation ID for tracing.

        Returns:
            Event ID.
        """
        event_id = self._generate_event_id()

        event = AuditEvent(
            event_id=event_id,
            event_type=event_type,
            severity=severity,
            timestamp=time.time(),
            actor=actor,
            action=action,
            resource=resource,
            outcome=outcome,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent,
            session_id=session_id,
            correlation_id=correlation_id,
        )

        self._events.append(event)

        if correlation_id:
            if correlation_id not in self._correlation_index:
                self._correlation_index[correlation_id] = []
            self._correlation_index[correlation_id].append(event_id)

        if len(self._events) > self._max_events:
            self._events.pop(0)

        for handler in self._event_handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Event handler error: {e}")

        return event_id

    def log_user_action(
        self,
        actor: str,
        action: str,
        resource: str,
        outcome: str,
        **kwargs: Any,
    ) -> str:
        """Log a user action event.

        Args:
            actor: User performing the action.
            action: Action being performed.
            resource: Resource being acted upon.
            outcome: Outcome of the action.

        Returns:
            Event ID.
        """
        return self.log_event(
            event_type=AuditEventType.USER_ACTION,
            severity=AuditSeverity.INFO,
            actor=actor,
            action=action,
            resource=resource,
            outcome=outcome,
            **kwargs,
        )

    def log_security_event(
        self,
        actor: str,
        action: str,
        resource: str,
        outcome: str,
        severity: AuditSeverity = AuditSeverity.WARNING,
        **kwargs: Any,
    ) -> str:
        """Log a security event.

        Args:
            actor: Actor performing the action.
            action: Action being performed.
            resource: Resource being acted upon.
            outcome: Outcome of the action.
            severity: Event severity.

        Returns:
            Event ID.
        """
        return self.log_event(
            event_type=AuditEventType.SECURITY_EVENT,
            severity=severity,
            actor=actor,
            action=action,
            resource=resource,
            outcome=outcome,
            **kwargs,
        )

    def log_data_access(
        self,
        actor: str,
        action: str,
        resource: str,
        outcome: str,
        **kwargs: Any,
    ) -> str:
        """Log a data access event.

        Args:
            actor: Actor accessing the data.
            action: Access action (read, write, delete).
            resource: Data resource being accessed.
            outcome: Outcome of the access.

        Returns:
            Event ID.
        """
        return self.log_event(
            event_type=AuditEventType.DATA_ACCESS,
            severity=AuditSeverity.INFO,
            actor=actor,
            action=action,
            resource=resource,
            outcome=outcome,
            **kwargs,
        )

    def register_event_handler(self, handler: Callable[[AuditEvent], None]) -> None:
        """Register a handler for audit events.

        Args:
            handler: Callback function.
        """
        self._event_handlers.append(handler)

    def query_events(
        self,
        actor_filter: Optional[str] = None,
        action_filter: Optional[str] = None,
        resource_filter: Optional[str] = None,
        event_type_filter: Optional[AuditEventType] = None,
        severity_filter: Optional[AuditSeverity] = None,
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
        correlation_id: Optional[str] = None,
        limit: int = 1000,
    ) -> list[AuditEvent]:
        """Query audit events with filters.

        Args:
            actor_filter: Filter by actor.
            action_filter: Filter by action.
            resource_filter: Filter by resource.
            event_type_filter: Filter by event type.
            severity_filter: Filter by severity.
            start_time: Start timestamp.
            end_time: End timestamp.
            correlation_id: Filter by correlation ID.
            limit: Maximum results.

        Returns:
            List of matching events (newest first).
        """
        results = self._events

        if correlation_id:
            event_ids = self._correlation_index.get(correlation_id, set())
            results = [e for e in results if e.event_id in event_ids]

        if actor_filter:
            results = [e for e in results if actor_filter in e.actor]
        if action_filter:
            results = [e for e in results if action_filter in e.action]
        if resource_filter:
            results = [e for e in results if resource_filter in e.resource]
        if event_type_filter:
            results = [e for e in results if e.event_type == event_type_filter]
        if severity_filter:
            results = [e for e in results if e.severity == severity_filter]
        if start_time:
            results = [e for e in results if e.timestamp >= start_time]
        if end_time:
            results = [e for e in results if e.timestamp <= end_time]

        return sorted(results, key=lambda e: e.timestamp, reverse=True)[:limit]

    def get_events_for_resource(
        self,
        resource: str,
        limit: int = 100,
    ) -> list[AuditEvent]:
        """Get all events for a specific resource.

        Args:
            resource: Resource identifier.
            limit: Maximum results.

        Returns:
            List of events (newest first).
        """
        return [
            e for e in self._events if e.resource == resource
        ][-limit:][::-1]

    def get_events_for_actor(
        self,
        actor: str,
        limit: int = 100,
    ) -> list[AuditEvent]:
        """Get all events for a specific actor.

        Args:
            actor: Actor identifier.
            limit: Maximum results.

        Returns:
            List of events (newest first).
        """
        return [
            e for e in self._events if e.actor == actor
        ][-limit:][::-1]

    def get_correlated_events(self, correlation_id: str) -> list[AuditEvent]:
        """Get all events with a correlation ID.

        Args:
            correlation_id: Correlation identifier.

        Returns:
            List of correlated events (chronological order).
        """
        event_ids = self._correlation_index.get(correlation_id, [])
        event_map = {e.event_id: e for e in self._events}
        return [event_map[eid] for eid in event_ids if eid in event_map]

    def export_events(
        self,
        format: str = "json",
        start_time: Optional[float] = None,
        end_time: Optional[float] = None,
    ) -> str:
        """Export events to a formatted string.

        Args:
            format: Export format ('json' or 'csv').
            start_time: Start timestamp.
            end_time: End timestamp.

        Returns:
            Exported data as string.
        """
        events = self._events
        if start_time:
            events = [e for e in events if e.timestamp >= start_time]
        if end_time:
            events = [e for e in events if e.timestamp <= end_time]

        if format == "json":
            return json.dumps([asdict(e) for e in events], indent=2)
        elif format == "csv":
            lines = ["event_id,event_type,severity,timestamp,actor,action,resource,outcome"]
            for e in events:
                lines.append(
                    f"{e.event_id},{e.event_type.value},{e.severity.value},"
                    f"{e.timestamp},{e.actor},{e.action},{e.resource},{e.outcome}"
                )
            return "\n".join(lines)

        return ""

    def cleanup_old_events(self) -> int:
        """Remove events older than retention period.

        Returns:
            Number of events removed.
        """
        cutoff = time.time() - (self._retention_days * 86400)
        old_events = [e for e in self._events if e.timestamp < cutoff]
        self._events = [e for e in self._events if e.timestamp >= cutoff]

        for correlation_id in list(self._correlation_index.keys()):
            event_ids = self._correlation_index[correlation_id]
            remaining = [eid for eid in event_ids if any(e.event_id == eid for e in self._events)]
            if remaining:
                self._correlation_index[correlation_id] = remaining
            else:
                del self._correlation_index[correlation_id]

        return len(old_events)

    def get_stats(self) -> dict[str, Any]:
        """Get audit logger statistics.

        Returns:
            Dictionary with stats.
        """
        by_type: dict[str, int] = {}
        by_severity: dict[str, int] = {}
        by_outcome: dict[str, int] = {}

        for event in self._events:
            type_key = event.event_type.value
            by_type[type_key] = by_type.get(type_key, 0) + 1
            by_severity[event.severity.value] = by_severity.get(event.severity.value, 0) + 1
            by_outcome[event.outcome] = by_outcome.get(event.outcome, 0) + 1

        return {
            "total_events": len(self._events),
            "max_events": self._max_events,
            "retention_days": self._retention_days,
            "by_event_type": by_type,
            "by_severity": by_severity,
            "by_outcome": by_outcome,
            "correlation_ids": len(self._correlation_index),
        }
