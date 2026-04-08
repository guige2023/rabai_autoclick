"""
Automation Audit Action Module.

Provides audit logging for automation workflows
with tracking, compliance, and reporting.
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import uuid

logger = logging.getLogger(__name__)


class AuditEventType(Enum):
    """Audit event types."""
    ACTION_START = "action_start"
    ACTION_COMPLETE = "action_complete"
    ACTION_FAIL = "action_fail"
    WORKFLOW_START = "workflow_start"
    WORKFLOW_COMPLETE = "workflow_complete"
    WORKFLOW_FAIL = "workflow_fail"
    CONDITION_EVAL = "condition_eval"
    VARIABLE_CHANGE = "variable_change"
    USER_ACTION = "user_action"
    SYSTEM_EVENT = "system_event"


class AuditSeverity(Enum):
    """Audit severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """Single audit event."""
    event_id: str
    event_type: AuditEventType
    timestamp: datetime
    workflow_id: Optional[str] = None
    action_id: Optional[str] = None
    user_id: Optional[str] = None
    severity: AuditSeverity = AuditSeverity.INFO
    message: str = ""
    details: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AuditLog:
    """Audit log containing events."""
    log_id: str
    workflow_id: Optional[str] = None
    events: List[AuditEvent] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None


class AuditLogger:
    """Logs audit events."""

    def __init__(self, workflow_id: Optional[str] = None):
        self.workflow_id = workflow_id
        self.events: List[AuditEvent] = []
        self._handlers: List[Callable] = []

    def log(
        self,
        event_type: AuditEventType,
        message: str,
        action_id: Optional[str] = None,
        user_id: Optional[str] = None,
        severity: AuditSeverity = AuditSeverity.INFO,
        details: Optional[Dict[str, Any]] = None
    ) -> AuditEvent:
        """Log an audit event."""
        event = AuditEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            timestamp=datetime.now(),
            workflow_id=self.workflow_id,
            action_id=action_id,
            user_id=user_id,
            severity=severity,
            message=message,
            details=details or {}
        )

        self.events.append(event)
        self._notify_handlers(event)

        return event

    def log_action_start(self, action_id: str, details: Optional[Dict] = None):
        """Log action start."""
        return self.log(
            AuditEventType.ACTION_START,
            f"Action started: {action_id}",
            action_id=action_id,
            details=details
        )

    def log_action_complete(self, action_id: str, details: Optional[Dict] = None):
        """Log action completion."""
        return self.log(
            AuditEventType.ACTION_COMPLETE,
            f"Action completed: {action_id}",
            action_id=action_id,
            details=details
        )

    def log_action_fail(self, action_id: str, error: str):
        """Log action failure."""
        return self.log(
            AuditEventType.ACTION_FAIL,
            f"Action failed: {action_id}",
            action_id=action_id,
            severity=AuditSeverity.ERROR,
            details={"error": error}
        )

    def register_handler(self, handler: Callable):
        """Register event handler."""
        self._handlers.append(handler)

    def _notify_handlers(self, event: AuditEvent):
        """Notify handlers of new event."""
        for handler in self._handlers:
            try:
                handler(event)
            except Exception as e:
                logger.error(f"Audit handler error: {e}")


class AuditRepository:
    """Stores and queries audit logs."""

    def __init__(self):
        self.logs: Dict[str, AuditLog] = {}
        self.events: List[AuditEvent] = []

    def add_log(self, audit_log: AuditLog):
        """Add an audit log."""
        self.logs[audit_log.log_id] = audit_log
        self.events.extend(audit_log.events)

    def query(
        self,
        workflow_id: Optional[str] = None,
        action_id: Optional[str] = None,
        event_type: Optional[AuditEventType] = None,
        severity: Optional[AuditSeverity] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[AuditEvent]:
        """Query audit events."""
        results = self.events

        if workflow_id:
            results = [e for e in results if e.workflow_id == workflow_id]

        if action_id:
            results = [e for e in results if e.action_id == action_id]

        if event_type:
            results = [e for e in results if e.event_type == event_type]

        if severity:
            results = [e for e in results if e.severity == severity]

        if start_time:
            results = [e for e in results if e.timestamp >= start_time]

        if end_time:
            results = [e for e in results if e.timestamp <= end_time]

        return results[-limit:]

    def get_workflow_timeline(self, workflow_id: str) -> List[AuditEvent]:
        """Get workflow timeline."""
        return sorted(
            [e for e in self.events if e.workflow_id == workflow_id],
            key=lambda e: e.timestamp
        )


class AuditReporter:
    """Generates audit reports."""

    def __init__(self, repository: AuditRepository):
        self.repository = repository

    def generate_summary(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Generate audit summary."""
        events = self.repository.query(
            start_time=start_time,
            end_time=end_time,
            limit=10000
        )

        event_counts: Dict[AuditEventType, int] = {}
        severity_counts: Dict[AuditSeverity, int] = {}
        workflow_counts: Dict[str, int] = {}

        for event in events:
            event_counts[event.event_type] = event_counts.get(event.event_type, 0) + 1
            severity_counts[event.severity] = severity_counts.get(event.severity, 0) + 1
            if event.workflow_id:
                workflow_counts[event.workflow_id] = workflow_counts.get(event.workflow_id, 0) + 1

        return {
            "total_events": len(events),
            "event_counts": {k.value: v for k, v in event_counts.items()},
            "severity_counts": {k.value: v for k, v in severity_counts.items()},
            "unique_workflows": len(workflow_counts),
            "time_range": {
                "start": min(e.timestamp for e in events).isoformat() if events else None,
                "end": max(e.timestamp for e in events).isoformat() if events else None
            }
        }

    def generate_workflow_report(self, workflow_id: str) -> Dict[str, Any]:
        """Generate report for specific workflow."""
        timeline = self.repository.get_workflow_timeline(workflow_id)

        if not timeline:
            return {"error": "Workflow not found"}

        start_event = timeline[0]
        end_event = timeline[-1] if len(timeline) > 1 else None

        duration = None
        if end_event:
            duration = (end_event.timestamp - start_event.timestamp).total_seconds()

        errors = [e for e in timeline if e.severity == AuditSeverity.ERROR]

        return {
            "workflow_id": workflow_id,
            "start_time": start_event.timestamp.isoformat(),
            "end_time": end_event.timestamp.isoformat() if end_event else None,
            "duration_seconds": duration,
            "total_events": len(timeline),
            "error_count": len(errors),
            "status": "completed" if end_event and end_event.event_type == AuditEventType.WORKFLOW_COMPLETE else "failed"
        }


def main():
    """Demonstrate audit logging."""
    logger = AuditLogger(workflow_id="wf-123")

    logger.log_action_start("action-1")
    logger.log_action_complete("action-1", {"result": "success"})
    logger.log_action_start("action-2")
    logger.log_action_fail("action-2", "Timeout error")

    print(f"Logged {len(logger.events)} events")
    for event in logger.events:
        print(f"  - {event.event_type.value}: {event.message}")


if __name__ == "__main__":
    main()
