"""
Automation Audit Action Module

Provides audit logging, compliance tracking, and activity recording.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import asyncio


class AuditEventType(Enum):
    """Audit event types."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LOGIN = "login"
    LOGOUT = "logout"
    ACCESS_DENIED = "access_denied"
    CONFIG_CHANGE = "config_change"
    SYSTEM_EVENT = "system_event"
    CUSTOM = "custom"


class AuditSeverity(Enum):
    """Audit event severity."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """An audit event."""
    event_id: str
    event_type: AuditEventType
    timestamp: datetime
    actor: dict[str, Any]  # Who performed the action
    resource: str  # What was affected
    action: str  # What action was taken
    result: str  # success, failure, partial
    metadata: dict[str, Any] = field(default_factory=dict)
    severity: AuditSeverity = AuditSeverity.INFO
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None
    request_id: Optional[str] = None
    correlation_id: Optional[str] = None


@dataclass
class AuditQuery:
    """Query parameters for audit log search."""
    event_types: Optional[list[AuditEventType]] = None
    actor_ids: Optional[list[str]] = None
    resource_pattern: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    severity: Optional[AuditSeverity] = None
    result: Optional[str] = None
    limit: int = 100
    offset: int = 0


@dataclass
class AuditSummary:
    """Summary of audit events."""
    total_events: int
    by_event_type: dict[str, int]
    by_severity: dict[str, int]
    by_result: dict[str, int]
    top_actors: list[tuple[str, int]]
    top_resources: list[tuple[str, int]]


class AuditLogger:
    """Logger for audit events with configurable storage."""
    
    def __init__(self, storage: Callable[[AuditEvent], Awaitable] = None):
        self._storage = storage
        self._events: list[AuditEvent] = []
        self._max_memory_events = 10000
    
    async def log(self, event: AuditEvent):
        """Log an audit event."""
        self._events.append(event)
        
        # Trim memory if needed
        if len(self._events) > self._max_memory_events:
            self._events = self._events[-self._max_memory_events:]
        
        if self._storage:
            await self._storage(event)
    
    def get_events(self, limit: int = 100) -> list[AuditEvent]:
        """Get recent events from memory."""
        return self._events[-limit:]
    
    def get_by_time_range(
        self,
        start: datetime,
        end: datetime
    ) -> list[AuditEvent]:
        """Get events within time range."""
        return [
            e for e in self._events
            if start <= e.timestamp <= end
        ]


class AutomationAuditAction:
    """Main audit action handler."""
    
    def __init__(self):
        self._logger = AuditLogger()
        self._retention_days = 90
        self._compliance_rules: dict[str, Callable] = {}
        self._alert_handlers: list[Callable] = []
        self._stats: dict[str, Any] = defaultdict(int)
    
    async def log_event(
        self,
        event_type: AuditEventType,
        actor: dict[str, Any],
        resource: str,
        action: str,
        result: str,
        severity: AuditSeverity = AuditSeverity.INFO,
        metadata: Optional[dict[str, Any]] = None,
        **kwargs
    ) -> AuditEvent:
        """
        Log an audit event.
        
        Args:
            event_type: Type of event
            actor: Who performed the action
            resource: What was affected
            action: What action was taken
            result: Outcome (success, failure, partial)
            severity: Event severity
            metadata: Additional event data
            **kwargs: Additional fields (ip_address, user_agent, etc.)
            
        Returns:
            The created AuditEvent
        """
        import uuid
        
        event = AuditEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            timestamp=datetime.now(),
            actor=actor,
            resource=resource,
            action=action,
            result=result,
            severity=severity,
            metadata=metadata or {},
            **kwargs
        )
        
        await self._logger.log(event)
        self._stats["events_logged"] += 1
        
        # Check compliance rules
        await self._check_compliance(event)
        
        # Trigger alerts if needed
        if severity in [AuditSeverity.ERROR, AuditSeverity.CRITICAL]:
            await self._trigger_alerts(event)
        
        return event
    
    async def log_create(
        self,
        actor: dict[str, Any],
        resource: str,
        resource_id: str,
        metadata: Optional[dict] = None
    ) -> AuditEvent:
        """Log a create event."""
        return await self.log_event(
            AuditEventType.CREATE,
            actor,
            f"{resource}/{resource_id}",
            f"create_{resource}",
            "success",
            metadata={"resource_id": resource_id, **(metadata or {})}
        )
    
    async def log_update(
        self,
        actor: dict[str, Any],
        resource: str,
        resource_id: str,
        changes: dict[str, Any],
        metadata: Optional[dict] = None
    ) -> AuditEvent:
        """Log an update event."""
        return await self.log_event(
            AuditEventType.UPDATE,
            actor,
            f"{resource}/{resource_id}",
            f"update_{resource}",
            "success",
            metadata={"changes": changes, **(metadata or {})}
        )
    
    async def log_delete(
        self,
        actor: dict[str, Any],
        resource: str,
        resource_id: str,
        metadata: Optional[dict] = None
    ) -> AuditEvent:
        """Log a delete event."""
        return await self.log_event(
            AuditEventType.DELETE,
            actor,
            f"{resource}/{resource_id}",
            f"delete_{resource}",
            "success",
            metadata={"resource_id": resource_id, **(metadata or {})}
        )
    
    async def log_access_denied(
        self,
        actor: dict[str, Any],
        resource: str,
        reason: str,
        **kwargs
    ) -> AuditEvent:
        """Log an access denied event."""
        return await self.log_event(
            AuditEventType.ACCESS_DENIED,
            actor,
            resource,
            "access",
            "denied",
            severity=AuditSeverity.WARNING,
            metadata={"reason": reason},
            **kwargs
        )
    
    async def query_events(
        self,
        query: AuditQuery
    ) -> list[AuditEvent]:
        """Query audit events."""
        events = self._logger.get_events(limit=10000)
        
        # Apply filters
        if query.event_types:
            events = [e for e in events if e.event_type in query.event_types]
        
        if query.actor_ids:
            events = [
                e for e in events
                if e.actor.get("id") in query.actor_ids
            ]
        
        if query.resource_pattern:
            import fnmatch
            events = [
                e for e in events
                if fnmatch.fnmatch(e.resource, query.resource_pattern)
            ]
        
        if query.start_time:
            events = [e for e in events if e.timestamp >= query.start_time]
        
        if query.end_time:
            events = [e for e in events if e.timestamp <= query.end_time]
        
        if query.severity:
            events = [e for e in events if e.severity == query.severity]
        
        if query.result:
            events = [e for e in events if e.result == query.result]
        
        # Apply pagination
        return events[query.offset:query.offset + query.limit]
    
    async def get_summary(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> AuditSummary:
        """Get summary statistics of audit events."""
        events = self._logger.get_events(limit=10000)
        
        if start_time:
            events = [e for e in events if e.timestamp >= start_time]
        if end_time:
            events = [e for e in events if e.timestamp <= end_time]
        
        # Count by type
        by_type = defaultdict(int)
        for event in events:
            by_type[event.event_type.value] += 1
        
        # Count by severity
        by_severity = defaultdict(int)
        for event in events:
            by_severity[event.severity.value] += 1
        
        # Count by result
        by_result = defaultdict(int)
        for event in events:
            by_result[event.result] += 1
        
        # Top actors
        actor_counts = defaultdict(int)
        for event in events:
            actor_id = event.actor.get("id", "unknown")
            actor_counts[actor_id] += 1
        top_actors = sorted(actor_counts.items(), key=lambda x: -x[1])[:10]
        
        # Top resources
        resource_counts = defaultdict(int)
        for event in events:
            resource_counts[event.resource] += 1
        top_resources = sorted(resource_counts.items(), key=lambda x: -x[1])[:10]
        
        return AuditSummary(
            total_events=len(events),
            by_event_type=dict(by_type),
            by_severity=dict(by_severity),
            by_result=dict(by_result),
            top_actors=top_actors,
            top_resources=top_resources
        )
    
    async def _check_compliance(self, event: AuditEvent):
        """Check event against compliance rules."""
        for rule_name, rule_fn in self._compliance_rules.items():
            try:
                await rule_fn(event)
            except Exception as e:
                self._stats["compliance_check_errors"] += 1
    
    async def _trigger_alerts(self, event: AuditEvent):
        """Trigger alerts for critical events."""
        for handler in self._alert_handlers:
            try:
                await handler(event)
            except Exception:
                self._stats["alert_errors"] += 1
    
    def register_compliance_rule(
        self,
        rule_name: str,
        rule_fn: Callable[[AuditEvent], Awaitable]
    ) -> "AutomationAuditAction":
        """Register a compliance checking rule."""
        self._compliance_rules[rule_name] = rule_fn
        return self
    
    def register_alert_handler(
        self,
        handler: Callable[[AuditEvent], Awaitable]
    ) -> "AutomationAuditAction":
        """Register an alert handler for critical events."""
        self._alert_handlers.append(handler)
        return self
    
    async def get_user_activity(
        self,
        user_id: str,
        days: int = 30
    ) -> list[AuditEvent]:
        """Get all activity for a specific user."""
        start_time = datetime.now() - timedelta(days=days)
        
        events = self._logger.get_by_time_range(start_time, datetime.now())
        
        return [
            e for e in events
            if e.actor.get("id") == user_id
        ]
    
    async def get_resource_history(
        self,
        resource: str,
        days: int = 30
    ) -> list[AuditEvent]:
        """Get all changes to a specific resource."""
        start_time = datetime.now() - timedelta(days=days)
        
        events = self._logger.get_by_time_range(start_time, datetime.now())
        
        return [
            e for e in events
            if e.resource.startswith(resource)
        ]
    
    def get_stats(self) -> dict[str, Any]:
        """Get audit statistics."""
        return {
            **dict(self._stats),
            "events_in_memory": len(self._logger._events),
            "compliance_rules": len(self._compliance_rules),
            "alert_handlers": len(self._alert_handlers)
        }
