"""Audit logger action module for RabAI AutoClick.

Provides audit logging with structured events, user tracking,
and searchable audit trails.
"""

import sys
import os
import time
import json
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field, asdict
from enum import Enum
from threading import Lock
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AuditEventType(Enum):
    """Types of audit events."""
    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    LOGIN = "login"
    LOGOUT = "logout"
    ACCESS = "access"
    ERROR = "error"
    CUSTOM = "custom"


class AuditSeverity(Enum):
    """Event severity levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """An audit log event."""
    id: str
    timestamp: float
    event_type: str
    actor: str  # User or service performing action
    resource: str  # Resource being acted upon
    action: str  # Action being performed
    severity: str = "info"
    success: bool = True
    details: Dict[str, Any] = field(default_factory=dict)
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class AuditLoggerAction(BaseAction):
    """Record and query audit events.
    
    Provides structured audit logging with filtering,
    search, and compliance reporting.
    """
    action_type = "audit_logger"
    display_name = "审计日志"
    description = "结构化审计日志和合规追踪"
    
    def __init__(self):
        super().__init__()
        self._events: List[AuditEvent] = []
        self._lock = Lock()
        self._max_events = 100000
        self._event_index = 0
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute audit operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'log', 'query', 'export', 'cleanup'
                - event: Event data (for log)
                - filters: Query filters (for query)
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'log').lower()
        
        if operation == 'log':
            return self._log_event(params)
        elif operation == 'query':
            return self._query_events(params)
        elif operation == 'export':
            return self._export_events(params)
        elif operation == 'cleanup':
            return self._cleanup(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _log_event(self, params: Dict[str, Any]) -> ActionResult:
        """Log an audit event."""
        event_type = params.get('event_type', 'custom')
        actor = params.get('actor', 'system')
        resource = params.get('resource', '')
        action = params.get('action', '')
        severity = params.get('severity', 'info')
        success = params.get('success', True)
        details = params.get('details', {})
        ip_address = params.get('ip_address')
        user_agent = params.get('user_agent')
        session_id = params.get('session_id')
        metadata = params.get('metadata', {})
        
        # Generate event ID
        event_id = f"evt_{int(time.time() * 1000)}_{self._event_index}"
        self._event_index += 1
        
        event = AuditEvent(
            id=event_id,
            timestamp=time.time(),
            event_type=event_type,
            actor=actor,
            resource=resource,
            action=action,
            severity=severity,
            success=success,
            details=details,
            ip_address=ip_address,
            user_agent=user_agent,
            session_id=session_id,
            metadata=metadata
        )
        
        with self._lock:
            self._events.append(event)
            
            # Trim old events if over limit
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events:]
        
        return ActionResult(
            success=True,
            message=f"Logged audit event {event_id}",
            data={'event_id': event_id}
        )
    
    def _query_events(self, params: Dict[str, Any]) -> ActionResult:
        """Query audit events."""
        filters = params.get('filters', {})
        limit = params.get('limit', 100)
        offset = params.get('offset', 0)
        
        start_time = filters.get('start_time', 0)
        end_time = filters.get('end_time', time.time())
        event_type = filters.get('event_type')
        actor = filters.get('actor')
        resource = filters.get('resource')
        severity = filters.get('severity')
        success = filters.get('success')
        
        results = []
        
        with self._lock:
            for event in self._events:
                # Time filter
                if event.timestamp < start_time or event.timestamp > end_time:
                    continue
                
                # Type filter
                if event_type and event.event_type != event_type:
                    continue
                
                # Actor filter
                if actor and event.actor != actor:
                    continue
                
                # Resource filter
                if resource and event.resource != resource:
                    continue
                
                # Severity filter
                if severity and event.severity != severity:
                    continue
                
                # Success filter
                if success is not None and event.success != success:
                    continue
                
                results.append(self._event_to_dict(event))
        
        # Apply pagination
        total = len(results)
        results = results[offset:offset + limit]
        
        return ActionResult(
            success=True,
            message=f"Found {total} events",
            data={
                'events': results,
                'total': total,
                'limit': limit,
                'offset': offset
            }
        )
    
    def _event_to_dict(self, event: AuditEvent) -> Dict[str, Any]:
        """Convert event to dict."""
        return {
            'id': event.id,
            'timestamp': event.timestamp,
            'timestamp_iso': datetime.utcfromtimestamp(event.timestamp).isoformat(),
            'event_type': event.event_type,
            'actor': event.actor,
            'resource': event.resource,
            'action': event.action,
            'severity': event.severity,
            'success': event.success,
            'details': event.details,
            'ip_address': event.ip_address,
            'user_agent': event.user_agent,
            'session_id': event.session_id,
            'metadata': event.metadata
        }
    
    def _export_events(self, params: Dict[str, Any]) -> ActionResult:
        """Export events to file or stream."""
        filters = params.get('filters', {})
        format_type = params.get('format', 'json')
        
        # Query events
        query_result = self._query_events({
            'filters': filters,
            'limit': filters.get('limit', 10000)
        })
        
        events = query_result.data.get('events', [])
        
        if format_type == 'json':
            export_data = json.dumps(events, indent=2)
        elif format_type == 'csv':
            # Simple CSV format
            if not events:
                export_data = ""
            else:
                headers = list(events[0].keys())
                rows = [[str(e.get(h, '')) for h in headers] for e in events]
                export_data = ','.join(headers) + '\n' + '\n'.join(','.join(r) for r in rows)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown format: {format_type}"
            )
        
        return ActionResult(
            success=True,
            message=f"Exported {len(events)} events",
            data={
                'count': len(events),
                'format': format_type,
                'data': export_data[:5000]  # Truncate if too large
            }
        )
    
    def _cleanup(self, params: Dict[str, Any]) -> ActionResult:
        """Clean up old audit events."""
        max_age = params.get('max_age', 2592000)  # 30 days default
        now = time.time()
        cutoff = now - max_age
        
        with self._lock:
            original_count = len(self._events)
            self._events = [e for e in self._events if e.timestamp >= cutoff]
            removed = original_count - len(self._events)
        
        return ActionResult(
            success=True,
            message=f"Removed {removed} events older than {max_age}s",
            data={'removed': removed, 'remaining': len(self._events)}
        )


class ComplianceReporterAction(BaseAction):
    """Generate compliance reports from audit logs."""
    action_type = "compliance_reporter"
    display_name = "合规报告"
    description = "审计日志合规报告生成"
    
    def __init__(self):
        super().__init__()
        self._logger = AuditLoggerAction()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate compliance report."""
        start_date = params.get('start_date')
        end_date = params.get('end_date', time.time())
        report_type = params.get('type', 'summary')
        
        # Query all events in range
        events_result = self._logger._query_events({
            'filters': {
                'start_time': start_date or (end_date - 86400 * 30),
                'end_time': end_date
            },
            'limit': 10000
        })
        
        events = events_result.data.get('events', [])
        
        # Generate report
        if report_type == 'summary':
            return self._generate_summary(events)
        elif report_type == 'security':
            return self._generate_security_report(events)
        elif report_type == 'access':
            return self._generate_access_report(events)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown report type: {report_type}"
            )
    
    def _generate_summary(self, events: List[Dict]) -> ActionResult:
        """Generate summary report."""
        from collections import Counter
        
        type_counts = Counter(e['event_type'] for e in events)
        severity_counts = Counter(e['severity'] for e in events)
        actor_counts = Counter(e['actor'] for e in events)
        
        return ActionResult(
            success=True,
            message="Summary report generated",
            data={
                'total_events': len(events),
                'event_types': dict(type_counts),
                'severity_breakdown': dict(severity_counts),
                'top_actors': dict(actor_counts.most_common(10))
            }
        )
    
    def _generate_security_report(self, events: List[Dict]) -> ActionResult:
        """Generate security-focused report."""
        from collections import Counter
        
        errors = [e for e in events if e['severity'] in ('error', 'critical')]
        failed_events = [e for e in events if not e['success']]
        
        failed_by_actor = Counter(e['actor'] for e in failed_events)
        error_by_resource = Counter(e['resource'] for e in errors)
        
        return ActionResult(
            success=True,
            message="Security report generated",
            data={
                'total_events': len(events),
                'failed_operations': len(failed_events),
                'errors': len(errors),
                'failed_by_actor': dict(failed_by_actor.most_common(10)),
                'error_by_resource': dict(error_by_resource.most_common(10))
            }
        )
    
    def _generate_access_report(self, events: List[Dict]) -> ActionResult:
        """Generate access-focused report."""
        from collections import Counter
        
        access_events = [e for e in events if e['event_type'] == 'access']
        by_resource = Counter(e['resource'] for e in access_events)
        by_actor = Counter(e['actor'] for e in access_events)
        
        return ActionResult(
            success=True,
            message="Access report generated",
            data={
                'total_access_events': len(access_events),
                'by_resource': dict(by_resource.most_common(20)),
                'by_actor': dict(by_actor.most_common(20))
            }
        )
