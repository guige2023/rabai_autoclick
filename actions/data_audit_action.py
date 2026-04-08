"""Data audit action module for RabAI AutoClick.

Provides data audit trail with compliance tracking,
access logging, change detection, and audit reports.
"""

import time
import sys
import os
import json
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, timedelta
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AuditEvent:
    """Represents an audit event."""
    
    def __init__(
        self,
        event_id: str,
        event_type: str,
        actor: str,
        resource: str,
        action: str,
        result: str,
        details: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.event_id = event_id
        self.event_type = event_type
        self.actor = actor
        self.resource = resource
        self.action = action
        self.result = result
        self.details = details or {}
        self.metadata = metadata or {}
        self.timestamp = time.time()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'event_id': self.event_id,
            'event_type': self.event_type,
            'actor': self.actor,
            'resource': self.resource,
            'action': self.action,
            'result': self.result,
            'details': self.details,
            'metadata': self.metadata,
            'timestamp': self.timestamp,
            'datetime': datetime.fromtimestamp(self.timestamp).isoformat()
        }


class DataAuditAction(BaseAction):
    """Track and audit data operations for compliance.
    
    Supports audit logging, access tracking, change detection,
    compliance reports, and retention policies.
    """
    action_type = "data_audit"
    display_name = "数据审计"
    description = "数据操作审计和合规追踪"
    
    def __init__(self):
        super().__init__()
        self._audit_log: List[AuditEvent] = []
        self._retention_days = 365
        self._lock = threading.RLock()
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute audit operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (log, query, report,
                   archive, purge), config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'log')
        
        if action == 'log':
            return self._log_event(params)
        elif action == 'query':
            return self._query_events(params)
        elif action == 'report':
            return self._generate_report(params)
        elif action == 'archive':
            return self._archive_events(params)
        elif action == 'purge':
            return self._purge_old_events(params)
        elif action == 'summary':
            return self._get_summary(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _log_event(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Log an audit event."""
        event_type = params.get('event_type', 'data_operation')
        actor = params.get('actor', 'system')
        resource = params.get('resource', '')
        action = params.get('action', '')
        result = params.get('result', 'success')
        details = params.get('details', {})
        metadata = params.get('metadata', {})
        
        event_id = params.get('event_id') or f"evt_{int(time.time() * 1000)}"
        
        event = AuditEvent(
            event_id=event_id,
            event_type=event_type,
            actor=actor,
            resource=resource,
            action=action,
            result=result,
            details=details,
            metadata=metadata
        )
        
        with self._lock:
            self._audit_log.append(event)
        
        return ActionResult(
            success=True,
            message=f"Audit event logged: {action}",
            data={'event_id': event_id}
        )
    
    def _query_events(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Query audit events."""
        actor = params.get('actor')
        resource = params.get('resource')
        action = params.get('action')
        result = params.get('result')
        event_type = params.get('event_type')
        start_time = params.get('start_time')
        end_time = params.get('end_time')
        limit = params.get('limit', 100)
        offset = params.get('offset', 0)
        
        with self._lock:
            results = self._audit_log
            
            if actor:
                results = [e for e in results if e.actor == actor]
            if resource:
                results = [e for e in results if e.resource == resource]
            if action:
                results = [e for e in results if e.action == action]
            if result:
                results = [e for e in results if e.result == result]
            if event_type:
                results = [e for e in results if e.event_type == event_type]
            if start_time:
                results = [e for e in results if e.timestamp >= start_time]
            if end_time:
                results = [e for e in results if e.timestamp <= end_time]
            
            total = len(results)
            results = results[offset:offset + limit]
        
        return ActionResult(
            success=True,
            message=f"Found {total} events, returning {len(results)}",
            data={
                'events': [e.to_dict() for e in results],
                'total': total,
                'limit': limit,
                'offset': offset
            }
        )
    
    def _generate_report(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Generate an audit report."""
        start_time = params.get('start_time', time.time() - 86400 * 7)
        end_time = params.get('end_time', time.time())
        group_by = params.get('group_by', 'action')
        
        with self._lock:
            events = [
                e for e in self._audit_log
                if start_time <= e.timestamp <= end_time
            ]
        
        report = {
            'period': {
                'start': datetime.fromtimestamp(start_time).isoformat(),
                'end': datetime.fromtimestamp(end_time).isoformat()
            },
            'total_events': len(events),
            'summary': {}
        }
        
        if group_by == 'action':
            action_counts = {}
            for event in events:
                action = event.action or 'unknown'
                action_counts[action] = action_counts.get(action, 0) + 1
            report['summary']['by_action'] = action_counts
        elif group_by == 'actor':
            actor_counts = {}
            for event in events:
                actor = event.actor or 'unknown'
                actor_counts[actor] = actor_counts.get(actor, 0) + 1
            report['summary']['by_actor'] = actor_counts
        elif group_by == 'resource':
            resource_counts = {}
            for event in events:
                resource = event.resource or 'unknown'
                resource_counts[resource] = resource_counts.get(resource, 0) + 1
            report['summary']['by_resource'] = resource_counts
        
        result_counts = {'success': 0, 'failure': 0}
        for event in events:
            if event.result in result_counts:
                result_counts[event.result] += 1
        report['summary']['by_result'] = result_counts
        
        return ActionResult(
            success=True,
            message=f"Generated audit report for period",
            data=report
        )
    
    def _archive_events(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Archive old audit events."""
        before_time = params.get('before_time', time.time() - 86400 * 30)
        
        with self._lock:
            to_archive = [e for e in self._audit_log if e.timestamp < before_time]
            remaining = [e for e in self._audit_log if e.timestamp >= before_time]
            
            archived_data = [e.to_dict() for e in to_archive]
            self._audit_log = remaining
        
        return ActionResult(
            success=True,
            message=f"Archived {len(to_archive)} events",
            data={
                'archived_count': len(to_archive),
                'remaining_count': len(remaining),
                'archived_data': archived_data
            }
        )
    
    def _purge_old_events(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Purge events older than retention period."""
        retention_days = params.get('retention_days', self._retention_days)
        cutoff_time = time.time() - (retention_days * 86400)
        
        with self._lock:
            before_count = len(self._audit_log)
            self._audit_log = [
                e for e in self._audit_log if e.timestamp >= cutoff_time
            ]
            purged_count = before_count - len(self._audit_log)
        
        return ActionResult(
            success=True,
            message=f"Purged {purged_count} events older than {retention_days} days",
            data={
                'purged_count': purged_count,
                'remaining_count': len(self._audit_log)
            }
        )
    
    def _get_summary(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get audit log summary."""
        with self._lock:
            total = len(self._audit_log)
            
            actors = set(e.actor for e in self._audit_log)
            resources = set(e.resource for e in self._audit_log)
            actions = set(e.action for e in self._audit_log)
            
            result_counts = {}
            for event in self._audit_log:
                result_counts[event.result] = result_counts.get(event.result, 0) + 1
            
            oldest = min((e.timestamp for e in self._audit_log), default=None)
            newest = max((e.timestamp for e in self._audit_log), default=None)
        
        return ActionResult(
            success=True,
            message=f"Audit summary: {total} events",
            data={
                'total_events': total,
                'unique_actors': len(actors),
                'unique_resources': len(resources),
                'unique_actions': len(actions),
                'by_result': result_counts,
                'oldest_event': datetime.fromtimestamp(oldest).isoformat() if oldest else None,
                'newest_event': datetime.fromtimestamp(newest).isoformat() if newest else None
            }
        )
