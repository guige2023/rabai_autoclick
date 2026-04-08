"""Audit Log action module for RabAI AutoClick.

Provides comprehensive audit logging for tracking system
events, user actions, and data changes with searchable logs.
"""

import sys
import os
import json
import time
import uuid
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class AuditLevel(Enum):
    """Audit log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"
    SECURITY = "security"


class AuditCategory(Enum):
    """Categories of audit events."""
    USER_ACTION = "user_action"
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    SYSTEM_EVENT = "system_event"
    SECURITY_EVENT = "security_event"
    BUSINESS_EVENT = "business_event"


@dataclass
class AuditEntry:
    """Represents an audit log entry."""
    entry_id: str
    timestamp: float
    level: AuditLevel
    category: AuditCategory
    actor: str  # Who performed the action
    action: str  # What was done
    resource_type: Optional[str] = None  # Type of resource affected
    resource_id: Optional[str] = None  # ID of resource affected
    details: Dict[str, Any] = field(default_factory=dict)
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    session_id: Optional[str] = None
    correlation_id: Optional[str] = None
    result: str = "success"  # success, failure, partial
    error_message: Optional[str] = None


@dataclass
class AuditQuery:
    """Query parameters for searching audit logs."""
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    levels: Optional[List[AuditLevel]] = None
    categories: Optional[List[AuditCategory]] = None
    actor: Optional[str] = None
    action: Optional[str] = None
    resource_type: Optional[str] = None
    resource_id: Optional[str] = None
    correlation_id: Optional[str] = None
    result: Optional[str] = None
    limit: int = 100
    offset: int = 0


class AuditLogger:
    """Audit logging system."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._logs: List[AuditEntry] = []
        self._max_log_size = 50000
        self._index: Dict[str, List[int]] = defaultdict(list)  # index keys -> log indices
        self._persistence_path = persistence_path
        self._load()
    
    def _load(self) -> None:
        """Load logs from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for entry_data in data.get("logs", []):
                        entry_data.pop('level', None)
                        entry_data.pop('category', None)
                        entry = AuditEntry(
                            level=AuditLevel(entry_data.pop('_level')),
                            category=AuditCategory(entry_data.pop('_category')),
                            **entry_data
                        )
                        self._logs.append(entry)
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist logs."""
        if self._persistence_path:
            try:
                data = {
                    "logs": [
                        {
                            "_level": e.level.value,
                            "_category": e.category.value,
                            "entry_id": e.entry_id,
                            "timestamp": e.timestamp,
                            "actor": e.actor,
                            "action": e.action,
                            "resource_type": e.resource_type,
                            "resource_id": e.resource_id,
                            "details": e.details,
                            "ip_address": e.ip_address,
                            "session_id": e.session_id,
                            "correlation_id": e.correlation_id,
                            "result": e.result,
                            "error_message": e.error_message
                        }
                        for e in self._logs[-self._max_log_size:]
                    ]
                }
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except OSError:
                pass
    
    def _build_index(self, entry: AuditEntry, index: int) -> None:
        """Build search index for an entry."""
        keys = [
            f"actor:{entry.actor}",
            f"action:{entry.action}",
            f"category:{entry.category.value}",
            f"level:{entry.level.value}",
            f"result:{entry.result}"
        ]
        if entry.resource_type:
            keys.append(f"resource_type:{entry.resource_type}")
        if entry.resource_id:
            keys.append(f"resource_id:{entry.resource_id}")
        if entry.correlation_id:
            keys.append(f"correlation:{entry.correlation_id}")
        
        for key in keys:
            self._index[key].append(index)
    
    def log(
        self,
        level: AuditLevel,
        category: AuditCategory,
        actor: str,
        action: str,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        result: str = "success",
        error_message: Optional[str] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        session_id: Optional[str] = None,
        correlation_id: Optional[str] = None
    ) -> str:
        """Log an audit entry."""
        entry = AuditEntry(
            entry_id=str(uuid.uuid4()),
            timestamp=time.time(),
            level=level,
            category=category,
            actor=actor,
            action=action,
            resource_type=resource_type,
            resource_id=resource_id,
            details=details or {},
            ip_address=ip_address,
            user_agent=user_agent,
            session_id=session_id,
            correlation_id=correlation_id,
            result=result,
            error_message=error_message
        )
        
        self._logs.append(entry)
        if len(self._logs) > self._max_log_size:
            self._logs = self._logs[-self._max_log_size:]
        
        index = len(self._logs) - 1
        self._build_index(entry, index)
        self._persist()
        
        return entry.entry_id
    
    def query(self, query: AuditQuery) -> List[AuditEntry]:
        """Query audit logs."""
        results = []
        
        for entry in self._logs:
            # Time filter
            if query.start_time and entry.timestamp < query.start_time:
                continue
            if query.end_time and entry.timestamp > query.end_time:
                continue
            
            # Level filter
            if query.levels and entry.level not in query.levels:
                continue
            
            # Category filter
            if query.categories and entry.category not in query.categories:
                continue
            
            # Actor filter
            if query.actor and entry.actor != query.actor:
                continue
            
            # Action filter
            if query.action and query.action not in entry.action:
                continue
            
            # Resource filter
            if query.resource_type and entry.resource_type != query.resource_type:
                continue
            if query.resource_id and entry.resource_id != query.resource_id:
                continue
            
            # Correlation filter
            if query.correlation_id and entry.correlation_id != query.correlation_id:
                continue
            
            # Result filter
            if query.result and entry.result != query.result:
                continue
            
            results.append(entry)
        
        # Sort by timestamp descending
        results.sort(key=lambda e: e.timestamp, reverse=True)
        
        return results[query.offset:query.offset + query.limit]
    
    def get_entry(self, entry_id: str) -> Optional[AuditEntry]:
        """Get a specific entry by ID."""
        for entry in self._logs:
            if entry.entry_id == entry_id:
                return entry
        return None
    
    def get_stats(self, 
                  start_time: Optional[float] = None,
                  end_time: Optional[float] = None) -> Dict[str, Any]:
        """Get audit statistics."""
        entries = self._logs
        if start_time:
            entries = [e for e in entries if e.timestamp >= start_time]
        if end_time:
            entries = [e for e in entries if e.timestamp <= end_time]
        
        by_level = defaultdict(int)
        by_category = defaultdict(int)
        by_result = defaultdict(int)
        actors = set()
        
        for entry in entries:
            by_level[entry.level.value] += 1
            by_category[entry.category.value] += 1
            by_result[entry.result] += 1
            actors.add(entry.actor)
        
        return {
            "total_entries": len(entries),
            "unique_actors": len(actors),
            "by_level": dict(by_level),
            "by_category": dict(by_category),
            "by_result": dict(by_result),
            "oldest_timestamp": entries[-1].timestamp if entries else None,
            "newest_timestamp": entries[0].timestamp if entries else None
        }


class AuditLogAction(BaseAction):
    """Comprehensive audit logging for tracking events.
    
    Supports multiple log levels, categories, searchable logs,
    and detailed audit trail with statistics.
    """
    action_type = "audit_log"
    display_name = "审计日志"
    description = "审计日志系统，追踪用户操作和系统事件"
    
    def __init__(self):
        super().__init__()
        self._logger = AuditLogger()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute audit log operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "log":
                return self._log(params)
            elif operation == "query":
                return self._query(params)
            elif operation == "get":
                return self._get(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _log(self, params: Dict[str, Any]) -> ActionResult:
        """Log an audit entry."""
        level = AuditLevel(params.get("level", "info"))
        category = AuditCategory(params.get("category", "user_action"))
        actor = params.get("actor", "system")
        action = params.get("action", "")
        
        entry_id = self._logger.log(
            level=level,
            category=category,
            actor=actor,
            action=action,
            resource_type=params.get("resource_type"),
            resource_id=params.get("resource_id"),
            details=params.get("details"),
            result=params.get("result", "success"),
            error_message=params.get("error_message"),
            ip_address=params.get("ip_address"),
            session_id=params.get("session_id"),
            correlation_id=params.get("correlation_id")
        )
        
        return ActionResult(success=True, message=f"Logged: {entry_id}",
                         data={"entry_id": entry_id})
    
    def _query(self, params: Dict[str, Any]) -> ActionResult:
        """Query audit logs."""
        query = AuditQuery(
            start_time=params.get("start_time"),
            end_time=params.get("end_time"),
            levels=[AuditLevel(l) for l in params.get("levels", [])] if params.get("levels") else None,
            categories=[AuditCategory(c) for c in params.get("categories", [])] if params.get("categories") else None,
            actor=params.get("actor"),
            action=params.get("action"),
            resource_type=params.get("resource_type"),
            resource_id=params.get("resource_id"),
            result=params.get("result"),
            limit=params.get("limit", 100)
        )
        
        results = self._logger.query(query)
        return ActionResult(success=True, message=f"Found {len(results)} entries",
                         data={"entries": [
                             {"entry_id": e.entry_id, "timestamp": e.timestamp,
                              "level": e.level.value, "category": e.category.value,
                              "actor": e.actor, "action": e.action, "result": e.result}
                             for e in results
                         ]})
    
    def _get(self, params: Dict[str, Any]) -> ActionResult:
        """Get a specific entry."""
        entry_id = params.get("entry_id", "")
        entry = self._logger.get_entry(entry_id)
        if not entry:
            return ActionResult(success=False, message="Entry not found")
        return ActionResult(success=True, message="Entry retrieved",
                         data={"entry_id": entry.entry_id, "timestamp": entry.timestamp,
                               "action": entry.action, "actor": entry.actor})
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get audit stats."""
        stats = self._logger.get_stats(
            params.get("start_time"),
            params.get("end_time")
        )
        return ActionResult(success=True, message="Stats retrieved", data=stats)
