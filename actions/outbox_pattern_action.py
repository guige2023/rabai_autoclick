"""Outbox Pattern action module for RabAI AutoClick.

Provides the transactional outbox pattern for reliable
event publishing alongside database operations.
"""

import sys
import os
import json
import time
import uuid
from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class OutboxStatus(Enum):
    """Status of an outbox entry."""
    PENDING = "pending"
    PUBLISHED = "published"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class OutboxEntry:
    """Represents an event in the outbox."""
    entry_id: str
    aggregate_type: str
    aggregate_id: str
    event_type: str
    payload: Dict[str, Any]
    status: OutboxStatus
    created_at: float
    published_at: Optional[float] = None
    attempts: int = 0
    max_attempts: int = 3
    last_error: Optional[str] = None
    expires_at: float = 0.0


class OutboxStore:
    """Transactional outbox for reliable event publishing."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._entries: Dict[str, OutboxEntry] = {}
        self._publish_handlers: Dict[str, callable] = {}
        self._persistence_path = persistence_path
        self._load()
    
    def _load(self) -> None:
        """Load outbox from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for entry_data in data.get("entries", []):
                        entry_data.pop('status', None)
                        entry = OutboxEntry(status=OutboxStatus(entry_data.pop('_status')),
                                           **entry_data)
                        self._entries[entry.entry_id] = entry
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist outbox."""
        if self._persistence_path:
            try:
                data = {
                    "entries": [
                        {
                            "_status": e.status.value,
                            "entry_id": e.entry_id,
                            "aggregate_type": e.aggregate_type,
                            "aggregate_id": e.aggregate_id,
                            "event_type": e.event_type,
                            "payload": e.payload,
                            "created_at": e.created_at,
                            "published_at": e.published_at,
                            "attempts": e.attempts,
                            "max_attempts": e.max_attempts,
                            "last_error": e.last_error,
                            "expires_at": e.expires_at
                        }
                        for e in self._entries.values()
                    ]
                }
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except OSError:
                pass
    
    def add_entry(
        self,
        aggregate_type: str,
        aggregate_id: str,
        event_type: str,
        payload: Dict[str, Any],
        ttl_seconds: float = 86400.0
    ) -> str:
        """Add an entry to the outbox."""
        entry_id = str(uuid.uuid4())
        entry = OutboxEntry(
            entry_id=entry_id,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=payload,
            status=OutboxStatus.PENDING,
            created_at=time.time(),
            expires_at=time.time() + ttl_seconds
        )
        self._entries[entry_id] = entry
        self._persist()
        return entry_id
    
    def register_handler(self, event_type: str, handler: callable) -> None:
        """Register a handler for an event type."""
        self._publish_handlers[event_type] = handler
    
    def publish_entry(self, entry_id: str) -> tuple[bool, str]:
        """Publish a single outbox entry."""
        entry = self._entries.get(entry_id)
        if not entry:
            return False, "Entry not found"
        
        if entry.status == OutboxStatus.PUBLISHED:
            return True, "Already published"
        
        handler = self._publish_handlers.get(entry.event_type)
        
        try:
            if handler:
                handler(entry.payload)
            else:
                # No handler, just mark as published
                pass
            
            entry.status = OutboxStatus.PUBLISHED
            entry.published_at = time.time()
            self._persist()
            return True, "Published"
        
        except Exception as e:
            entry.attempts += 1
            entry.last_error = str(e)
            
            if entry.attempts >= entry.max_attempts:
                entry.status = OutboxStatus.FAILED
            
            self._persist()
            return False, str(e)
    
    def publish_pending(self, batch_size: int = 100) -> Dict[str, int]:
        """Publish all pending entries.
        
        Returns counts of published, failed, and expired.
        """
        now = time.time()
        stats = {"published": 0, "failed": 0, "expired": 0}
        
        pending = [
            e for e in self._entries.values()
            if e.status == OutboxStatus.PENDING
        ]
        
        for entry in pending[:batch_size]:
            if entry.expires_at < now:
                entry.status = OutboxStatus.EXPIRED
                stats["expired"] += 1
                continue
            
            success, _ = self.publish_entry(entry.entry_id)
            if success:
                stats["published"] += 1
            else:
                stats["failed"] += 1
        
        self._persist()
        return stats
    
    def retry_failed(self, entry_id: str) -> bool:
        """Reset a failed entry for retry."""
        entry = self._entries.get(entry_id)
        if not entry or entry.status != OutboxStatus.FAILED:
            return False
        entry.status = OutboxStatus.PENDING
        entry.attempts = 0
        entry.last_error = None
        self._persist()
        return True
    
    def get_pending_count(self) -> int:
        """Get count of pending entries."""
        return sum(1 for e in self._entries.values() if e.status == OutboxStatus.PENDING)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get outbox statistics."""
        by_status = {}
        for e in self._entries.values():
            status_name by_status = e.status.value
           [status_name] = by_status.get(status_name, 0) + 1
        
        return {
            "total_entries": len(self._entries),
            "by_status": by_status,
            "pending_count": self.get_pending_count()
        }


class OutboxPatternAction(BaseAction):
    """Transactional outbox pattern for reliable events.
    
    Ensures events are atomically stored with data changes
    and reliably published to consumers.
    """
    action_type = "outbox_pattern"
    display_name = "发件箱模式"
    description = "事务性发件箱模式，确保事件可靠发布"
    
    def __init__(self):
        super().__init__()
        self._outbox = OutboxStore()
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute outbox operation."""
        operation = params.get("operation", "")
        
        try:
            if operation == "add":
                return self._add(params)
            elif operation == "publish":
                return self._publish(params)
            elif operation == "publish_pending":
                return self._publish_pending(params)
            elif operation == "retry":
                return self._retry(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _add(self, params: Dict[str, Any]) -> ActionResult:
        """Add entry to outbox."""
        entry_id = self._outbox.add_entry(
            aggregate_type=params.get("aggregate_type", ""),
            aggregate_id=params.get("aggregate_id", ""),
            event_type=params.get("event_type", ""),
            payload=params.get("payload", {}),
            ttl_seconds=params.get("ttl_seconds", 86400.0)
        )
        return ActionResult(success=True, message=f"Added: {entry_id}",
                         data={"entry_id": entry_id})
    
    def _publish(self, params: Dict[str, Any]) -> ActionResult:
        """Publish a single entry."""
        entry_id = params.get("entry_id", "")
        success, msg = self._outbox.publish_entry(entry_id)
        return ActionResult(success=success, message=msg)
    
    def _publish_pending(self, params: Dict[str, Any]) -> ActionResult:
        """Publish all pending entries."""
        batch_size = params.get("batch_size", 100)
        stats = self._outbox.publish_pending(batch_size)
        return ActionResult(success=True, message="Published",
                         data={"published": stats["published"],
                               "failed": stats["failed"],
                               "expired": stats["expired"]})
    
    def _retry(self, params: Dict[str, Any]) -> ActionResult:
        """Retry a failed entry."""
        entry_id = params.get("entry_id", "")
        retried = self._outbox.retry_failed(entry_id)
        return ActionResult(success=retried, message="Retried" if retried else "Cannot retry")
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get outbox stats."""
        stats = self._outbox.get_stats()
        return ActionResult(success=True, message="Stats", data=stats)
