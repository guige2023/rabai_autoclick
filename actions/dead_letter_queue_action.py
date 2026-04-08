"""Dead Letter Queue action module for RabAI AutoClick.

Provides DLQ management with message tracking, retry scheduling,
error categorization, and alerting thresholds.
"""

import sys
import os
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
from collections import deque, defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class DLQEntryStatus(Enum):
    """Status of a dead letter queue entry."""
    PENDING = "pending"      # Awaiting manual review
    RETRY_SCHEDULED = "retry_scheduled"  # Scheduled for retry
    RETRYING = "retrying"     # Currently being retried
    RESOLVED = "resolved"     # Successfully processed after retry
    DISCARDED = "discarded"   # Manually discarded
    EXPIRED = "expired"       # Past retention period


class ErrorCategory(Enum):
    """Categorization of error types."""
    TRANSIENT = "transient"       # Network timeout, temp failure
    DATA = "data"                 # Invalid data, schema violation
    RESOURCE = "resource"         # Out of memory, disk full
    AUTHENTICATION = "auth"       # Auth failures, permissions
    EXTERNAL_SERVICE = "external"  # Third-party API failures
    UNKNOWN = "unknown"


@dataclass
class DLQEntry:
    """Represents a message in the dead letter queue."""
    entry_id: str
    original_topic: str
    original_message_id: str
    payload: Any
    error_message: str
    error_category: ErrorCategory
    error_timestamp: float
    retry_count: int = 0
    max_retries: int = 3
    status: DLQEntryStatus = DLQEntryStatus.PENDING
    next_retry_at: Optional[float] = None
    resolved_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    stack_trace: Optional[str] = None
    source_system: str = ""
    tags: List[str] = field(default_factory=list)


@dataclass
class DLQConfig:
    """Configuration for a DLQ."""
    name: str
    source_topic: str
    max_size: int = 1000
    retention_seconds: float = 604800.0  # 7 days
    auto_retry_enabled: bool = False
    auto_retry_delay_seconds: float = 3600.0  # 1 hour
    max_auto_retries: int = 3
    alert_threshold: int = 100
    description: str = ""


@dataclass
class DLQStats:
    """Statistics for a DLQ."""
    queue_name: str
    total_entries: int = 0
    pending_count: int = 0
    retry_scheduled_count: int = 0
    resolved_count: int = 0
    discarded_count: int = 0
    expired_count: int = 0
    entries_by_category: Dict[str, int] = field(default_factory=dict)
    oldest_entry_age: float = 0.0
    last_activity_time: Optional[float] = None


class DLQManager:
    """Dead letter queue management system."""
    
    def __init__(self, persistence_path: Optional[str] = None):
        self._queues: Dict[str, DLQConfig] = {}
        self._entries: Dict[str, deque] = defaultdict(lambda: deque(maxlen=5000))
        self._entry_index: Dict[str, DLQEntry] = {}  # entry_id -> entry
        self._retry_callbacks: Dict[str, Callable] = {}
        self._alert_callbacks: List[Callable] = []
        self._persistence_path = persistence_path
        self._load()
    
    def _load(self) -> None:
        """Load DLQ data from persistence."""
        if self._persistence_path and os.path.exists(self._persistence_path):
            try:
                with open(self._persistence_path, 'r') as f:
                    data = json.load(f)
                    for name, config_data in data.get("queues", {}).items():
                        self._queues[name] = DLQConfig(**config_data)
                    for entry_data in data.get("entries", []):
                        entry = DLQEntry(
                            entry_id=entry_data["entry_id"],
                            original_topic=entry_data["original_topic"],
                            original_message_id=entry_data["original_message_id"],
                            payload=entry_data["payload"],
                            error_message=entry_data["error_message"],
                            error_category=ErrorCategory(entry_data["error_category"]),
                            error_timestamp=entry_data["error_timestamp"],
                            retry_count=entry_data.get("retry_count", 0),
                            status=DLQEntryStatus(entry_data.get("status", "pending")),
                            metadata=entry_data.get("metadata", {}),
                            stack_trace=entry_data.get("stack_trace")
                        )
                        self._entries[entry.original_topic].append(entry)
                        self._entry_index[entry.entry_id] = entry
            except (json.JSONDecodeError, TypeError, KeyError):
                pass
    
    def _persist(self) -> None:
        """Persist DLQ data."""
        if self._persistence_path:
            try:
                data = {
                    "queues": {name: vars(config) for name, config in self._queues.items()},
                    "entries": [
                        {
                            "entry_id": e.entry_id,
                            "original_topic": e.original_topic,
                            "original_message_id": e.original_message_id,
                            "payload": e.payload,
                            "error_message": e.error_message,
                            "error_category": e.error_category.value,
                            "error_timestamp": e.error_timestamp,
                            "retry_count": e.retry_count,
                            "status": e.status.value,
                            "metadata": e.metadata,
                            "stack_trace": e.stack_trace
                        }
                        for e in list(self._entry_index.values())[:1000]  # Limit persist size
                    ]
                }
                with open(self._persistence_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            except OSError:
                pass
    
    def _categorize_error(self, error_message: str, 
                          exception_type: Optional[str] = None) -> ErrorCategory:
        """Categorize an error based on message and type."""
        error_lower = error_message.lower()
        
        # Transient errors
        transient_keywords = ["timeout", "temporary", "connection refused", 
                              "network", "unavailable", "retry"]
        if any(kw in error_lower for kw in transient_keywords):
            return ErrorCategory.TRANSIENT
        
        # Data errors
        data_keywords = ["invalid", "malformed", "schema", "validation",
                         "not found", "missing", "empty"]
        if any(kw in error_lower for kw in data_keywords):
            return ErrorCategory.DATA
        
        # Resource errors
        resource_keywords = ["memory", "disk", "space", "cpu", "quota", "limit"]
        if any(kw in error_lower for kw in resource_keywords):
            return ErrorCategory.RESOURCE
        
        # Auth errors
        auth_keywords = ["auth", "permission", "denied", "unauthorized",
                         "forbidden", "token", "credential"]
        if any(kw in error_lower for kw in auth_keywords):
            return ErrorCategory.AUTHENTICATION
        
        # External service errors
        external_keywords = ["api", "external", "third party", "upstream",
                             "downstream", "service unavailable"]
        if any(kw in error_lower for kw in external_keywords):
            return ErrorCategory.EXTERNAL_SERVICE
        
        return ErrorCategory.UNKNOWN
    
    def create_queue(self, config: DLQConfig) -> None:
        """Create a new DLQ."""
        self._queues[config.name] = config
    
    def delete_queue(self, name: str) -> bool:
        """Delete a DLQ and all its entries."""
        if name in self._queues:
            del self._queues[name]
            self._entries[name].clear()
            # Remove indexed entries for this queue
            to_remove = [
                eid for eid, e in self._entry_index.items()
                if e.original_topic == name
            ]
            for eid in to_remove:
                del self._entry_index[eid]
            self._persist()
            return True
        return False
    
    def add_entry(
        self,
        source_topic: str,
        message_id: str,
        payload: Any,
        error_message: str,
        exception_type: Optional[str] = None,
        stack_trace: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        source_system: str = ""
    ) -> str:
        """Add an entry to the DLQ.
        
        Returns the entry ID.
        """
        entry_id = str(uuid.uuid4())
        
        # Check if message already in DLQ
        existing = [
            e for e in self._entries[source_topic]
            if e.original_message_id == message_id
        ]
        if existing:
            # Update existing entry
            existing[0].retry_count += 1
            existing[0].error_message = error_message
            existing[0].stack_trace = stack_trace
            existing[0].error_timestamp = time.time()
            self._persist()
            return existing[0].entry_id
        
        entry = DLQEntry(
            entry_id=entry_id,
            original_topic=source_topic,
            original_message_id=message_id,
            payload=payload,
            error_message=error_message,
            error_category=self._categorize_error(error_message, exception_type),
            error_timestamp=time.time(),
            stack_trace=stack_trace,
            metadata=metadata or {},
            source_system=source_system
        )
        
        self._entries[source_topic].append(entry)
        self._entry_index[entry_id] = entry
        
        # Check alert threshold
        queue_config = self._queues.get(source_topic)
        if queue_config and len(self._entries[source_topic]) >= queue_config.alert_threshold:
            self._trigger_alert(source_topic, len(self._entries[source_topic]))
        
        self._persist()
        return entry_id
    
    def get_entry(self, entry_id: str) -> Optional[DLQEntry]:
        """Get a DLQ entry by ID."""
        return self._entry_index.get(entry_id)
    
    def list_entries(
        self,
        source_topic: Optional[str] = None,
        status: Optional[DLQEntryStatus] = None,
        category: Optional[ErrorCategory] = None,
        limit: int = 100,
        offset: int = 0
    ) -> List[DLQEntry]:
        """List DLQ entries with optional filtering."""
        entries = list(self._entry_index.values())
        
        if source_topic:
            entries = [e for e in entries if e.original_topic == source_topic]
        if status:
            entries = [e for e in entries if e.status == status]
        if category:
            entries = [e for e in entries if e.error_category == category]
        
        # Sort by error timestamp (newest first)
        entries.sort(key=lambda e: e.error_timestamp, reverse=True)
        
        return entries[offset:offset + limit]
    
    def update_entry_status(
        self,
        entry_id: str,
        status: DLQEntryStatus,
        resolved_at: Optional[float] = None
    ) -> bool:
        """Update the status of a DLQ entry."""
        entry = self._entry_index.get(entry_id)
        if not entry:
            return False
        
        entry.status = status
        if resolved_at:
            entry.resolved_at = resolved_at
        elif status == DLQEntryStatus.RESOLVED:
            entry.resolved_at = time.time()
        
        self._persist()
        return True
    
    def schedule_retry(
        self,
        entry_id: str,
        retry_at: Optional[float] = None,
        delay_seconds: Optional[float] = None
    ) -> bool:
        """Schedule a retry for a DLQ entry."""
        entry = self._entry_index.get(entry_id)
        if not entry:
            return False
        
        entry.status = DLQEntryStatus.RETRY_SCHEDULED
        
        if retry_at:
            entry.next_retry_at = retry_at
        elif delay_seconds:
            entry.next_retry_at = time.time() + delay_seconds
        else:
            entry.next_retry_at = time.time() + 3600  # Default 1 hour
        
        self._persist()
        return True
    
    def retry_entry(self, entry_id: str, 
                    processor: Optional[Callable] = None) -> tuple[bool, Any]:
        """Attempt to retry processing a DLQ entry.
        
        Args:
            entry_id: The DLQ entry ID.
            processor: Optional custom processor function.
        
        Returns:
            Tuple of (success, result_or_error).
        """
        entry = self._entry_index.get(entry_id)
        if not entry:
            return False, "Entry not found"
        
        entry.status = DLQEntryStatus.RETRYING
        
        try:
            if processor:
                result = processor(entry.payload)
            else:
                # Simulate processing
                result = {"processed": True, "entry_id": entry_id}
            
            self.update_entry_status(entry_id, DLQEntryStatus.RESOLVED)
            return True, result
        
        except Exception as e:
            entry.retry_count += 1
            entry.error_message = str(e)
            
            if entry.retry_count >= entry.max_retries:
                entry.status = DLQEntryStatus.PENDING
                entry.next_retry_at = None
            else:
                entry.status = DLQEntryStatus.PENDING
                entry.next_retry_at = time.time() + (60 * (2 ** entry.retry_count))
            
            self._persist()
            return False, str(e)
    
    def discard_entry(self, entry_id: str) -> bool:
        """Discard a DLQ entry permanently."""
        entry = self._entry_index.get(entry_id)
        if not entry:
            return False
        
        entry.status = DLQEntryStatus.DISCARDED
        entry.resolved_at = time.time()
        self._persist()
        return True
    
    def purge_expired(self, source_topic: Optional[str] = None) -> int:
        """Remove expired entries from DLQ.
        
        Returns count of purged entries.
        """
        cutoff = time.time()
        purged = 0
        
        topics = [source_topic] if source_topic else list(self._entries.keys())
        
        for topic in topics:
            if topic not in self._queues:
                continue
            
            retention = self._queues[topic].retention_seconds
            queue_entries = self._entries[topic]
            new_entries = deque(maxlen=self._queues[topic].max_size)
            
            while queue_entries:
                entry = queue_entries.popleft()
                if entry.status != DLQEntryStatus.EXPIRED and \
                   cutoff - entry.error_timestamp < retention:
                    new_entries.append(entry)
                else:
                    entry.status = DLQEntryStatus.EXPIRED
                    self._entry_index.pop(entry.entry_id, None)
                    purged += 1
            
            self._entries[topic] = new_entries
        
        if purged > 0:
            self._persist()
        return purged
    
    def get_stats(self, source_topic: Optional[str] = None) -> Dict[str, Any]:
        """Get DLQ statistics."""
        if source_topic:
            entries = [e for e in self._entry_index.values()
                      if e.original_topic == source_topic]
            queue_config = self._queues.get(source_topic)
            return self._compute_stats(source_topic, entries, queue_config)
        
        stats = {}
        for topic in self._queues.keys():
            entries = [e for e in self._entry_index.values()
                      if e.original_topic == topic]
            stats[topic] = self._compute_stats(topic, entries, self._queues[topic])
        
        return stats
    
    def _compute_stats(self, topic: str, entries: List[DLQEntry],
                       config: Optional[DLQConfig]) -> Dict[str, Any]:
        """Compute statistics for a DLQ."""
        if not entries:
            return {
                "queue_name": topic,
                "total_entries": 0,
                "pending_count": 0
            }
        
        by_status = defaultdict(int)
        by_category = defaultdict(int)
        oldest = max(e.error_timestamp for e in entries)
        
        for e in entries:
            by_status[e.status.value] += 1
            by_category[e.error_category.value] += 1
        
        return {
            "queue_name": topic,
            "total_entries": len(entries),
            "pending_count": by_status.get("pending", 0),
            "retry_scheduled_count": by_status.get("retry_scheduled", 0),
            "retrying_count": by_status.get("retrying", 0),
            "resolved_count": by_status.get("resolved", 0),
            "discarded_count": by_status.get("discarded", 0),
            "expired_count": by_status.get("expired", 0),
            "entries_by_category": dict(by_category),
            "oldest_entry_age_seconds": time.time() - oldest,
            "last_activity_time": max(e.error_timestamp for e in entries),
            "alert_threshold": config.alert_threshold if config else 100,
            "is_throttling": (len(entries) >= config.alert_threshold) if config else False
        }
    
    def _trigger_alert(self, topic: str, count: int) -> None:
        """Trigger alert callbacks when threshold exceeded."""
        for callback in self._alert_callbacks:
            try:
                callback(topic, count)
            except Exception:
                pass
    
    def register_alert_callback(self, callback: Callable) -> None:
        """Register an alert callback."""
        self._alert_callbacks.append(callback)
    
    def register_retry_callback(self, topic: str, callback: Callable) -> None:
        """Register a retry processor for a topic."""
        self._retry_callbacks[topic] = callback


class DeadLetterQueueAction(BaseAction):
    """Manage dead letter queues for failed messages.
    
    Supports entry tracking, retry scheduling, error categorization,
    statistics, and alerting thresholds.
    """
    action_type = "dead_letter_queue"
    display_name = "死信队列"
    description = "管理死信队列，支持消息追踪、重试调度和错误分类"
    
    def __init__(self):
        super().__init__()
        self._manager: Optional[DLQManager] = None
    
    def _get_manager(self, params: Dict[str, Any]) -> DLQManager:
        """Get or create the DLQ manager."""
        if self._manager is None:
            persistence_path = params.get("persistence_path")
            self._manager = DLQManager(persistence_path)
        return self._manager
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute DLQ operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: "create_queue", "delete_queue", "add_entry",
                  "get_entry", "list_entries", "update_status", "schedule_retry",
                  "retry", "discard", "purge_expired", "get_stats"
                - For queue ops: name, source_topic, config
                - For entry ops: entry_id, source_topic, payload, error_message
                - For list: source_topic, status, category, limit
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get("operation", "")
        
        try:
            if operation == "create_queue":
                return self._create_queue(params)
            elif operation == "delete_queue":
                return self._delete_queue(params)
            elif operation == "add_entry":
                return self._add_entry(params)
            elif operation == "get_entry":
                return self._get_entry(params)
            elif operation == "list_entries":
                return self._list_entries(params)
            elif operation == "update_status":
                return self._update_status(params)
            elif operation == "schedule_retry":
                return self._schedule_retry(params)
            elif operation == "retry":
                return self._retry_entry(params)
            elif operation == "discard":
                return self._discard_entry(params)
            elif operation == "purge_expired":
                return self._purge_expired(params)
            elif operation == "get_stats":
                return self._get_stats(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"DLQ error: {str(e)}")
    
    def _create_queue(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new DLQ."""
        manager = self._get_manager(params)
        name = params.get("name", "")
        source_topic = params.get("source_topic", "")
        
        if not name or not source_topic:
            return ActionResult(success=False, message="name and source_topic are required")
        
        config = DLQConfig(
            name=name,
            source_topic=source_topic,
            max_size=params.get("max_size", 1000),
            retention_seconds=params.get("retention_seconds", 604800.0),
            auto_retry_enabled=params.get("auto_retry_enabled", False),
            alert_threshold=params.get("alert_threshold", 100),
            description=params.get("description", "")
        )
        manager.create_queue(config)
        return ActionResult(
            success=True,
            message=f"DLQ '{name}' created for topic '{source_topic}'",
            data={"name": name, "source_topic": source_topic}
        )
    
    def _delete_queue(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a DLQ."""
        manager = self._get_manager(params)
        name = params.get("name", "")
        if not name:
            return ActionResult(success=False, message="queue name is required")
        
        deleted = manager.delete_queue(name)
        return ActionResult(
            success=deleted,
            message=f"DLQ '{name}' deleted" if deleted else f"DLQ '{name}' not found"
        )
    
    def _add_entry(self, params: Dict[str, Any]) -> ActionResult:
        """Add an entry to DLQ."""
        manager = self._get_manager(params)
        source_topic = params.get("source_topic", "")
        message_id = params.get("message_id", str(uuid.uuid4()))
        payload = params.get("payload")
        error_message = params.get("error_message", "Unknown error")
        
        if not source_topic:
            return ActionResult(success=False, message="source_topic is required")
        
        entry_id = manager.add_entry(
            source_topic=source_topic,
            message_id=message_id,
            payload=payload,
            error_message=error_message,
            exception_type=params.get("exception_type"),
            stack_trace=params.get("stack_trace"),
            metadata=params.get("metadata"),
            source_system=params.get("source_system", "")
        )
        return ActionResult(
            success=True,
            message=f"Entry added to DLQ: {entry_id}",
            data={"entry_id": entry_id, "source_topic": source_topic}
        )
    
    def _get_entry(self, params: Dict[str, Any]) -> ActionResult:
        """Get a DLQ entry."""
        manager = self._get_manager(params)
        entry_id = params.get("entry_id", "")
        
        if not entry_id:
            return ActionResult(success=False, message="entry_id is required")
        
        entry = manager.get_entry(entry_id)
        if not entry:
            return ActionResult(success=False, message=f"Entry '{entry_id}' not found")
        
        return ActionResult(
            success=True,
            message=f"Entry retrieved: {entry_id}",
            data={
                "entry_id": entry.entry_id,
                "original_topic": entry.original_topic,
                "status": entry.status.value,
                "error_category": entry.error_category.value,
                "error_message": entry.error_message,
                "retry_count": entry.retry_count
            }
        )
    
    def _list_entries(self, params: Dict[str, Any]) -> ActionResult:
        """List DLQ entries."""
        manager = self._get_manager(params)
        source_topic = params.get("source_topic")
        status = params.get("status")
        category = params.get("category")
        limit = params.get("limit", 100)
        
        if status:
            status = DLQEntryStatus(status)
        if category:
            category = ErrorCategory(category)
        
        entries = manager.list_entries(source_topic, status, category, limit)
        return ActionResult(
            success=True,
            message=f"Found {len(entries)} entries",
            data={
                "entries": [
                    {"entry_id": e.entry_id, "original_topic": e.original_topic,
                     "status": e.status.value, "error_category": e.error_category.value,
                     "retry_count": e.retry_count, "error_timestamp": e.error_timestamp}
                    for e in entries
                ]
            }
        )
    
    def _update_status(self, params: Dict[str, Any]) -> ActionResult:
        """Update DLQ entry status."""
        manager = self._get_manager(params)
        entry_id = params.get("entry_id", "")
        status = params.get("status", "")
        
        if not entry_id or not status:
            return ActionResult(success=False, message="entry_id and status are required")
        
        status_enum = DLQEntryStatus(status)
        updated = manager.update_entry_status(entry_id, status_enum)
        return ActionResult(
            success=updated,
            message=f"Entry status updated to '{status}'" if updated else f"Entry '{entry_id}' not found"
        )
    
    def _schedule_retry(self, params: Dict[str, Any]) -> ActionResult:
        """Schedule retry for an entry."""
        manager = self._get_manager(params)
        entry_id = params.get("entry_id", "")
        delay_seconds = params.get("delay_seconds")
        
        if not entry_id:
            return ActionResult(success=False, message="entry_id is required")
        
        scheduled = manager.schedule_retry(entry_id, delay_seconds=delay_seconds)
        return ActionResult(
            success=scheduled,
            message="Retry scheduled" if scheduled else f"Entry '{entry_id}' not found"
        )
    
    def _retry_entry(self, params: Dict[str, Any]) -> ActionResult:
        """Retry processing an entry."""
        manager = self._get_manager(params)
        entry_id = params.get("entry_id", "")
        
        if not entry_id:
            return ActionResult(success=False, message="entry_id is required")
        
        success, result = manager.retry_entry(entry_id)
        return ActionResult(
            success=success,
            message="Retry succeeded" if success else f"Retry failed: {result}",
            data={"success": success, "result": result}
        )
    
    def _discard_entry(self, params: Dict[str, Any]) -> ActionResult:
        """Discard a DLQ entry."""
        manager = self._get_manager(params)
        entry_id = params.get("entry_id", "")
        
        if not entry_id:
            return ActionResult(success=False, message="entry_id is required")
        
        discarded = manager.discard_entry(entry_id)
        return ActionResult(
            success=discarded,
            message="Entry discarded" if discarded else f"Entry '{entry_id}' not found"
        )
    
    def _purge_expired(self, params: Dict[str, Any]) -> ActionResult:
        """Purge expired entries."""
        manager = self._get_manager(params)
        source_topic = params.get("source_topic")
        
        purged = manager.purge_expired(source_topic)
        return ActionResult(
            success=True,
            message=f"Purged {purged} expired entries"
        )
    
    def _get_stats(self, params: Dict[str, Any]) -> ActionResult:
        """Get DLQ statistics."""
        manager = self._get_manager(params)
        source_topic = params.get("source_topic")
        
        stats = manager.get_stats(source_topic)
        return ActionResult(
            success=True,
            message="Stats retrieved",
            data={"stats": stats}
        )
