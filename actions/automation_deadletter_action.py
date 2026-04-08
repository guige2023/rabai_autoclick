"""Automation dead letter action module for RabAI AutoClick.

Provides dead letter queue handling for failed automation:
- DeadLetterQueue: Store failed automation tasks
- DeadLetterProcessor: Process dead letter items
- RetryScheduler: Schedule retries for dead letter items
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
import json
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict, deque

import sys
import os
_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class DeadLetterReason(Enum):
    """Reasons for dead letter."""
    MAX_RETRIES_EXCEEDED = "max_retries_exceeded"
    TIMEOUT = "timeout"
    INVALID_INPUT = "invalid_input"
    SYSTEM_ERROR = "system_error"
    DEPENDENCY_FAILURE = "dependency_failure"


@dataclass
class DeadLetterItem:
    """Dead letter queue item."""
    item_id: str
    original_payload: Any
    reason: DeadLetterReason
    error_message: str
    attempt_count: int
    created_at: float = field(default_factory=time.time)
    last_attempt: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DeadLetterConfig:
    """Configuration for dead letter queue."""
    max_items: int = 10000
    retention_time: float = 86400.0
    auto_retry: bool = False
    retry_delay: float = 3600.0
    max_retries: int = 3


class DeadLetterQueue:
    """Dead letter queue for failed automation items."""
    
    def __init__(self, name: str, config: DeadLetterConfig):
        self.name = name
        self.config = config
        self._queue: deque = deque(maxlen=config.max_items)
        self._by_id: Dict[str, DeadLetterItem] = {}
        self._lock = threading.RLock()
        self._stats = {"total_added": 0, "total_processed": 0, "total_retried": 0, "total_discarded": 0}
    
    def add(self, item_id: str, payload: Any, reason: DeadLetterReason, error_message: str, attempt_count: int = 0, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Add item to dead letter queue."""
        item = DeadLetterItem(
            item_id=item_id,
            original_payload=payload,
            reason=reason,
            error_message=error_message,
            attempt_count=attempt_count,
            metadata=metadata or {}
        )
        
        with self._lock:
            if item_id in self._by_id:
                return False
            
            self._queue.append(item)
            self._by_id[item_id] = item
            self._stats["total_added"] += 1
            return True
    
    def get(self, item_id: str) -> Optional[DeadLetterItem]:
        """Get dead letter item by ID."""
        with self._lock:
            return self._by_id.get(item_id)
    
    def get_all(self) -> List[DeadLetterItem]:
        """Get all dead letter items."""
        with self._lock:
            return list(self._queue)
    
    def get_by_reason(self, reason: DeadLetterReason) -> List[DeadLetterItem]:
        """Get dead letter items by reason."""
        with self._lock:
            return [item for item in self._queue if item.reason == reason]
    
    def process(self, item_id: str, processor: Callable) -> Tuple[bool, Any]:
        """Process a dead letter item."""
        item = self.get(item_id)
        if not item:
            return False, None
        
        try:
            result = processor(item)
            with self._lock:
                self._stats["total_processed"] += 1
            return True, result
        except Exception as e:
            return False, str(e)
    
    def retry(self, item_id: str) -> Tuple[bool, Optional[Any]]:
        """Schedule retry for item."""
        item = self.get(item_id)
        if not item:
            return False, None
        
        with self._lock:
            self._stats["total_retried"] += 1
        
        return True, item.original_payload
    
    def discard(self, item_id: str) -> bool:
        """Discard a dead letter item."""
        with self._lock:
            if item_id not in self._by_id:
                return False
            
            self._by_id.pop(item_id)
            self._queue = deque([i for i in self._queue if i.item_id != item_id], maxlen=self.config.max_items)
            self._stats["total_discarded"] += 1
            return True
    
    def cleanup(self) -> int:
        """Remove expired items."""
        cutoff = time.time() - self.config.retention_time
        removed = 0
        
        with self._lock:
            to_remove = [item for item in self._queue if item.created_at < cutoff]
            for item in to_remove:
                self._by_id.pop(item.item_id, None)
                removed += 1
            
            self._queue = deque([i for i in self._queue if i.item_id not in [x.item_id for x in to_remove]], maxlen=self.config.max_items)
        
        return removed
    
    def get_stats(self) -> Dict[str, Any]:
        """Get dead letter statistics."""
        with self._lock:
            by_reason = defaultdict(int)
            for item in self._queue:
                by_reason[item.reason.value] += 1
            
            return {
                "name": self.name,
                "total_items": len(self._queue),
                "by_reason": dict(by_reason),
                **{k: v for k, v in self._stats.items()},
            }


class AutomationDeadLetterAction(BaseAction):
    """Automation dead letter action."""
    action_type = "automation_deadletter"
    display_name = "自动化死信"
    description = "自动化死信队列处理"
    
    def __init__(self):
        super().__init__()
        self._queues: Dict[str, DeadLetterQueue] = {}
        self._lock = threading.Lock()
    
    def _get_queue(self, name: str, config: DeadLetterConfig) -> DeadLetterQueue:
        """Get or create dead letter queue."""
        with self._lock:
            if name not in self._queues:
                self._queues[name] = DeadLetterQueue(name, config)
            return self._queues[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute dead letter operation."""
        try:
            queue_name = params.get("queue", "default")
            command = params.get("command", "add")
            
            config = DeadLetterConfig(
                max_items=params.get("max_items", 10000),
                retention_time=params.get("retention_time", 86400.0),
                auto_retry=params.get("auto_retry", False),
            )
            
            dlq = self._get_queue(queue_name, config)
            
            if command == "add":
                item_id = params.get("item_id")
                payload = params.get("payload")
                reason_str = params.get("reason", "system_error").upper()
                
                try:
                    reason = DeadLetterReason[reason_str]
                except KeyError:
                    reason = DeadLetterReason.SYSTEM_ERROR
                
                error_message = params.get("error_message", "")
                success = dlq.add(item_id, payload, reason, error_message)
                return ActionResult(success=success)
            
            elif command == "get":
                item_id = params.get("item_id")
                item = dlq.get(item_id)
                if item:
                    return ActionResult(success=True, data={"item_id": item.item_id, "reason": item.reason.value, "payload": item.original_payload})
                return ActionResult(success=False, message="Item not found")
            
            elif command == "list":
                items = dlq.get_all()
                return ActionResult(success=True, data={"items": [{"id": i.item_id, "reason": i.reason.value} for i in items]})
            
            elif command == "retry":
                item_id = params.get("item_id")
                success, payload = dlq.retry(item_id)
                return ActionResult(success=success, data={"payload": payload})
            
            elif command == "discard":
                item_id = params.get("item_id")
                success = dlq.discard(item_id)
                return ActionResult(success=success)
            
            elif command == "cleanup":
                removed = dlq.cleanup()
                return ActionResult(success=True, data={"removed": removed})
            
            elif command == "stats":
                stats = dlq.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationDeadLetterAction error: {str(e)}")
