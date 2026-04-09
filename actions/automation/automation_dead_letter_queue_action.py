"""Automation Dead Letter Queue Action Module.

Provides dead letter queue handling for failed automation tasks
including error tracking, retry management, and dead letter processing.

Example:
    >>> from actions.automation.automation_dead_letter_queue_action import AutomationDeadLetterQueueAction
    >>> action = AutomationDeadLetterQueueAction()
    >>> action.push_failed_task(task_id, error)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional
import json
import threading
import time
import uuid


class FailureReason(Enum):
    """Reason for task failure."""
    TIMEOUT = "timeout"
    VALIDATION_ERROR = "validation_error"
    DEPENDENCY_FAILED = "dependency_failed"
    RESOURCE_UNAVAILABLE = "resource_unavailable"
    MAX_RETRIES = "max_retries"
    UNKNOWN = "unknown"


class DLQStatus(Enum):
    """Status of dead letter item."""
    PENDING = "pending"
    UNDER_REVIEW = "under_review"
    RETRY_SCHEDULED = "retry_scheduled"
    RESOLVED = "resolved"
    DISCARDED = "discarded"


@dataclass
class FailedTask:
    """Failed task in dead letter queue.
    
    Attributes:
        task_id: Unique task identifier
        original_task: Original task data
        error: Error message
        failure_reason: Reason for failure
        attempts: Number of attempts made
        failed_at: Failure timestamp
        last_retry_at: Last retry timestamp
    """
    task_id: str
    original_task: Dict[str, Any]
    error: str
    failure_reason: FailureReason
    attempts: int = 0
    failed_at: float = field(default_factory=time.time)
    last_retry_at: Optional[float] = None
    stack_trace: Optional[str] = None
    context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DLQConfig:
    """Configuration for dead letter queue.
    
    Attributes:
        max_size: Maximum queue size
        retention_days: Days to retain failed tasks
        auto_retry_enabled: Enable automatic retry
        max_retry_attempts: Maximum retry attempts
        retry_backoff: Backoff multiplier for retries
    """
    max_size: int = 10000
    retention_days: int = 30
    auto_retry_enabled: bool = True
    max_retry_attempts: int = 3
    retry_backoff: float = 2.0


@dataclass
class DLQStats:
    """Dead letter queue statistics.
    
    Attributes:
        total_failed: Total failed tasks
        pending_count: Pending tasks count
        resolved_count: Resolved tasks count
        avg_retry_time: Average retry time
    """
    total_failed: int
    pending_count: int
    resolved_count: int
    discarded_count: int
    avg_retry_time: float


class AutomationDeadLetterQueueAction:
    """Dead letter queue handler for automation tasks.
    
    Manages failed tasks with tracking, retry scheduling,
    and configurable resolution workflows.
    
    Attributes:
        config: DLQ configuration
        _queue: Failed task queue
        _retry_schedule: Scheduled retries
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[DLQConfig] = None,
    ) -> None:
        """Initialize dead letter queue action.
        
        Args:
            config: DLQ configuration
        """
        self.config = config or DLQConfig()
        self._queue: Dict[str, FailedTask] = {}
        self._retry_schedule: Dict[str, float] = {}
        self._resolved: set = set()
        self._discarded: set = set()
        self._lock = threading.RLock()
        self._total_failed = 0
    
    def push_failed_task(
        self,
        task_id: str,
        error: Exception,
        original_task: Dict[str, Any],
        failure_reason: Optional[FailureReason] = None,
        context: Optional[Dict[str, Any]] = None,
        stack_trace: Optional[str] = None,
    ) -> str:
        """Push failed task to dead letter queue.
        
        Args:
            task_id: Unique task identifier
            error: Exception that caused failure
            original_task: Original task data
            failure_reason: Reason for failure
            context: Additional context
            stack_trace: Error stack trace
        
        Returns:
            DLQ item ID
        """
        dlq_id = str(uuid.uuid4())
        
        reason = failure_reason or self._classify_error(error)
        
        failed_task = FailedTask(
            task_id=task_id,
            original_task=original_task,
            error=str(error),
            failure_reason=reason,
            context=context or {},
            stack_trace=stack_trace,
        )
        
        with self._lock:
            if len(self._queue) >= self.config.max_size:
                self._evict_oldest()
            
            self._queue[dlq_id] = failed_task
            self._total_failed += 1
        
        return dlq_id
    
    def _evict_oldest(self) -> None:
        """Evict oldest item from queue."""
        if not self._queue:
            return
        
        oldest_key = min(
            self._queue.keys(),
            key=lambda k: self._queue[k].failed_at,
        )
        del self._queue[oldest_key]
    
    def get_item(self, dlq_id: str) -> Optional[FailedTask]:
        """Get failed task by ID.
        
        Args:
            dlq_id: Dead letter queue item ID
        
        Returns:
            FailedTask if found
        """
        with self._lock:
            return self._queue.get(dlq_id)
    
    def get_pending_tasks(
        self,
        limit: int = 100,
        failure_reason: Optional[FailureReason] = None,
    ) -> List[FailedTask]:
        """Get pending failed tasks.
        
        Args:
            limit: Maximum tasks to return
            failure_reason: Filter by failure reason
        
        Returns:
            List of failed tasks
        """
        with self._lock:
            tasks = [
                t for t in self._queue.values()
                if t.failed_at not in self._resolved
                and t.task_id not in self._resolved
            ]
            
            if failure_reason:
                tasks = [
                    t for t in tasks
                    if t.failure_reason == failure_reason
                ]
            
            tasks.sort(key=lambda t: t.failed_at)
            return tasks[:limit]
    
    def schedule_retry(
        self,
        dlq_id: str,
        retry_func: Callable[[], Any],
    ) -> bool:
        """Schedule retry for failed task.
        
        Args:
            dlq_id: Dead letter queue item ID
            retry_func: Function to retry
        
        Returns:
            True if retry scheduled
        """
        with self._lock:
            if dlq_id not in self._queue:
                return False
            
            task = self._queue[dlq_id]
            
            if task.attempts >= self.config.max_retry_attempts:
                return False
            
            delay = self.config.retry_backoff ** task.attempts
            retry_time = time.time() + delay
            
            self._retry_schedule[dlq_id] = retry_time
            task.attempts += 1
            task.last_retry_at = time.time()
            
            return True
    
    def resolve_item(self, dlq_id: str) -> bool:
        """Mark item as resolved.
        
        Args:
            dlq_id: Dead letter queue item ID
        
        Returns:
            True if resolved
        """
        with self._lock:
            if dlq_id not in self._queue:
                return False
            
            self._resolved.add(dlq_id)
            return True
    
    def discard_item(self, dlq_id: str) -> bool:
        """Discard item permanently.
        
        Args:
            dlq_id: Dead letter queue item ID
        
        Returns:
            True if discarded
        """
        with self._lock:
            if dlq_id not in self._queue:
                return False
            
            self._discarded.add(dlq_id)
            return True
    
    def get_due_retries(self) -> List[str]:
        """Get DLQ IDs with due retries.
        
        Returns:
            List of DLQ IDs
        """
        current_time = time.time()
        
        with self._lock:
            return [
                dlq_id for dlq_id, retry_time
                in self._retry_schedule.items()
                if retry_time <= current_time
            ]
    
    def get_stats(self) -> DLQStats:
        """Get DLQ statistics.
        
        Returns:
            DLQStats
        """
        with self._lock:
            pending = [
                t for t in self._queue.values()
                if t.task_id not in self._resolved
                and t.task_id not in self._discarded
            ]
            
            retry_times = [
                t.last_retry_at - t.failed_at
                for t in self._queue.values()
                if t.last_retry_at
            ]
            
            return DLQStats(
                total_failed=self._total_failed,
                pending_count=len(pending),
                resolved_count=len(self._resolved),
                discarded_count=len(self._discarded),
                avg_retry_time=(
                    sum(retry_times) / len(retry_times)
                    if retry_times else 0.0
                ),
            )
    
    def cleanup_expired(self) -> int:
        """Clean up expired items based on retention policy.
        
        Returns:
            Number of items cleaned
        """
        cutoff_time = time.time() - (self.config.retention_days * 86400)
        cleaned = 0
        
        with self._lock:
            to_remove = [
                dlq_id for dlq_id, task in self._queue.items()
                if task.failed_at < cutoff_time
            ]
            
            for dlq_id in to_remove:
                del self._queue[dlq_id]
                cleaned += 1
            
            self._resolved.clear()
            self._discarded.clear()
        
        return cleaned
    
    def _classify_error(
        self,
        error: Exception,
    ) -> FailureReason:
        """Classify error type.
        
        Args:
            error: Exception
        
        Returns:
            FailureReason
        """
        error_str = str(error).lower()
        
        if "timeout" in error_str:
            return FailureReason.TIMEOUT
        if "valid" in error_str:
            return FailureReason.VALIDATION_ERROR
        if "depend" in error_str or "precursor" in error_str:
            return FailureReason.DEPENDENCY_FAILED
        if "resource" in error_str or "unavailable" in error_str:
            return FailureReason.RESOURCE_UNAVAILABLE
        if "retry" in error_str or "max" in error_str:
            return FailureReason.MAX_RETRIES
        
        return FailureReason.UNKNOWN
