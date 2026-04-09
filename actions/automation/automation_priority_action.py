"""Automation Priority Action Module.

Provides priority-based task scheduling for automation workflows
including weighted priority queues and priority inheritance.

Example:
    >>> from actions.automation.automation_priority_action import AutomationPriorityAction
    >>> action = AutomationPriorityAction()
    >>> await action.enqueue(task, priority=10)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from heapq import heappush, heappop
from typing import Any, Callable, Dict, List, Optional
import threading
import time
import uuid


class PriorityLevel(Enum):
    """Standard priority levels."""
    CRITICAL = 0
    HIGH = 25
    NORMAL = 50
    LOW = 75
    BACKGROUND = 100


@dataclass
class PrioritizedTask:
    """Task with priority.
    
    Attributes:
        task_id: Unique task identifier
        priority: Priority value (lower = higher)
        data: Task data
        created_at: Creation timestamp
        scheduled_at: Scheduled execution time
        weight: Task weight for tie-breaking
        metadata: Additional metadata
    """
    task_id: str
    priority: int
    data: Dict[str, Any]
    created_at: float
    scheduled_at: float
    weight: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PriorityConfig:
    """Configuration for priority scheduling.
    
    Attributes:
        default_priority: Default priority level
        aging_factor: Priority aging factor per minute
        max_priority: Maximum priority value
        enable_aging: Enable priority aging
        enable_weighting: Enable weight-based scheduling
    """
    default_priority: int = PriorityLevel.NORMAL.value
    aging_factor: float = 0.5
    max_priority: int = 1000
    enable_aging: bool = True
    enable_weighting: bool = True


@dataclass
class PriorityStats:
    """Priority queue statistics.
    
    Attributes:
        total_enqueued: Total tasks enqueued
        total_dequeued: Total tasks dequeued
        avg_wait_time: Average wait time
        priority_distribution: Priority distribution
    """
    total_enqueued: int
    total_dequeued: int
    avg_wait_time: float
    queue_size: int
    priority_distribution: Dict[int, int]


class AutomationPriorityAction:
    """Priority scheduler for automation tasks.
    
    Manages task prioritization with aging support,
    weighted scheduling, and configurable priority levels.
    
    Attributes:
        config: Priority configuration
        _queue: Priority queue heap
        _metadata: Task metadata
        _lock: Thread safety lock
    """
    
    def __init__(
        self,
        config: Optional[PriorityConfig] = None,
    ) -> None:
        """Initialize priority action.
        
        Args:
            config: Priority configuration
        """
        self.config = config or PriorityConfig()
        self._queue: List[PrioritizedTask] = []
        self._metadata: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.RLock()
        self._total_enqueued = 0
        self._total_dequeued = 0
        self._wait_times: List[float] = []
    
    async def enqueue(
        self,
        data: Dict[str, Any],
        priority: Optional[int] = None,
        scheduled_at: Optional[float] = None,
        weight: float = 1.0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Enqueue task with priority.
        
        Args:
            data: Task data
            priority: Priority level
            scheduled_at: Scheduled execution time
            weight: Task weight
            metadata: Additional metadata
        
        Returns:
            Task ID
        """
        task_id = str(uuid.uuid4())
        priority = priority or self.config.default_priority
        
        task = PrioritizedTask(
            task_id=task_id,
            priority=priority,
            data=data,
            created_at=time.time(),
            scheduled_at=scheduled_at or time.time(),
            weight=weight,
            metadata=metadata or {},
        )
        
        with self._lock:
            heappush(self._queue, task)
            self._metadata[task_id] = {
                "priority": priority,
                "weight": weight,
                "metadata": metadata or {},
            }
            self._total_enqueued += 1
        
        return task_id
    
    async def dequeue(self) -> Optional[PrioritizedTask]:
        """Dequeue highest priority task.
        
        Returns:
            PrioritizedTask if available
        """
        with self._lock:
            current_time = time.time()
            
            while self._queue:
                task = heappop(self._queue)
                
                if task.scheduled_at > current_time:
                    heappush(self._queue, task)
                    return None
                
                wait_time = current_time - task.created_at
                self._wait_times.append(wait_time)
                self._total_dequeued += 1
                
                if len(self._wait_times) > 10000:
                    self._wait_times = self._wait_times[-5000:]
                
                return task
            
            return None
    
    async def peek(self) -> Optional[PrioritizedTask]:
        """View highest priority task without removing.
        
        Returns:
            PrioritizedTask if available
        """
        with self._lock:
            current_time = time.time()
            
            for task in self._queue:
                if task.scheduled_at <= current_time:
                    return task
            
            return self._queue[0] if self._queue else None
    
    async def update_priority(
        self,
        task_id: str,
        new_priority: int,
    ) -> bool:
        """Update task priority.
        
        Args:
            task_id: Task identifier
            new_priority: New priority value
        
        Returns:
            True if updated
        """
        with self._lock:
            for i, task in enumerate(self._queue):
                if task.task_id == task_id:
                    new_task = PrioritizedTask(
                        task_id=task.task_id,
                        priority=new_priority,
                        data=task.data,
                        created_at=task.created_at,
                        scheduled_at=task.scheduled_at,
                        weight=task.weight,
                        metadata=task.metadata,
                    )
                    
                    self._queue[i] = new_task
                    self._metadata[task_id]["priority"] = new_priority
                    
                    return True
            
            return False
    
    async def remove(self, task_id: str) -> bool:
        """Remove task from queue.
        
        Args:
            task_id: Task identifier
        
        Returns:
            True if removed
        """
        with self._lock:
            original_len = len(self._queue)
            
            self._queue = [
                t for t in self._queue
                if t.task_id != task_id
            ]
            
            if task_id in self._metadata:
                del self._metadata[task_id]
            
            return len(self._queue) < original_len
    
    async def get_task(self, task_id: str) -> Optional[PrioritizedTask]:
        """Get task by ID.
        
        Args:
            task_id: Task identifier
        
        Returns:
            PrioritizedTask if found
        """
        with self._lock:
            for task in self._queue:
                if task.task_id == task_id:
                    return task
            return None
    
    async def get_pending_count(self, min_priority: int = 0) -> int:
        """Get count of pending tasks at or above priority.
        
        Args:
            min_priority: Minimum priority threshold
        
        Returns:
            Count of pending tasks
        """
        with self._lock:
            current_time = time.time()
            return sum(
                1 for t in self._queue
                if t.priority <= min_priority and t.scheduled_at <= current_time
            )
    
    async def apply_aging(self) -> int:
        """Apply priority aging to queued tasks.
        
        Returns:
            Number of tasks aged
        """
        if not self.config.enable_aging:
            return 0
        
        current_time = time.time()
        aged_count = 0
        
        with self._lock:
            for i, task in enumerate(self._queue):
                age_minutes = (current_time - task.created_at) / 60
                
                if age_minutes > 0:
                    aged_priority = max(
                        0,
                        task.priority - int(age_minutes * self.config.aging_factor)
                    )
                    
                    if aged_priority < task.priority:
                        new_task = PrioritizedTask(
                            task_id=task.task_id,
                            priority=aged_priority,
                            data=task.data,
                            created_at=task.created_at,
                            scheduled_at=task.scheduled_at,
                            weight=task.weight,
                            metadata=task.metadata,
                        )
                        
                        self._queue[i] = new_task
                        aged_count += 1
        
        return aged_count
    
    def get_stats(self) -> PriorityStats:
        """Get priority queue statistics.
        
        Returns:
            PriorityStats
        """
        with self._lock:
            priority_dist: Dict[int, int] = {}
            for task in self._queue:
                priority_dist[task.priority] = (
                    priority_dist.get(task.priority, 0) + 1
                )
            
            avg_wait = (
                sum(self._wait_times) / len(self._wait_times)
                if self._wait_times else 0.0
            )
            
            return PriorityStats(
                total_enqueued=self._total_enqueued,
                total_dequeued=self._total_dequeued,
                avg_wait_time=avg_wait,
                queue_size=len(self._queue),
                priority_distribution=priority_dist,
            )
