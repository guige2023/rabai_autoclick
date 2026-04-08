"""Automation priority action module for RabAI AutoClick.

Provides priority-based task scheduling for automation:
- TaskPriorityScheduler: Schedule automation tasks by priority
- PriorityBasedExecutor: Execute automation tasks with priority awareness
- TaskQueueManager: Manage prioritized automation queues
"""

from typing import Any, Callable, Dict, List, Optional, Tuple
import time
import threading
import logging
import heapq
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class TaskPriority(Enum):
    """Task priority levels."""
    CRITICAL = 0
    URGENT = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    DEFERRED = 5


@dataclass
class AutomationTask:
    """Automation task definition."""
    task_id: str
    name: str
    action: Callable
    priority: TaskPriority = TaskPriority.NORMAL
    deadline: Optional[float] = None
    timeout: float = 60.0
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    scheduled_at: Optional[float] = None


@dataclass
class ScheduledTask:
    """Scheduled task wrapper for heap queue."""
    priority: int
    deadline: Optional[float]
    created_at: float
    task: AutomationTask
    
    def __lt__(self, other: "ScheduledTask") -> bool:
        if self.priority != other.priority:
            return self.priority < other.priority
        if self.deadline is not None and other.deadline is not None:
            if abs(self.deadline - other.deadline) > 0.001:
                return self.deadline < other.deadline
        return self.created_at < other.created_at


@dataclass
class PrioritySchedulerConfig:
    """Configuration for priority scheduler."""
    max_queue_size: int = 1000
    default_timeout: float = 60.0
    default_retry: int = 0
    deadline_aware: bool = True
    aging_enabled: bool = True
    aging_interval: float = 300.0
    aging_factor: float = 1.0
    max_concurrent: int = 5


class TaskPriorityScheduler:
    """Schedule automation tasks by priority."""
    
    def __init__(self, name: str, config: Optional[PrioritySchedulerConfig] = None):
        self.name = name
        self.config = config or PrioritySchedulerConfig()
        self._heap: List[ScheduledTask] = []
        self._tasks: Dict[str, AutomationTask] = {}
        self._running_tasks: Dict[str, Any] = {}
        self._completed_tasks: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._not_empty = threading.Condition(self._lock)
        self._running = False
        self._executor_thread: Optional[threading.Thread] = None
        self._stats = {"total_scheduled": 0, "total_completed": 0, "total_failed": 0, "total_cancelled": 0}
    
    def schedule(self, task: AutomationTask) -> bool:
        """Schedule a task."""
        with self._lock:
            if len(self._heap) >= self.config.max_queue_size:
                return False
            
            self._tasks[task.task_id] = task
            
            deadline = None
            if self.config.deadline_aware and task.deadline:
                deadline = task.deadline
            
            scheduled = ScheduledTask(
                priority=task.priority.value,
                deadline=deadline,
                created_at=task.created_at,
                task=task,
            )
            
            heapq.heappush(self._heap, scheduled)
            self._stats["total_scheduled"] += 1
            self._not_empty.notify()
            
            return True
    
    def unschedule(self, task_id: str) -> bool:
        """Unschedule a task."""
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                return True
            for i, st in enumerate(self._heap):
                if st.task.task_id == task_id:
                    self._heap.pop(i)
                    heapq.heapify(self._heap)
                    return True
            return False
    
    def _get_next_task(self, timeout: Optional[float] = None) -> Optional[ScheduledTask]:
        """Get next task from queue."""
        with self._not_empty:
            if timeout is None:
                while not self._heap:
                    self._not_empty.wait()
            else:
                end_time = time.time() + timeout
                while not self._heap:
                    remaining = end_time - time.time()
                    if remaining <= 0:
                        return None
                    self._not_empty.wait(remaining)
            
            if self._heap:
                return heapq.heappop(self._heap)
        return None
    
    def _execute_task(self, task: AutomationTask) -> Tuple[bool, Any]:
        """Execute a single task."""
        for attempt in range(task.retry_count + 1):
            try:
                result = task.action()
                return True, result
            except Exception as e:
                if attempt < task.retry_count:
                    time.sleep(1.0 * (attempt + 1))
                else:
                    return False, e
        return False, None
    
    def start(self):
        """Start scheduler executor."""
        with self._lock:
            if self._running:
                return
            self._running = True
            self._executor_thread = threading.Thread(target=self._executor_loop, daemon=True)
            self._executor_thread.start()
    
    def stop(self):
        """Stop scheduler executor."""
        with self._lock:
            self._running = False
            self._not_empty.notify_all()
            if self._executor_thread:
                self._executor_thread.join(timeout=5.0)
    
    def _executor_loop(self):
        """Main executor loop."""
        while self._running:
            scheduled_task = self._get_next_task(timeout=1.0)
            if scheduled_task is None:
                continue
            
            task = scheduled_task.task
            
            with self._lock:
                self._running_tasks[task.task_id] = time.time()
            
            success, result = self._execute_task(task)
            
            with self._lock:
                self._running_tasks.pop(task.task_id, None)
                self._completed_tasks[task.task_id] = {"success": success, "result": result, "completed_at": time.time()}
                
                if success:
                    self._stats["total_completed"] += 1
                else:
                    self._stats["total_failed"] += 1
    
    def get_pending_count(self) -> int:
        """Get number of pending tasks."""
        with self._lock:
            return len(self._heap)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scheduler statistics."""
        with self._lock:
            priority_counts = defaultdict(int)
            for st in self._heap:
                priority_counts[st.task.priority.name] += 1
            
            return {
                "name": self.name,
                "pending_tasks": len(self._heap),
                "running_tasks": len(self._running_tasks),
                "completed_tasks": len(self._completed_tasks),
                "priority_distribution": dict(priority_counts),
                **{k: v for k, v in self._stats.items()},
            }


class AutomationPriorityAction(BaseAction):
    """Automation priority action."""
    action_type = "automation_priority"
    display_name = "自动化优先级"
    description = "自动化任务优先级调度"
    
    def __init__(self):
        super().__init__()
        self._schedulers: Dict[str, TaskPriorityScheduler] = {}
        self._lock = threading.Lock()
    
    def _get_scheduler(self, name: str, config: Optional[PrioritySchedulerConfig] = None) -> TaskPriorityScheduler:
        """Get or create scheduler."""
        with self._lock:
            if name not in self._schedulers:
                self._schedulers[name] = TaskPriorityScheduler(name, config)
            return self._schedulers[name]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute priority operation."""
        try:
            scheduler_name = params.get("scheduler", "default")
            command = params.get("command", "schedule")
            
            config = PrioritySchedulerConfig(
                deadline_aware=params.get("deadline_aware", True),
                aging_enabled=params.get("aging_enabled", True),
                max_concurrent=params.get("max_concurrent", 5),
            )
            
            scheduler = self._get_scheduler(scheduler_name, config)
            
            if command == "schedule":
                task_id = params.get("task_id")
                name = params.get("name", task_id)
                action = params.get("action")
                priority_str = params.get("priority", "normal").upper()
                
                try:
                    priority = TaskPriority[priority_str]
                except KeyError:
                    priority = TaskPriority.NORMAL
                
                if not task_id or not action:
                    return ActionResult(success=False, message="task_id and action required")
                
                task = AutomationTask(
                    task_id=task_id,
                    name=name,
                    action=action,
                    priority=priority,
                    deadline=params.get("deadline"),
                    timeout=params.get("timeout", 60.0),
                    retry_count=params.get("retry_count", 0),
                )
                
                success = scheduler.schedule(task)
                return ActionResult(success=success, message=f"Task {task_id} scheduled" if success else "Queue full")
            
            elif command == "unschedule":
                task_id = params.get("task_id")
                success = scheduler.unschedule(task_id)
                return ActionResult(success=success)
            
            elif command == "start":
                scheduler.start()
                return ActionResult(success=True, message="Scheduler started")
            
            elif command == "stop":
                scheduler.stop()
                return ActionResult(success=True, message="Scheduler stopped")
            
            elif command == "stats":
                stats = scheduler.get_stats()
                return ActionResult(success=True, data={"stats": stats})
            
            return ActionResult(success=False, message=f"Unknown command: {command}")
            
        except Exception as e:
            return ActionResult(success=False, message=f"AutomationPriorityAction error: {str(e)}")
