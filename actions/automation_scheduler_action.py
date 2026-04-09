"""
Automation Task Scheduler Module.

Provides cron-style scheduling with priority queues, rate limiting,
and distributed lock support for task coordination.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set
from urllib.parse import urlparse
import logging
import hashlib

logger = logging.getLogger(__name__)


class ScheduleType(Enum):
    """Type of scheduling strategy."""
    CRON = "cron"
    INTERVAL = "interval"
    ONE_TIME = "one_time"
    DELAYED = "delayed"


class TaskPriority(Enum):
    """Task priority levels."""
    CRITICAL = 0
    HIGH = 1
    NORMAL = 2
    LOW = 3


@dataclass
class ScheduledTask:
    """Container for a scheduled task."""
    task_id: str
    func: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    schedule_type: ScheduleType = ScheduleType.DELAYED
    priority: TaskPriority = TaskPriority.NORMAL
    next_run: float = 0.0
    interval: float = 0.0
    cron_expr: Optional[str] = None
    max_retries: int = 3
    timeout: float = 60.0
    enabled: bool = True
    tags: Set[str] = field(default_factory=set)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __lt__(self, other: ScheduledTask) -> bool:
        """Compare tasks by priority and next run time."""
        if self.priority != other.priority:
            return self.priority.value < other.priority.value
        return self.next_run < other.next_run


@dataclass
class ExecutionResult:
    """Result of a task execution."""
    task_id: str
    started_at: float
    completed_at: float
    success: bool
    result: Any = None
    error: Optional[str] = None
    retry_count: int = 0


class TaskScheduler:
    """
    Task scheduler with cron, interval, and one-time scheduling support.
    
    Example:
        scheduler = TaskScheduler()
        
        # Add interval task
        scheduler.add_interval_task(
            task_id="heartbeat",
            func=send_heartbeat,
            interval=60.0,
            priority=TaskPriority.HIGH
        )
        
        # Add cron task
        scheduler.add_cron_task(
            task_id="daily_report",
            func=generate_report,
            cron_expr="0 8 * * *"
        )
        
        await scheduler.start()
    """
    
    def __init__(
        self,
        max_concurrent: int = 10,
        lock_timeout: float = 30.0,
        enable_distributed_lock: bool = False,
        lock_backend: Optional[Any] = None,
    ) -> None:
        """
        Initialize the task scheduler.
        
        Args:
            max_concurrent: Maximum concurrent task executions.
            lock_timeout: Lock acquisition timeout in seconds.
            enable_distributed_lock: Enable distributed locking.
            lock_backend: Backend for distributed locks (Redis, etc.).
        """
        self.max_concurrent = max_concurrent
        self.lock_timeout = lock_timeout
        self.enable_distributed_lock = enable_distributed_lock
        self.lock_backend = lock_backend
        
        self._tasks: Dict[str, ScheduledTask] = {}
        self._task_queue: List[ScheduledTask] = []
        self._running_tasks: Set[str] = set()
        self._execution_history: List[ExecutionResult] = []
        self._running = False
        self._lock = asyncio.Lock()
        
    def add_interval_task(
        self,
        task_id: str,
        func: Callable[..., Any],
        interval: float,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        max_retries: int = 3,
        timeout: float = 60.0,
        tags: Optional[Set[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Add a task that runs at fixed intervals.
        
        Args:
            task_id: Unique task identifier.
            func: Callable to execute.
            interval: Interval in seconds.
            args: Positional arguments for func.
            kwargs: Keyword arguments for func.
            priority: Task priority.
            max_retries: Maximum retry attempts.
            timeout: Execution timeout in seconds.
            tags: Optional tags for grouping.
            metadata: Optional metadata.
            
        Returns:
            Task ID.
        """
        if interval <= 0:
            raise ValueError(f"Interval must be positive: {interval}")
            
        task = ScheduledTask(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.INTERVAL,
            priority=priority,
            next_run=time.time() + interval,
            interval=interval,
            max_retries=max_retries,
            timeout=timeout,
            tags=tags or set(),
            metadata=metadata or {},
        )
        
        self._tasks[task_id] = task
        self._rebuild_queue()
        logger.info(f"Added interval task: {task_id} (interval={interval}s)")
        return task_id
        
    def add_cron_task(
        self,
        task_id: str,
        func: Callable[..., Any],
        cron_expr: str,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        max_retries: int = 3,
        timeout: float = 60.0,
    ) -> str:
        """
        Add a task with cron expression scheduling.
        
        Args:
            task_id: Unique task identifier.
            func: Callable to execute.
            cron_expr: Cron expression (minute hour day month weekday).
            args: Positional arguments for func.
            kwargs: Keyword arguments for func.
            priority: Task priority.
            max_retries: Maximum retry attempts.
            timeout: Execution timeout in seconds.
            
        Returns:
            Task ID.
        """
        next_run = self._parse_cron_next_run(cron_expr)
        
        task = ScheduledTask(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.CRON,
            priority=priority,
            next_run=next_run,
            cron_expr=cron_expr,
            max_retries=max_retries,
            timeout=timeout,
        )
        
        self._tasks[task_id] = task
        self._rebuild_queue()
        logger.info(f"Added cron task: {task_id} (expr={cron_expr})")
        return task_id
        
    def add_delayed_task(
        self,
        task_id: str,
        func: Callable[..., Any],
        delay: float,
        args: tuple = (),
        kwargs: Optional[Dict[str, Any]] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
    ) -> str:
        """
        Add a one-time delayed task.
        
        Args:
            task_id: Unique task identifier.
            func: Callable to execute.
            delay: Delay in seconds before execution.
            args: Positional arguments for func.
            kwargs: Keyword arguments for func.
            priority: Task priority.
            
        Returns:
            Task ID.
        """
        task = ScheduledTask(
            task_id=task_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            schedule_type=ScheduleType.DELAYED,
            priority=priority,
            next_run=time.time() + delay,
        )
        
        self._tasks[task_id] = task
        self._rebuild_queue()
        logger.info(f"Added delayed task: {task_id} (delay={delay}s)")
        return task_id
        
    def remove_task(self, task_id: str) -> bool:
        """
        Remove a scheduled task.
        
        Args:
            task_id: Task identifier.
            
        Returns:
            True if task was removed.
        """
        if task_id in self._tasks:
            del self._tasks[task_id]
            self._rebuild_queue()
            logger.info(f"Removed task: {task_id}")
            return True
        return False
        
    def pause_task(self, task_id: str) -> bool:
        """Pause a task temporarily."""
        if task_id in self._tasks:
            self._tasks[task_id].enabled = False
            self._rebuild_queue()
            return True
        return False
        
    def resume_task(self, task_id: str) -> bool:
        """Resume a paused task."""
        if task_id in self._tasks:
            self._tasks[task_id].enabled = True
            self._rebuild_queue()
            return True
        return False
        
    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        """Get task by ID."""
        return self._tasks.get(task_id)
        
    def list_tasks(self, tags: Optional[Set[str]] = None) -> List[ScheduledTask]:
        """
        List all scheduled tasks.
        
        Args:
            tags: Optional filter by tags.
            
        Returns:
            List of tasks.
        """
        tasks = list(self._tasks.values())
        if tags:
            tasks = [t for t in tasks if t.tags & tags]
        return sorted(tasks, key=lambda t: (t.priority.value, t.next_run))
        
    async def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        logger.info("Task scheduler started")
        
        while self._running:
            try:
                await self._process_next()
            except Exception as e:
                logger.error(f"Scheduler error: {e}")
                
    async def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        logger.info("Task scheduler stopped")
        
    async def _process_next(self) -> None:
        """Process the next scheduled task."""
        async with self._lock:
            if not self._task_queue:
                await asyncio.sleep(0.1)
                return
                
            # Check concurrent limit
            if len(self._running_tasks) >= self.max_concurrent:
                await asyncio.sleep(0.1)
                return
                
            task = self._task_queue[0]
            now = time.time()
            
            if task.next_run > now:
                # Wait until next task is due
                wait_time = min(task.next_run - now, 1.0)
                await asyncio.sleep(wait_time)
                return
                
            # Remove from queue
            self._task_queue.pop(0)
            
            if not task.enabled:
                return
                
            # Acquire lock if distributed locking enabled
            if self.enable_distributed_lock:
                acquired = await self._acquire_distributed_lock(task.task_id)
                if not acquired:
                    # Reschedule
                    task.next_run = time.time() + task.interval if task.interval else now + 60
                    self._rebuild_queue()
                    return
                    
            # Execute task
            self._running_tasks.add(task.task_id)
            
        # Run outside lock
        try:
            await self._execute_task(task)
        finally:
            async with self._lock:
                self._running_tasks.discard(task.task_id)
                if self.enable_distributed_lock:
                    await self._release_distributed_lock(task.task_id)
                    
            # Reschedule recurring tasks
            async with self._lock:
                if task.schedule_type in (ScheduleType.INTERVAL, ScheduleType.CRON):
                    if task.schedule_type == ScheduleType.INTERVAL:
                        task.next_run = time.time() + task.interval
                    else:
                        task.next_run = self._parse_cron_next_run(task.cron_expr)
                    self._rebuild_queue()
                    
    async def _execute_task(self, task: ScheduledTask) -> ExecutionResult:
        """Execute a scheduled task."""
        started_at = time.time()
        result = ExecutionResult(
            task_id=task.task_id,
            started_at=started_at,
            completed_at=0,
            success=False,
            retry_count=0,
        )
        
        for attempt in range(task.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(task.func):
                    result.result = await asyncio.wait_for(
                        task.func(*task.args, **task.kwargs),
                        timeout=task.timeout
                    )
                else:
                    result.result = await asyncio.wait_for(
                        asyncio.to_thread(task.func, *task.args, **task.kwargs),
                        timeout=task.timeout
                    )
                result.success = True
                break
            except asyncio.TimeoutError:
                result.error = f"Task timed out after {task.timeout}s"
                logger.warning(f"Task {task.task_id} timed out (attempt {attempt + 1})")
            except Exception as e:
                result.error = str(e)
                logger.warning(f"Task {task.task_id} failed: {e} (attempt {attempt + 1})")
                result.retry_count = attempt + 1
                
        result.completed_at = time.time()
        self._execution_history.append(result)
        
        # Keep history bounded
        if len(self._execution_history) > 1000:
            self._execution_history = self._execution_history[-500:]
            
        return result
        
    def _rebuild_queue(self) -> None:
        """Rebuild the priority queue."""
        self._task_queue = sorted(
            [t for t in self._tasks.values() if t.enabled],
            key=lambda t: (t.priority.value, t.next_run)
        )
        
    def _parse_cron_next_run(self, cron_expr: Optional[str]) -> float:
        """Parse cron expression and calculate next run time."""
        if not cron_expr:
            return time.time()
            
        # Simple cron parser (minute hour day month weekday)
        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expr}")
            
        now = datetime.now()
        
        # Very simple: if minute specified, use it
        try:
            minute = int(parts[0]) if parts[0] != "*" else now.minute
            hour = int(parts[1]) if parts[1] != "*" else now.hour
            
            next_run = now.replace(minute=minute, second=0, microsecond=0)
            if hour < now.hour or (hour == now.hour and minute <= now.minute):
                next_run += timedelta(days=1)
            elif hour > now.hour:
                next_run = next_run.replace(hour=hour)
                
            return next_run.timestamp()
        except ValueError:
            return time.time() + 3600
            
    async def _acquire_distributed_lock(self, task_id: str) -> bool:
        """Acquire distributed lock for task."""
        if not self.lock_backend:
            return True
            
        lock_key = f"scheduler:lock:{task_id}"
        try:
            result = await self.lock_backend.set(
                lock_key, "1", nx=True, ex=int(self.lock_timeout)
            )
            return bool(result)
        except Exception as e:
            logger.error(f"Failed to acquire lock: {e}")
            return True  # Proceed without lock
            
    async def _release_distributed_lock(self, task_id: str) -> None:
        """Release distributed lock for task."""
        if not self.lock_backend:
            return
            
        lock_key = f"scheduler:lock:{task_id}"
        try:
            await self.lock_backend.delete(lock_key)
        except Exception as e:
            logger.error(f"Failed to release lock: {e}")
            
    def get_execution_history(
        self,
        task_id: Optional[str] = None,
        limit: int = 100,
    ) -> List[ExecutionResult]:
        """Get execution history."""
        history = self._execution_history
        if task_id:
            history = [h for h in history if h.task_id == task_id]
        return history[-limit:]
