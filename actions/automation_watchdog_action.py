"""Automation Watchdog Action Module.

Monitors automation workflows for timeouts and hangs with:
- Task deadline tracking
- Heartbeat monitoring
- Automatic termination
- Recovery procedures
- Graceful shutdown

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class TaskState(Enum):
    """Watchdog task states."""
    PENDING = auto()
    RUNNING = auto()
    TIMED_OUT = auto()
    CANCELLED = auto()
    COMPLETED = auto()


@dataclass
class WatchdogTask:
    """Task being monitored by watchdog."""
    task_id: str
    name: str
    coroutine: Any
    state: TaskState = TaskState.PENDING
    started_at: Optional[float] = None
    deadline: Optional[float] = None
    last_heartbeat: Optional[float] = None
    timeout_seconds: float = 300.0
    on_timeout: Optional[Callable] = None
    on_complete: Optional[Callable] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WatchdogMetrics:
    """Watchdog metrics."""
    tasks_tracked: int = 0
    tasks_completed: int = 0
    tasks_timed_out: int = 0
    tasks_cancelled: int = 0
    heartbeats_received: int = 0
    recovery_actions: int = 0


class AutomationWatchdog:
    """Monitors automation tasks for hangs and timeouts.
    
    Features:
    - Task deadline tracking
    - Heartbeat-based liveness detection
    - Automatic timeout handling
    - Recovery action triggers
    - Graceful shutdown support
    """
    
    def __init__(
        self,
        name: str = "default",
        default_timeout: float = 300.0,
        heartbeat_interval: float = 30.0,
        check_interval: float = 5.0
    ):
        self.name = name
        self.default_timeout = default_timeout
        self.heartbeat_interval = heartbeat_interval
        self.check_interval = check_interval
        self._tasks: Dict[str, WatchdogTask] = {}
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._metrics = WatchdogMetrics()
    
    async def start(self) -> None:
        """Start the watchdog monitor."""
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info(f"Watchdog '{self.name}' started")
    
    async def stop(self) -> None:
        """Stop the watchdog monitor."""
        self._running = False
        
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        
        for task in self._tasks.values():
            if task.state == TaskState.RUNNING:
                await self.cancel_task(task.task_id)
        
        logger.info(f"Watchdog '{self.name}' stopped")
    
    async def register_task(
        self,
        task_id: str,
        name: str,
        coroutine: Any,
        timeout_seconds: Optional[float] = None,
        on_timeout: Optional[Callable] = None,
        on_complete: Optional[Callable] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> WatchdogTask:
        """Register a task for monitoring.
        
        Args:
            task_id: Unique task identifier
            name: Task name
            coroutine: Coroutine to execute
            timeout_seconds: Task timeout
            on_timeout: Callback on timeout
            on_complete: Callback on completion
            metadata: Additional metadata
            
        Returns:
            Watchdog task configuration
        """
        timeout = timeout_seconds or self.default_timeout
        
        task = WatchdogTask(
            task_id=task_id,
            name=name,
            coroutine=coroutine,
            timeout_seconds=timeout,
            deadline=time.time() + timeout,
            on_timeout=on_timeout,
            on_complete=on_complete,
            metadata=metadata or {}
        )
        
        async with self._lock:
            self._tasks[task_id] = task
            self._metrics.tasks_tracked += 1
        
        logger.info(f"Registered task: {task_id} (timeout: {timeout}s)")
        return task
    
    async def start_task(self, task_id: str) -> Any:
        """Start executing a registered task.
        
        Args:
            task_id: Task ID to start
            
        Returns:
            Task result
        """
        async with self._lock:
            if task_id not in self._tasks:
                raise ValueError(f"Task {task_id} not found")
            
            task = self._tasks[task_id]
            task.state = TaskState.RUNNING
            task.started_at = time.time()
            task.last_heartbeat = time.time()
        
        try:
            result = await asyncio.wait_for(
                task.coroutine,
                timeout=task.timeout_seconds
            )
            
            await self._complete_task(task_id, result)
            return result
            
        except asyncio.TimeoutError:
            await self._timeout_task(task_id)
            raise
        
        except asyncio.CancelledError:
            await self._cancel_task(task_id)
            raise
    
    async def heartbeat(self, task_id: str) -> bool:
        """Record heartbeat for a task.
        
        Args:
            task_id: Task ID
            
        Returns:
            True if heartbeat recorded
        """
        async with self._lock:
            if task_id not in self._tasks:
                return False
            
            task = self._tasks[task_id]
            task.last_heartbeat = time.time()
            task.deadline = time.time() + task.timeout_seconds
            
            self._metrics.heartbeats_received += 1
            return True
    
    async def cancel_task(self, task_id: str) -> bool:
        """Cancel a monitored task.
        
        Args:
            task_id: Task ID to cancel
            
        Returns:
            True if cancelled
        """
        return await self._cancel_task(task_id)
    
    async def _cancel_task(self, task_id: str) -> bool:
        """Internal cancel implementation."""
        async with self._lock:
            if task_id not in self._tasks:
                return False
            
            task = self._tasks[task_id]
            task.state = TaskState.CANCELLED
            self._metrics.tasks_cancelled += 1
            
            logger.info(f"Cancelled task: {task_id}")
            return True
    
    async def _complete_task(self, task_id: str, result: Any) -> None:
        """Mark task as completed."""
        async with self._lock:
            if task_id not in self._tasks:
                return
            
            task = self._tasks[task_id]
            task.state = TaskState.COMPLETED
            self._metrics.tasks_completed += 1
        
        if task.on_complete:
            try:
                await task.on_complete(result)
            except Exception as e:
                logger.error(f"Task complete callback error: {e}")
        
        logger.debug(f"Task completed: {task_id}")
    
    async def _timeout_task(self, task_id: str) -> None:
        """Handle task timeout."""
        async with self._lock:
            if task_id not in self._tasks:
                return
            
            task = self._tasks[task_id]
            task.state = TaskState.TIMED_OUT
            self._metrics.tasks_timed_out += 1
        
        if task.on_timeout:
            self._metrics.recovery_actions += 1
            try:
                await task.on_timeout()
            except Exception as e:
                logger.error(f"Timeout callback error: {e}")
        
        logger.warning(f"Task timed out: {task_id}")
    
    async def _monitor_loop(self) -> None:
        """Main monitoring loop."""
        while self._running:
            try:
                await asyncio.sleep(self.check_interval)
                await self._check_timeouts()
                await self._cleanup_completed()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Watchdog monitor error: {e}")
    
    async def _check_timeouts(self) -> None:
        """Check for timed out tasks."""
        now = time.time()
        
        async with self._lock:
            timed_out = [
                task_id for task_id, task in self._tasks.items()
                if task.state == TaskState.RUNNING
                and task.deadline and now > task.deadline
            ]
        
        for task_id in timed_out:
            await self._timeout_task(task_id)
    
    async def _cleanup_completed(self) -> None:
        """Clean up completed tasks."""
        async with self._lock:
            completed = [
                task_id for task_id, task in self._tasks.items()
                if task.state in (TaskState.COMPLETED, TaskState.CANCELLED, TaskState.TIMED_OUT)
            ]
            
            for task_id in completed:
                del self._tasks[task_id]
    
    def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a monitored task.
        
        Args:
            task_id: Task ID
            
        Returns:
            Task status dictionary
        """
        if task_id not in self._tasks:
            return None
        
        task = self._tasks[task_id]
        
        return {
            "task_id": task.task_id,
            "name": task.name,
            "state": task.state.name,
            "started_at": task.started_at,
            "last_heartbeat": task.last_heartbeat,
            "deadline": task.deadline,
            "timeout_seconds": task.timeout_seconds,
            "metadata": task.metadata
        }
    
    def list_tasks(
        self,
        states: Optional[Set[TaskState]] = None
    ) -> List[Dict[str, Any]]:
        """List monitored tasks.
        
        Args:
            states: Optional state filter
            
        Returns:
            List of task status dictionaries
        """
        tasks = list(self._tasks.values())
        
        if states:
            tasks = [t for t in tasks if t.state in states]
        
        return [
            {
                "task_id": t.task_id,
                "name": t.name,
                "state": t.state.name
            }
            for t in tasks
        ]
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get watchdog metrics."""
        return {
            "tasks_tracked": self._metrics.tasks_tracked,
            "tasks_completed": self._metrics.tasks_completed,
            "tasks_timed_out": self._metrics.tasks_timed_out,
            "tasks_cancelled": self._metrics.tasks_cancelled,
            "heartbeats_received": self._metrics.heartbeats_received,
            "recovery_actions": self._metrics.recovery_actions,
            "active_tasks": sum(
                1 for t in self._tasks.values()
                if t.state == TaskState.RUNNING
            )
        }


class WatchdogGroup:
    """Groups multiple watchdogs for coordinated monitoring."""
    
    def __init__(self, name: str = "default"):
        self.name = name
        self._watchdogs: Dict[str, AutomationWatchdog] = {}
        self._lock = asyncio.Lock()
    
    def register_watchdog(self, watchdog: AutomationWatchdog) -> None:
        """Register a watchdog.
        
        Args:
            watchdog: Watchdog to register
        """
        self._watchdogs[watchdog.name] = watchdog
    
    def get_watchdog(self, name: str) -> Optional[AutomationWatchdog]:
        """Get a watchdog by name."""
        return self._watchdogs.get(name)
    
    async def start_all(self) -> None:
        """Start all watchdogs."""
        for watchdog in self._watchdogs.values():
            await watchdog.start()
    
    async def stop_all(self) -> None:
        """Stop all watchdogs."""
        for watchdog in self._watchdogs.values():
            await watchdog.stop()
    
    def get_all_metrics(self) -> Dict[str, Dict[str, Any]]:
        """Get metrics from all watchdogs."""
        return {
            name: wd.get_metrics()
            for name, wd in self._watchdogs.items()
        }
