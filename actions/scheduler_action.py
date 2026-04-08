"""Scheduler action module for RabAI AutoClick.

Provides cron-based and interval-based task scheduling
with timezone support and misfire handling.
"""

import time
import sys
import os
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
import threading
import croniter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ScheduledTask:
    """A scheduled task definition."""
    id: str
    name: str
    action: str
    params: Dict[str, Any]
    schedule_type: str
    cron_expr: Optional[str] = None
    interval_seconds: Optional[int] = None
    timezone: str = "UTC"
    enabled: bool = True
    misfire_policy: str = "fire_once"
    last_run: Optional[float] = None
    next_run: Optional[float] = None


class SchedulerAction(BaseAction):
    """Scheduler action for time-based task execution.
    
    Supports cron expressions and fixed interval scheduling
    with timezone support and misfire handling policies.
    """
    action_type = "scheduler"
    display_name = "任务调度器"
    description = "Cron和间隔任务调度器"
    
    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, ScheduledTask] = {}
        self._lock = threading.RLock()
        self._listeners: List[Callable] = []
    
    def add_task(self, task: ScheduledTask) -> None:
        """Add a scheduled task.
        
        Args:
            task: ScheduledTask to add.
        """
        with self._lock:
            task.next_run = self._calculate_next_run(task)
            self._tasks[task.id] = task
    
    def remove_task(self, task_id: str) -> bool:
        """Remove a scheduled task.
        
        Args:
            task_id: ID of task to remove.
            
        Returns:
            True if task was removed, False if not found.
        """
        with self._lock:
            if task_id in self._tasks:
                del self._tasks[task_id]
                return True
            return False
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute scheduler operations.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                operation: add|remove|list|trigger|due
                task: Task definition (for add)
                task_id: Task ID (for remove/trigger).
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'list')
        
        if operation == 'add':
            task = self._parse_task(params.get('task', {}))
            self.add_task(task)
            return ActionResult(
                success=True,
                message=f"Task {task.id} scheduled",
                data={'task_id': task.id, 'next_run': task.next_run}
            )
        elif operation == 'remove':
            success = self.remove_task(params.get('task_id', ''))
            return ActionResult(
                success=success,
                message=f"Task {'removed' if success else 'not found'}"
            )
        elif operation == 'list':
            return self._list_tasks()
        elif operation == 'trigger':
            return self._trigger_task(params.get('task_id', ''), context)
        elif operation == 'due':
            return self._get_due_tasks()
        else:
            return ActionResult(success=False, message=f"Unknown operation: {operation}")
    
    def _parse_task(self, task_def: Dict[str, Any]) -> ScheduledTask:
        """Parse task definition into ScheduledTask."""
        return ScheduledTask(
            id=task_def['id'],
            name=task_def.get('name', task_def['id']),
            action=task_def['action'],
            params=task_def.get('params', {}),
            schedule_type=task_def.get('schedule_type', 'cron'),
            cron_expr=task_def.get('cron_expr'),
            interval_seconds=task_def.get('interval_seconds'),
            timezone=task_def.get('timezone', 'UTC'),
            enabled=task_def.get('enabled', True),
            misfire_policy=task_def.get('misfire_policy', 'fire_once')
        )
    
    def _calculate_next_run(self, task: ScheduledTask) -> Optional[float]:
        """Calculate next run time for a task."""
        now = datetime.now(timezone.utc)
        
        if task.schedule_type == 'cron' and task.cron_expr:
            try:
                tz = timezone.utc
                if task.timezone != 'UTC':
                    import pytz
                    tz = pytz.timezone(task.timezone)
                
                cron = croniter.croniter(task.cron_expr, now, tz=tz)
                return cron.get_next_timestamp()
            except (ValueError, KeyError):
                return None
        
        elif task.schedule_type == 'interval' and task.interval_seconds:
            if task.last_run:
                return task.last_run + task.interval_seconds
            return time.time()
        
        return None
    
    def _list_tasks(self) -> ActionResult:
        """List all scheduled tasks."""
        with self._lock:
            tasks_data = []
            for task in self._tasks.values():
                tasks_data.append({
                    'id': task.id,
                    'name': task.name,
                    'schedule_type': task.schedule_type,
                    'cron_expr': task.cron_expr,
                    'interval_seconds': task.interval_seconds,
                    'timezone': task.timezone,
                    'enabled': task.enabled,
                    'last_run': task.last_run,
                    'next_run': task.next_run
                })
        
        return ActionResult(
            success=True,
            message=f"{len(tasks_data)} scheduled tasks",
            data={'tasks': tasks_data}
        )
    
    def _trigger_task(self, task_id: str, context: Any) -> ActionResult:
        """Manually trigger a task."""
        with self._lock:
            task = self._tasks.get(task_id)
        
        if not task:
            return ActionResult(success=False, message=f"Task {task_id} not found")
        
        task.last_run = time.time()
        task.next_run = self._calculate_next_run(task)
        
        return ActionResult(
            success=True,
            message=f"Task {task_id} triggered",
            data={
                'task_id': task_id,
                'action': task.action,
                'params': task.params,
                'last_run': task.last_run,
                'next_run': task.next_run
            }
        )
    
    def _get_due_tasks(self) -> ActionResult:
        """Get tasks that are due for execution."""
        now = time.time()
        due_tasks = []
        
        with self._lock:
            for task in self._tasks.values():
                if not task.enabled:
                    continue
                
                if task.next_run and task.next_run <= now:
                    if task.misfire_policy == 'skip' and task.last_run:
                        expected_next = task.last_run + (task.interval_seconds or 0)
                        if abs(expected_next - task.next_run) > (task.interval_seconds or 0) * 2:
                            continue
                    
                    due_tasks.append({
                        'id': task.id,
                        'name': task.name,
                        'action': task.action,
                        'params': task.params,
                        'overdue_by': now - task.next_run
                    })
        
        return ActionResult(
            success=True,
            message=f"{len(due_tasks)} tasks due",
            data={'due_tasks': due_tasks}
        )
    
    def register_listener(self, listener: Callable) -> None:
        """Register a listener for task events.
        
        Args:
            listener: Callable that receives (event_type, task, result) events.
        """
        self._listeners.append(listener)
