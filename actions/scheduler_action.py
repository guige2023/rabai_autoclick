"""Scheduler action module for RabAI AutoClick.

Provides task scheduling with cron expressions, interval-based scheduling,
calendar scheduling, and scheduled task management.
"""

import json
import time
import sys
import os
import threading
from typing import Any, Dict, List, Optional, Union, Callable
from datetime import datetime, timedelta
from crontab import CronTab
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ScheduledTask:
    """Represents a scheduled task."""
    
    def __init__(
        self,
        task_id: str,
        name: str,
        schedule_type: str,
        schedule_config: Dict[str, Any],
        callback: Optional[Callable] = None,
        enabled: bool = True,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.task_id = task_id
        self.name = name
        self.schedule_type = schedule_type
        self.schedule_config = schedule_config
        self.callback = callback
        self.enabled = enabled
        self.metadata = metadata or {}
        self.created_at = time.time()
        self.last_run = None
        self.next_run = None
        self.run_count = 0
        self.total_runs = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert task to dictionary."""
        return {
            'task_id': self.task_id,
            'name': self.name,
            'schedule_type': self.schedule_type,
            'schedule_config': self.schedule_config,
            'enabled': self.enabled,
            'created_at': self.created_at,
            'last_run': self.last_run,
            'next_run': self.next_run,
            'run_count': self.run_count,
            'metadata': self.metadata
        }


class SchedulerAction(BaseAction):
    """Schedule and manage recurring tasks.
    
    Supports cron expressions, interval-based scheduling,
    calendar-based scheduling, and task lifecycle management.
    """
    action_type = "scheduler"
    display_name = "任务调度"
    description = "任务调度器，支持cron和间隔调度"
    
    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, ScheduledTask] = {}
        self._lock = threading.RLock()
        self._running = False
        self._scheduler_thread: Optional[threading.Thread] = None
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute scheduler operations.
        
        Args:
            context: Execution context.
            params: Dict with keys: action (schedule, unschedule, list,
                   enable, disable, start, stop), task_config.
        
        Returns:
            ActionResult with operation result.
        """
        action = params.get('action', 'schedule')
        
        if action == 'schedule':
            return self._schedule_task(params)
        elif action == 'unschedule':
            return self._unschedule_task(params)
        elif action == 'list':
            return self._list_tasks(params)
        elif action == 'enable':
            return self._enable_task(params)
        elif action == 'disable':
            return self._disable_task(params)
        elif action == 'start':
            return self._start_scheduler(params)
        elif action == 'stop':
            return self._stop_scheduler(params)
        elif action == 'trigger':
            return self._trigger_task(params)
        elif action == 'next_run':
            return self._get_next_run(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown action: {action}"
            )
    
    def _schedule_task(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Schedule a new task."""
        name = params.get('name', '')
        if not name:
            return ActionResult(success=False, message="name is required")
        
        schedule_type = params.get('schedule_type', 'interval')
        schedule_config = params.get('schedule_config', {})
        enabled = params.get('enabled', True)
        metadata = params.get('metadata', {})
        
        task_id = params.get('task_id', str(uuid.uuid4()))
        
        if schedule_type == 'cron':
            if 'expression' not in schedule_config:
                return ActionResult(success=False, message="cron expression required")
        elif schedule_type == 'interval':
            if 'seconds' not in schedule_config and 'minutes' not in schedule_config:
                return ActionResult(success=False, message="interval seconds or minutes required")
        elif schedule_type == 'calendar':
            if 'day' not in schedule_config and 'hour' not in schedule_config:
                return ActionResult(success=False, message="calendar schedule requires day or hour")
        else:
            return ActionResult(success=False, message=f"Unknown schedule type: {schedule_type}")
        
        task = ScheduledTask(
            task_id=task_id,
            name=name,
            schedule_type=schedule_type,
            schedule_config=schedule_config,
            enabled=enabled,
            metadata=metadata
        )
        
        task.next_run = self._calculate_next_run(task)
        
        with self._lock:
            self._tasks[task_id] = task
        
        return ActionResult(
            success=True,
            message=f"Task '{name}' scheduled with ID {task_id}",
            data=task.to_dict()
        )
    
    def _unschedule_task(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Unschedule a task."""
        task_id = params.get('task_id')
        
        if not task_id:
            return ActionResult(success=False, message="task_id is required")
        
        with self._lock:
            if task_id in self._tasks:
                task = self._tasks.pop(task_id)
                return ActionResult(
                    success=True,
                    message=f"Task '{task.name}' unscheduled",
                    data={'task_id': task_id}
                )
        
        return ActionResult(
            success=False,
            message=f"Task {task_id} not found"
        )
    
    def _list_tasks(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """List all scheduled tasks."""
        enabled_only = params.get('enabled_only', False)
        schedule_type_filter = params.get('schedule_type')
        
        with self._lock:
            tasks = list(self._tasks.values())
        
        if enabled_only:
            tasks = [t for t in tasks if t.enabled]
        
        if schedule_type_filter:
            tasks = [t for t in tasks if t.schedule_type == schedule_type_filter]
        
        tasks_data = [t.to_dict() for t in tasks]
        
        return ActionResult(
            success=True,
            message=f"Found {len(tasks)} tasks",
            data={'tasks': tasks_data, 'count': len(tasks_data)}
        )
    
    def _enable_task(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Enable a scheduled task."""
        task_id = params.get('task_id')
        
        if not task_id:
            return ActionResult(success=False, message="task_id is required")
        
        with self._lock:
            if task_id not in self._tasks:
                return ActionResult(success=False, message=f"Task {task_id} not found")
            
            task = self._tasks[task_id]
            task.enabled = True
            task.next_run = self._calculate_next_run(task)
        
        return ActionResult(
            success=True,
            message=f"Task '{task.name}' enabled",
            data=task.to_dict()
        )
    
    def _disable_task(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Disable a scheduled task."""
        task_id = params.get('task_id')
        
        if not task_id:
            return ActionResult(success=False, message="task_id is required")
        
        with self._lock:
            if task_id not in self._tasks:
                return ActionResult(success=False, message=f"Task {task_id} not found")
            
            task = self._tasks[task_id]
            task.enabled = False
            task.next_run = None
        
        return ActionResult(
            success=True,
            message=f"Task '{task.name}' disabled",
            data=task.to_dict()
        )
    
    def _start_scheduler(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Start the scheduler background thread."""
        if self._running:
            return ActionResult(
                success=True,
                message="Scheduler already running",
                data={'running': True}
            )
        
        self._running = True
        self._scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self._scheduler_thread.start()
        
        return ActionResult(
            success=True,
            message="Scheduler started",
            data={'running': True}
        )
    
    def _stop_scheduler(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Stop the scheduler background thread."""
        self._running = False
        
        if self._scheduler_thread:
            self._scheduler_thread.join(timeout=5)
            self._scheduler_thread = None
        
        return ActionResult(
            success=True,
            message="Scheduler stopped",
            data={'running': False}
        )
    
    def _trigger_task(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Manually trigger a task."""
        task_id = params.get('task_id')
        
        if not task_id:
            return ActionResult(success=False, message="task_id is required")
        
        with self._lock:
            if task_id not in self._tasks:
                return ActionResult(success=False, message=f"Task {task_id} not found")
            
            task = self._tasks[task_id]
            task.last_run = time.time()
            task.run_count += 1
            task.next_run = self._calculate_next_run(task)
        
        return ActionResult(
            success=True,
            message=f"Task '{task.name}' triggered",
            data=task.to_dict()
        )
    
    def _get_next_run(
        self,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Get next run time for a task."""
        task_id = params.get('task_id')
        
        if not task_id:
            return ActionResult(success=False, message="task_id is required")
        
        with self._lock:
            if task_id not in self._tasks:
                return ActionResult(success=False, message=f"Task {task_id} not found")
            
            task = self._tasks[task_id]
        
        if task.next_run:
            next_run_dt = datetime.fromtimestamp(task.next_run)
            return ActionResult(
                success=True,
                message=f"Next run: {next_run_dt.isoformat()}",
                data={'next_run': task.next_run, 'next_run_dt': next_run_dt.isoformat()}
            )
        
        return ActionResult(
            success=False,
            message="Task has no scheduled next run (disabled)"
        )
    
    def _calculate_next_run(self, task: ScheduledTask) -> Optional[float]:
        """Calculate next run timestamp for a task."""
        now = time.time()
        
        if task.schedule_type == 'interval':
            seconds = task.schedule_config.get('seconds', 0)
            minutes = task.schedule_config.get('minutes', 0)
            hours = task.schedule_config.get('hours', 0)
            interval_seconds = seconds + (minutes * 60) + (hours * 3600)
            
            if interval_seconds <= 0:
                return None
            
            if task.last_run:
                return task.last_run + interval_seconds
            return now + interval_seconds
        
        elif task.schedule_type == 'cron':
            try:
                expression = task.schedule_config.get('expression', '')
                cron = CronTab(expression)
                return cron.next(now)
            except Exception:
                return None
        
        elif task.schedule_type == 'calendar':
            now_dt = datetime.now()
            target_hour = task.schedule_config.get('hour', now_dt.hour)
            target_minute = task.schedule_config.get('minute', 0)
            target_day = task.schedule_config.get('day')
            
            next_run_dt = now_dt.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            
            if next_run_dt <= now_dt:
                if target_day:
                    next_run_dt += timedelta(days=1)
                else:
                    next_run_dt += timedelta(days=1)
            
            return next_run_dt.timestamp()
        
        return None
    
    def _scheduler_loop(self):
        """Main scheduler loop running in background thread."""
        while self._running:
            try:
                now = time.time()
                
                with self._lock:
                    for task_id, task in list(self._tasks.items()):
                        if task.enabled and task.next_run and now >= task.next_run:
                            task.last_run = now
                            task.run_count += 1
                            task.next_run = self._calculate_next_run(task)
                
                time.sleep(1)
                
            except Exception as e:
                time.sleep(1)
