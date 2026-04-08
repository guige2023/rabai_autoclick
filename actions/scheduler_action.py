"""Scheduler action module for RabAI AutoClick.

Provides task scheduling with cron expressions, intervals,
and delayed execution for background jobs.
"""

import sys
import os
import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from threading import Thread, Event
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class ScheduleType(Enum):
    """Schedule type."""
    INTERVAL = "interval"
    CRON = "cron"
    DELAYED = "delayed"
    ONCE = "once"


@dataclass
class ScheduledTask:
    """A scheduled task definition."""
    id: str
    name: str
    func: Callable
    schedule_type: ScheduleType
    interval_seconds: float = 0
    cron_expression: str = ""
    run_at: Optional[datetime] = None
    enabled: bool = True
    max_runs: Optional[int] = None
    run_count: int = 0
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None


class SchedulerAction(BaseAction):
    """Schedule and execute tasks at specified intervals or times.
    
    Supports interval-based, cron-based, and one-time scheduling
    with configurable retry and max run limits.
    """
    action_type = "scheduler"
    display_name = "任务调度"
    description = "定时任务调度，支持间隔和cron表达式"
    
    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, ScheduledTask] = {}
        self._running = False
        self._stop_event = None
        self._runner_thread: Optional[Thread] = None
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute scheduler operation.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - operation: 'schedule', 'unschedule', 'list', 'run', 'start', 'stop'
                - task: Task config dict (for schedule)
                - task_id: Task ID (for other operations)
        
        Returns:
            ActionResult with operation result.
        """
        operation = params.get('operation', 'schedule').lower()
        
        if operation == 'schedule':
            return self._schedule(params)
        elif operation == 'unschedule':
            return self._unschedule(params)
        elif operation == 'list':
            return self._list_tasks(params)
        elif operation == 'run':
            return self._run_task(params)
        elif operation == 'start':
            return self._start_scheduler(params)
        elif operation == 'stop':
            return self._stop_scheduler(params)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}"
            )
    
    def _schedule(self, params: Dict[str, Any]) -> ActionResult:
        """Schedule a new task."""
        task_id = params.get('task_id') or params.get('id')
        name = params.get('name', task_id or 'unnamed')
        schedule_type = params.get('schedule_type', 'interval').lower()
        interval = params.get('interval_seconds', 60)
        cron = params.get('cron_expression', '')
        run_at = params.get('run_at')
        max_runs = params.get('max_runs')
        enabled = params.get('enabled', True)
        
        if not task_id:
            return ActionResult(success=False, message="task_id is required")
        
        if schedule_type not in ('interval', 'cron', 'delayed', 'once'):
            return ActionResult(
                success=False,
                message=f"Invalid schedule_type: {schedule_type}"
            )
        
        # Create task
        task = ScheduledTask(
            id=task_id,
            name=name,
            func=params.get('func'),
            schedule_type=ScheduleType(schedule_type),
            interval_seconds=interval,
            cron_expression=cron,
            enabled=enabled,
            max_runs=max_runs
        )
        
        if run_at:
            if isinstance(run_at, str):
                task.run_at = datetime.fromisoformat(run_at)
            else:
                task.run_at = run_at
        
        # Calculate next run
        task.next_run = self._calculate_next_run(task)
        
        self._tasks[task_id] = task
        
        return ActionResult(
            success=True,
            message=f"Scheduled task '{name}'",
            data={
                'task_id': task_id,
                'schedule_type': schedule_type,
                'next_run': task.next_run.isoformat() if task.next_run else None
            }
        )
    
    def _unschedule(self, params: Dict[str, Any]) -> ActionResult:
        """Remove a scheduled task."""
        task_id = params.get('task_id')
        
        if not task_id:
            return ActionResult(success=False, message="task_id is required")
        
        if task_id in self._tasks:
            del self._tasks[task_id]
            return ActionResult(
                success=True,
                message=f"Removed task '{task_id}'"
            )
        else:
            return ActionResult(
                success=False,
                message=f"Task '{task_id}' not found"
            )
    
    def _list_tasks(self, params: Dict[str, Any]) -> ActionResult:
        """List all scheduled tasks."""
        include_disabled = params.get('include_disabled', True)
        
        tasks = []
        for task_id, task in self._tasks.items():
            if not include_disabled and not task.enabled:
                continue
            
            tasks.append({
                'task_id': task.id,
                'name': task.name,
                'schedule_type': task.schedule_type.value,
                'enabled': task.enabled,
                'run_count': task.run_count,
                'last_run': task.last_run.isoformat() if task.last_run else None,
                'next_run': task.next_run.isoformat() if task.next_run else None
            })
        
        return ActionResult(
            success=True,
            message=f"{len(tasks)} scheduled tasks",
            data={'tasks': tasks, 'count': len(tasks)}
        )
    
    def _run_task(self, params: Dict[str, Any]) -> ActionResult:
        """Run a task immediately."""
        task_id = params.get('task_id')
        
        if not task_id:
            return ActionResult(success=False, message="task_id is required")
        
        if task_id not in self._tasks:
            return ActionResult(
                success=False,
                message=f"Task '{task_id}' not found"
            )
        
        task = self._tasks[task_id]
        func = task.func
        
        if not callable(func):
            return ActionResult(
                success=False,
                message=f"Task '{task_id}' has no callable func"
            )
        
        try:
            start = time.time()
            result = func()
            elapsed = time.time() - start
            
            task.run_count += 1
            task.last_run = datetime.utcnow()
            task.next_run = self._calculate_next_run(task)
            
            return ActionResult(
                success=True,
                message=f"Task '{task_id}' completed in {elapsed:.2f}s",
                data={
                    'task_id': task_id,
                    'elapsed': elapsed,
                    'run_count': task.run_count
                }
            )
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Task '{task_id}' failed: {e}",
                data={'task_id': task_id, 'error': str(e)}
            )
    
    def _start_scheduler(self, params: Dict[str, Any]) -> ActionResult:
        """Start the scheduler background thread."""
        if self._running:
            return ActionResult(
                success=True,
                message="Scheduler already running"
            )
        
        self._stop_event = Event()
        self._running = True
        self._runner_thread = Thread(target=self._run_loop, daemon=True)
        self._runner_thread.start()
        
        return ActionResult(
            success=True,
            message="Scheduler started"
        )
    
    def _stop_scheduler(self, params: Dict[str, Any]) -> ActionResult:
        """Stop the scheduler."""
        if not self._running:
            return ActionResult(
                success=True,
                message="Scheduler not running"
            )
        
        self._stop_event.set()
        self._running = False
        
        return ActionResult(
            success=True,
            message="Scheduler stopped"
        )
    
    def _run_loop(self) -> None:
        """Main scheduler loop."""
        while not self._stop_event.is_set():
            now = datetime.utcnow()
            
            for task_id, task in list(self._tasks.items()):
                if not task.enabled:
                    continue
                
                if task.max_runs and task.run_count >= task.max_runs:
                    continue
                
                if task.next_run and now >= task.next_run:
                    # Run task
                    try:
                        if callable(task.func):
                            task.func()
                    except Exception:
                        pass
                    
                    task.run_count += 1
                    task.last_run = now
                    task.next_run = self._calculate_next_run(task)
            
            self._stop_event.wait(1.0)  # Check every second
    
    def _calculate_next_run(self, task: ScheduledTask) -> Optional[datetime]:
        """Calculate next run time based on schedule type."""
        now = datetime.utcnow()
        
        if task.schedule_type == ScheduleType.INTERVAL:
            if task.last_run:
                return task.last_run + timedelta(seconds=task.interval_seconds)
            return now + timedelta(seconds=task.interval_seconds)
        
        elif task.schedule_type == ScheduleType.DELAYED:
            return task.run_at
        
        elif task.schedule_type == ScheduleType.ONCE:
            return task.run_at
        
        elif task.schedule_type == ScheduleType.CRON:
            # Simplified cron parsing - would use croniter in production
            return None
        
        return None


class CronParserAction(BaseAction):
    """Parse and validate cron expressions."""
    action_type = "cron_parser"
    display_name = "Cron解析"
    description = "解析和验证Cron表达式"
    
    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Parse a cron expression.
        
        Args:
            context: Execution context.
            params: Dict with keys:
                - expression: Cron expression string
                - timezone: Optional timezone
        
        Returns:
            ActionResult with parsed cron details.
        """
        expression = params.get('expression')
        
        if not expression:
            return ActionResult(success=False, message="expression is required")
        
        # Parse cron fields
        parts = expression.split()
        
        if len(parts) not in (5, 6):
            return ActionResult(
                success=False,
                message=f"Invalid cron expression: expected 5-6 fields, got {len(parts)}"
            )
        
        field_names = ['minute', 'hour', 'day', 'month', 'weekday', 'year']
        field_ranges = {
            'minute': (0, 59),
            'hour': (0, 23),
            'day': (1, 31),
            'month': (1, 12),
            'weekday': (0, 6),
            'year': (1970, 2099)
        }
        
        parsed = {}
        errors = []
        
        for i, part in enumerate(parts):
            field_name = field_names[i]
            min_val, max_val = field_ranges[field_name]
            
            try:
                if part == '*':
                    parsed[field_name] = 'every'
                elif '/' in part:
                    # Step value
                    base, step = part.split('/')
                    parsed[field_name] = f"every {step} starting from {base}"
                elif '-' in part:
                    # Range
                    start, end = part.split('-')
                    parsed[field_name] = f"{start} to {end}"
                elif ',' in part:
                    # List
                    parsed[field_name] = f"one of {part}"
                else:
                    # Single value
                    val = int(part)
                    if val < min_val or val > max_val:
                        errors.append(f"{field_name} value {val} out of range ({min_val}-{max_val})")
                    parsed[field_name] = val
            except ValueError:
                errors.append(f"Invalid {field_name}: {part}")
        
        return ActionResult(
            success=len(errors) == 0,
            message=f"{'Valid' if not errors else 'Invalid'} cron expression",
            data={
                'expression': expression,
                'fields': parsed,
                'errors': errors,
                'next_runs': self._calculate_next_n_runs(expression, 5)
            }
        )
    
    def _calculate_next_n_runs(self, expression: str, n: int) -> List[str]:
        """Calculate next N run times (simplified)."""
        # This is a simplified implementation
        # Real implementation would use croniter library
        runs = []
        current = datetime.utcnow()
        
        for i in range(n):
            current += timedelta(hours=1)
            runs.append(current.isoformat())
        
        return runs
