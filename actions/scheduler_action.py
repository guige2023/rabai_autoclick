"""Scheduler action module for RabAI AutoClick.

Provides task scheduling operations including cron-like scheduling,
interval-based execution, and delayed task execution.
"""

import os
import sys
import time
import threading
import sched
import json
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


@dataclass
class ScheduledTask:
    """Represents a scheduled task.
    
    Attributes:
        name: Task identifier name.
        func: Callable to execute.
        interval: Execution interval in seconds (for repeating tasks).
        cron: Cron expression for cron-based scheduling.
        args: Positional arguments for the callable.
        kwargs: Keyword arguments for the callable.
        next_run: Next scheduled run timestamp.
        last_run: Last execution timestamp.
        run_count: Number of times task has been executed.
        active: Whether the task is currently active.
    """
    name: str
    func: Callable
    interval: Optional[float] = None
    cron: Optional[str] = None
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    next_run: float = 0.0
    last_run: float = 0.0
    run_count: int = 0
    active: bool = True


class Scheduler:
    """Task scheduler with interval and cron support.
    
    Provides scheduling of tasks with support for:
    - Fixed interval execution
    - Cron-like scheduling expressions
    - One-time delayed execution
    - Task cancellation and management
    """
    
    def __init__(self) -> None:
        """Initialize the scheduler."""
        self._tasks: Dict[str, ScheduledTask] = {}
        self._scheduler = sched.scheduler(time.time, time.sleep)
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._lock = threading.Lock()
    
    def add_interval_task(
        self,
        name: str,
        func: Callable,
        interval: float,
        args: Optional[tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        start_immediately: bool = False
    ) -> str:
        """Add a task that executes at fixed intervals.
        
        Args:
            name: Unique task identifier.
            func: Callable to execute.
            interval: Interval in seconds between executions.
            args: Positional arguments for func.
            kwargs: Keyword arguments for func.
            start_immediately: Whether to run immediately or wait for interval.
            
        Returns:
            Task name.
        """
        with self._lock:
            if name in self._tasks:
                raise ValueError(f"Task '{name}' already exists")
            
            kwargs = kwargs or {}
            args = args or ()
            
            task = ScheduledTask(
                name=name,
                func=func,
                interval=interval,
                next_run=time.time() if start_immediately else time.time() + interval
            )
            
            self._tasks[name] = task
            
            if self._running:
                self._schedule_task(task)
            
            return name
    
    def add_cron_task(
        self,
        name: str,
        func: Callable,
        cron_expr: str,
        args: Optional[tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> str:
        """Add a task based on a cron expression.
        
        Args:
            name: Unique task identifier.
            func: Callable to execute.
            cron_expr: Cron expression (minute hour day month weekday).
            args: Positional arguments for func.
            kwargs: Keyword arguments for func.
            
        Returns:
            Task name.
        """
        with self._lock:
            if name in self._tasks:
                raise ValueError(f"Task '{name}' already exists")
            
            next_run = self._parse_cron_next(cron_expr)
            
            kwargs = kwargs or {}
            args = args or ()
            
            task = ScheduledTask(
                name=name,
                func=func,
                cron=cron_expr,
                next_run=next_run
            )
            
            self._tasks[name] = task
            
            if self._running:
                self._schedule_task(task)
            
            return name
    
    def add_delayed_task(
        self,
        name: str,
        func: Callable,
        delay: float,
        args: Optional[tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None
    ) -> str:
        """Add a one-time task that executes after a delay.
        
        Args:
            name: Unique task identifier.
            func: Callable to execute.
            delay: Delay in seconds before execution.
            args: Positional arguments for func.
            kwargs: Keyword arguments for func.
            
        Returns:
            Task name.
        """
        with self._lock:
            if name in self._tasks:
                raise ValueError(f"Task '{name}' already exists")
            
            kwargs = kwargs or {}
            args = args or ()
            
            task = ScheduledTask(
                name=name,
                func=func,
                interval=None,
                next_run=time.time() + delay
            )
            task.kwargs = kwargs
            task.args = args
            
            self._tasks[name] = task
            
            if self._running:
                self._schedule_task(task)
            
            return name
    
    def remove_task(self, name: str) -> bool:
        """Remove and cancel a task.
        
        Args:
            name: Task name to remove.
            
        Returns:
            True if task was removed, False if not found.
        """
        with self._lock:
            if name in self._tasks:
                self._tasks[name].active = False
                del self._tasks[name]
                return True
            return False
    
    def get_task(self, name: str) -> Optional[ScheduledTask]:
        """Get a task by name.
        
        Args:
            name: Task name.
            
        Returns:
            ScheduledTask or None if not found.
        """
        return self._tasks.get(name)
    
    def list_tasks(self) -> List[Dict[str, Any]]:
        """List all scheduled tasks.
        
        Returns:
            List of task information dictionaries.
        """
        with self._lock:
            return [
                {
                    "name": task.name,
                    "interval": task.interval,
                    "cron": task.cron,
                    "next_run": task.next_run,
                    "last_run": task.last_run,
                    "run_count": task.run_count,
                    "active": task.active
                }
                for task in self._tasks.values()
            ]
    
    def start(self) -> None:
        """Start the scheduler in a background thread."""
        if self._running:
            return
        
        self._running = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
    
    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        
        if self._thread:
            self._thread.join(timeout=5)
            self._thread = None
        
        self._scheduler.cancel_all()
    
    def _run_loop(self) -> None:
        """Main scheduler loop running in background thread."""
        while self._running:
            try:
                self._scheduler.run(blocking=True, timeout=1.0)
            except Exception:
                pass
            
            with self._lock:
                for task in list(self._tasks.values()):
                    if task.active and task.next_run <= time.time():
                        self._execute_task(task)
    
    def _execute_task(self, task: ScheduledTask) -> None:
        """Execute a scheduled task.
        
        Args:
            task: Task to execute.
        """
        try:
            task.func(*task.args, **task.kwargs)
            task.last_run = time.time()
            task.run_count += 1
            
            if task.interval:
                task.next_run = time.time() + task.interval
                self._schedule_task(task)
            elif task.cron:
                task.next_run = self._parse_cron_next(task.cron)
                self._schedule_task(task)
            else:
                task.active = False
        
        except Exception as e:
            task.last_run = time.time()
            
            if task.interval:
                task.next_run = time.time() + task.interval
                self._schedule_task(task)
            else:
                task.active = False
    
    def _schedule_task(self, task: ScheduledTask) -> None:
        """Schedule a task in the internal scheduler.
        
        Args:
            task: Task to schedule.
        """
        delay = max(0, task.next_run - time.time())
        
        self._scheduler.enter(
            delay=delay,
            priority=1,
            action=self._execute_task,
            argument=(task,)
        )
    
    def _parse_cron_next(self, cron_expr: str) -> float:
        """Parse a cron expression and calculate next run time.
        
        Simplified cron parser supporting: minute hour day month weekday
        
        Args:
            cron_expr: Cron expression string.
            
        Returns:
            Unix timestamp of next execution.
        """
        parts = cron_expr.split()
        if len(parts) != 5:
            raise ValueError(f"Invalid cron expression: {cron_expr}")
        
        now = datetime.now()
        next_run = now.replace(second=0, microsecond=0)
        
        minute_str, hour_str, day_str, month_str, weekday_str = parts
        
        minute = self._parse_cron_field(minute_str, 0, 59)
        
        if minute is not None and minute <= next_run.minute:
            next_run += timedelta(hours=1)
        
        if minute is not None:
            next_run = next_run.replace(minute=minute)
        
        hour = self._parse_cron_field(hour_str, 0, 23)
        if hour is not None:
            next_run = next_run.replace(hour=hour)
        
        return next_run.timestamp()
    
    def _parse_cron_field(
        self,
        field: str,
        min_val: int,
        max_val: int
    ) -> Optional[int]:
        """Parse a single cron field.
        
        Args:
            field: Cron field string.
            min_val: Minimum valid value.
            max_val: Maximum valid value.
            
        Returns:
            Parsed integer value or None if not applicable.
        """
        if field == "*":
            return None
        
        if "," in field:
            values = field.split(",")
            return int(values[0])
        
        if "/" in field:
            parts = field.split("/")
            base = int(parts[0]) if parts[0] != "*" else min_val
            step = int(parts[1])
            now = datetime.now()
            value = base + ((now.minute - base) // step) * step
            return min_val + (value - min_val) % (max_val - min_val + 1)
        
        return int(field)


class SchedulerAction(BaseAction):
    """Scheduler action for task scheduling.
    
    Supports interval, cron, and delayed task execution.
    """
    action_type: str = "scheduler"
    display_name: str = "调度动作"
    description: str = "任务调度，支持间隔执行、CRON表达式和延迟任务"
    
    def __init__(self) -> None:
        super().__init__()
        self._scheduler: Optional[Scheduler] = None
    
    def get_required_params(self) -> List[str]:
        """Return required parameters for this action."""
        return ["operation"]
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute scheduler operation.
        
        Args:
            context: Execution context.
            params: Operation and parameters.
            
        Returns:
            ActionResult with operation outcome.
        """
        start_time = time.time()
        
        try:
            operation = params.get("operation", "start")
            
            if operation == "start":
                return self._start(start_time)
            elif operation == "stop":
                return self._stop(start_time)
            elif operation == "add_interval":
                return self._add_interval(params, start_time)
            elif operation == "add_cron":
                return self._add_cron(params, start_time)
            elif operation == "add_delayed":
                return self._add_delayed(params, start_time)
            elif operation == "remove":
                return self._remove(params, start_time)
            elif operation == "list":
                return self._list_tasks(start_time)
            elif operation == "status":
                return self._status(start_time)
            else:
                return ActionResult(
                    success=False,
                    message=f"Unknown operation: {operation}",
                    duration=time.time() - start_time
                )
        
        except Exception as e:
            return ActionResult(
                success=False,
                message=f"Scheduler operation failed: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _ensure_scheduler(self) -> Scheduler:
        """Ensure scheduler exists."""
        if self._scheduler is None:
            self._scheduler = Scheduler()
        return self._scheduler
    
    def _start(self, start_time: float) -> ActionResult:
        """Start the scheduler."""
        scheduler = self._ensure_scheduler()
        scheduler.start()
        
        return ActionResult(
            success=True,
            message="Scheduler started",
            duration=time.time() - start_time
        )
    
    def _stop(self, start_time: float) -> ActionResult:
        """Stop the scheduler."""
        if self._scheduler:
            self._scheduler.stop()
        
        return ActionResult(
            success=True,
            message="Scheduler stopped",
            duration=time.time() - start_time
        )
    
    def _add_interval(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add an interval task."""
        scheduler = self._ensure_scheduler()
        name = params.get("name", "")
        interval = params.get("interval", 60)
        start_immediately = params.get("start_immediately", False)
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        if interval <= 0:
            return ActionResult(
                success=False,
                message="interval must be positive",
                duration=time.time() - start_time
            )
        
        def noop_task() -> None:
            pass
        
        try:
            scheduler.add_interval_task(
                name=name,
                func=noop_task,
                interval=interval,
                start_immediately=start_immediately
            )
            
            return ActionResult(
                success=True,
                message=f"Added interval task '{name}' (every {interval}s)",
                data={"name": name, "interval": interval},
                duration=time.time() - start_time
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=str(e),
                duration=time.time() - start_time
            )
    
    def _add_cron(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a cron task."""
        scheduler = self._ensure_scheduler()
        name = params.get("name", "")
        cron_expr = params.get("cron", "")
        
        if not name or not cron_expr:
            return ActionResult(
                success=False,
                message="name and cron are required",
                duration=time.time() - start_time
            )
        
        def noop_task() -> None:
            pass
        
        try:
            scheduler.add_cron_task(
                name=name,
                func=noop_task,
                cron_expr=cron_expr
            )
            
            return ActionResult(
                success=True,
                message=f"Added cron task '{name}' ({cron_expr})",
                data={"name": name, "cron": cron_expr},
                duration=time.time() - start_time
            )
        except (ValueError, Exception) as e:
            return ActionResult(
                success=False,
                message=f"Failed to add cron task: {str(e)}",
                duration=time.time() - start_time
            )
    
    def _add_delayed(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Add a delayed task."""
        scheduler = self._ensure_scheduler()
        name = params.get("name", "")
        delay = params.get("delay", 60)
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        if delay <= 0:
            return ActionResult(
                success=False,
                message="delay must be positive",
                duration=time.time() - start_time
            )
        
        def noop_task() -> None:
            pass
        
        try:
            scheduler.add_delayed_task(
                name=name,
                func=noop_task,
                delay=delay
            )
            
            return ActionResult(
                success=True,
                message=f"Added delayed task '{name}' (execute in {delay}s)",
                data={"name": name, "delay": delay},
                duration=time.time() - start_time
            )
        except ValueError as e:
            return ActionResult(
                success=False,
                message=str(e),
                duration=time.time() - start_time
            )
    
    def _remove(self, params: Dict[str, Any], start_time: float) -> ActionResult:
        """Remove a task."""
        if not self._scheduler:
            return ActionResult(
                success=False,
                message="Scheduler not started",
                duration=time.time() - start_time
            )
        
        name = params.get("name", "")
        
        if not name:
            return ActionResult(
                success=False,
                message="name is required",
                duration=time.time() - start_time
            )
        
        removed = self._scheduler.remove_task(name)
        
        return ActionResult(
            success=removed,
            message=f"Removed task '{name}'" if removed else f"Task '{name}' not found",
            data={"removed": removed},
            duration=time.time() - start_time
        )
    
    def _list_tasks(self, start_time: float) -> ActionResult:
        """List all tasks."""
        if not self._scheduler:
            return ActionResult(
                success=True,
                message="Scheduler not started, no tasks",
                data={"tasks": [], "count": 0},
                duration=time.time() - start_time
            )
        
        tasks = self._scheduler.list_tasks()
        
        return ActionResult(
            success=True,
            message=f"Found {len(tasks)} scheduled tasks",
            data={"tasks": tasks, "count": len(tasks)},
            duration=time.time() - start_time
        )
    
    def _status(self, start_time: float) -> ActionResult:
        """Get scheduler status."""
        running = self._scheduler is not None and self._scheduler._running
        task_count = len(self._scheduler.list_tasks()) if self._scheduler else 0
        
        return ActionResult(
            success=True,
            message=f"Scheduler {'running' if running else 'stopped'} with {task_count} tasks",
            data={"running": running, "task_count": task_count},
            duration=time.time() - start_time
        )
