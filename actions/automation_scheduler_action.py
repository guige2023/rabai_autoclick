"""Automation scheduler action module for RabAI AutoClick.

Provides scheduling operations for automation workflows:
- CronSchedulerAction: Schedule tasks with cron expressions
- IntervalSchedulerAction: Schedule tasks at intervals
- OneTimeSchedulerAction: Schedule one-time tasks
- PrioritySchedulerAction: Schedule tasks by priority
- RateLimitedSchedulerAction: Schedule with rate limiting
"""

from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
import time

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ScheduledTask:
    """Represents a scheduled task."""
    
    def __init__(self, task_id: str, name: str, action: Callable, schedule: Any, params: Dict = None):
        self.task_id = task_id
        self.name = name
        self.action = action
        self.schedule = schedule
        self.params = params or {}
        self.last_run: Optional[datetime] = None
        self.next_run: Optional[datetime] = None
        self.run_count = 0
        self.enabled = True
    
    def to_dict(self) -> Dict:
        return {
            "task_id": self.task_id,
            "name": self.name,
            "schedule": str(self.schedule),
            "last_run": self.last_run.isoformat() if self.last_run else None,
            "next_run": self.next_run.isoformat() if self.next_run else None,
            "run_count": self.run_count,
            "enabled": self.enabled
        }


class CronSchedulerAction(BaseAction):
    """Schedule tasks with cron expressions."""
    action_type = "cron_scheduler"
    display_name = "Cron调度"
    description = "使用Cron表达式调度任务"
    
    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, ScheduledTask] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "schedule")
            
            if operation == "schedule":
                return self._schedule_task(params)
            elif operation == "list":
                return self._list_tasks()
            elif operation == "cancel":
                return self._cancel_task(params)
            elif operation == "due":
                return self._get_due_tasks()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _schedule_task(self, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id", str(int(time.time())))
        name = params.get("name")
        cron_expr = params.get("cron_expression")
        action = params.get("action")
        
        if not name or not cron_expr:
            return ActionResult(success=False, message="name and cron_expression are required")
        
        task = ScheduledTask(task_id, name, action, cron_expr, params.get("params"))
        task.next_run = self._calculate_next_run(cron_expr)
        
        self._tasks[task_id] = task
        
        return ActionResult(
            success=True,
            message=f"Task scheduled: {name}",
            data={
                "task_id": task_id,
                "name": name,
                "cron_expression": cron_expr,
                "next_run": task.next_run.isoformat()
            }
        )
    
    def _calculate_next_run(self, cron_expr: str) -> datetime:
        now = datetime.now()
        
        parts = cron_expr.split()
        if len(parts) >= 5:
            minute = parts[0]
            hour = parts[1]
            day = parts[2]
            month = parts[3]
            weekday = parts[4]
        
        next_run = now + timedelta(hours=1)
        next_run = next_run.replace(minute=0, second=0, microsecond=0)
        
        return next_run
    
    def _list_tasks(self) -> ActionResult:
        tasks = [task.to_dict() for task in self._tasks.values()]
        
        return ActionResult(
            success=True,
            message=f"{len(tasks)} scheduled tasks",
            data={"tasks": tasks, "count": len(tasks)}
        )
    
    def _cancel_task(self, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id")
        
        if task_id in self._tasks:
            del self._tasks[task_id]
            return ActionResult(success=True, message=f"Task {task_id} cancelled")
        
        return ActionResult(success=False, message=f"Task {task_id} not found")
    
    def _get_due_tasks(self) -> ActionResult:
        now = datetime.now()
        due_tasks = []
        
        for task in self._tasks.values():
            if task.enabled and task.next_run and task.next_run <= now:
                due_tasks.append(task.to_dict())
        
        return ActionResult(
            success=True,
            message=f"{len(due_tasks)} tasks due for execution",
            data={"due_tasks": due_tasks}
        )


class IntervalSchedulerAction(BaseAction):
    """Schedule tasks at intervals."""
    action_type = "interval_scheduler"
    display_name = "间隔调度"
    description = "按固定间隔调度任务"
    
    def __init__(self):
        super().__init__()
        self._scheduled_tasks: Dict[str, Dict] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "schedule")
            
            if operation == "schedule":
                return self._schedule_interval(params)
            elif operation == "list":
                return self._list_intervals()
            elif operation == "cancel":
                return self._cancel_interval(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _schedule_interval(self, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id", str(int(time.time())))
        interval_seconds = params.get("interval_seconds", 60)
        
        self._scheduled_tasks[task_id] = {
            "task_id": task_id,
            "interval_seconds": interval_seconds,
            "name": params.get("name"),
            "last_run": None,
            "next_run": datetime.now() + timedelta(seconds=interval_seconds),
            "action": params.get("action"),
            "params": params.get("params", {})
        }
        
        return ActionResult(
            success=True,
            message=f"Interval task scheduled",
            data={
                "task_id": task_id,
                "interval_seconds": interval_seconds,
                "next_run": self._scheduled_tasks[task_id]["next_run"].isoformat()
            }
        )
    
    def _list_intervals(self) -> ActionResult:
        tasks = []
        for task in self._scheduled_tasks.values():
            tasks.append({
                "task_id": task["task_id"],
                "name": task["name"],
                "interval_seconds": task["interval_seconds"],
                "last_run": task["last_run"].isoformat() if task["last_run"] else None,
                "next_run": task["next_run"].isoformat() if task["next_run"] else None
            })
        
        return ActionResult(
            success=True,
            message=f"{len(tasks)} interval tasks",
            data={"tasks": tasks}
        )
    
    def _cancel_interval(self, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id")
        
        if task_id in self._scheduled_tasks:
            del self._scheduled_tasks[task_id]
            return ActionResult(success=True, message=f"Interval task {task_id} cancelled")
        
        return ActionResult(success=False, message=f"Task {task_id} not found")


class OneTimeSchedulerAction(BaseAction):
    """Schedule one-time tasks."""
    action_type = "one_time_scheduler"
    display_name = "一次性调度"
    description = "调度一次性任务"
    
    def __init__(self):
        super().__init__()
        self._one_time_tasks: Dict[str, Dict] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "schedule")
            
            if operation == "schedule":
                return self._schedule_one_time(params)
            elif operation == "list":
                return self._list_one_time()
            elif operation == "cancel":
                return self._cancel_one_time(params)
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _schedule_one_time(self, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id", str(int(time.time())))
        run_at = params.get("run_at")
        
        if isinstance(run_at, str):
            from dateutil import parser
            run_at = parser.parse(run_at)
        
        self._one_time_tasks[task_id] = {
            "task_id": task_id,
            "name": params.get("name"),
            "run_at": run_at,
            "action": params.get("action"),
            "params": params.get("params", {}),
            "executed": False
        }
        
        return ActionResult(
            success=True,
            message="One-time task scheduled",
            data={
                "task_id": task_id,
                "name": params.get("name"),
                "run_at": run_at.isoformat() if run_at else None
            }
        )
    
    def _list_one_time(self) -> ActionResult:
        tasks = []
        for task in self._one_time_tasks.values():
            if not task["executed"]:
                tasks.append({
                    "task_id": task["task_id"],
                    "name": task["name"],
                    "run_at": task["run_at"].isoformat() if task["run_at"] else None,
                    "executed": task["executed"]
                })
        
        return ActionResult(
            success=True,
            message=f"{len(tasks)} pending one-time tasks",
            data={"tasks": tasks}
        )
    
    def _cancel_one_time(self, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id")
        
        if task_id in self._one_time_tasks:
            del self._one_time_tasks[task_id]
            return ActionResult(success=True, message=f"One-time task {task_id} cancelled")
        
        return ActionResult(success=False, message=f"Task {task_id} not found")


class PrioritySchedulerAction(BaseAction):
    """Schedule tasks by priority."""
    action_type = "priority_scheduler"
    display_name = "优先级调度"
    description = "按优先级调度任务"
    
    def __init__(self):
        super().__init__()
        self._priority_queue: List[Dict] = []
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "enqueue")
            
            if operation == "enqueue":
                return self._enqueue_task(params)
            elif operation == "dequeue":
                return self._dequeue_task()
            elif operation == "list":
                return self._list_queue()
            elif operation == "peek":
                return self._peek_queue()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _enqueue_task(self, params: Dict[str, Any]) -> ActionResult:
        priority = params.get("priority", 5)
        task = {
            "task_id": params.get("task_id", str(int(time.time()))),
            "name": params.get("name"),
            "priority": priority,
            "action": params.get("action"),
            "params": params.get("params", {}),
            "enqueued_at": datetime.now()
        }
        
        self._priority_queue.append(task)
        self._priority_queue.sort(key=lambda x: x["priority"], reverse=True)
        
        return ActionResult(
            success=True,
            message=f"Task enqueued with priority {priority}",
            data={
                "task_id": task["task_id"],
                "priority": priority,
                "queue_size": len(self._priority_queue)
            }
        )
    
    def _dequeue_task(self) -> ActionResult:
        if not self._priority_queue:
            return ActionResult(success=False, message="Queue is empty")
        
        task = self._priority_queue.pop(0)
        
        return ActionResult(
            success=True,
            message=f"Task dequeued: {task['name']}",
            data={
                "task": task,
                "remaining": len(self._priority_queue)
            }
        )
    
    def _list_queue(self) -> ActionResult:
        return ActionResult(
            success=True,
            message=f"Priority queue: {len(self._priority_queue)} tasks",
            data={
                "tasks": self._priority_queue,
                "count": len(self._priority_queue)
            }
        )
    
    def _peek_queue(self) -> ActionResult:
        if not self._priority_queue:
            return ActionResult(success=False, message="Queue is empty")
        
        return ActionResult(
            success=True,
            message=f"Next task: {self._priority_queue[0]['name']}",
            data={"task": self._priority_queue[0]}
        )


class RateLimitedSchedulerAction(BaseAction):
    """Schedule with rate limiting."""
    action_type = "rate_limited_scheduler"
    display_name = "限流调度"
    description = "带速率限制的任务调度"
    
    def __init__(self):
        super().__init__()
        self._rate_limit = 10
        self._window_seconds = 60
        self._task_history: List[datetime] = []
        self._scheduled: Dict[str, Dict] = {}
    
    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "schedule")
            
            if operation == "schedule":
                return self._schedule_rate_limited(params)
            elif operation == "configure":
                return self._configure_rate_limit(params)
            elif operation == "status":
                return self._get_rate_limit_status()
            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Error: {str(e)}")
    
    def _schedule_rate_limited(self, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id", str(int(time.time())))
        
        if not self._can_execute():
            wait_time = self._get_wait_time()
            return ActionResult(
                success=False,
                message="Rate limit exceeded",
                data={
                    "task_id": task_id,
                    "allowed": False,
                    "wait_seconds": wait_time
                }
            )
        
        self._task_history.append(datetime.now())
        
        self._scheduled[task_id] = {
            "task_id": task_id,
            "name": params.get("name"),
            "action": params.get("action"),
            "params": params.get("params", {}),
            "scheduled_at": datetime.now()
        }
        
        return ActionResult(
            success=True,
            message=f"Task scheduled within rate limit",
            data={
                "task_id": task_id,
                "allowed": True,
                "tasks_in_window": len(self._task_history)
            }
        )
    
    def _can_execute(self) -> bool:
        now = datetime.now()
        cutoff = now - timedelta(seconds=self._window_seconds)
        
        self._task_history = [t for t in self._task_history if t > cutoff]
        
        return len(self._task_history) < self._rate_limit
    
    def _get_wait_time(self) -> float:
        if not self._task_history:
            return 0
        
        oldest = min(self._task_history)
        elapsed = (datetime.now() - oldest).total_seconds()
        return max(0, self._window_seconds - elapsed)
    
    def _configure_rate_limit(self, params: Dict[str, Any]) -> ActionResult:
        rate_limit = params.get("rate_limit")
        window_seconds = params.get("window_seconds")
        
        if rate_limit is not None:
            self._rate_limit = rate_limit
        if window_seconds is not None:
            self._window_seconds = window_seconds
        
        return ActionResult(
            success=True,
            message="Rate limit configured",
            data={
                "rate_limit": self._rate_limit,
                "window_seconds": self._window_seconds
            }
        )
    
    def _get_rate_limit_status(self) -> ActionResult:
        now = datetime.now()
        cutoff = now - timedelta(seconds=self._window_seconds)
        self._task_history = [t for t in self._task_history if t > cutoff]
        
        return ActionResult(
            success=True,
            message="Rate limit status",
            data={
                "rate_limit": self._rate_limit,
                "window_seconds": self._window_seconds,
                "current_usage": len(self._task_history),
                "available": self._rate_limit - len(self._task_history),
                "utilization": len(self._task_history) / self._rate_limit if self._rate_limit > 0 else 0
            }
        )
