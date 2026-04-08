"""Scheduler action module for RabAI AutoClick.

Provides scheduling operations:
- ScheduleTaskAction: Schedule a task
- ScheduleCronAction: Schedule via cron
- ScheduleIntervalAction: Schedule by interval
- ScheduleDelayAction: Delay execution
- ScheduleCancelAction: Cancel scheduled task
- ScheduleListAction: List scheduled tasks
- SchedulePauseAction: Pause scheduled task
- ScheduleResumeAction: Resume scheduled task
"""

import os
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SchedulerStore:
    """In-memory scheduler storage."""
    
    _tasks: Dict[str, Dict[str, Any]] = {}
    _task_id = 1
    
    @classmethod
    def add(cls, task: Dict[str, Any]) -> str:
        task["id"] = cls._task_id
        cls._task_id += 1
        cls._tasks[task["id"]] = task
        return str(task["id"])
    
    @classmethod
    def get(cls, task_id: str) -> Optional[Dict[str, Any]]:
        return cls._tasks.get(int(task_id))
    
    @classmethod
    def list_all(cls) -> List[Dict[str, Any]]:
        return list(cls._tasks.values())
    
    @classmethod
    def cancel(cls, task_id: str) -> bool:
        tid = int(task_id)
        if tid in cls._tasks:
            cls._tasks[tid]["status"] = "cancelled"
            return True
        return False
    
    @classmethod
    def pause(cls, task_id: str) -> bool:
        tid = int(task_id)
        if tid in cls._tasks:
            cls._tasks[tid]["status"] = "paused"
            return True
        return False
    
    @classmethod
    def resume(cls, task_id: str) -> bool:
        tid = int(task_id)
        if tid in cls._tasks:
            cls._tasks[tid]["status"] = "scheduled"
            return True
        return False


class ScheduleTaskAction(BaseAction):
    """Schedule a task."""
    action_type = "schedule_task"
    display_name = "计划任务"
    description = "安排计划任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_name = params.get("name", "")
            execute_at = params.get("execute_at", time.time() + 60)
            payload = params.get("payload", {})
            repeat = params.get("repeat", False)
            interval = params.get("interval", 0)
            
            if not task_name:
                return ActionResult(success=False, message="name required")
            
            task = {
                "name": task_name,
                "execute_at": execute_at if isinstance(execute_at, float) else datetime.fromisoformat(execute_at).timestamp(),
                "payload": payload,
                "status": "scheduled",
                "repeat": repeat,
                "interval": interval,
                "created_at": time.time()
            }
            
            task_id = SchedulerStore.add(task)
            
            return ActionResult(
                success=True,
                message=f"Scheduled task: {task_name} (ID: {task_id})",
                data={"task_id": task_id, "task": task}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schedule task failed: {str(e)}")


class ScheduleCronAction(BaseAction):
    """Schedule via cron expression."""
    action_type = "schedule_cron"
    display_name = "Cron调度"
    description = "Cron表达式调度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            cron_expr = params.get("cron", "* * * * *")
            task_name = params.get("name", "")
            payload = params.get("payload", {})
            
            parts = cron_expr.split()
            if len(parts) != 5:
                return ActionResult(success=False, message="Invalid cron expression (need 5 fields)")
            
            task = {
                "name": task_name or f"cron-{int(time.time())}",
                "cron": cron_expr,
                "payload": payload,
                "status": "scheduled",
                "type": "cron",
                "created_at": time.time()
            }
            
            task_id = SchedulerStore.add(task)
            
            return ActionResult(
                success=True,
                message=f"Scheduled cron task: {cron_expr} (ID: {task_id})",
                data={"task_id": task_id, "cron": cron_expr}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schedule cron failed: {str(e)}")


class ScheduleIntervalAction(BaseAction):
    """Schedule by interval."""
    action_type = "schedule_interval"
    display_name = "间隔调度"
    description = "间隔调度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            interval = params.get("interval", 60)
            task_name = params.get("name", "")
            payload = params.get("payload", {})
            start_now = params.get("start_now", True)
            
            if interval < 1:
                return ActionResult(success=False, message="Interval must be >= 1 second")
            
            task = {
                "name": task_name or f"interval-{int(time.time())}",
                "interval": interval,
                "payload": payload,
                "status": "scheduled",
                "type": "interval",
                "start_now": start_now,
                "created_at": time.time()
            }
            
            task_id = SchedulerStore.add(task)
            
            return ActionResult(
                success=True,
                message=f"Scheduled interval task every {interval}s (ID: {task_id})",
                data={"task_id": task_id, "interval": interval}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schedule interval failed: {str(e)}")


class ScheduleDelayAction(BaseAction):
    """Delay execution."""
    action_type = "schedule_delay"
    display_name = "延迟执行"
    description = "延迟执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            delay_seconds = params.get("delay", 60)
            task_name = params.get("name", "")
            payload = params.get("payload", {})
            
            execute_at = time.time() + delay_seconds
            
            task = {
                "name": task_name or f"delayed-{int(time.time())}",
                "execute_at": execute_at,
                "payload": payload,
                "status": "scheduled",
                "type": "delay",
                "created_at": time.time()
            }
            
            task_id = SchedulerStore.add(task)
            
            return ActionResult(
                success=True,
                message=f"Delayed task scheduled for {delay_seconds}s later (ID: {task_id})",
                data={"task_id": task_id, "execute_at": execute_at, "delay": delay_seconds}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schedule delay failed: {str(e)}")


class ScheduleCancelAction(BaseAction):
    """Cancel scheduled task."""
    action_type = "schedule_cancel"
    display_name = "取消计划"
    description = "取消计划任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_id = params.get("task_id", "")
            
            if not task_id:
                return ActionResult(success=False, message="task_id required")
            
            cancelled = SchedulerStore.cancel(task_id)
            
            return ActionResult(
                success=cancelled,
                message=f"Cancelled task: {task_id}" if cancelled else f"Task not found: {task_id}",
                data={"task_id": task_id, "cancelled": cancelled}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schedule cancel failed: {str(e)}")


class ScheduleListAction(BaseAction):
    """List scheduled tasks."""
    action_type = "schedule_list"
    display_name = "计划列表"
    description = "列出计划任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            status = params.get("status", "")
            
            tasks = SchedulerStore.list_all()
            
            if status:
                tasks = [t for t in tasks if t.get("status") == status]
            
            return ActionResult(
                success=True,
                message=f"Found {len(tasks)} scheduled tasks",
                data={"tasks": tasks, "count": len(tasks)}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schedule list failed: {str(e)}")


class SchedulePauseAction(BaseAction):
    """Pause scheduled task."""
    action_type = "schedule_pause"
    display_name = "暂停计划"
    description = "暂停计划任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_id = params.get("task_id", "")
            
            if not task_id:
                return ActionResult(success=False, message="task_id required")
            
            paused = SchedulerStore.pause(task_id)
            
            return ActionResult(
                success=paused,
                message=f"Paused task: {task_id}" if paused else f"Task not found: {task_id}",
                data={"task_id": task_id, "paused": paused}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schedule pause failed: {str(e)}")


class ScheduleResumeAction(BaseAction):
    """Resume scheduled task."""
    action_type = "schedule_resume"
    display_name = "恢复计划"
    description = "恢复计划任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            task_id = params.get("task_id", "")
            
            if not task_id:
                return ActionResult(success=False, message="task_id required")
            
            resumed = SchedulerStore.resume(task_id)
            
            return ActionResult(
                success=resumed,
                message=f"Resumed task: {task_id}" if resumed else f"Task not found: {task_id}",
                data={"task_id": task_id, "resumed": resumed}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Schedule resume failed: {str(e)}")
