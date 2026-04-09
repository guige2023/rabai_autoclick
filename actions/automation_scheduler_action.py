"""Automation scheduling action module for RabAI AutoClick.

Provides scheduling operations:
- CronScheduleAction: Cron-based scheduling
- IntervalScheduleAction: Interval-based scheduling
- CalendarScheduleAction: Calendar-based scheduling
- ScheduleManagerAction: Manage scheduled tasks
"""

import sys
import os
import time
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field
import threading

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult

logger = logging.getLogger(__name__)


@dataclass
class ScheduledTask:
    """Represents a scheduled automation task."""
    task_id: str
    name: str
    action_type: str
    params: Dict[str, Any]
    schedule_type: str
    schedule_value: str
    enabled: bool = True
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    run_count: int = 0
    error_count: int = 0


class ScheduleRegistry:
    """Registry for scheduled tasks."""
    
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._tasks = {}
                    cls._instance._running = False
        return cls._instance

    def add_task(self, task: ScheduledTask) -> None:
        self._tasks[task.task_id] = task

    def remove_task(self, task_id: str) -> bool:
        if task_id in self._tasks:
            del self._tasks[task_id]
            return True
        return False

    def get_task(self, task_id: str) -> Optional[ScheduledTask]:
        return self._tasks.get(task_id)

    def list_tasks(self, enabled: Optional[bool] = None) -> List[ScheduledTask]:
        tasks = list(self._tasks.values())
        if enabled is not None:
            tasks = [t for t in tasks if t.enabled == enabled]
        return tasks

    def update_next_run(self, task_id: str, next_run: datetime) -> None:
        task = self.get_task(task_id)
        if task:
            task.next_run = next_run


_registry = ScheduleRegistry()


def parse_cron(cron_expr: str) -> Dict[str, int]:
    """Parse a simple cron expression (min hour day month dow)."""
    parts = cron_expr.split()
    if len(parts) != 5:
        raise ValueError("Cron expression must have 5 fields")
    return {
        "minute": int(parts[0]) if parts[0] != "*" else -1,
        "hour": int(parts[1]) if parts[1] != "*" else -1,
        "day": int(parts[2]) if parts[2] != "*" else -1,
        "month": int(parts[3]) if parts[3] != "*" else -1,
        "dow": int(parts[4]) if parts[4] != "*" else -1
    }


def get_next_cron_run(cron_expr: str, from_time: Optional[datetime] = None) -> datetime:
    """Calculate next run time from cron expression."""
    parts = parse_cron(cron_expr)
    dt = from_time or datetime.now()
    for _ in range(366 * 24 * 60):
        dt += timedelta(minutes=1)
        match = True
        if parts["minute"] != -1 and dt.minute != parts["minute"]:
            match = False
        if parts["hour"] != -1 and dt.hour != parts["hour"]:
            match = False
        if parts["day"] != -1 and dt.day != parts["day"]:
            match = False
        if parts["month"] != -1 and dt.month != parts["month"]:
            match = False
        if parts["dow"] != -1 and dt.weekday() != parts["dow"]:
            match = False
        if match:
            return dt
    raise RuntimeError("Could not find next cron run in 1 year")


class CronScheduleAction(BaseAction):
    """Schedule a task using cron expression."""
    action_type = "automation_cron_schedule"
    display_name = "Cron定时调度"
    description = "使用Cron表达式调度自动化任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id", "")
        name = params.get("name", "")
        action_type = params.get("action_type", "")
        cron_expr = params.get("cron_expr", "")
        params_dict = params.get("params", {})

        if not task_id or not name or not cron_expr:
            return ActionResult(success=False, message="task_id、name和cron_expr都是必需的")

        try:
            next_run = get_next_cron_run(cron_expr)
        except ValueError as e:
            return ActionResult(success=False, message=f"Cron表达式错误: {e}")

        task = ScheduledTask(
            task_id=task_id,
            name=name,
            action_type=action_type,
            params=params_dict,
            schedule_type="cron",
            schedule_value=cron_expr,
            next_run=next_run
        )
        _registry.add_task(task)

        return ActionResult(
            success=True,
            message=f"任务 {name} 已调度，下次于 {next_run.strftime('%Y-%m-%d %H:%M')}",
            data={"task_id": task_id, "next_run": next_run.isoformat()}
        )


class IntervalScheduleAction(BaseAction):
    """Schedule a task using interval."""
    action_type = "automation_interval_schedule"
    display_name = "间隔定时调度"
    description = "使用固定间隔调度自动化任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id", "")
        name = params.get("name", "")
        action_type = params.get("action_type", "")
        interval_seconds = params.get("interval_seconds", 60)
        params_dict = params.get("params", {})

        if not task_id or not name:
            return ActionResult(success=False, message="task_id和name是必需的")

        valid, msg = self.validate_positive(interval_seconds, "interval_seconds", allow_zero=False)
        if not valid:
            return ActionResult(success=False, message=msg)

        next_run = datetime.now() + timedelta(seconds=interval_seconds)
        task = ScheduledTask(
            task_id=task_id,
            name=name,
            action_type=action_type,
            params=params_dict,
            schedule_type="interval",
            schedule_value=str(interval_seconds),
            next_run=next_run
        )
        _registry.add_task(task)

        return ActionResult(
            success=True,
            message=f"任务 {name} 已调度，间隔 {interval_seconds}秒",
            data={"task_id": task_id, "next_run": next_run.isoformat()}
        )


class CalendarScheduleAction(BaseAction):
    """Schedule a task using calendar dates."""
    action_type = "automation_calendar_schedule"
    display_name = "日历定时调度"
    description = "使用日历日期调度自动化任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        task_id = params.get("task_id", "")
        name = params.get("name", "")
        action_type = params.get("action_type", "")
        run_times = params.get("run_times", [])
        params_dict = params.get("params", {})

        if not task_id or not name or not run_times:
            return ActionResult(success=False, message="task_id、name和run_times是必需的")

        if not isinstance(run_times, list):
            return ActionResult(success=False, message="run_times必须是时间字符串列表")

        next_run = None
        now = datetime.now()
        for rt in run_times:
            try:
                dt = datetime.strptime(rt, "%Y-%m-%d %H:%M")
                if dt > now:
                    if next_run is None or dt < next_run:
                        next_run = dt
            except ValueError:
                continue

        if next_run is None:
            return ActionResult(success=False, message="没有找到有效的未来运行时间")

        task = ScheduledTask(
            task_id=task_id,
            name=name,
            action_type=action_type,
            params=params_dict,
            schedule_type="calendar",
            schedule_value=",".join(run_times),
            next_run=next_run
        )
        _registry.add_task(task)

        return ActionResult(
            success=True,
            message=f"任务 {name} 已调度，首次运行于 {next_run.strftime('%Y-%m-%d %H:%M')}",
            data={"task_id": task_id, "next_run": next_run.isoformat()}
        )


class ScheduleManagerAction(BaseAction):
    """Manage scheduled tasks (list, cancel, pause, resume)."""
    action_type = "automation_schedule_manager"
    display_name = "调度任务管理"
    description = "管理已调度的自动化任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        operation = params.get("operation", "list")
        task_id = params.get("task_id", "")

        if operation == "list":
            enabled_only = params.get("enabled_only", False)
            tasks = _registry.list_tasks(enabled=None if not enabled_only else True)
            result = [
                {
                    "task_id": t.task_id,
                    "name": t.name,
                    "schedule_type": t.schedule_type,
                    "schedule_value": t.schedule_value,
                    "enabled": t.enabled,
                    "next_run": t.next_run.isoformat() if t.next_run else None,
                    "last_run": t.last_run.isoformat() if t.last_run else None,
                    "run_count": t.run_count
                }
                for t in tasks
            ]
            return ActionResult(
                success=True,
                message=f"找到 {len(result)} 个调度任务",
                data={"tasks": result}
            )

        if not task_id:
            return ActionResult(success=False, message="operation为list时不需要task_id，其他操作需要")

        task = _registry.get_task(task_id)
        if not task:
            return ActionResult(success=False, message=f"任务 {task_id} 不存在")

        if operation == "cancel":
            _registry.remove_task(task_id)
            return ActionResult(success=True, message=f"任务 {task_id} 已取消")

        if operation == "pause":
            task.enabled = False
            return ActionResult(success=True, message=f"任务 {task_id} 已暂停")

        if operation == "resume":
            task.enabled = True
            if task.schedule_type == "cron":
                task.next_run = get_next_cron_run(task.schedule_value)
            elif task.schedule_type == "interval":
                interval = float(task.schedule_value)
                task.next_run = datetime.now() + timedelta(seconds=interval)
            return ActionResult(success=True, message=f"任务 {task_id} 已恢复")

        return ActionResult(success=False, message=f"未知操作: {operation}")
