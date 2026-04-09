"""Automation Guardian Action Module.

Provides watchdog/guardian pattern for monitoring automation health,
detecting stalls, and triggering recovery actions.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


@dataclass
class GuardianConfig:
    """Guardian monitoring configuration."""
    name: str
    check_interval: float = 5.0
    stall_threshold: float = 30.0
    max_restarts: int = 3
    restart_cooldown: float = 10.0


@dataclass
class MonitoredTask:
    """State of a monitored task."""
    task_id: str
    name: str
    last_check_time: float = 0.0
    last_progress_time: float = 0.0
    restart_count: int = 0
    last_restart_time: float = 0.0
    status: str = "running"  # running, stalled, recovering, stopped
    progress_hints: List[str] = field(default_factory=list)


class AutomationGuardianAction(BaseAction):
    """Guardian/watcher for automation health monitoring.

    Monitors automation tasks for stalls, logs progress,
    and triggers recovery actions when needed.

    Args:
        context: Execution context.
        params: Dict with keys:
            - tasks: List[Dict] with task configs
            - operation: Operation type (register, check, recover, status)
            - task_id: ID of task to monitor
    """
    action_type = "automation_guardian"
    display_name = "自动化守护"
    description = "监控自动化任务健康状态与恢复"

    def get_required_params(self) -> List[str]:
        return ["operation"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "tasks": [],
            "task_id": None,
            "stall_threshold": 30.0,
            "check_interval": 5.0,
            "max_restarts": 3,
        }

    def __init__(self) -> None:
        super().__init__()
        self._tasks: Dict[str, MonitoredTask] = {}
        self._check_callbacks: Dict[str, Callable] = {}
        self._recovery_handlers: Dict[str, Callable] = {}

    def execute(
        self,
        context: Any,
        params: Dict[str, Any]
    ) -> ActionResult:
        """Execute guardian operation."""
        start_time = time.time()

        operation = params.get("operation", "status")
        tasks = params.get("tasks", [])
        task_id = params.get("task_id")
        stall_threshold = params.get("stall_threshold", 30.0)
        max_restarts = params.get("max_restarts", 3)

        if operation == "register":
            return self._register_tasks(tasks, stall_threshold, max_restarts, start_time)
        elif operation == "check":
            return self._check_task(task_id, start_time)
        elif operation == "recover":
            return self._recover_task(task_id, start_time)
        elif operation == "report_progress":
            return self._report_progress(task_id, params.get("progress_info"), start_time)
        elif operation == "status":
            return self._get_guardian_status(start_time)
        elif operation == "deregister":
            return self._deregister_task(task_id, start_time)
        else:
            return ActionResult(
                success=False,
                message=f"Unknown operation: {operation}",
                duration=time.time() - start_time
            )

    def _register_tasks(
        self,
        tasks: List[Dict],
        stall_threshold: float,
        max_restarts: int,
        start_time: float
    ) -> ActionResult:
        """Register tasks for monitoring."""
        registered = 0
        for task_config in tasks:
            task_id = task_config.get("task_id") or task_config.get("name")
            name = task_config.get("name", task_id)
            if task_id:
                self._tasks[task_id] = MonitoredTask(
                    task_id=task_id,
                    name=name,
                    last_check_time=time.time(),
                    last_progress_time=time.time(),
                    stall_threshold=stall_threshold,
                    max_restarts=max_restarts,
                )
                registered += 1

        return ActionResult(
            success=True,
            message=f"Registered {registered} tasks for monitoring",
            data={"registered_count": registered},
            duration=time.time() - start_time
        )

    def _check_task(self, task_id: Optional[str], start_time: float) -> ActionResult:
        """Check if a task is stalled."""
        if not task_id:
            return ActionResult(success=False, message="task_id required for check", duration=time.time() - start_time)

        if task_id not in self._tasks:
            return ActionResult(success=False, message=f"Task '{task_id}' not registered", duration=time.time() - start_time)

        task = self._tasks[task_id]
        now = time.time()
        elapsed_since_progress = now - task.last_progress_time
        elapsed_since_check = now - task.last_check_time

        task.last_check_time = now

        is_stalled = elapsed_since_progress > task.stall_threshold
        task.status = "stalled" if is_stalled else "running"

        return ActionResult(
            success=not is_stalled,
            message=f"Task '{task_id}' is {'STALLED' if is_stalled else 'running'}",
            data={
                "task_id": task_id,
                "status": task.status,
                "seconds_since_progress": elapsed_since_progress,
                "seconds_since_check": elapsed_since_check,
                "restart_count": task.restart_count,
                "progress_hints": task.progress_hints[-5:] if task.progress_hints else []
            },
            duration=time.time() - start_time
        )

    def _recover_task(self, task_id: Optional[str], start_time: float) -> ActionResult:
        """Attempt to recover a stalled task."""
        if not task_id:
            return ActionResult(success=False, message="task_id required for recover", duration=time.time() - start_time)

        if task_id not in self._tasks:
            return ActionResult(success=False, message=f"Task '{task_id}' not registered", duration=time.time() - start_time)

        task = self._tasks[task_id]

        if task.restart_count >= task.max_restarts:
            return ActionResult(
                success=False,
                message=f"Max restarts ({task.max_restarts}) reached for '{task_id}'",
                data={"task_id": task_id, "restart_count": task.restart_count, "max_restarts": task.max_restarts},
                duration=time.time() - start_time
            )

        now = time.time()
        if task.last_restart_time > 0 and (now - task.last_restart_time) < 10.0:
            return ActionResult(
                success=False,
                message=f"Cooldown not elapsed for '{task_id}'",
                data={"task_id": task_id, "cooldown_remaining": 10.0 - (now - task.last_restart_time)},
                duration=time.time() - start_time
            )

        task.restart_count += 1
        task.last_restart_time = now
        task.last_progress_time = now
        task.status = "recovering"

        return ActionResult(
            success=True,
            message=f"Recovery initiated for '{task_id}' (attempt {task.restart_count}/{task.max_restarts})",
            data={
                "task_id": task_id,
                "restart_count": task.restart_count,
                "max_restarts": task.max_restarts,
                "status": "recovering"
            },
            duration=time.time() - start_time
        )

    def _report_progress(self, task_id: Optional[str], progress_info: Any, start_time: float) -> ActionResult:
        """Report progress for a monitored task."""
        if not task_id:
            return ActionResult(success=False, message="task_id required", duration=time.time() - start_time)

        if task_id not in self._tasks:
            return ActionResult(success=False, message=f"Task '{task_id}' not registered", duration=time.time() - start_time)

        task = self._tasks[task_id]
        task.last_progress_time = time.time()
        task.status = "running"

        hint = str(progress_info)[:100] if progress_info else "progress update"
        task.progress_hints.append(f"{time.strftime('%H:%M:%S')}: {hint}")
        if len(task.progress_hints) > 20:
            task.progress_hints = task.progress_hints[-20:]

        return ActionResult(
            success=True,
            message=f"Progress recorded for '{task_id}'",
            data={"task_id": task_id, "progress_hints_count": len(task.progress_hints)},
            duration=time.time() - start_time
        )

    def _get_guardian_status(self, start_time: float) -> ActionResult:
        """Get overall guardian monitoring status."""
        summary = {
            "total_tasks": len(self._tasks),
            "running": sum(1 for t in self._tasks.values() if t.status == "running"),
            "stalled": sum(1 for t in self._tasks.values() if t.status == "stalled"),
            "recovering": sum(1 for t in self._tasks.values() if t.status == "recovering"),
        }
        tasks_detail = {
            tid: {
                "name": t.name,
                "status": t.status,
                "restart_count": t.restart_count,
                "seconds_since_progress": time.time() - t.last_progress_time,
            }
            for tid, t in self._tasks.items()
        }
        return ActionResult(
            success=True,
            message="Guardian status retrieved",
            data={"summary": summary, "tasks": tasks_detail},
            duration=time.time() - start_time
        )

    def _deregister_task(self, task_id: Optional[str], start_time: float) -> ActionResult:
        """Deregister a task from monitoring."""
        if not task_id:
            return ActionResult(success=False, message="task_id required", duration=time.time() - start_time)

        if task_id in self._tasks:
            del self._tasks[task_id]
            return ActionResult(success=True, message=f"Task '{task_id}' deregistered", duration=time.time() - start_time)
        return ActionResult(success=False, message=f"Task '{task_id}' not found", duration=time.time() - start_time)


# Monkey-patch MonitoredTask to accept stall_threshold and max_restarts
def _patch_monitored_task():
    original_init = MonitoredTask.__init__
    def new_init(self, task_id: str, name: str, last_check_time: float = 0.0,
                 last_progress_time: float = 0.0, restart_count: int = 0,
                 last_restart_time: float = 0.0, status: str = "running",
                 progress_hints: List[str] = None, stall_threshold: float = 30.0,
                 max_restarts: int = 3):
        self.task_id = task_id
        self.name = name
        self.last_check_time = last_check_time
        self.last_progress_time = last_progress_time
        self.restart_count = restart_count
        self.last_restart_time = last_restart_time
        self.status = status
        self.progress_hints = progress_hints or []
        self.stall_threshold = stall_threshold
        self.max_restarts = max_restarts
    MonitoredTask.__init__ = new_init
_patch_monitored_task()
