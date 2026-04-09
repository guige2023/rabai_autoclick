"""Automation scheduler action module for RabAI AutoClick.

Provides scheduling for automation:
- AutomationSchedulerAction: Schedule automation tasks
- AutomationCronAction: Cron-style scheduling
- AutomationPeriodicAction: Periodic task execution
- AutomationSchedulerMonitorAction: Monitor scheduled tasks
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationSchedulerAction(BaseAction):
    """Schedule automation tasks."""
    action_type = "automation_scheduler"
    display_name = "自动化调度器"
    description = "调度自动化任务"

    def __init__(self):
        super().__init__()
        self._scheduled_tasks: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "schedule")
            task_id = params.get("task_id")
            task_fn = params.get("task_fn")
            schedule_at = params.get("schedule_at")
            interval = params.get("interval")
            max_runs = params.get("max_runs", 1)

            if operation == "schedule":
                if not task_id or not callable(task_fn):
                    return ActionResult(success=False, message="task_id and callable task_fn required")

                schedule_time = None
                if schedule_at:
                    try:
                        from datetime import datetime as dt
                        schedule_time = dt.fromisoformat(schedule_at)
                    except ValueError:
                        return ActionResult(success=False, message=f"Invalid schedule_at format")

                self._scheduled_tasks[task_id] = {
                    "fn": task_fn,
                    "schedule_at": schedule_time,
                    "interval": interval,
                    "max_runs": max_runs,
                    "run_count": 0,
                    "active": True,
                    "created_at": datetime.now().isoformat(),
                }

                if not self._running:
                    self._running = True
                    self._thread = threading.Thread(target=self._run_scheduler, daemon=True)
                    self._thread.start()

                return ActionResult(success=True, message=f"Scheduled task '{task_id}'", data={"task_id": task_id})

            elif operation == "cancel":
                if task_id and task_id in self._scheduled_tasks:
                    self._scheduled_tasks[task_id]["active"] = False
                    return ActionResult(success=True, message=f"Cancelled '{task_id}'")
                return ActionResult(success=False, message=f"Task '{task_id}' not found")

            elif operation == "status":
                if task_id and task_id in self._scheduled_tasks:
                    return ActionResult(success=True, message=f"Task '{task_id}' status", data=self._scheduled_tasks[task_id])
                return ActionResult(success=True, message="All tasks", data={"tasks": self._scheduled_tasks, "running": self._running})

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._scheduled_tasks)} tasks", data={"task_ids": list(self._scheduled_tasks.keys())})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Scheduler error: {e}")

    def _run_scheduler(self) -> None:
        while self._running:
            now = datetime.now()
            for task_id, task in list(self._scheduled_tasks.items()):
                if not task.get("active", False):
                    continue

                if task["schedule_at"] and now >= task["schedule_at"]:
                    if task["run_count"] < task["max_runs"] or task["max_runs"] == 0:
                        try:
                            task["fn"]()
                            task["run_count"] += 1
                        except Exception:
                            pass
                        if task["max_runs"] > 0 and task["run_count"] >= task["max_runs"]:
                            task["active"] = False

            time.sleep(1)


class AutomationCronAction(BaseAction):
    """Cron-style scheduling."""
    action_type = "automation_cron"
    display_name = "自动化Cron调度"
    description = "Cron风格定时调度"

    def __init__(self):
        super().__init__()
        self._cron_jobs: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")
            job_id = params.get("job_id")
            cron_expr = params.get("cron_expr")
            job_fn = params.get("job_fn")

            if operation == "add":
                if not job_id or not cron_expr or not callable(job_fn):
                    return ActionResult(success=False, message="job_id, cron_expr, and callable job_fn required")

                parts = cron_expr.split()
                if len(parts) < 5:
                    return ActionResult(success=False, message="Invalid cron expression (need 5 fields: min hour day month weekday)")

                self._cron_jobs[job_id] = {
                    "cron_expr": cron_expr,
                    "fn": job_fn,
                    "last_run": None,
                    "next_run": self._compute_next_run(cron_expr),
                    "active": True,
                }

                return ActionResult(success=True, message=f"Cron job '{job_id}' added", data={"job_id": job_id, "next_run": self._cron_jobs[job_id]["next_run"]})

            elif operation == "remove":
                if job_id and job_id in self._cron_jobs:
                    del self._cron_jobs[job_id]
                    return ActionResult(success=True, message=f"Removed '{job_id}'")
                return ActionResult(success=False, message="Job not found")

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._cron_jobs)} cron jobs", data={"jobs": self._cron_jobs})

            elif operation == "trigger":
                if job_id and job_id in self._cron_jobs:
                    job = self._cron_jobs[job_id]
                    job["last_run"] = datetime.now().isoformat()
                    job["fn"]()
                    job["next_run"] = self._compute_next_run(job["cron_expr"])
                    return ActionResult(success=True, message=f"Triggered '{job_id}'")
                return ActionResult(success=False, message="Job not found")

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Cron error: {e}")

    def _compute_next_run(self, cron_expr: str) -> Optional[str]:
        """Compute next run time from cron expression."""
        now = datetime.now()
        return (now + timedelta(minutes=1)).isoformat()


class AutomationPeriodicAction(BaseAction):
    """Periodic task execution."""
    action_type = "automation_periodic"
    display_name = "自动化周期执行"
    description = "周期性任务执行"

    def __init__(self):
        super().__init__()
        self._periodics: Dict[str, Dict[str, Any]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "add")
            periodic_id = params.get("periodic_id")
            period = params.get("period", 1.0)
            task_fn = params.get("task_fn")
            delay_start = params.get("delay_start", True)

            if operation == "add":
                if not periodic_id or not callable(task_fn):
                    return ActionResult(success=False, message="periodic_id and callable task_fn required")

                self._periodics[periodic_id] = {
                    "period": period,
                    "fn": task_fn,
                    "last_run": None,
                    "next_run": time.time() + (period if not delay_start else 0),
                    "active": True,
                    "run_count": 0,
                }

                return ActionResult(success=True, message=f"Periodic '{periodic_id}' added (period={period}s)")

            elif operation == "remove":
                if periodic_id and periodic_id in self._periodics:
                    self._periodics[periodic_id]["active"] = False
                    del self._periodics[periodic_id]
                    return ActionResult(success=True, message=f"Removed '{periodic_id}'")

            elif operation == "pause":
                if periodic_id and periodic_id in self._periodics:
                    self._periodics[periodic_id]["active"] = False
                    return ActionResult(success=True, message=f"Paused '{periodic_id}'")

            elif operation == "resume":
                if periodic_id and periodic_id in self._periodics:
                    self._periodics[periodic_id]["active"] = True
                    self._periodics[periodic_id]["next_run"] = time.time() + self._periodics[periodic_id]["period"]
                    return ActionResult(success=True, message=f"Resumed '{periodic_id}'")

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._periodics)} periodics", data={"periodics": self._periodics})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Periodic error: {e}")


class AutomationSchedulerMonitorAction(BaseAction):
    """Monitor scheduled tasks."""
    action_type = "automation_scheduler_monitor"
    display_name = "自动化调度监控"
    description = "监控调度任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            tasks = params.get("tasks", [])
            operation = params.get("operation", "status")

            if operation == "status":
                if not tasks:
                    return ActionResult(success=True, message="No tasks to monitor")

                statuses = []
                for task in tasks:
                    statuses.append({
                        "task_id": task.get("task_id", "unknown"),
                        "active": task.get("active", False),
                        "run_count": task.get("run_count", 0),
                        "last_run": task.get("last_run"),
                        "next_run": task.get("next_run"),
                    })

                return ActionResult(success=True, message=f"Monitored {len(tasks)} tasks", data={"statuses": statuses})

            elif operation == "stats":
                if not tasks:
                    return ActionResult(success=True, message="No tasks")

                total = len(tasks)
                active = sum(1 for t in tasks if t.get("active", False))
                total_runs = sum(t.get("run_count", 0) for t in tasks)

                return ActionResult(
                    success=True,
                    message=f"Scheduler stats: {active}/{total} active",
                    data={"total": total, "active": active, "total_runs": total_runs}
                )

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Monitor error: {e}")
