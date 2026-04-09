"""Automation scheduling and task queue action module for RabAI AutoClick.

Provides:
- AutomationSchedulerAction: Schedule automation tasks
- TaskQueueAction: Task queue management
- TaskSchedulerAction: Advanced task scheduling
- WorkerPoolAction: Worker pool management
"""

import time
import json
import threading
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from enum import Enum
import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ScheduleType(str, Enum):
    """Schedule types."""
    ONCE = "once"
    RECURRING = "recurring"
    CRON = "cron"
    INTERVAL = "interval"


class TaskStatus(str, Enum):
    """Task status."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AutomationSchedulerAction(BaseAction):
    """Schedule automation tasks."""
    action_type = "automation_scheduler"
    display_name = "自动化调度器"
    description = "任务定时调度"

    def __init__(self):
        super().__init__()
        self._schedules: Dict[str, Dict] = {}
        self._schedule_history: Dict[str, List[Dict]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "schedule")
            schedule_name = params.get("schedule_name", "")

            if operation == "schedule":
                if not schedule_name:
                    return ActionResult(success=False, message="schedule_name required")

                schedule_type = params.get("type", ScheduleType.RECURRING.value)
                interval_seconds = params.get("interval_seconds", 3600)
                cron_expr = params.get("cron_expression", "")

                self._schedules[schedule_name] = {
                    "name": schedule_name,
                    "type": schedule_type,
                    "task_name": params.get("task_name", ""),
                    "task_params": params.get("task_params", {}),
                    "interval_seconds": interval_seconds,
                    "cron_expression": cron_expr,
                    "enabled": params.get("enabled", True),
                    "created_at": time.time(),
                    "last_run": None,
                    "next_run": self._calculate_next_run(schedule_type, interval_seconds, cron_expr),
                    "run_count": 0
                }
                self._schedule_history[schedule_name] = []

                return ActionResult(
                    success=True,
                    data={
                        "schedule": schedule_name,
                        "type": schedule_type,
                        "next_run": self._schedules[schedule_name]["next_run"]
                    },
                    message=f"Schedule '{schedule_name}' created"
                )

            elif operation == "trigger":
                if schedule_name not in self._schedules:
                    return ActionResult(success=False, message=f"Schedule '{schedule_name}' not found")

                schedule = self._schedules[schedule_name]
                schedule["last_run"] = time.time()
                schedule["next_run"] = self._calculate_next_run(
                    schedule["type"],
                    schedule["interval_seconds"],
                    schedule["cron_expression"]
                )
                schedule["run_count"] += 1

                self._schedule_history[schedule_name].append({
                    "triggered_at": time.time(),
                    "run_number": schedule["run_count"]
                })

                return ActionResult(
                    success=True,
                    data={
                        "schedule": schedule_name,
                        "last_run": schedule["last_run"],
                        "next_run": schedule["next_run"],
                        "run_count": schedule["run_count"]
                    },
                    message=f"Schedule '{schedule_name}' triggered (run #{schedule['run_count']})"
                )

            elif operation == "due":
                now = time.time()
                due = []
                for name, sched in self._schedules.items():
                    if sched["enabled"] and sched.get("next_run") and sched["next_run"] <= now:
                        due.append(name)
                return ActionResult(success=True, data={"due_schedules": due, "count": len(due)})

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={
                        "schedules": [
                            {
                                "name": name,
                                "type": s["type"],
                                "enabled": s["enabled"],
                                "last_run": s["last_run"],
                                "next_run": s["next_run"],
                                "run_count": s["run_count"]
                            }
                            for name, s in self._schedules.items()
                        ]
                    }
                )

            elif operation == "toggle":
                if schedule_name not in self._schedules:
                    return ActionResult(success=False, message=f"Schedule '{schedule_name}' not found")
                self._schedules[schedule_name]["enabled"] = not self._schedules[schedule_name]["enabled"]
                return ActionResult(
                    success=True,
                    data={"schedule": schedule_name, "enabled": self._schedules[schedule_name]["enabled"]},
                    message=f"Schedule '{schedule_name}' {'enabled' if self._schedules[schedule_name]['enabled'] else 'disabled'}"
                )

            elif operation == "history":
                if schedule_name not in self._schedule_history:
                    return ActionResult(success=False, message=f"No history for '{schedule_name}'")
                return ActionResult(
                    success=True,
                    data={"schedule": schedule_name, "history": self._schedule_history[schedule_name]}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Scheduler error: {str(e)}")

    def _calculate_next_run(self, schedule_type: str, interval_seconds: int, cron_expr: str) -> float:
        if schedule_type == ScheduleType.ONCE.value:
            return None
        elif schedule_type == ScheduleType.INTERVAL.value:
            return time.time() + interval_seconds
        elif schedule_type == ScheduleType.RECURRING.value:
            return time.time() + interval_seconds
        return time.time() + 3600


class TaskQueueAction(BaseAction):
    """Task queue management."""
    action_type = "task_queue"
    display_name = "任务队列"
    description = "任务队列管理"

    def __init__(self):
        super().__init__()
        self._queues: Dict[str, List[Dict]] = {}
        self._queue_config: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "enqueue")
            queue_name = params.get("queue_name", "default")

            if operation == "create":
                if queue_name in self._queues:
                    return ActionResult(success=True, data={"queue": queue_name}, message=f"Queue '{queue_name}' already exists")

                self._queues[queue_name] = []
                self._queue_config[queue_name] = {
                    "name": queue_name,
                    "max_size": params.get("max_size", 10000),
                    "priority_enabled": params.get("priority_enabled", False),
                    "created_at": time.time()
                }
                return ActionResult(success=True, data={"queue": queue_name}, message=f"Queue '{queue_name}' created")

            elif operation == "enqueue":
                if queue_name not in self._queues:
                    self._queues[queue_name] = []

                task = {
                    "task_id": params.get("task_id", f"task_{int(time.time() * 1000)}"),
                    "payload": params.get("payload", {}),
                    "priority": params.get("priority", 0),
                    "enqueued_at": time.time(),
                    "status": TaskStatus.PENDING.value,
                    "retries": 0
                }

                if self._queue_config.get(queue_name, {}).get("priority_enabled"):
                    self._queues[queue_name].append(task)
                    self._queues[queue_name].sort(key=lambda t: t["priority"], reverse=True)
                else:
                    self._queues[queue_name].append(task)

                return ActionResult(
                    success=True,
                    data={"task_id": task["task_id"], "queue": queue_name, "queue_size": len(self._queues[queue_name])}
                )

            elif operation == "dequeue":
                if queue_name not in self._queues or not self._queues[queue_name]:
                    return ActionResult(success=True, data={"task": None, "queue_size": 0})

                task = self._queues[queue_name].pop(0)
                task["dequeued_at"] = time.time()
                task["status"] = TaskStatus.QUEUED.value

                return ActionResult(
                    success=True,
                    data={"task": task, "queue_size": len(self._queues[queue_name])}
                )

            elif operation == "size":
                size = len(self._queues.get(queue_name, []))
                return ActionResult(success=True, data={"queue": queue_name, "size": size})

            elif operation == "clear":
                if queue_name in self._queues:
                    self._queues[queue_name] = []
                return ActionResult(success=True, message=f"Queue '{queue_name}' cleared")

            elif operation == "list":
                return ActionResult(
                    success=True,
                    data={"queues": list(self._queues.keys())}
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Queue error: {str(e)}")


class TaskSchedulerAction(BaseAction):
    """Advanced task scheduling with priorities."""
    action_type = "task_scheduler"
    display_name = "任务调度器"
    description = "优先级任务调度"

    def __init__(self):
        super().__init__()
        self._tasks: Dict[str, Dict] = {}
        self._scheduled_tasks: Dict[str, List[str]] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "submit")
            task_id = params.get("task_id", "")

            if operation == "submit":
                if not task_id:
                    task_id = f"task_{int(time.time() * 1000)}"

                self._tasks[task_id] = {
                    "task_id": task_id,
                    "name": params.get("task_name", task_id),
                    "payload": params.get("payload", {}),
                    "priority": params.get("priority", 0),
                    "created_at": time.time(),
                    "scheduled_at": params.get("scheduled_at"),
                    "deadline": params.get("deadline"),
                    "status": TaskStatus.PENDING.value,
                    "attempts": 0,
                    "max_attempts": params.get("max_attempts", 3)
                }

                return ActionResult(
                    success=True,
                    data={"task_id": task_id, "status": self._tasks[task_id]["status"]},
                    message=f"Task '{task_id}' submitted"
                )

            elif operation == "schedule":
                if not task_id:
                    return ActionResult(success=False, message="task_id required")

                if task_id not in self._tasks:
                    return ActionResult(success=False, message=f"Task '{task_id}' not found")

                scheduled_time = params.get("scheduled_time", time.time())
                self._tasks[task_id]["scheduled_at"] = scheduled_time

                schedule_key = str(int(scheduled_time // 60))
                if schedule_key not in self._scheduled_tasks:
                    self._scheduled_tasks[schedule_key] = []
                self._scheduled_tasks[schedule_key].append(task_id)

                return ActionResult(success=True, data={"task_id": task_id, "scheduled_at": scheduled_time})

            elif operation == "execute":
                if not task_id:
                    return ActionResult(success=False, message="task_id required")

                if task_id not in self._tasks:
                    return ActionResult(success=False, message=f"Task '{task_id}' not found")

                task = self._tasks[task_id]
                task["status"] = TaskStatus.RUNNING.value
                task["attempts"] += 1
                task["started_at"] = time.time()

                success = params.get("success", True)
                if success:
                    task["status"] = TaskStatus.COMPLETED.value
                    task["completed_at"] = time.time()
                else:
                    if task["attempts"] >= task["max_attempts"]:
                        task["status"] = TaskStatus.FAILED.value
                        task["failed_at"] = time.time()
                    else:
                        task["status"] = TaskStatus.PENDING.value

                return ActionResult(
                    success=success,
                    data={
                        "task_id": task_id,
                        "status": task["status"],
                        "attempts": task["attempts"]
                    }
                )

            elif operation == "cancel":
                if task_id in self._tasks:
                    self._tasks[task_id]["status"] = TaskStatus.CANCELLED.value
                return ActionResult(success=True, message=f"Task '{task_id}' cancelled")

            elif operation == "list":
                status_filter = params.get("status")
                tasks = list(self._tasks.values())
                if status_filter:
                    tasks = [t for t in tasks if t["status"] == status_filter]
                return ActionResult(success=True, data={"tasks": tasks, "count": len(tasks)})

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Task scheduler error: {str(e)}")


class WorkerPoolAction(BaseAction):
    """Worker pool management."""
    action_type = "worker_pool"
    display_name = "工作池管理"
    description = "工作池管理"

    def __init__(self):
        super().__init__()
        self._pools: Dict[str, Dict] = {}
        self._workers: Dict[str, Dict] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            pool_name = params.get("pool_name", "")

            if operation == "create":
                if not pool_name:
                    return ActionResult(success=False, message="pool_name required")

                self._pools[pool_name] = {
                    "name": pool_name,
                    "size": params.get("size", 4),
                    "worker_type": params.get("worker_type", "default"),
                    "created_at": time.time(),
                    "active_workers": 0,
                    "total_tasks_processed": 0
                }
                return ActionResult(success=True, data={"pool": pool_name}, message=f"Pool '{pool_name}' created")

            elif operation == "register_worker":
                worker_id = params.get("worker_id", f"worker_{int(time.time() * 1000)}")
                pool_name = params.get("pool_name", "")

                self._workers[worker_id] = {
                    "worker_id": worker_id,
                    "pool": pool_name,
                    "status": "idle",
                    "registered_at": time.time(),
                    "tasks_completed": 0
                }
                return ActionResult(success=True, data={"worker": worker_id, "pool": pool_name})

            elif operation == "assign":
                worker_id = params.get("worker_id", "")
                task = params.get("task", {})

                if worker_id not in self._workers:
                    return ActionResult(success=False, message=f"Worker '{worker_id}' not found")

                self._workers[worker_id]["status"] = "busy"
                self._workers[worker_id]["current_task"] = task

                return ActionResult(success=True, data={"worker": worker_id, "task": task.get("task_id", "unknown")})

            elif operation == "release":
                worker_id = params.get("worker_id", "")
                if worker_id in self._workers:
                    self._workers[worker_id]["status"] = "idle"
                    self._workers[worker_id]["tasks_completed"] += 1
                    self._workers[worker_id].pop("current_task", None)

                return ActionResult(success=True, message=f"Worker '{worker_id}' released")

            elif operation == "status":
                return ActionResult(
                    success=True,
                    data={
                        "pools": {k: {"size": v["size"], "active": v["active_workers"]} for k, v in self._pools.items()},
                        "workers": {k: {"pool": v["pool"], "status": v["status"]} for k, v in self._workers.items()}
                    }
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Worker pool error: {str(e)}")
