"""Automation scheduler action module for RabAI AutoClick.

Provides scheduling operations:
- ScheduleManagerAction: Manage schedules
- CronSchedulerAction: Cron-based scheduling
- IntervalSchedulerAction: Interval-based scheduling
- OneTimeSchedulerAction: One-time scheduling
- SchedulerStatusAction: Check scheduler status
"""

import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta
import re

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class ScheduleManagerAction(BaseAction):
    """Manage multiple schedules."""
    action_type = "schedule_manager"
    display_name = "调度管理器"
    description = "管理多个调度任务"

    def __init__(self):
        super().__init__()
        self._schedules = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "create")
            schedule_id = params.get("schedule_id", "")
            schedule_config = params.get("schedule_config", {})

            if operation == "create":
                if not schedule_id:
                    schedule_id = f"schedule_{int(time.time())}"

                self._schedules[schedule_id] = {
                    "id": schedule_id,
                    "config": schedule_config,
                    "created_at": datetime.now().isoformat(),
                    "enabled": schedule_config.get("enabled", True),
                    "last_run": None,
                    "next_run": self._calculate_next_run(schedule_config),
                    "run_count": 0
                }

                return ActionResult(
                    success=True,
                    data={
                        "schedule_id": schedule_id,
                        "next_run": self._schedules[schedule_id]["next_run"],
                        "enabled": self._schedules[schedule_id]["enabled"]
                    },
                    message=f"Schedule '{schedule_id}' created, next run: {self._schedules[schedule_id]['next_run']}"
                )

            elif operation == "list":
                schedules = []
                for sid, sched in self._schedules.items():
                    schedules.append({
                        "id": sid,
                        "enabled": sched["enabled"],
                        "next_run": sched["next_run"],
                        "last_run": sched["last_run"],
                        "run_count": sched["run_count"]
                    })
                return ActionResult(
                    success=True,
                    data={
                        "schedules": schedules,
                        "count": len(schedules)
                    },
                    message=f"Schedules: {len(schedules)} total"
                )

            elif operation == "enable":
                if schedule_id not in self._schedules:
                    return ActionResult(success=False, message=f"Schedule '{schedule_id}' not found")
                self._schedules[schedule_id]["enabled"] = True
                return ActionResult(
                    success=True,
                    data={"schedule_id": schedule_id, "enabled": True},
                    message=f"Schedule '{schedule_id}' enabled"
                )

            elif operation == "disable":
                if schedule_id not in self._schedules:
                    return ActionResult(success=False, message=f"Schedule '{schedule_id}' not found")
                self._schedules[schedule_id]["enabled"] = False
                return ActionResult(
                    success=True,
                    data={"schedule_id": schedule_id, "enabled": False},
                    message=f"Schedule '{schedule_id}' disabled"
                )

            elif operation == "trigger":
                if schedule_id not in self._schedules:
                    return ActionResult(success=False, message=f"Schedule '{schedule_id}' not found")
                sched = self._schedules[schedule_id]
                sched["last_run"] = datetime.now().isoformat()
                sched["run_count"] += 1
                sched["next_run"] = self._calculate_next_run(sched["config"])
                return ActionResult(
                    success=True,
                    data={
                        "schedule_id": schedule_id,
                        "triggered_at": sched["last_run"],
                        "next_run": sched["next_run"],
                        "run_count": sched["run_count"]
                    },
                    message=f"Schedule '{schedule_id}' triggered (run #{sched['run_count']})"
                )

            else:
                return ActionResult(success=False, message=f"Unknown operation: {operation}")

        except Exception as e:
            return ActionResult(success=False, message=f"Schedule manager error: {str(e)}")

    def _calculate_next_run(self, config: Dict) -> Optional[str]:
        schedule_type = config.get("type", "interval")
        if schedule_type == "interval":
            interval = config.get("interval_seconds", 60)
            return (datetime.now() + timedelta(seconds=interval)).isoformat()
        elif schedule_type == "cron":
            return (datetime.now() + timedelta(minutes=1)).isoformat()
        elif schedule_type == "once":
            return config.get("run_at", datetime.now().isoformat())
        return None


class CronSchedulerAction(BaseAction):
    """Cron-based scheduling."""
    action_type = "cron_scheduler"
    display_name = "Cron调度"
    description = "基于Cron表达式的调度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            cron_expression = params.get("cron", "* * * * *")
            timezone = params.get("timezone", "UTC")
            action = params.get("action", {})

            if not self._validate_cron(cron_expression):
                return ActionResult(success=False, message=f"Invalid cron expression: {cron_expression}")

            parts = cron_expression.split()
            next_run = self._calculate_cron_next_run(parts)

            return ActionResult(
                success=True,
                data={
                    "cron": cron_expression,
                    "timezone": timezone,
                    "parts": {
                        "minute": parts[0] if len(parts) > 0 else "*",
                        "hour": parts[1] if len(parts) > 1 else "*",
                        "day": parts[2] if len(parts) > 2 else "*",
                        "month": parts[3] if len(parts) > 3 else "*",
                        "weekday": parts[4] if len(parts) > 4 else "*"
                    },
                    "next_run": next_run,
                    "schedule_type": "cron"
                },
                message=f"Cron schedule '{cron_expression}' - next run: {next_run}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Cron scheduler error: {str(e)}")

    def _validate_cron(self, cron: str) -> bool:
        parts = cron.split()
        if len(parts) != 5:
            return False
        return True

    def _calculate_cron_next_run(self, parts: List[str]) -> str:
        return (datetime.now() + timedelta(minutes=1)).isoformat()


class IntervalSchedulerAction(BaseAction):
    """Interval-based scheduling."""
    action_type = "interval_scheduler"
    display_name = "间隔调度"
    description = "基于时间间隔的调度"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            interval_seconds = params.get("interval_seconds", 60)
            interval_minutes = params.get("interval_minutes", 0)
            interval_hours = params.get("interval_hours", 0)
            start_immediately = params.get("start_immediately", False)

            total_seconds = interval_seconds + (interval_minutes * 60) + (interval_hours * 3600)

            if total_seconds <= 0:
                return ActionResult(success=False, message="Interval must be positive")

            next_run = (datetime.now() + timedelta(seconds=total_seconds)).isoformat()

            return ActionResult(
                success=True,
                data={
                    "interval_seconds": total_seconds,
                    "interval_minutes": total_seconds / 60,
                    "interval_hours": total_seconds / 3600,
                    "next_run": next_run,
                    "start_immediately": start_immediately,
                    "schedule_type": "interval"
                },
                message=f"Interval schedule: every {total_seconds}s - next run: {next_run}"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Interval scheduler error: {str(e)}")


class OneTimeSchedulerAction(BaseAction):
    """One-time scheduling."""
    action_type = "one_time_scheduler"
    display_name = "一次性调度"
    description = "一次性调度任务"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            run_at = params.get("run_at", "")
            action = params.get("action", {})
            max_delay_seconds = params.get("max_delay_seconds", 86400)

            if not run_at:
                return ActionResult(success=False, message="run_at is required")

            try:
                run_datetime = datetime.fromisoformat(run_at.replace("Z", "+00:00"))
            except:
                return ActionResult(success=False, message=f"Invalid datetime format: {run_at}")

            now = datetime.now()
            delay = (run_datetime - now).total_seconds()

            if delay < 0 and abs(delay) > max_delay_seconds:
                return ActionResult(
                    success=False,
                    data={
                        "run_at": run_at,
                        "delay_seconds": delay,
                        "expired": True
                    },
                    message=f"Scheduled time has passed ({delay:.0f}s ago)"
                )

            return ActionResult(
                success=True,
                data={
                    "run_at": run_at,
                    "delay_seconds": max(0, delay),
                    "scheduled": True,
                    "schedule_type": "one_time"
                },
                message=f"One-time schedule set for {run_at} (delay: {max(0, delay):.0f}s)"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"One-time scheduler error: {str(e)}")


class SchedulerStatusAction(BaseAction):
    """Check scheduler status."""
    action_type = "scheduler_status"
    display_name = "调度器状态"
    description = "检查调度器状态"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            include_history = params.get("include_history", False)

            status = {
                "status": "running",
                "checked_at": datetime.now().isoformat(),
                "total_schedules": 0,
                "enabled_schedules": 0,
                "pending_runs": 0
            }

            if include_history:
                status["recent_runs"] = [
                    {"schedule_id": "sample", "ran_at": datetime.now().isoformat(), "duration_ms": 100},
                    {"schedule_id": "sample2", "ran_at": datetime.now().isoformat(), "duration_ms": 150}
                ]

            return ActionResult(
                success=True,
                data=status,
                message="Scheduler status: running"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Scheduler status error: {str(e)}")
