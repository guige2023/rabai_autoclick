"""
Scheduler utilities - cron parsing, interval calculation, task scheduling simulation.
"""
from typing import Any, Dict, List, Optional, Callable
import time
import logging
from datetime import datetime, timedelta
from collections import deque

logger = logging.getLogger(__name__)


class BaseAction:
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        raise NotImplementedError


def _parse_cron(cron_str: str) -> Dict[str, List[int]]:
    parts = cron_str.split()
    if len(parts) != 5:
        return {}
    names = ["minute", "hour", "day", "month", "weekday"]
    result = {}
    for i, (name, part) in enumerate(zip(names, parts)):
        if part == "*":
            result[name] = list(range(60 if i == 0 else 24 if i == 1 else 31 if i == 2 else 12))
        elif "," in part:
            result[name] = [int(x) for x in part.split(",")]
        elif "/" in part:
            base, step = part.split("/")
            start = 0 if base == "*" else int(base)
            step_val = int(step)
            max_val = 60 if i == 0 else 24 if i == 1 else 31 if i == 2 else 12
            result[name] = list(range(start, max_val, step_val))
        elif "-" in part:
            start, end = part.split("-")
            result[name] = list(range(int(start), int(end) + 1))
        else:
            result[name] = [int(part)]
    return result


def _next_run(cron_str: str, from_time: Optional[datetime] = None) -> Optional[datetime]:
    cron = _parse_cron(cron_str)
    if not cron:
        return None
    current = from_time or datetime.now()
    for _ in range(366 * 24 * 60):
        minute_match = current.minute in cron.get("minute", [current.minute])
        hour_match = current.hour in cron.get("hour", [current.hour])
        day_match = current.day in cron.get("day", [current.day])
        month_match = current.month in cron.get("month", [current.month])
        dow_match = current.weekday() in cron.get("weekday", [current.weekday()])
        if minute_match and hour_match and day_match and month_match and dow_match:
            return current
        current += timedelta(minutes=1)
    return None


class SchedulerAction(BaseAction):
    """Scheduler operations.

    Provides cron parsing, next run calculation, interval scheduling, task queue.
    """

    def __init__(self) -> None:
        self._tasks: deque = deque()

    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        operation = params.get("operation", "parse_cron")
        cron_str = params.get("cron", "")

        try:
            if operation == "parse_cron":
                if not cron_str:
                    return {"success": False, "error": "cron string required"}
                result = _parse_cron(cron_str)
                return {"success": True, "cron": result, "expression": cron_str}

            elif operation == "next_run":
                if not cron_str:
                    return {"success": False, "error": "cron string required"}
                from_str = params.get("from")
                from_time = datetime.fromisoformat(from_str) if from_str else None
                next_time = _next_run(cron_str, from_time)
                if next_time:
                    return {"success": True, "next_run": next_time.isoformat(), "timestamp": next_time.timestamp()}
                return {"success": False, "error": "Could not calculate next run"}

            elif operation == "is_due":
                if not cron_str:
                    return {"success": False, "error": "cron string required"}
                now = datetime.now()
                cron = _parse_cron(cron_str)
                if not cron:
                    return {"success": False, "error": "Invalid cron expression"}
                minute_match = now.minute in cron.get("minute", [now.minute])
                hour_match = now.hour in cron.get("hour", [now.hour])
                day_match = now.day in cron.get("day", [now.day])
                month_match = now.month in cron.get("month", [now.month])
                dow_match = now.weekday() in cron.get("weekday", [now.weekday()])
                is_due = minute_match and hour_match and day_match and month_match and dow_match
                return {"success": True, "is_due": is_due, "checked_at": now.isoformat()}

            elif operation == "schedule_interval":
                interval_seconds = int(params.get("interval_seconds", 60))
                last_run = params.get("last_run")
                last_ts = datetime.fromisoformat(last_run).timestamp() if last_run else 0.0
                elapsed = time.time() - last_ts
                is_due = elapsed >= interval_seconds
                next_in = max(0, interval_seconds - elapsed) if not is_due else 0
                return {"success": True, "is_due": is_due, "elapsed_seconds": round(elapsed, 2), "next_in_seconds": round(next_in, 2)}

            elif operation == "add_task":
                task_name = params.get("name", "unnamed")
                interval = int(params.get("interval_seconds", 60))
                self._tasks.append({"name": task_name, "interval": interval, "added_at": time.time()})
                return {"success": True, "tasks_queued": len(self._tasks), "task": task_name}

            elif operation == "list_tasks":
                return {"success": True, "tasks": list(self._tasks), "count": len(self._tasks)}

            elif operation == "due_tasks":
                now = time.time()
                due = []
                remaining = deque()
                for task in self._tasks:
                    elapsed = now - task["added_at"]
                    if elapsed >= task["interval"]:
                        due.append(task)
                    else:
                        remaining.append(task)
                self._tasks = remaining
                return {"success": True, "due": due, "count": len(due)}

            elif operation == "human_interval":
                seconds = int(params.get("seconds", 60))
                if seconds < 60:
                    return {"success": True, "human": f"{seconds} seconds"}
                elif seconds < 3600:
                    return {"success": True, "human": f"{seconds // 60} minutes"}
                elif seconds < 86400:
                    return {"success": True, "human": f"{seconds // 3600} hours"}
                else:
                    return {"success": True, "human": f"{seconds // 86400} days"}

            elif operation == "parse_interval":
                interval_str = params.get("interval", "")
                units = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
                for unit, multiplier in units.items():
                    if interval_str.endswith(unit):
                        num = interval_str[:-1]
                        return {"success": True, "seconds": int(num) * multiplier, "original": interval_str}
                return {"success": True, "seconds": int(interval_str), "original": interval_str}

            else:
                return {"success": False, "error": f"Unknown operation: {operation}"}

        except Exception as e:
            logger.error(f"SchedulerAction error: {e}")
            return {"success": False, "error": str(e)}


def execute(context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
    return SchedulerAction().execute(context, params)
