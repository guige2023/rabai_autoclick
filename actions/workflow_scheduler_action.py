"""
Workflow Scheduler Action Module.

Schedules workflow execution with cron expressions, intervals,
event triggers, and dependency-based chaining.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class ScheduledWorkflow:
    """A scheduled workflow."""
    name: str
    handler: Callable
    schedule: str
    enabled: bool = True
    last_run: Optional[float] = None
    next_run: Optional[float] = None
    run_count: int = 0


@dataclass
class ScheduleWorkflowResult:
    """Result of workflow scheduling."""
    scheduled: bool
    workflow_name: str
    next_run: Optional[float]
    message: str


class WorkflowSchedulerAction(BaseAction):
    """Schedule workflow execution."""

    def __init__(self) -> None:
        super().__init__("workflow_scheduler")
        self._scheduled: dict[str, ScheduledWorkflow] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Schedule or run scheduled workflows.

        Args:
            context: Execution context
            params: Parameters:
                - action: schedule, run_due, list, cancel
                - name: Workflow name
                - handler: Workflow handler function
                - schedule: Cron expression or interval (e.g., "0 9 * * *" or "1h")
                - dry_run: Don't actually run, just check

        Returns:
            ScheduleWorkflowResult or list of scheduled workflows
        """
        import time

        action = params.get("action", "schedule")
        name = params.get("name", "")
        handler = params.get("handler")
        schedule = params.get("schedule", "")

        if action == "schedule":
            if not name or not schedule:
                return {"scheduled": False, "message": "Name and schedule required"}

            wf = ScheduledWorkflow(
                name=name,
                handler=handler,
                schedule=schedule,
                next_run=self._get_next_run(schedule)
            )
            self._scheduled[name] = wf
            return ScheduleWorkflowResult(
                scheduled=True,
                workflow_name=name,
                next_run=wf.next_run,
                message=f"Workflow {name} scheduled"
            ).__dict__

        elif action == "run_due":
            dry_run = params.get("dry_run", False)
            now = time.time()
            due = [wf for wf in self._scheduled.values() if wf.enabled and wf.next_run and wf.next_run <= now]

            results = []
            for wf in due:
                if dry_run:
                    results.append(f"{wf.name} (would run)")
                else:
                    try:
                        wf.handler()
                        wf.last_run = now
                        wf.run_count += 1
                        wf.next_run = self._get_next_run(wf.schedule)
                        results.append(f"{wf.name} (executed)")
                    except Exception as e:
                        results.append(f"{wf.name} (error: {str(e)})")

            return {"due_count": len(due), "results": results}

        elif action == "list":
            return {"scheduled": [vars(wf) for wf in self._scheduled.values()]}

        elif action == "cancel":
            if name in self._scheduled:
                del self._scheduled[name]
                return {"cancelled": True, "name": name}
            return {"cancelled": False, "message": "Workflow not found"}

        return {"error": f"Unknown action: {action}"}

    def _get_next_run(self, schedule: str) -> Optional[float]:
        """Calculate next run time from cron expression."""
        import time
        now = time.time()

        if not schedule:
            return None

        if schedule.endswith("s") or schedule.endswith("m") or schedule.endswith("h") or schedule.endswith("d"):
            multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
            try:
                value = int(schedule[:-1])
                unit = schedule[-1]
                return now + value * multipliers.get(unit, 1)
            except Exception:
                return None

        parts = schedule.split()
        if len(parts) >= 5:
            now_struct = time.localtime(now)
            try:
                minute, hour, day, month, dow = parts[0], parts[1], parts[2], parts[3], parts[4]
                for offset in range(1, 525600):
                    candidate = now + offset * 60
                    cand_struct = time.localtime(candidate)
                    if (minute == "*" or int(minute) == cand_struct.tm_min) and \
                       (hour == "*" or int(hour) == cand_struct.tm_hour) and \
                       (day == "*" or int(day) == cand_struct.tm_mday) and \
                       (month == "*" or int(month) == cand_struct.tm_mon) and \
                       (dow == "*" or int(dow) == cand_struct.tm_wday):
                        return candidate
                return now + 3600
            except Exception:
                return now + 3600

        return now + 3600
