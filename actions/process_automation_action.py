"""Process Automation Action Module.

Automates business processes with task scheduling,
workflow triggers, and event-driven execution.
"""

from __future__ import annotations

import sys
import os
import time
import json
import hashlib
import asyncio
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


class TriggerType(Enum):
    """Types of process triggers."""
    SCHEDULE = "schedule"
    EVENT = "event"
    WEBHOOK = "webhook"
    MANUAL = "manual"
    CONDITION = "condition"
    FILE_WATCH = "file_watch"
    API_CALL = "api_call"


class ProcessStatus(Enum):
    """Status of a process execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    WAITING = "waiting"


class ScheduleType(Enum):
    """Type of schedule."""
    ONCE = "once"
    RECURRING = "recurring"
    CRON = "cron"
    INTERVAL = "interval"


@dataclass
class ProcessTask:
    """A process automation task."""
    task_id: str
    name: str
    description: str = ""
    trigger_type: TriggerType
    trigger_config: Dict[str, Any] = field(default_factory=dict)
    action_config: Dict[str, Any] = field(default_factory=dict)
    schedule: Optional[Dict[str, Any]] = None
    conditions: Optional[Dict[str, Any]] = None
    enabled: bool = True
    timeout: float = 60.0
    retry_config: Dict[str, Any] = field(default_factory=dict)
    notifications: Dict[str, List[str]] = field(default_factory=dict)


@dataclass
class ProcessExecution:
    """Record of a process execution."""
    execution_id: str
    task_id: str
    status: ProcessStatus
    started_at: float
    completed_at: Optional[float] = None
    trigger_type: TriggerType
    trigger_data: Dict[str, Any] = field(default_factory=dict)
    result: Any = None
    error: Optional[str] = None
    logs: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Schedule:
    """Schedule configuration."""
    schedule_type: ScheduleType
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    interval_seconds: int = 0
    cron_expression: str = ""
    max_runs: Optional[int] = None
    run_count: int = 0


class ProcessAutomationAction(BaseAction):
    """
    Business process automation with triggers and scheduling.

    Automates tasks based on schedules, events, webhooks,
    and conditions with full execution tracking.

    Example:
        automation = ProcessAutomationAction()
        result = automation.execute(ctx, {
            "action": "create_task",
            "name": "daily_report",
            "trigger_type": "schedule"
        })
    """
    action_type = "process_automation"
    display_name = "流程自动化"
    description = "基于触发器和调度的业务流程自动化"

    def __init__(self) -> None:
        super().__init__()
        self._tasks: Dict[str, ProcessTask] = {}
        self._executions: Dict[str, ProcessExecution] = {}
        self._schedules: Dict[str, Schedule] = {}
        self._listeners: Dict[str, List[Callable]] = {}
        self._running = False
        self._schedule_thread: Optional[Any] = None

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a process automation action.

        Args:
            context: Execution context.
            params: Dict with keys: action, task_id, etc.

        Returns:
            ActionResult with execution result.
        """
        action = params.get("action", "")

        try:
            if action == "create_task":
                return self._create_task(params)
            elif action == "run_task":
                return self._run_task(params)
            elif action == "get_task":
                return self._get_task(params)
            elif action == "list_tasks":
                return self._list_tasks(params)
            elif action == "enable_task":
                return self._enable_task(params)
            elif action == "disable_task":
                return self._disable_task(params)
            elif action == "get_execution":
                return self._get_execution(params)
            elif action == "get_execution_history":
                return self._get_execution_history(params)
            elif action == "cancel_execution":
                return self._cancel_execution(params)
            elif action == "trigger_event":
                return self._trigger_event(params)
            elif action == "delete_task":
                return self._delete_task(params)
            else:
                return ActionResult(success=False, message=f"Unknown action: {action}")
        except Exception as e:
            return ActionResult(success=False, message=f"Automation error: {str(e)}")

    def _create_task(self, params: Dict[str, Any]) -> ActionResult:
        """Create a new automation task."""
        name = params.get("name", "")
        trigger_type_str = params.get("trigger_type", "manual")
        trigger_config = params.get("trigger_config", {})
        action_config = params.get("action_config", {})
        schedule_data = params.get("schedule")
        conditions = params.get("conditions")
        timeout = params.get("timeout", 60.0)
        retry_config = params.get("retry_config", {})

        if not name:
            return ActionResult(success=False, message="name is required")

        try:
            trigger_type = TriggerType(trigger_type_str)
        except ValueError:
            return ActionResult(success=False, message=f"Invalid trigger type: {trigger_type_str}")

        task_id = self._generate_task_id(name)

        task = ProcessTask(
            task_id=task_id,
            name=name,
            description=params.get("description", ""),
            trigger_type=trigger_type,
            trigger_config=trigger_config,
            action_config=action_config,
            timeout=timeout,
            retry_config=retry_config,
            notifications=params.get("notifications", {}),
        )

        if conditions:
            task.conditions = conditions

        if schedule_data:
            task.schedule = schedule_data
            schedule = self._build_schedule(schedule_data)
            self._schedules[task_id] = schedule

        self._tasks[task_id] = task

        return ActionResult(
            success=True,
            message=f"Task created: {task_id}",
            data={
                "task_id": task_id,
                "name": name,
                "trigger_type": trigger_type.value,
                "enabled": task.enabled,
            }
        )

    def _run_task(self, params: Dict[str, Any]) -> ActionResult:
        """Run an automation task."""
        task_id = params.get("task_id", "")
        trigger_data = params.get("trigger_data", {})

        if task_id not in self._tasks:
            return ActionResult(success=False, message=f"Task not found: {task_id}")

        task = self._tasks[task_id]

        if not task.enabled:
            return ActionResult(success=False, message=f"Task is disabled: {task_id}")

        if task.conditions and not self._evaluate_conditions(task.conditions, trigger_data):
            return ActionResult(
                success=False,
                message=f"Task conditions not met: {task_id}",
                data={"skipped": True}
            )

        execution_id = self._generate_execution_id()
        execution = ProcessExecution(
            execution_id=execution_id,
            task_id=task_id,
            status=ProcessStatus.RUNNING,
            started_at=time.time(),
            trigger_type=task.trigger_type,
            trigger_data=trigger_data,
        )

        self._executions[execution_id] = execution

        try:
            result = self._execute_task_action(task, trigger_data)

            execution.status = ProcessStatus.COMPLETED
            execution.completed_at = time.time()
            execution.result = result

            self._notify(task, execution, "success")

            if task.schedule:
                schedule = self._schedules.get(task_id)
                if schedule:
                    schedule.run_count += 1

            return ActionResult(
                success=True,
                message=f"Task completed: {task_id}",
                data={
                    "execution_id": execution_id,
                    "task_id": task_id,
                    "status": ProcessStatus.COMPLETED.value,
                    "duration": execution.completed_at - execution.started_at,
                }
            )

        except Exception as e:
            execution.status = ProcessStatus.FAILED
            execution.completed_at = time.time()
            execution.error = str(e)

            self._notify(task, execution, "failure")

            retry_count = trigger_data.get("_retry_count", 0)
            max_retries = task.retry_config.get("max_retries", 0)

            if retry_count < max_retries:
                delay = task.retry_config.get("delay", 1.0) * (2 ** retry_count)
                return ActionResult(
                    success=False,
                    message=f"Task failed, will retry: {str(e)}",
                    data={
                        "execution_id": execution_id,
                        "will_retry": True,
                        "retry_count": retry_count + 1,
                        "retry_delay": delay,
                    }
                )

            return ActionResult(
                success=False,
                message=f"Task failed: {str(e)}",
                data={
                    "execution_id": execution_id,
                    "task_id": task_id,
                    "status": ProcessStatus.FAILED.value,
                    "error": str(e),
                }
            )

    def _execute_task_action(self, task: ProcessTask, trigger_data: Dict[str, Any]) -> Any:
        """Execute the actual task action."""
        action_type = task.action_config.get("type", "noop")
        action_params = task.action_config.get("params", {})

        if action_type == "noop":
            return {"executed": True, "action": "noop"}

        elif action_type == "http_request":
            return self._execute_http_action(action_params)

        elif action_type == "script":
            return self._execute_script_action(action_params)

        elif action_type == "send_notification":
            return self._execute_notification_action(action_params)

        elif action_type == "data_transform":
            return self._execute_transform_action(action_params, trigger_data)

        elif action_type == "file_operation":
            return self._execute_file_action(action_params)

        elif action_type == "subprocess":
            return self._execute_subprocess_action(action_params)

        else:
            return {"executed": True, "action": action_type, "params": action_params}

    def _execute_http_action(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute an HTTP request action."""
        import urllib.request
        import urllib.parse

        url = params.get("url", "")
        method = params.get("method", "GET").upper()
        headers = params.get("headers", {})
        body = params.get("body")

        if not url:
            raise ValueError("URL is required for HTTP action")

        req = urllib.request.Request(url, method=method)

        for key, value in headers.items():
            req.add_header(key, value)

        if body and method in ("POST", "PUT", "PATCH"):
            if isinstance(body, dict):
                body = json.dumps(body).encode("utf-8")
                req.add_header("Content-Type", "application/json")
            elif isinstance(body, str):
                body = body.encode("utf-8")

        with urllib.request.urlopen(req, timeout=30) as response:
            response_body = response.read().decode("utf-8")

            try:
                result_data = json.loads(response_body)
            except json.JSONDecodeError:
                result_data = {"raw": response_body}

            return {
                "status_code": response.status,
                "body": result_data,
            }

    def _execute_script_action(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a script action."""
        script = params.get("script", "")
        language = params.get("language", "python")

        if not script:
            return {"executed": False, "error": "No script provided"}

        if language == "python":
            local_vars: Dict[str, Any] = {}
            try:
                exec(script, {}, local_vars)
                return {"executed": True, "result": local_vars.get("result")}
            except Exception as e:
                return {"executed": False, "error": str(e)}

        return {"executed": True, "script": script, "language": language}

    def _execute_notification_action(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a notification action."""
        channel = params.get("channel", "log")
        message = params.get("message", "")
        recipients = params.get("recipients", [])

        return {
            "executed": True,
            "channel": channel,
            "message": message,
            "recipients": recipients,
            "sent": True,
        }

    def _execute_transform_action(self, params: Dict[str, Any], trigger_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a data transformation action."""
        transform_type = params.get("type", "map")
        data = params.get("data", trigger_data)
        mappings = params.get("mappings", {})

        if transform_type == "map":
            result = {}
            for old_key, new_key in mappings.items():
                result[new_key] = data.get(old_key, data.get(new_key))
            return {"executed": True, "result": result}

        return {"executed": True, "data": data}

    def _execute_file_action(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a file operation action."""
        operation = params.get("operation", "read")
        file_path = params.get("path", "")
        content = params.get("content", "")

        if operation == "read":
            if not os.path.exists(file_path):
                return {"executed": False, "error": "File not found"}
            with open(file_path, "r") as f:
                return {"executed": True, "content": f.read()}

        elif operation == "write":
            with open(file_path, "w") as f:
                f.write(content)
            return {"executed": True, "path": file_path}

        elif operation == "append":
            with open(file_path, "a") as f:
                f.write(content)
            return {"executed": True, "path": file_path}

        elif operation == "delete":
            if os.path.exists(file_path):
                os.remove(file_path)
            return {"executed": True, "deleted": file_path}

        return {"executed": True, "operation": operation}

    def _execute_subprocess_action(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a subprocess action."""
        import subprocess

        command = params.get("command", "")
        shell = params.get("shell", True)
        cwd = params.get("cwd")

        if not command:
            return {"executed": False, "error": "No command provided"}

        result = subprocess.run(
            command,
            shell=shell,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=params.get("timeout", 30),
        )

        return {
            "executed": True,
            "returncode": result.returncode,
            "stdout": result.stdout,
            "stderr": result.stderr,
        }

    def _get_task(self, params: Dict[str, Any]) -> ActionResult:
        """Get a task by ID."""
        task_id = params.get("task_id", "")

        if task_id not in self._tasks:
            return ActionResult(success=False, message=f"Task not found: {task_id}")

        task = self._tasks[task_id]
        return ActionResult(
            success=True,
            data={
                "task_id": task.task_id,
                "name": task.name,
                "description": task.description,
                "trigger_type": task.trigger_type.value,
                "enabled": task.enabled,
                "timeout": task.timeout,
                "schedule": task.schedule,
            }
        )

    def _list_tasks(self, params: Dict[str, Any]) -> ActionResult:
        """List all tasks with optional filters."""
        enabled_only = params.get("enabled_only", False)
        trigger_filter = params.get("trigger_type")

        tasks = list(self._tasks.values())

        if enabled_only:
            tasks = [t for t in tasks if t.enabled]

        if trigger_filter:
            try:
                trigger_type = TriggerType(trigger_filter)
                tasks = [t for t in tasks if t.trigger_type == trigger_type]
            except ValueError:
                pass

        task_list = [
            {
                "task_id": t.task_id,
                "name": t.name,
                "trigger_type": t.trigger_type.value,
                "enabled": t.enabled,
            }
            for t in tasks
        ]

        return ActionResult(
            success=True,
            data={"tasks": task_list, "count": len(task_list)}
        )

    def _enable_task(self, params: Dict[str, Any]) -> ActionResult:
        """Enable a task."""
        task_id = params.get("task_id", "")

        if task_id not in self._tasks:
            return ActionResult(success=False, message=f"Task not found: {task_id}")

        self._tasks[task_id].enabled = True
        return ActionResult(success=True, message=f"Task enabled: {task_id}")

    def _disable_task(self, params: Dict[str, Any]) -> ActionResult:
        """Disable a task."""
        task_id = params.get("task_id", "")

        if task_id not in self._tasks:
            return ActionResult(success=False, message=f"Task not found: {task_id}")

        self._tasks[task_id].enabled = False
        return ActionResult(success=True, message=f"Task disabled: {task_id}")

    def _get_execution(self, params: Dict[str, Any]) -> ActionResult:
        """Get an execution by ID."""
        execution_id = params.get("execution_id", "")

        if execution_id not in self._executions:
            return ActionResult(success=False, message=f"Execution not found: {execution_id}")

        exec_data = self._executions[execution_id]
        return ActionResult(
            success=True,
            data={
                "execution_id": exec_data.execution_id,
                "task_id": exec_data.task_id,
                "status": exec_data.status.value,
                "started_at": exec_data.started_at,
                "completed_at": exec_data.completed_at,
                "result": exec_data.result,
                "error": exec_data.error,
            }
        )

    def _get_execution_history(self, params: Dict[str, Any]) -> ActionResult:
        """Get execution history for a task."""
        task_id = params.get("task_id", "")
        limit = params.get("limit", 50)

        executions = [
            e for e in self._executions.values()
            if e.task_id == task_id
        ]

        executions.sort(key=lambda e: e.started_at, reverse=True)

        return ActionResult(
            success=True,
            data={
                "executions": [
                    {
                        "execution_id": e.execution_id,
                        "status": e.status.value,
                        "started_at": e.started_at,
                        "completed_at": e.completed_at,
                        "duration": (e.completed_at - e.started_at) if e.completed_at else None,
                    }
                    for e in executions[:limit]
                ],
                "count": len(executions),
            }
        )

    def _cancel_execution(self, params: Dict[str, Any]) -> ActionResult:
        """Cancel a running execution."""
        execution_id = params.get("execution_id", "")

        if execution_id not in self._executions:
            return ActionResult(success=False, message=f"Execution not found: {execution_id}")

        execution = self._executions[execution_id]

        if execution.status == ProcessStatus.RUNNING:
            execution.status = ProcessStatus.CANCELLED
            execution.completed_at = time.time()
            return ActionResult(success=True, message=f"Execution cancelled: {execution_id}")
        else:
            return ActionResult(
                success=False,
                message=f"Cannot cancel execution in status: {execution.status.value}"
            )

    def _trigger_event(self, params: Dict[str, Any]) -> ActionResult:
        """Trigger an event that may activate tasks."""
        event_name = params.get("event_name", "")
        event_data = params.get("event_data", {})

        if not event_name:
            return ActionResult(success=False, message="event_name is required")

        triggered_tasks = []

        for task_id, task in self._tasks.items():
            if not task.enabled:
                continue

            if task.trigger_type != TriggerType.EVENT:
                continue

            if task.trigger_config.get("event_name") == event_name:
                result = self._run_task({
                    "task_id": task_id,
                    "trigger_data": {**event_data, "_event_name": event_name}
                })

                if result.success:
                    triggered_tasks.append({
                        "task_id": task_id,
                        "name": task.name,
                        "execution_id": result.data.get("execution_id"),
                    })

        return ActionResult(
            success=True,
            message=f"Event triggered {len(triggered_tasks)} tasks",
            data={"triggered_tasks": triggered_tasks}
        )

    def _delete_task(self, params: Dict[str, Any]) -> ActionResult:
        """Delete a task."""
        task_id = params.get("task_id", "")

        if task_id not in self._tasks:
            return ActionResult(success=False, message=f"Task not found: {task_id}")

        del self._tasks[task_id]

        if task_id in self._schedules:
            del self._schedules[task_id]

        return ActionResult(success=True, message=f"Task deleted: {task_id}")

    def _build_schedule(self, schedule_data: Dict[str, Any]) -> Schedule:
        """Build a Schedule from data."""
        schedule_type_str = schedule_data.get("type", "once")

        try:
            schedule_type = ScheduleType(schedule_type_str)
        except ValueError:
            schedule_type = ScheduleType.ONCE

        return Schedule(
            schedule_type=schedule_type,
            start_time=schedule_data.get("start_time"),
            end_time=schedule_data.get("end_time"),
            interval_seconds=schedule_data.get("interval_seconds", 0),
            cron_expression=schedule_data.get("cron_expression", ""),
            max_runs=schedule_data.get("max_runs"),
        )

    def _evaluate_conditions(
        self,
        conditions: Dict[str, Any],
        trigger_data: Dict[str, Any],
    ) -> bool:
        """Evaluate task conditions."""
        condition_type = conditions.get("type", "always")

        if condition_type == "always":
            return True
        elif condition_type == "never":
            return False
        elif condition_type == "expression":
            expr = conditions.get("expression", "True")
            try:
                context = {"data": trigger_data}
                return eval(expr, {"__builtins__": {}}, context)
            except Exception:
                return False
        elif condition_type == "data_match":
            field_name = conditions.get("field")
            expected_value = conditions.get("value")
            operator = conditions.get("operator", "eq")

            actual_value = trigger_data.get(field_name)

            if operator == "eq":
                return actual_value == expected_value
            elif operator == "ne":
                return actual_value != expected_value
            elif operator == "gt":
                return actual_value > expected_value
            elif operator == "lt":
                return actual_value < expected_value
            elif operator == "in":
                return actual_value in expected_value

        return True

    def _notify(self, task: ProcessTask, execution: ProcessExecution, status: str) -> None:
        """Send notifications for task completion."""
        if status not in task.notifications:
            return

        for channel in task.notifications[status]:
            if channel == "log":
                pass

    def _generate_task_id(self, name: str) -> str:
        """Generate a unique task ID."""
        raw = f"{name}:{time.time()}"
        return f"task_{hashlib.sha1(raw.encode()).hexdigest()[:10]}"

    def _generate_execution_id(self) -> str:
        """Generate a unique execution ID."""
        return f"exec_{hashlib.sha1(str(time.time_ns()).encode()).hexdigest()[:12]}"

    def get_task_statistics(self) -> Dict[str, Any]:
        """Get statistics about tasks and executions."""
        total_tasks = len(self._tasks)
        enabled_tasks = sum(1 for t in self._tasks.values() if t.enabled)

        by_trigger: Dict[str, int] = {}
        for task in self._tasks.values():
            trigger = task.trigger_type.value
            by_trigger[trigger] = by_trigger.get(trigger, 0) + 1

        total_executions = len(self._executions)
        by_status: Dict[str, int] = {}
        for exec_data in self._executions.values():
            status = exec_data.status.value
            by_status[status] = by_status.get(status, 0) + 1

        return {
            "total_tasks": total_tasks,
            "enabled_tasks": enabled_tasks,
            "by_trigger": by_trigger,
            "total_executions": total_executions,
            "by_status": by_status,
        }

    def register_listener(self, event: str, listener: Callable) -> None:
        """Register a listener for automation events."""
        if event not in self._listeners:
            self._listeners[event] = []
        self._listeners[event].append(listener)
