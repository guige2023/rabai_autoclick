"""Automation delay action module for RabAI AutoClick.

Provides delay and timing operations for automation:
- AutomationDelayAction: Configurable delays
- AutomationThrottleAction: Throttle automation execution rate
- AutomationDebounceAction: Debounce rapid events
- AutomationIntervalAction: Execute at fixed intervals
"""

import time
import threading
from typing import Any, Dict, List, Optional, Callable
from datetime import datetime, timedelta
from collections import deque

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AutomationDelayAction(BaseAction):
    """Configurable delay actions."""
    action_type = "automation_delay"
    display_name = "自动化延迟"
    description = "可配置的延迟操作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            delay_type = params.get("delay_type", "fixed")
            duration = params.get("duration", 1.0)
            jitter = params.get("jitter", 0.0)
            callback = params.get("callback")

            if delay_type == "fixed":
                actual_delay = duration
            elif delay_type == "random":
                import random
                actual_delay = random.uniform(duration * 0.5, duration * 1.5)
            elif delay_type == "exponential":
                actual_delay = min(duration * 2, 60.0)
            elif delay_type == "progressive":
                actual_delay = duration
            else:
                actual_delay = duration

            if jitter > 0:
                import random
                actual_delay += random.uniform(-jitter, jitter)

            actual_delay = max(0.01, actual_delay)
            time.sleep(actual_delay)

            return ActionResult(
                success=True,
                message=f"Delay completed: {actual_delay:.3f}s",
                data={"requested_duration": duration, "actual_duration": actual_delay, "delay_type": delay_type}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Delay error: {e}")


class AutomationThrottleAction(BaseAction):
    """Throttle automation execution rate."""
    action_type = "automation_throttle"
    display_name = "自动化节流"
    description = "限制自动化执行速率"

    def __init__(self):
        super().__init__()
        self._last_execution: Optional[float] = None
        self._execution_count = 0
        self._lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            min_interval = params.get("min_interval", 1.0)
            action = params.get("action")
            burst_allowance = params.get("burst_allowance", 0)
            callback = params.get("callback")

            with self._lock:
                now = time.time()

                if self._last_execution is not None:
                    elapsed = now - self._last_execution
                    if elapsed < min_interval:
                        if self._execution_count >= burst_allowance:
                            wait_time = min_interval - elapsed
                            return ActionResult(
                                success=False,
                                message=f"Throttled: wait {wait_time:.3f}s",
                                data={"throttled": True, "wait_time": wait_time, "execution_count": self._execution_count}
                            )

                self._execution_count += 1
                if self._execution_count > burst_allowance + 1:
                    self._execution_count = 1

                self._last_execution = now

            if callable(action):
                result = action()
                return ActionResult(success=True, message=f"Throttled execution #{self._execution_count}", data={"execution_count": self._execution_count, "result": result})
            return ActionResult(success=True, message=f"Throttled execution #{self._execution_count}", data={"execution_count": self._execution_count})
        except Exception as e:
            return ActionResult(success=False, message=f"Throttle error: {e}")


class AutomationDebounceAction(BaseAction):
    """Debounce rapid events."""
    action_type = "automation_debounce"
    display_name = "自动化防抖"
    description = "防止抖动导致的重复执行"

    def __init__(self):
        super().__init__()
        self._last_call_time: Optional[float] = None
        self._pending_call: Optional[Callable] = None
        self._lock = threading.Lock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            debounce_window = params.get("debounce_window", 0.5)
            action = params.get("action")
            immediate = params.get("immediate", False)
            callback = params.get("callback")

            with self._lock:
                now = time.time()

                if self._last_call_time is not None:
                    elapsed = now - self._last_call_time
                    if elapsed < debounce_window:
                        if immediate:
                            return ActionResult(success=True, message="Debounced (immediate)", data={"debounced": True, "waited": False})
                        return ActionResult(success=True, message="Debounced", data={"debounced": True, "waited": True})

                self._last_call_time = now

            if callable(action):
                result = action()
                return ActionResult(success=True, message="Debounced action executed", data={"executed": True, "result": result})
            return ActionResult(success=True, message="Debounced action executed", data={"executed": True})
        except Exception as e:
            return ActionResult(success=False, message=f"Debounce error: {e}")


class AutomationIntervalAction(BaseAction):
    """Execute at fixed intervals."""
    action_type = "automation_interval"
    display_name = "自动化定时执行"
    description = "固定间隔执行任务"

    def __init__(self):
        super().__init__()
        self._interval_tasks: Dict[str, Dict[str, Any]] = {}
        self._running = False
        self._thread: Optional[threading.Thread] = None

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            operation = params.get("operation", "start")
            task_id = params.get("task_id", "default")
            interval = params.get("interval", 1.0)
            max_runs = params.get("max_runs", 0)
            action = params.get("action")

            if operation == "start":
                if not callable(action):
                    return ActionResult(success=False, message="action must be callable")

                self._interval_tasks[task_id] = {
                    "interval": interval,
                    "action": action,
                    "max_runs": max_runs,
                    "run_count": 0,
                    "start_time": time.time(),
                    "running": True,
                }

                if not self._running:
                    self._running = True
                    self._thread = threading.Thread(target=self._run_loop, daemon=True)
                    self._thread.start()

                return ActionResult(success=True, message=f"Interval task '{task_id}' started", data={"task_id": task_id, "interval": interval})

            elif operation == "stop":
                if task_id in self._interval_tasks:
                    self._interval_tasks[task_id]["running"] = False
                    return ActionResult(success=True, message=f"Task '{task_id}' stopped")
                return ActionResult(success=False, message=f"Task '{task_id}' not found")

            elif operation == "status":
                if task_id and task_id in self._interval_tasks:
                    task = self._interval_tasks[task_id]
                    return ActionResult(success=True, message=f"Task '{task_id}' status", data=task)
                return ActionResult(success=True, message="All tasks", data={"tasks": self._interval_tasks, "running": self._running})

            elif operation == "list":
                return ActionResult(success=True, message=f"{len(self._interval_tasks)} tasks", data={"task_ids": list(self._interval_tasks.keys())})

            return ActionResult(success=False, message=f"Unknown operation: {operation}")
        except Exception as e:
            return ActionResult(success=False, message=f"Interval error: {e}")

    def _run_loop(self) -> None:
        """Internal loop for interval tasks."""
        while self._running:
            now = time.time()
            for task_id, task in list(self._interval_tasks.items()):
                if not task.get("running", False):
                    continue

                elapsed = now - task.get("start_time", now)
                expected_runs = int(elapsed / task["interval"]) + 1
                actual_runs = task.get("run_count", 0)

                if expected_runs > actual_runs and (task["max_runs"] == 0 or actual_runs < task["max_runs"]):
                    try:
                        task["action"]()
                    except Exception:
                        pass
                    task["run_count"] = actual_runs + 1

            time.sleep(0.1)

            if not any(t.get("running", False) for t in self._interval_tasks.values()):
                self._running = False
