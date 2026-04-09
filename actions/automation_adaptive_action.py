"""Automation adaptive action module for RabAI AutoClick.

Provides adaptive automation operations:
- AutomationAdaptiveSpeedAction: Adjust execution speed dynamically
- AutomationAdaptiveRetryAction: Adaptive retry based on failure patterns
- AutomationAdaptiveBranchAction: Dynamic branching based on runtime data
- AutomationAdaptiveScaleAction: Scale automation based on load
"""

import time
import random
from typing import Any, Dict, List, Optional
from datetime import datetime
from enum import Enum

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class AdaptationStrategy(Enum):
    """Adaptation strategies."""
    CONSERVATIVE = "conservative"
    MODERATE = "moderate"
    AGGRESSIVE = "aggressive"


class AutomationAdaptiveSpeedAction(BaseAction):
    """Adjust automation execution speed dynamically."""
    action_type = "automation_adaptive_speed"
    display_name = "自动化自适应速度"
    description = "动态调整自动化执行速度"

    def __init__(self):
        super().__init__()
        self._current_speed = 1.0
        self._error_history: List[bool] = []
        self._max_history = 20

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            strategy = params.get("strategy", "moderate")
            initial_speed = params.get("initial_speed", 1.0)

            if not steps:
                return ActionResult(success=False, message="steps list is required")

            self._current_speed = initial_speed
            strategy_enum = AdaptationStrategy(strategy.lower())
            results = []

            for i, step in enumerate(steps):
                delay = step.get("delay", 0)
                adjusted_delay = delay / self._current_speed

                time.sleep(max(0.01, adjusted_delay))

                success = step.get("success", True)
                results.append({"step": i, "success": success, "delay_used": adjusted_delay})
                self._update_speed(success, strategy_enum)

            recent_results = results[-self._max_history:]
            success_rate = sum(1 for r in recent_results if r["success"]) / len(recent_results) if recent_results else 1.0

            return ActionResult(
                success=success_rate >= 0.8,
                message=f"Adaptive speed execution: {len(results)} steps, final speed={self._current_speed:.2f}",
                data={"steps_executed": len(results), "final_speed": self._current_speed, "success_rate": success_rate, "results": results}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adaptive speed error: {e}")

    def _update_speed(self, success: bool, strategy: AdaptationStrategy) -> None:
        """Update speed based on success/failure."""
        self._error_history.append(success)
        if len(self._error_history) > self._max_history:
            self._error_history = self._error_history[-self._max_history:]

        recent_success_rate = sum(1 for s in self._error_history[-5:]) / min(len(self._error_history), 5)

        if strategy == AdaptationStrategy.CONSERVATIVE:
            delta = 0.05
        elif strategy == AdaptationStrategy.AGGRESSIVE:
            delta = 0.2
        else:
            delta = 0.1

        if not success:
            self._current_speed = max(0.1, self._current_speed * (1 - delta * 2))
        elif recent_success_rate > 0.9 and self._current_speed < 5.0:
            self._current_speed = min(5.0, self._current_speed * (1 + delta))


class AutomationAdaptiveRetryAction(BaseAction):
    """Adaptive retry based on failure patterns."""
    action_type = "automation_adaptive_retry"
    display_name = "自动化自适应重试"
    description = "基于失败模式的自适应重试"

    def __init__(self):
        super().__init__()
        self._failure_patterns: Dict[str, int] = {}

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action")
            max_retries = params.get("max_retries", 3)
            failure_threshold = params.get("failure_threshold", 3)
            callback = params.get("callback")

            if not action:
                return ActionResult(success=False, message="action is required")

            action_key = str(action)
            consecutive_failures = self._failure_patterns.get(action_key, 0)

            adjusted_retries = min(max_retries + consecutive_failures // failure_threshold, 10)
            attempt = 0

            while attempt <= adjusted_retries:
                try:
                    if callable(action):
                        result = action()
                    else:
                        result = {"success": True, "message": "Action executed"}

                    if result.get("success", False):
                        self._failure_patterns[action_key] = 0
                        return ActionResult(
                            success=True,
                            message=f"Action succeeded on attempt {attempt + 1}",
                            data={"attempts": attempt + 1, "adjusted_retries": adjusted_retries}
                        )
                except Exception as e:
                    pass

                attempt += 1
                if attempt <= adjusted_retries:
                    backoff = min(2.0 ** attempt, 30.0)
                    time.sleep(backoff + random.uniform(0, 1))

            self._failure_patterns[action_key] = consecutive_failures + 1
            return ActionResult(
                success=False,
                message=f"Action failed after {attempt} attempts",
                data={"attempts": attempt, "adjusted_retries": adjusted_retries}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adaptive retry error: {e}")


class AutomationAdaptiveBranchAction(BaseAction):
    """Dynamic branching based on runtime data."""
    action_type = "automation_adaptive_branch"
    display_name = "自动化自适应分支"
    description = "基于运行时数据的动态分支"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            branches = params.get("branches", [])
            condition_evaluator = params.get("condition_evaluator")
            default_branch = params.get("default_branch", 0)

            if not branches:
                return ActionResult(success=False, message="branches list is required")

            selected_idx = default_branch
            selected_reason = "default"

            for i, branch in enumerate(branches):
                condition = branch.get("condition")
                if condition and callable(condition_evaluator):
                    try:
                        if condition_evaluator(condition, context):
                            selected_idx = i
                            selected_reason = f"condition matched: {condition}"
                            break
                    except Exception:
                        pass
                elif condition and isinstance(condition, str):
                    try:
                        if eval(condition, {"context": context, "len": len, "str": str, "int": int, "float": float, "bool": bool}):
                            selected_idx = i
                            selected_reason = f"condition matched: {condition}"
                            break
                    except Exception:
                        pass

            selected_branch = branches[selected_idx]
            action_result = selected_branch.get("action")

            if callable(action_result):
                result = action_result()
            else:
                result = {"success": True, "message": "Branch action executed"}

            return ActionResult(
                success=result.get("success", False),
                message=f"Selected branch {selected_idx}: {selected_reason}",
                data={"selected_branch": selected_idx, "reason": selected_reason, "result": result}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adaptive branch error: {e}")


class AutomationAdaptiveScaleAction(BaseAction):
    """Scale automation workers based on load."""
    action_type = "automation_adaptive_scale"
    display_name = "自动化自适应扩缩容"
    description = "基于负载的自适应扩缩容"

    def __init__(self):
        super().__init__()
        self._worker_count = 1
        self._load_history: List[float] = []

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            tasks = params.get("tasks", [])
            min_workers = params.get("min_workers", 1)
            max_workers = params.get("max_workers", 10)
            scale_threshold_up = params.get("scale_threshold_up", 0.7)
            scale_threshold_down = params.get("scale_threshold_down", 0.3)
            callback = params.get("callback")

            if not tasks:
                return ActionResult(success=False, message="tasks list is required")

            from concurrent.futures import ThreadPoolExecutor, as_completed

            current_load = len(tasks) / max_workers
            self._load_history.append(current_load)
            if len(self._load_history) > 10:
                self._load_history = self._load_history[-10:]

            avg_load = sum(self._load_history) / len(self._load_history)

            if avg_load > scale_threshold_up and self._worker_count < max_workers:
                self._worker_count = min(max_workers, self._worker_count + 1)
            elif avg_load < scale_threshold_down and self._worker_count > min_workers:
                self._worker_count = max(min_workers, self._worker_count - 1)

            results = []
            with ThreadPoolExecutor(max_workers=self._worker_count) as executor:
                futures = {executor.submit(self._execute_task, task): task for task in tasks}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        results.append(result)
                    except Exception as e:
                        results.append({"success": False, "error": str(e)})

            success_count = sum(1 for r in results if r.get("success", False))
            return ActionResult(
                success=success_count == len(results),
                message=f"Scaled to {self._worker_count} workers, executed {len(tasks)} tasks",
                data={"workers": self._worker_count, "tasks": len(tasks), "success": success_count, "results": results}
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Adaptive scale error: {e}")

    def _execute_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single task."""
        time.sleep(random.uniform(0.01, 0.1))
        return {"success": True, "task": task.get("name", "unnamed")}
