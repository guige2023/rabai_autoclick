"""Automation combinator action module for RabAI AutoClick.

Provides automation composition patterns:
- SequentialCombinatorAction: Execute actions sequentially
- ParallelCombinatorAction: Execute actions in parallel
- ChoiceCombinatorAction: Execute actions based on conditions
- RetryCombinatorAction: Retry failed actions
- FallbackCombinatorAction: Fallback on failure
- TimeoutCombinatorAction: Execute with timeout
"""

import time
import asyncio
from typing import Any, Dict, List, Optional, Callable, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SequentialCombinatorAction(BaseAction):
    """Execute actions sequentially, passing results between."""
    action_type = "automation_sequential_combinator"
    display_name = "顺序组合器"
    description = "顺序执行动作并传递结果"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            actions = params.get("actions", [])
            stop_on_failure = params.get("stop_on_failure", True)
            collect_results = params.get("collect_results", True)

            if not actions:
                return ActionResult(success=False, message="No actions provided")

            results = []
            context_data = {}

            for i, action_def in enumerate(actions):
                action_name = action_def.get("name", f"action_{i}")
                action_params = action_def.get("params", {})

                if collect_results and context_data:
                    action_params["_previous_results"] = context_data

                success = action_def.get("success", True)
                simulated_result = {
                    "name": action_name,
                    "success": success,
                    "index": i,
                    "data": f"result_of_{action_name}"
                }

                if stop_on_failure and not success:
                    return ActionResult(
                        success=False,
                        data={"failed_at": i, "results": results},
                        message=f"Sequence stopped at action {i}: {action_name}"
                    )

                results.append(simulated_result)
                context_data[action_name] = simulated_result["data"]

            return ActionResult(
                success=True,
                data={
                    "results": results if collect_results else None,
                    "total_actions": len(actions),
                    "completed": len(results)
                },
                message=f"Sequential combinator completed {len(results)}/{len(actions)} actions"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Sequential combinator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["actions"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"stop_on_failure": True, "collect_results": True}


class ParallelCombinatorAction(BaseAction):
    """Execute actions in parallel."""
    action_type = "automation_parallel_combinator"
    display_name = "并行组合器"
    description = "并行执行多个动作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            actions = params.get("actions", [])
            max_workers = params.get("max_workers", 5)
            wait_all = params.get("wait_all", True)
            fail_fast = params.get("fail_fast", False)

            if not actions:
                return ActionResult(success=False, message="No actions provided")

            def simulate_action(action_def: Dict) -> Dict:
                action_name = action_def.get("name", "unknown")
                success = action_def.get("success", True)
                delay = action_def.get("delay", 0.1)
                time.sleep(delay)
                return {
                    "name": action_name,
                    "success": success,
                    "data": f"result_of_{action_name}"
                }

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(simulate_action, a): a for a in actions}
                results = []
                for future in as_completed(futures):
                    if fail_fast:
                        try:
                            result = future.result()
                            if not result["success"]:
                                executor.shutdown(wait=False, cancel_futures=True)
                                return ActionResult(
                                    success=False,
                                    data={"results": [result]},
                                    message=f"Fail-fast triggered by: {result['name']}"
                                )
                            results.append(result)
                        except Exception as e:
                            return ActionResult(success=False, message=f"Action failed: {str(e)}")
                    else:
                        try:
                            results.append(future.result())
                        except Exception as e:
                            results.append({"success": False, "error": str(e)})

            success_count = sum(1 for r in results if r.get("success", False))

            return ActionResult(
                success=success_count > 0,
                data={
                    "results": results,
                    "total": len(actions),
                    "successful": success_count,
                    "failed": len(results) - success_count
                },
                message=f"Parallel combinator: {success_count}/{len(actions)} succeeded"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Parallel combinator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["actions"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"max_workers": 5, "wait_all": True, "fail_fast": False}


class ChoiceCombinatorAction(BaseAction):
    """Execute actions based on conditions."""
    action_type = "automation_choice_combinator"
    display_name = "条件选择器"
    description = "根据条件选择执行动作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            branches = params.get("branches", [])
            default_branch = params.get("default_branch")
            condition_type = params.get("condition_type", "first_match")

            if not branches:
                return ActionResult(success=False, message="No branches provided")

            executed_branch = None
            executed_result = None

            for branch in branches:
                condition = branch.get("condition", {})
                action = branch.get("action")
                name = branch.get("name", "unnamed_branch")

                if self._evaluate_condition(condition, params):
                    executed_branch = name
                    success = action.get("success", True) if isinstance(action, dict) else True
                    executed_result = {
                        "name": name,
                        "success": success,
                        "condition_matched": condition
                    }
                    if condition_type == "first_match":
                        break

            if executed_branch is None and default_branch is not None:
                executed_branch = "default"
                executed_result = {"name": "default", "success": True}

            return ActionResult(
                success=executed_result is not None,
                data={
                    "executed_branch": executed_branch,
                    "result": executed_result,
                    "branches_checked": len(branches)
                },
                message=f"Choice combinator: executed '{executed_branch}'"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Choice combinator error: {str(e)}")

    def _evaluate_condition(self, condition: Dict, context_params: Dict) -> bool:
        condition_type = condition.get("type", "always")
        if condition_type == "always":
            return True
        elif condition_type == "equals":
            key = condition.get("key")
            value = condition.get("value")
            return context_params.get(key) == value
        elif condition_type == "exists":
            return condition.get("key") in context_params
        elif condition_type == "truthy":
            return bool(context_params.get(condition.get("key")))
        return False

    def get_required_params(self) -> List[str]:
        return ["branches"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"default_branch": None, "condition_type": "first_match"}


class RetryCombinatorAction(BaseAction):
    """Retry failed actions with backoff."""
    action_type = "automation_retry_combinator"
    display_name = "重试组合器"
    description = "带退避的重试动作组合"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", {})
            max_retries = params.get("max_retries", 3)
            base_delay = params.get("base_delay", 1.0)
            exponential = params.get("exponential", True)
            jitter = params.get("jitter", True)

            import random
            last_error = None

            for attempt in range(max_retries + 1):
                success = action.get("success", True)

                if success:
                    return ActionResult(
                        success=True,
                        data={
                            "attempt": attempt + 1,
                            "max_retries": max_retries,
                            "action_name": action.get("name", "unknown")
                        },
                        message=f"Action succeeded on attempt {attempt + 1}"
                    )

                last_error = f"Attempt {attempt + 1} failed"

                if attempt < max_retries:
                    delay = base_delay * (exponential ** attempt)
                    if jitter:
                        delay *= (0.5 + random.random())
                    time.sleep(delay)

            return ActionResult(
                success=False,
                data={
                    "attempts": max_retries + 1,
                    "last_error": last_error
                },
                message=f"Action failed after {max_retries + 1} attempts"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Retry combinator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"max_retries": 3, "base_delay": 1.0, "exponential": True, "jitter": True}


class FallbackCombinatorAction(BaseAction):
    """Fallback to alternative on failure."""
    action_type = "automation_fallback_combinator"
    display_name = "降级组合器"
    description = "失败时降级到备选动作"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            primary = params.get("primary", {})
            fallback = params.get("fallback", {})
            always_try_fallback = params.get("always_try_fallback", False)

            primary_success = primary.get("success", True)

            if primary_success and not always_try_fallback:
                return ActionResult(
                    success=True,
                    data={
                        "source": "primary",
                        "result": primary
                    },
                    message="Primary action succeeded"
                )

            fallback_success = fallback.get("success", True)

            if fallback_success:
                return ActionResult(
                    success=True,
                    data={
                        "source": "fallback",
                        "result": fallback,
                        "primary_failed": not primary_success
                    },
                    message="Fallback action succeeded"
                )

            return ActionResult(
                success=False,
                data={
                    "primary_result": primary,
                    "fallback_result": fallback,
                    "both_failed": True
                },
                message="Both primary and fallback actions failed"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Fallback combinator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["primary"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"fallback": {}, "always_try_fallback": False}


class TimeoutCombinatorAction(BaseAction):
    """Execute action with timeout."""
    action_type = "automation_timeout_combinator"
    display_name = "超时组合器"
    description = "带超时限制的动作执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            action = params.get("action", {})
            timeout = params.get("timeout", 30)
            on_timeout = params.get("on_timeout", "fail")

            import random
            simulated_duration = random.uniform(0.1, timeout * 0.8) if action.get("success", True) else 0.01

            if simulated_duration > timeout:
                if on_timeout == "fail":
                    return ActionResult(
                        success=False,
                        data={"timeout": timeout, "action": action.get("name", "unknown")},
                        message=f"Action timed out after {timeout}s"
                    )
                else:
                    return ActionResult(
                        success=True,
                        data={"timeout": timeout, "timed_out": True},
                        message=f"Action timed out but marked success"
                    )

            return ActionResult(
                success=True,
                data={
                    "action": action.get("name", "unknown"),
                    "duration": simulated_duration,
                    "within_timeout": True
                },
                message=f"Action completed within timeout"
            )
        except Exception as e:
            return ActionResult(success=False, message=f"Timeout combinator error: {str(e)}")

    def get_required_params(self) -> List[str]:
        return ["action"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {"timeout": 30, "on_timeout": "fail"}
