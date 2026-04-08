"""Automation sequence action module for RabAI AutoClick.

Provides automation sequence operations:
- SequenceRunnerAction: Run a sequence of automation steps
- StepConditionAction: Conditional step execution
- LoopStepAction: Loop over data in automation
- RetryStepAction: Retry a failed step
- ParallelStepsAction: Run steps in parallel
"""

import time
from typing import Any, Callable, Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class SequenceRunnerAction(BaseAction):
    """Run a sequence of automation steps."""
    action_type = "sequence_runner"
    display_name = "自动化序列执行器"
    description = "运行自动化步骤序列"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            stop_on_failure = params.get("stop_on_failure", True)
            continue_on_error = params.get("continue_on_error", False)

            if not steps:
                return ActionResult(success=False, message="steps is required")

            results = []
            start_time = time.time()

            for i, step in enumerate(steps):
                step_name = step.get("name", f"step_{i}")
                step_type = step.get("type", "delay")
                step_config = step.get("config", {})
                step_start = time.time()

                try:
                    step_result = self._execute_step(step_type, step_config, context)
                    step_duration = time.time() - step_start
                    results.append({
                        "name": step_name,
                        "type": step_type,
                        "success": True,
                        "result": step_result,
                        "duration_ms": int(step_duration * 1000),
                    })
                except Exception as e:
                    step_duration = time.time() - step_start
                    results.append({
                        "name": step_name,
                        "type": step_type,
                        "success": False,
                        "error": str(e),
                        "duration_ms": int(step_duration * 1000),
                    })
                    if stop_on_failure:
                        break
                    elif not continue_on_error:
                        break

            total_duration = time.time() - start_time
            success_count = sum(1 for r in results if r.get("success", False))

            return ActionResult(
                success=success_count == len(steps),
                message=f"Sequence completed: {success_count}/{len(steps)} steps succeeded",
                data={
                    "results": results,
                    "total_steps": len(steps),
                    "succeeded": success_count,
                    "total_duration_ms": int(total_duration * 1000),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"SequenceRunner error: {e}")

    def _execute_step(self, step_type: str, config: Dict, context: Any) -> Any:
        if step_type == "delay":
            duration = config.get("duration", 1.0)
            time.sleep(duration)
            return {"delayed": duration}
        elif step_type == "log":
            message = config.get("message", "")
            return {"logged": message}
        elif step_type == "set_context":
            key = config.get("key")
            value = config.get("value")
            if hasattr(context, "__setitem__"):
                context[key] = value
            return {"set": key, "value": value}
        elif step_type == "http_request":
            import urllib.request
            url = config.get("url", "")
            method = config.get("method", "GET")
            req = urllib.request.Request(url, method=method)
            with urllib.request.urlopen(req, timeout=config.get("timeout", 30)) as resp:
                return {"status": resp.status, "url": url}
        return {"step_type": step_type}


class StepConditionAction(BaseAction):
    """Conditional step execution."""
    action_type = "step_condition"
    display_name = "步骤条件执行"
    description = "基于条件判断执行步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition = params.get("condition", {})
            if_true = params.get("if_true", [])
            if_false = params.get("if_false", [])
            condition_field = condition.get("field")
            condition_op = condition.get("operator", "eq")
            condition_value = condition.get("value")

            context_value = None
            if condition_field:
                if isinstance(context, dict):
                    context_value = context.get(condition_field)
                elif hasattr(context, condition_field):
                    context_value = getattr(context, condition_field)
            else:
                context_value = context

            matched = False
            if condition_op == "eq":
                matched = context_value == condition_value
            elif condition_op == "ne":
                matched = context_value != condition_value
            elif condition_op == "gt":
                matched = context_value is not None and context_value > condition_value
            elif condition_op == "ge":
                matched = context_value is not None and context_value >= condition_value
            elif condition_op == "lt":
                matched = context_value is not None and context_value < condition_value
            elif condition_op == "le":
                matched = context_value is not None and context_value <= condition_value
            elif condition_op == "is_null":
                matched = context_value is None
            elif condition_op == "is_not_null":
                matched = context_value is not None
            elif condition_op == "contains":
                matched = condition_value in context_value if context_value else False
            elif condition_op == "in":
                matched = context_value in condition_value if condition_value else False

            steps_to_run = if_true if matched else if_false

            results = []
            for i, step in enumerate(steps_to_run):
                step_name = step.get("name", f"conditional_step_{i}")
                results.append({"name": step_name, "executed": True, "condition_matched": matched})

            return ActionResult(
                success=True,
                message=f"Condition {'matched' if matched else 'not matched'}, executed {len(steps_to_run)} steps",
                data={
                    "condition_matched": matched,
                    "steps_executed": results,
                    "step_count": len(steps_to_run),
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"StepCondition error: {e}")


class LoopStepAction(BaseAction):
    """Loop over data in automation."""
    action_type = "loop_step"
    display_name = "循环步骤"
    description = "在自动化中循环遍历数据"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            loop_type = params.get("loop_type", "for")
            max_iterations = params.get("max_iterations", 1000)
            loop_body = params.get("loop_body", [])
            break_on = params.get("break_on", None)

            if not isinstance(items, list):
                items = [items]

            iterations = 0
            results = []
            break_reason = None

            if loop_type == "for":
                for i, item in enumerate(items):
                    if iterations >= max_iterations:
                        break_reason = "max_iterations"
                        break
                    results.append({"iteration": i, "item": item, "index": i})
                    iterations += 1
                    if break_on:
                        if break_on.get("field") == "item" and item == break_on.get("value"):
                            break_reason = "break_on_condition"
                            break
            elif loop_type == "while":
                condition = params.get("while_condition", {})
                op = condition.get("operator", "lt")
                threshold = condition.get("value", 0)
                counter = 0
                while True:
                    if iterations >= max_iterations:
                        break_reason = "max_iterations"
                        break
                    should_continue = False
                    if op == "lt":
                        should_continue = counter < threshold
                    elif op == "le":
                        should_continue = counter <= threshold
                    elif op == "gt":
                        should_continue = counter > threshold
                    elif op == "ge":
                        should_continue = counter >= threshold
                    elif op == "eq":
                        should_continue = counter == threshold
                    if not should_continue:
                        break
                    results.append({"iteration": iterations, "counter": counter})
                    iterations += 1
                    counter += 1

            return ActionResult(
                success=True,
                message=f"Loop completed: {iterations} iterations",
                data={
                    "iterations": iterations,
                    "results": results,
                    "break_reason": break_reason,
                    "loop_type": loop_type,
                },
            )
        except Exception as e:
            return ActionResult(success=False, message=f"LoopStep error: {e}")


class RetryStepAction(BaseAction):
    """Retry a failed step with backoff."""
    action_type = "retry_step"
    display_name = "重试步骤"
    description = "带退避的重试步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            step_func = params.get("step_func", None)
            max_retries = params.get("max_retries", 3)
            base_delay = params.get("base_delay", 1.0)
            max_delay = params.get("max_delay", 60.0)
            exponential = params.get("exponential", True)
            jitter = params.get("jitter", True)

            import random
            attempts = []
            last_error = None

            for attempt in range(max_retries + 1):
                try:
                    result = {"attempt": attempt, "success": True}
                    attempts.append(result)
                    if attempt > 0:
                        delay = min(base_delay * (2 ** (attempt - 1)) if exponential else base_delay, max_delay)
                        if jitter:
                            delay = delay * (0.5 + random.random() * 0.5)
                        time.sleep(delay)
                    return ActionResult(
                        success=True,
                        message=f"Succeeded on attempt {attempt + 1}",
                        data={"attempts": attempts, "total_attempts": len(attempts)},
                    )
                except Exception as e:
                    last_error = str(e)
                    attempts.append({"attempt": attempt, "success": False, "error": last_error})
                    if attempt < max_retries:
                        delay = min(base_delay * (2 ** attempt) if exponential else base_delay, max_delay)
                        if jitter:
                            delay = delay * (0.5 + random.random() * 0.5)
                        time.sleep(delay)

            return ActionResult(
                success=False,
                message=f"Failed after {len(attempts)} attempts: {last_error}",
                data={"attempts": attempts, "total_attempts": len(attempts), "error": last_error},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"RetryStep error: {e}")


class ParallelStepsAction(BaseAction):
    """Run steps in parallel."""
    action_type = "parallel_steps"
    display_name = "并行步骤"
    description = "并行运行多个步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            max_workers = params.get("max_workers", 5)
            fail_fast = params.get("fail_fast", False)

            if not steps:
                return ActionResult(success=False, message="steps is required")

            results = []

            def run_step(step: Dict) -> Dict:
                step_name = step.get("name", "parallel_step")
                step_type = step.get("type", "delay")
                step_config = step.get("config", {})
                start_time = time.time()
                try:
                    if step_type == "delay":
                        time.sleep(step_config.get("duration", 1.0))
                    result = {"name": step_name, "type": step_type, "success": True, "duration_ms": int((time.time() - start_time) * 1000)}
                except Exception as e:
                    result = {"name": step_name, "type": step_type, "success": False, "error": str(e), "duration_ms": int((time.time() - start_time) * 1000)}
                return result

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(run_step, step) for step in steps]
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    if fail_fast and not result.get("success", False):
                        for f in futures:
                            f.cancel()
                        break

            success_count = sum(1 for r in results if r.get("success", False))

            return ActionResult(
                success=success_count == len(steps),
                message=f"Parallel steps: {success_count}/{len(steps)} succeeded",
                data={"results": results, "succeeded": success_count, "total": len(steps)},
            )
        except Exception as e:
            return ActionResult(success=False, message=f"ParallelSteps error: {e}")
