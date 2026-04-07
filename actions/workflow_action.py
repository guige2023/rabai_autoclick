"""Workflow automation action module for RabAI AutoClick.

Provides workflow automation operations:
- WorkflowCreateAction: Create workflow
- WorkflowExecuteAction: Execute workflow steps
- WorkflowConditionalAction: Conditional branching
- WorkflowLoopAction: Loop over items
- WorkflowParallelExecuteAction: Parallel execution
- WorkflowWaitAction: Wait/delay actions
"""

import time
from typing import Any, Callable, Dict, List, Optional

import sys
import os

_parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _parent_dir)
from core.base_action import BaseAction, ActionResult


class WorkflowCreateAction(BaseAction):
    """Create a workflow."""
    action_type = "workflow_create"
    display_name = "创建工作流"
    description = "创建自动化工作流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            name = params.get("name", "unnamed")
            steps = params.get("steps", [])
            description = params.get("description", "")

            workflow = {
                "name": name,
                "description": description,
                "steps": steps,
                "step_count": len(steps),
                "created": time.time()
            }

            return ActionResult(
                success=True,
                message=f"Created workflow '{name}' with {len(steps)} steps",
                data={"workflow": workflow}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Create workflow error: {str(e)}")


class WorkflowExecuteAction(BaseAction):
    """Execute workflow steps."""
    action_type = "workflow_execute"
    display_name = "执行工作流"
    description = "执行自动化工作流"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            steps = params.get("steps", [])
            data = params.get("data", {})
            stop_on_error = params.get("stop_on_error", True)

            if not steps:
                return ActionResult(success=False, message="No steps to execute")

            results = []
            current_data = data.copy()

            for i, step in enumerate(steps):
                step_name = step.get("name", f"step_{i}")
                step_type = step.get("type", "pass")
                step_params = step.get("params", {})
                step_timeout = step.get("timeout", 30)

                try:
                    result = self._execute_step(step_type, step_params, current_data, step_timeout)
                    results.append({
                        "step": step_name,
                        "success": result.success,
                        "message": result.message,
                        "data": result.data
                    })

                    if result.data:
                        current_data.update(result.data)

                    if not result.success and stop_on_error:
                        return ActionResult(
                            success=False,
                            message=f"Workflow stopped at step '{step_name}': {result.message}",
                            data={"results": results, "completed_steps": len(results)}
                        )

                except Exception as e:
                    results.append({"step": step_name, "success": False, "error": str(e)})
                    if stop_on_error:
                        return ActionResult(
                            success=False,
                            message=f"Workflow error at step '{step_name}': {str(e)}",
                            data={"results": results, "completed_steps": len(results)}
                        )

            return ActionResult(
                success=True,
                message=f"Workflow completed: {len([r for r in results if r.get('success')])}/{len(results)} steps succeeded",
                data={"results": results, "data": current_data, "completed_steps": len(results)}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Workflow execute error: {str(e)}")

    def _execute_step(self, step_type: str, params: Dict, data: Dict, timeout: int) -> ActionResult:
        """Execute a single workflow step."""
        if step_type == "pass":
            return ActionResult(success=True, message="Pass step")

        elif step_type == "log":
            message = params.get("message", "")
            level = params.get("level", "info")
            return ActionResult(success=True, message=f"[{level}] {message}")

        elif step_type == "set":
            key = params.get("key", "")
            value = params.get("value", None)
            if callable(value):
                value = value(data)
            return ActionResult(success=True, message=f"Set {key}", data={key: value})

        elif step_type == "get":
            key = params.get("key", "")
            default = params.get("default", None)
            return ActionResult(success=True, message=f"Get {key}", data={"result": data.get(key, default)})

        elif step_type == "wait":
            seconds = params.get("seconds", 1)
            time.sleep(seconds)
            return ActionResult(success=True, message=f"Waited {seconds}s")

        elif step_type == "condition":
            condition = params.get("condition", "")
            then_value = params.get("then", True)
            else_value = params.get("else", False)
            result = then_value if self._evaluate_condition(condition, data) else else_value
            return ActionResult(success=True, message=f"Condition evaluated", data={"result": result})

        elif step_type == "transform":
            field = params.get("field", "")
            operation = params.get("operation", "passthrough")
            value = data.get(field, "")

            if operation == "uppercase":
                value = str(value).upper()
            elif operation == "lowercase":
                value = str(value).lower()
            elif operation == "trim":
                value = str(value).strip()
            elif operation == "length":
                value = len(value) if hasattr(value, "__len__") else 0

            return ActionResult(success=True, message=f"Transformed {field}", data={field: value})

        else:
            return ActionResult(success=False, message=f"Unknown step type: {step_type}")

    def _evaluate_condition(self, condition: str, data: Dict) -> bool:
        """Evaluate a simple condition."""
        try:
            for key, value in data.items():
                condition = condition.replace(key, repr(value))
            return eval(condition, {"__builtins__": {}}, {})
        except:
            return False


class WorkflowConditionalAction(BaseAction):
    """Conditional branching in workflow."""
    action_type = "workflow_conditional"
    display_name = "工作流条件"
    description = "工作流条件分支"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            condition = params.get("condition", "")
            data = params.get("data", {})
            then_steps = params.get("then", [])
            else_steps = params.get("else", [])

            if not condition:
                return ActionResult(success=False, message="condition required")

            if self._evaluate_condition(condition, data):
                executed = then_steps
                branch = "then"
            else:
                executed = else_steps
                branch = "else"

            results = []
            for step in executed:
                step_type = step.get("type", "pass")
                step_params = step.get("params", {})
                result = self._execute_simple_step(step_type, step_params, data)
                results.append(result)

            return ActionResult(
                success=True,
                message=f"Executed {branch} branch with {len(results)} steps",
                data={"branch": branch, "results": results}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Conditional error: {str(e)}")

    def _evaluate_condition(self, condition: str, data: Dict) -> bool:
        try:
            for key, value in data.items():
                condition = condition.replace(key, repr(value))
            return eval(condition, {"__builtins__": {}}, {})
        except:
            return False

    def _execute_simple_step(self, step_type: str, params: Dict, data: Dict) -> ActionResult:
        if step_type == "pass":
            return ActionResult(success=True, message="Pass")
        elif step_type == "log":
            return ActionResult(success=True, message=params.get("message", ""))
        elif step_type == "set":
            return ActionResult(success=True, message=f"Set {params.get('key')}", data={params.get("key"): params.get("value")})
        else:
            return ActionResult(success=True, message=f"Step: {step_type}")


class WorkflowLoopAction(BaseAction):
    """Loop over items in workflow."""
    action_type = "workflow_loop"
    display_name = "工作流循环"
    description = "工作流循环执行"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            items = params.get("items", [])
            loop_type = params.get("type", "for")
            steps = params.get("steps", [])
            max_iterations = params.get("max_iterations", 100)
            condition = params.get("condition", "")

            if not items and loop_type != "while":
                return ActionResult(success=False, message="No items to iterate")

            results = []
            data = {}
            iteration = 0

            if loop_type == "for":
                for i, item in enumerate(items[:max_iterations]):
                    iteration = i
                    data["item"] = item
                    data["index"] = i
                    data["key"] = item if isinstance(item, str) else i

                    for step in steps:
                        result = self._execute_simple_step(step.get("type", "pass"), step.get("params", {}), data)
                        results.append({"iteration": i, "step": step.get("name", "step"), "result": result})

            elif loop_type == "while":
                while self._evaluate_condition(condition, data) and iteration < max_iterations:
                    iteration += 1
                    data["iteration"] = iteration

                    for step in steps:
                        result = self._execute_simple_step(step.get("type", "pass"), step.get("params", {}), data)
                        results.append({"iteration": iteration, "step": step.get("name", "step"), "result": result})

            elif loop_type == "do_while":
                while iteration < max_iterations:
                    iteration += 1
                    data["iteration"] = iteration

                    for step in steps:
                        result = self._execute_simple_step(step.get("type", "pass"), step.get("params", {}), data)
                        results.append({"iteration": iteration, "step": step.get("name", "step"), "result": result})

                    if not self._evaluate_condition(condition, data):
                        break

            return ActionResult(
                success=True,
                message=f"Loop completed {iteration} iterations",
                data={"results": results, "iterations": iteration}
            )

        except Exception as e:
            return ActionResult(success=False, message=f"Loop error: {str(e)}")

    def _evaluate_condition(self, condition: str, data: Dict) -> bool:
        try:
            for key, value in data.items():
                condition = condition.replace(key, repr(value))
            return eval(condition, {"__builtins__": {}}, {})
        except:
            return False

    def _execute_simple_step(self, step_type: str, params: Dict, data: Dict) -> ActionResult:
        if step_type == "pass":
            return ActionResult(success=True, message="Pass")
        elif step_type == "log":
            return ActionResult(success=True, message=params.get("message", ""))
        elif step_type == "set":
            return ActionResult(success=True, message=f"Set {params.get('key')}", data={params.get("key"): params.get("value")})
        else:
            return ActionResult(success=True, message=f"Step: {step_type}")


class WorkflowParallelExecuteAction(BaseAction):
    """Parallel execution in workflow."""
    action_type = "workflow_parallel"
    display_name = "并行执行工作流"
    description = "并行执行工作流步骤"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            branches = params.get("branches", [])
            wait_all = params.get("wait_all", True)

            if not branches:
                return ActionResult(success=False, message="No branches to execute")

            import concurrent.futures

            def execute_branch(branch):
                results = []
                data = {}
                for step in branch.get("steps", []):
                    result = self._execute_simple_step(step.get("type", "pass"), step.get("params", {}), data)
                    results.append(result)
                    if result.data:
                        data.update(result.data)
                return {"branch": branch.get("name", "unnamed"), "results": results, "data": data}

            with concurrent.futures.ThreadPoolExecutor(max_workers=len(branches)) as executor:
                futures = {executor.submit(execute_branch, branch): branch for branch in branches}
                branch_results = {}

                for future in concurrent.futures.as_completed(futures, timeout=30):
                    try:
                        result = future.result()
                        branch_results[result["branch"]] = result
                    except Exception as e:
                        branch_results[futures[future].get("name", "unnamed")] = {"error": str(e)}

            if wait_all:
                return ActionResult(
                    success=True,
                    message=f"Parallel execution completed: {len(branch_results)} branches",
                    data={"branches": branch_results, "branch_count": len(branch_results)}
                )
            else:
                first_result = list(branch_results.values())[0]
                return ActionResult(
                    success=True,
                    message=f"First branch completed",
                    data={"result": first_result}
                )

        except Exception as e:
            return ActionResult(success=False, message=f"Parallel execute error: {str(e)}")

    def _execute_simple_step(self, step_type: str, params: Dict, data: Dict) -> ActionResult:
        if step_type == "pass":
            return ActionResult(success=True, message="Pass")
        elif step_type == "log":
            return ActionResult(success=True, message=params.get("message", ""))
        elif step_type == "set":
            return ActionResult(success=True, message=f"Set {params.get('key')}", data={params.get("key"): params.get("value")})
        else:
            return ActionResult(success=True, message=f"Step: {step_type}")


class WorkflowWaitAction(BaseAction):
    """Wait/delay in workflow."""
    action_type = "workflow_wait"
    display_name = "工作流等待"
    description = "工作流等待/延迟"

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        try:
            wait_type = params.get("type", "time")
            seconds = params.get("seconds", 1)
            until = params.get("until", None)

            if wait_type == "time":
                time.sleep(seconds)
                return ActionResult(success=True, message=f"Waited {seconds}s")

            elif wait_type == "until":
                if until:
                    target = self._parse_time(until)
                    if target:
                        now = time.time()
                        wait_seconds = target - now
                        if wait_seconds > 0:
                            time.sleep(wait_seconds)
                            return ActionResult(success=True, message=f"Waited until {until}")
                        else:
                            return ActionResult(success=True, message="Time already passed")
                    else:
                        return ActionResult(success=False, message=f"Invalid time format: {until}")
                else:
                    return ActionResult(success=False, message="until time required")

            elif wait_type == "random":
                min_seconds = params.get("min", 1)
                max_seconds = params.get("max", 5)
                import random
                wait_time = random.uniform(min_seconds, max_seconds)
                time.sleep(wait_time)
                return ActionResult(success=True, message=f"Waited {wait_time:.2f}s (random)")

            else:
                time.sleep(seconds)
                return ActionResult(success=True, message=f"Waited {seconds}s")

        except Exception as e:
            return ActionResult(success=False, message=f"Wait error: {str(e)}")

    def _parse_time(self, time_str: str) -> Optional[float]:
        """Parse time string to timestamp."""
        try:
            if ":" in time_str:
                from datetime import datetime
                fmt = "%H:%M:%S" if time_str.count(":") == 2 else "%H:%M"
                target_dt = datetime.strptime(time_str, fmt)
                now = datetime.now()
                target_dt = target_dt.replace(year=now.year, month=now.month, day=now.day)
                if target_dt < now:
                    target_dt = target_dt.replace(day=now.day + 1)
                return target_dt.timestamp()
        except:
            pass
        return None
