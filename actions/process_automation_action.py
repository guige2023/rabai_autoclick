"""Process Automation Action Module.

Provides workflow-based process automation with parallel execution,
conditional branching, error handling, and state management.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.base_action import BaseAction, ActionResult


logger = logging.getLogger(__name__)


class StepStatus(Enum):
    """Status of a process step."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class StepType(Enum):
    """Types of process steps."""
    ACTION = "action"
    CONDITION = "condition"
    LOOP = "loop"
    PARALLEL = "parallel"
    WAIT = "wait"
    NOTIFY = "notify"
    SCRIPT = "script"


@dataclass
class ProcessStep:
    """A single step in a process automation."""
    id: str
    type: StepType
    name: str
    config: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    retry_delay: float = 1.0
    timeout: float = 60.0
    continue_on_error: bool = False
    conditions: Optional[Dict[str, Any]] = None


@dataclass
class StepResult:
    """Result of a step execution."""
    step_id: str
    status: StepStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    attempts: int = 1


@dataclass
class ProcessContext:
    """Shared context for process execution."""
    variables: Dict[str, Any] = field(default_factory=dict)
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    start_time: float = 0.0
    end_time: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    def set_var(self, key: str, value: Any) -> None:
        """Set a context variable."""
        self.variables[key] = value

    def get_var(self, key: str, default: Any = None) -> Any:
        """Get a context variable."""
        return self.variables.get(key, default)

    def set_result(self, step_id: str, result: StepResult) -> None:
        """Store a step result."""
        self.step_results[step_id] = result

    def get_result(self, step_id: str) -> Optional[StepResult]:
        """Get a step result."""
        return self.step_results.get(step_id)


class ProcessRunner:
    """Executes process steps with dependencies."""

    def __init__(self, steps: List[ProcessStep]):
        self.steps = {s.id: s for s in steps}
        self._lock = threading.RLock()

    def get_executable_steps(
        self, context: ProcessContext, completed: Set[str]
    ) -> List[ProcessStep]:
        """Get steps that are ready to execute."""
        executable = []
        for step in self.steps.values():
            if step.id in completed:
                continue
            # Check if dependencies are met
            deps = step.config.get("depends_on", [])
            if all(d in completed for d in deps):
                # Check conditions
                if step.conditions:
                    if not self._evaluate_conditions(step.conditions, context):
                        continue
                executable.append(step)
        return executable

    def _evaluate_conditions(
        self, conditions: Dict[str, Any], context: ProcessContext
    ) -> bool:
        """Evaluate step conditions."""
        for key, expected in conditions.items():
            value = context.get_var(key)
            if value != expected:
                return False
        return True


class ProcessAutomationAction(BaseAction):
    """Process Automation Action for workflow execution.

    Provides a complete process automation engine with step execution,
    conditional logic, parallel processing, and error recovery.

    Examples:
        >>> action = ProcessAutomationAction()
        >>> result = action.execute(ctx, {
        ...     "steps": [
        ...         {"id": "s1", "type": "action", "name": "Step 1"},
        ...         {"id": "s2", "type": "action", "name": "Step 2", "depends_on": ["s1"]},
        ...     ]
        ... })
    """

    action_type = "process_automation"
    display_name = "流程自动化"
    description = "工作流自动化引擎，支持并行、条件、错误恢复"

    def __init__(self):
        super().__init__()
        self._active_processes: Dict[str, ProcessContext] = {}
        self._lock = threading.RLock()

    def execute(self, context: Any, params: Dict[str, Any]) -> ActionResult:
        """Execute a process automation workflow.

        Args:
            context: Execution context.
            params: Dict with keys:
                - process_id: Unique ID for this process run
                - steps: List of ProcessStep definitions
                - variables: Initial variables
                - max_parallel: Max parallel steps (default: 3)
                - stop_on_first_error: Stop on first error (default: False)
                - async_mode: Run asynchronously (default: False)

        Returns:
            ActionResult with process execution results.
        """
        process_id = params.get("process_id", f"process_{int(time.time())}")
        steps_config = params.get("steps", [])
        initial_vars = params.get("variables", {})
        max_parallel = params.get("max_parallel", 3)
        stop_on_error = params.get("stop_on_first_error", False)
        async_mode = params.get("async_mode", False)

        # Build steps
        steps = []
        for cfg in steps_config:
            if isinstance(cfg, ProcessStep):
                steps.append(cfg)
            else:
                cfg = dict(cfg)
                cfg["type"] = StepType(cfg.get("type", "action"))
                steps.append(ProcessStep(**cfg))

        if not steps:
            return ActionResult(
                success=False,
                message="No steps defined for process"
            )

        # Create process context
        proc_context = ProcessContext(
            variables=dict(initial_vars),
            start_time=time.time(),
            metadata={"process_id": process_id, "max_parallel": max_parallel}
        )

        with self._lock:
            self._active_processes[process_id] = proc_context

        try:
            if async_mode:
                # Run in background thread
                thread = threading.Thread(
                    target=self._run_process,
                    args=(process_id, steps, max_parallel, stop_on_error)
                )
                thread.start()
                return ActionResult(
                    success=True,
                    message=f"Process '{process_id}' started asynchronously",
                    data={
                        "process_id": process_id,
                        "total_steps": len(steps),
                        "async": True,
                    }
                )
            else:
                # Run synchronously
                results = self._run_process(
                    process_id, steps, max_parallel, stop_on_error
                )
                return results

        except Exception as e:
            logger.exception(f"Process automation failed: {process_id}")
            return ActionResult(
                success=False,
                message=f"Process error: {str(e)}",
                data={"process_id": process_id}
            )

    def _run_process(
        self,
        process_id: str,
        steps: List[ProcessStep],
        max_parallel: int,
        stop_on_error: bool,
    ) -> ActionResult:
        """Run the process synchronously."""
        runner = ProcessRunner(steps)
        completed: Set[str] = set()
        failed: Set[str] = set()
        step_outputs: Dict[str, Any] = {}
        start_time = time.time()

        while True:
            # Get next executable steps
            next_steps = runner.get_executable_steps(
                self._active_processes.get(process_id) or ProcessContext(),
                completed
            )

            if not next_steps:
                break

            # Limit parallel execution
            next_steps = next_steps[:max_parallel]

            # Execute steps in parallel
            threads = []
            for step in next_steps:
                t = threading.Thread(
                    target=self._execute_step,
                    args=(process_id, step, step_outputs)
                )
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            # Check results
            for step in next_steps:
                proc_context = self._active_processes.get(process_id)
                if proc_context:
                    result = proc_context.get_result(step.id)
                    if result and result.status == StepStatus.SUCCESS:
                        completed.add(step.id)
                        step_outputs[step.id] = result.output
                    elif result and result.status == StepStatus.FAILED:
                        failed.add(step.id)
                        if stop_on_error:
                            break
                    elif step.conditions and not runner._evaluate_conditions(
                        step.conditions, proc_context
                    ):
                        completed.add(step.id)  # Mark as completed (skipped)

            if stop_on_error and failed:
                break

        proc_context = self._active_processes.get(process_id)
        if proc_context:
            proc_context.end_time = time.time()

        duration_ms = (time.time() - start_time) * 1000
        success = len(failed) == 0

        return ActionResult(
            success=success,
            message=f"Process {'succeeded' if success else 'failed with errors'}",
            data={
                "process_id": process_id,
                "total_steps": len(steps),
                "completed_steps": len(completed),
                "failed_steps": len(failed),
                "duration_ms": duration_ms,
                "step_outputs": step_outputs,
            }
        )

    def _execute_step(
        self,
        process_id: str,
        step: ProcessStep,
        outputs: Dict[str, Any],
    ) -> None:
        """Execute a single step."""
        proc_context = self._active_processes.get(process_id)
        if proc_context is None:
            return

        step_start = time.time()
        result = StepResult(
            step_id=step.id,
            status=StepStatus.RUNNING,
            attempts=1,
        )

        try:
            # Execute based on step type
            if step.type == StepType.ACTION:
                output = self._execute_action_step(step, proc_context)
                result.output = output
                result.status = StepStatus.SUCCESS

            elif step.type == StepType.CONDITION:
                passed = self._evaluate_condition(step, proc_context)
                result.output = passed
                result.status = StepStatus.SUCCESS

            elif step.type == StepType.WAIT:
                wait_time = step.config.get("seconds", 1.0)
                time.sleep(wait_time)
                result.output = {"waited": wait_time}
                result.status = StepStatus.SUCCESS

            elif step.type == StepType.NOTIFY:
                message = step.config.get("message", "")
                result.output = {"notified": message}
                result.status = StepStatus.SUCCESS

            elif step.type == StepType.LOOP:
                iterations = self._execute_loop(step, proc_context)
                result.output = {"iterations": iterations}
                result.status = StepStatus.SUCCESS

            else:
                result.output = {"skipped": True}
                result.status = StepStatus.SKIPPED

        except Exception as e:
            logger.error(f"Step {step.id} failed: {e}")
            result.status = StepStatus.FAILED
            result.error = str(e)

        result.duration_ms = (time.time() - step_start) * 1000
        proc_context.set_result(step.id, result)

    def _execute_action_step(
        self, step: ProcessStep, proc_context: ProcessContext
    ) -> Any:
        """Execute an action step."""
        action_name = step.config.get("action")
        action_params = step.config.get("params", {})
        # Substitute variables in params
        resolved_params = self._resolve_params(action_params, proc_context)
        # In a real implementation, this would dispatch to the action system
        return {"action": action_name, "params": resolved_params, "executed": True}

    def _evaluate_condition(
        self, step: ProcessStep, proc_context: ProcessContext
    ) -> bool:
        """Evaluate a condition step."""
        condition = step.config.get("condition")
        if not condition:
            return True

        # Simple condition evaluation
        var = proc_context.get_var(condition.get("variable"))
        expected = condition.get("equals")
        operator = condition.get("operator", "equals")

        if operator == "equals":
            return var == expected
        elif operator == "not_equals":
            return var != expected
        elif operator == "exists":
            return var is not None
        elif operator == "not_exists":
            return var is None
        elif operator == "greater_than":
            return var > expected
        elif operator == "less_than":
            return var < expected
        return False

    def _execute_loop(
        self, step: ProcessStep, proc_context: ProcessContext
    ) -> int:
        """Execute a loop step."""
        max_iterations = step.config.get("max_iterations", 10)
        iterations = 0
        for i in range(max_iterations):
            proc_context.set_var("loop_index", i)
            iterations += 1
        return iterations

    def _resolve_params(
        self, params: Dict[str, Any], proc_context: ProcessContext
    ) -> Dict[str, Any]:
        """Resolve variable references in parameters."""
        resolved = {}
        for k, v in params.items():
            if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
                var_name = v[2:-1]
                resolved[k] = proc_context.get_var(var_name, v)
            elif isinstance(v, dict):
                resolved[k] = self._resolve_params(v, proc_context)
            elif isinstance(v, list):
                resolved[k] = [
                    self._resolve_params({"": item}, proc_context)[""]
                    if isinstance(item, dict) and any(
                        str(p).startswith("${") for p in item
                    ) else item
                    for item in v
                ]
            else:
                resolved[k] = v
        return resolved

    def get_process_status(self, process_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a running process."""
        with self._lock:
            proc_context = self._active_processes.get(process_id)
            if proc_context is None:
                return None
            return {
                "process_id": process_id,
                "variables": proc_context.variables,
                "step_results": {
                    k: {"status": v.status.value, "output": v.output, "error": v.error}
                    for k, v in proc_context.step_results.items()
                },
                "duration_ms": (
                    (proc_context.end_time or time.time()) - proc_context.start_time
                ) * 1000,
            }

    def get_required_params(self) -> List[str]:
        return ["steps"]

    def get_optional_params(self) -> Dict[str, Any]:
        return {
            "process_id": "",
            "variables": {},
            "max_parallel": 3,
            "stop_on_first_error": False,
            "async_mode": False,
        }
