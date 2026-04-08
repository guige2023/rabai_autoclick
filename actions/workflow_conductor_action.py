"""
Workflow Conductor Action Module.

Conducts complex multi-step workflows with branching logic,
conditional execution, loops, and error recovery.
"""
from typing import Any, Optional, Callable
from dataclasses import dataclass, field
from actions.base_action import BaseAction


@dataclass
class WorkflowStep:
    """A step in the workflow."""
    id: str
    type: str  # task, branch, loop, parallel, wait
    handler: Optional[Callable] = None
    condition: Optional[Callable] = None
    steps: list["WorkflowStep"] = field(default_factory=list)
    max_iterations: int = 100
    retry_on_error: bool = True
    max_retries: int = 3


@dataclass
class ConductorResult:
    """Result of workflow execution."""
    success: bool
    completed_steps: list[str]
    failed_step: Optional[str]
    output: Any
    errors: list[str]


class WorkflowConductorAction(BaseAction):
    """Conduct complex multi-step workflows."""

    def __init__(self) -> None:
        super().__init__("workflow_conductor")
        self._steps_registry: dict[str, Callable] = {}

    def execute(self, context: dict, params: dict) -> dict:
        """
        Execute a workflow.

        Args:
            context: Execution context
            params: Parameters:
                - workflow: Workflow definition
                - initial_data: Starting data for workflow
                - steps: List of step configs

        Returns:
            ConductorResult with workflow execution details
        """
        import time

        initial_data = params.get("initial_data")
        step_configs = params.get("steps", [])

        steps = []
        for cfg in step_configs:
            step = WorkflowStep(
                id=cfg.get("id", ""),
                type=cfg.get("type", "task"),
                handler=self._steps_registry.get(cfg.get("handler_name")) if cfg.get("handler_name") else cfg.get("handler"),
                condition=cfg.get("condition"),
                steps=[WorkflowStep(id=s.get("id", ""), type=s.get("type", "task"), handler=s.get("handler")) for s in cfg.get("steps", [])],
                max_iterations=cfg.get("max_iterations", 100),
                retry_on_error=cfg.get("retry_on_error", True),
                max_retries=cfg.get("max_retries", 3)
            )
            steps.append(step)

        completed = []
        failed_step = None
        errors = []
        current_data = initial_data

        for step in steps:
            result = self._execute_step(step, current_data, errors)
            if result["success"]:
                completed.append(step.id)
                current_data = result["output"]
            else:
                failed_step = step.id
                if not step.retry_on_error:
                    break

        return ConductorResult(
            success=failed_step is None,
            completed_steps=completed,
            failed_step=failed_step,
            output=current_data,
            errors=errors
        )

    def _execute_step(self, step: WorkflowStep, data: Any, errors: list[str]) -> dict:
        """Execute a single workflow step."""
        import time

        if step.type == "task":
            for attempt in range(step.max_retries):
                try:
                    if step.handler:
                        output = step.handler(data)
                        return {"success": True, "output": output}
                    return {"success": True, "output": data}
                except Exception as e:
                    if attempt == step.max_retries - 1:
                        errors.append(f"Step {step.id}: {str(e)}")
                        return {"success": False, "output": data}
                    time.sleep(1 * (attempt + 1))

        elif step.type == "branch":
            if step.condition:
                try:
                    branch_data = step.condition(data)
                    for substep in step.steps:
                        if substep.type == "task":
                            result = self._execute_step(substep, branch_data, errors)
                            if result["success"]:
                                return result
                    return {"success": True, "output": data}
                except Exception as e:
                    errors.append(f"Branch {step.id}: {str(e)}")
                    return {"success": False, "output": data}
            return {"success": True, "output": data}

        elif step.type == "loop":
            iteration = 0
            loop_data = data
            while iteration < step.max_iterations:
                for substep in step.steps:
                    result = self._execute_step(substep, loop_data, errors)
                    if not result["success"]:
                        return result
                    loop_data = result["output"]
                iteration += 1
            return {"success": True, "output": loop_data}

        elif step.type == "parallel":
            import concurrent.futures
            results = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(step.steps)) as executor:
                futures = [executor.submit(self._execute_step, s, data, errors) for s in step.steps]
                for future in concurrent.futures.as_completed(futures):
                    results.append(future.result())
            if all(r["success"] for r in results):
                return {"success": True, "output": results[-1]["output"] if results else data}
            return {"success": False, "output": data}

        return {"success": True, "output": data}

    def register_step(self, name: str, handler: Callable) -> None:
        """Register a step handler."""
        self._steps_registry[name] = handler
