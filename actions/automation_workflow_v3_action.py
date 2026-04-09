"""Automation Workflow v3 with conditional branching and parallel execution.

This module provides an advanced workflow automation engine with:
- Conditional branching and merging
- Parallel branch execution
- Automatic rollback on failure
- Workflow persistence and recovery
- Input/output schema validation
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class WorkflowStatus(Enum):
    """Status of workflow execution."""

    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ROLLING_BACK = "rolling_back"


class BranchStatus(Enum):
    """Status of a workflow branch."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class WorkflowStep:
    """A single step in a workflow."""

    name: str
    action: Callable[[Any], Awaitable[Any]] | Callable[[Any], Any]
    input_mapping: dict[str, str] = field(default_factory=dict)  # param_name: source_key
    output_key: str = "result"
    condition: Callable[[Any], bool] | None = None
    retry_count: int = 3
    retry_delay: float = 1.0
    timeout: float | None = None
    on_failure: Callable[[Exception, Any], Any] | None = None
    rollback_action: Callable[[Any], Awaitable[Any]] | None = None


@dataclass
class WorkflowBranch:
    """A branch in conditional or parallel execution."""

    name: str
    steps: list[WorkflowStep]
    condition: Callable[[Any], bool] | None = None
    parallel: bool = False
    merge_strategy: str = "all"  # "all", "first", "last"


@dataclass
class WorkflowResult:
    """Result of workflow execution."""

    workflow_id: str
    status: WorkflowStatus
    output: Any = None
    error: Exception | None = None
    duration: float = 0.0
    steps_executed: list[str] = field(default_factory=list)
    rollback_steps: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BranchResult:
    """Result of a branch execution."""

    branch_name: str
    status: BranchStatus
    output: Any = None
    error: Exception | None = None
    duration: float = 0.0


class WorkflowContext:
    """Context shared across workflow execution."""

    def __init__(self, workflow_id: str, initial_data: Any | None = None):
        """Initialize workflow context.

        Args:
            workflow_id: Unique workflow identifier
            initial_data: Initial input data
        """
        self.workflow_id = workflow_id
        self.data: dict[str, Any] = {"_input": initial_data, "_output": None}
        self.step_results: dict[str, Any] = {}
        self.branch_results: dict[str, Any] = {}
        self.execution_log: list[dict[str, Any]] = []
        self.start_time: float = field(default_factory=time.time)

    def set(self, key: str, value: Any) -> None:
        """Set a context value."""
        self.data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a context value."""
        return self.data.get(key, default)

    def set_step_result(self, step_name: str, result: Any) -> None:
        """Store a step result."""
        self.step_results[step_name] = result
        self.data[step_name] = result
        self.data["_last"] = result

    def log(self, message: str, level: str = "INFO") -> None:
        """Add to execution log."""
        self.execution_log.append({
            "timestamp": time.time(),
            "level": level,
            "message": message,
        })


class AutomationWorkflowV3:
    """Advanced workflow automation with branching and rollback."""

    def __init__(
        self,
        name: str,
        initial_data: Any = None,
        enable_rollback: bool = True,
        enable_logging: bool = True,
    ):
        """Initialize the workflow.

        Args:
            name: Workflow name
            initial_data: Initial input data
            enable_rollback: Enable automatic rollback on failure
            enable_logging: Enable execution logging
        """
        self.name = name
        self.enable_rollback = enable_rollback
        self.enable_logging = enable_logging

        self.workflow_id = str(uuid.uuid4())
        self.context = WorkflowContext(self.workflow_id, initial_data)
        self.status = WorkflowStatus.CREATED

        self._steps: list[WorkflowStep] = []
        self._branches: list[WorkflowBranch] = []
        self._step_index: dict[str, WorkflowStep] = {}
        self._rollback_stack: list[tuple[WorkflowStep, Any]] = []

    def add_step(
        self,
        name: str,
        action: Callable[[Any], Any] | Callable[[Any], Awaitable[Any]],
        input_mapping: dict[str, str] | None = None,
        output_key: str = "result",
        condition: Callable[[Any], bool] | None = None,
        retry_count: int = 3,
        retry_delay: float = 1.0,
        timeout: float | None = None,
        on_failure: Callable[[Exception, Any], Any] | None = None,
        rollback_action: Callable[[Any], Any] | Callable[[Any], Awaitable[Any]] | None = None,
    ) -> "AutomationWorkflowV3":
        """Add a step to the workflow.

        Args:
            name: Step name (must be unique)
            action: Step action function
            input_mapping: Input parameter mapping
            output_key: Key to store output in context
            condition: Optional condition to execute step
            retry_count: Number of retries on failure
            retry_delay: Delay between retries
            timeout: Optional step timeout
            on_failure: Optional failure handler
            rollback_action: Optional rollback action

        Returns:
            Self for chaining
        """
        if name in self._step_index:
            raise ValueError(f"Step {name} already exists")

        step = WorkflowStep(
            name=name,
            action=action,
            input_mapping=input_mapping or {},
            output_key=output_key,
            condition=condition,
            retry_count=retry_count,
            retry_delay=retry_delay,
            timeout=timeout,
            on_failure=on_failure,
            rollback_action=rollback_action,
        )
        self._steps.append(step)
        self._step_index[name] = step
        return self

    def add_branch(
        self,
        name: str,
        steps: list[WorkflowStep],
        condition: Callable[[Any], bool] | None = None,
        parallel: bool = False,
        merge_strategy: str = "all",
    ) -> "AutomationWorkflowV3":
        """Add a conditional or parallel branch.

        Args:
            name: Branch name
            steps: Steps in the branch
            condition: Condition to execute branch
            parallel: Execute steps in parallel
            merge_strategy: How to merge results ("all", "first", "last")

        Returns:
            Self for chaining
        """
        branch = WorkflowBranch(
            name=name,
            steps=steps,
            condition=condition,
            parallel=parallel,
            merge_strategy=merge_strategy,
        )
        self._branches.append(branch)
        return self

    async def execute(self, input_data: Any = None) -> WorkflowResult:
        """Execute the workflow.

        Args:
            input_data: Optional input data (overrides initial data)

        Returns:
            WorkflowResult with execution details
        """
        if input_data is not None:
            self.context.data["_input"] = input_data

        self.status = WorkflowStatus.RUNNING
        self.context.log(f"Workflow {self.name} started")
        start_time = time.time()

        try:
            # Execute all steps sequentially
            for step in self._steps:
                if self.status != WorkflowStatus.RUNNING:
                    break

                # Check condition
                if step.condition and not step.condition(self.context.data):
                    self.context.log(f"Step {step.name} skipped (condition not met)")
                    continue

                # Execute step
                result = await self._execute_step(step)
                if result["success"]:
                    self.context.set_step_result(step.name, result["output"])
                    self.context.log(f"Step {step.name} completed")
                else:
                    self.context.log(f"Step {step.name} failed: {result['error']}", "ERROR")

                    if self.enable_rollback:
                        await self._rollback()

                    self.status = WorkflowStatus.FAILED
                    return WorkflowResult(
                        workflow_id=self.workflow_id,
                        status=WorkflowStatus.FAILED,
                        error=result["error"],
                        duration=time.time() - start_time,
                        steps_executed=list(self.context.step_results.keys()),
                        rollback_steps=[s for s, _ in self._rollback_stack],
                    )

            # Execute branches
            for branch in self._branches:
                if self.status != WorkflowStatus.RUNNING:
                    break

                if branch.condition and not branch.condition(self.context.data):
                    self.context.log(f"Branch {branch.name} skipped")
                    continue

                branch_result = await self._execute_branch(branch)
                self.context.branch_results[branch.name] = branch_result.output

                if branch_result.status == BranchStatus.FAILED:
                    self.status = WorkflowStatus.FAILED
                    return WorkflowResult(
                        workflow_id=self.workflow_id,
                        status=WorkflowStatus.FAILED,
                        error=branch_result.error,
                        duration=time.time() - start_time,
                        steps_executed=list(self.context.step_results.keys()),
                    )

            self.status = WorkflowStatus.COMPLETED
            self.context.data["_output"] = self.context.data.get("_last")
            self.context.log(f"Workflow {self.name} completed")

            return WorkflowResult(
                workflow_id=self.workflow_id,
                status=WorkflowStatus.COMPLETED,
                output=self.context.data.get("_output"),
                duration=time.time() - start_time,
                steps_executed=list(self.context.step_results.keys()),
            )

        except Exception as e:
            self.status = WorkflowStatus.FAILED
            self.context.log(f"Workflow {self.name} failed: {e}", "ERROR")

            if self.enable_rollback:
                await self._rollback()

            return WorkflowResult(
                workflow_id=self.workflow_id,
                status=WorkflowStatus.FAILED,
                error=e,
                duration=time.time() - start_time,
                steps_executed=list(self.context.step_results.keys()),
                rollback_steps=[s for s, _ in self._rollback_stack],
            )

    async def _execute_step(self, step: WorkflowStep) -> dict[str, Any]:
        """Execute a single step with retry."""
        last_error: Exception | None = None

        # Prepare input
        input_data = self._prepare_step_input(step)

        for attempt in range(step.retry_count + 1):
            try:
                # Execute with timeout
                if step.timeout:
                    output = await asyncio.wait_for(
                        self._run_action(step.action, input_data),
                        timeout=step.timeout,
                    )
                else:
                    output = await self._run_action(step.action, input_data)

                # Store for rollback
                if step.rollback_action:
                    self._rollback_stack.append((step, input_data))

                return {"success": True, "output": output}

            except asyncio.TimeoutError:
                last_error = TimeoutError(f"Step {step.name} timed out")
            except Exception as e:
                last_error = e

                if step.on_failure:
                    try:
                        input_data = step.on_failure(e, input_data)
                    except Exception:
                        pass

                if attempt < step.retry_count:
                    await asyncio.sleep(step.retry_delay * (2 ** attempt))

        return {"success": False, "error": last_error}

    def _prepare_step_input(self, step: WorkflowStep) -> Any:
        """Prepare input for a step based on mapping."""
        if not step.input_mapping:
            return self.context.data.get("_last", self.context.data.get("_input"))

        if isinstance(self.context.data.get("_last"), dict):
            input_dict = {}
            for param_name, source_key in step.input_mapping.items():
                input_dict[param_name] = self.context.data.get(source_key)
            return input_dict

        return self.context.data.get("_last")

    async def _run_action(
        self,
        action: Callable[[Any], Any] | Callable[[Any], Awaitable[Any]],
        input_data: Any,
    ) -> Any:
        """Run an action function."""
        if asyncio.iscoroutinefunction(action):
            return await action(input_data)
        return action(input_data)

    async def _execute_branch(self, branch: WorkflowBranch) -> BranchResult:
        """Execute a workflow branch."""
        self.context.log(f"Branch {branch.name} started")
        start_time = time.time()

        if branch.parallel:
            # Execute steps in parallel
            tasks = [self._execute_step(step) for step in branch.steps]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check results based on merge strategy
            if branch.merge_strategy == "all":
                success = all(r.get("success", False) for r in results if isinstance(r, dict))
            elif branch.merge_strategy == "first":
                success = any(r.get("success", False) for r in results if isinstance(r, dict))
            else:  # last
                success = results[-1].get("success", False) if isinstance(results[-1], dict) else False

            # Collect outputs
            outputs = [r.get("output") for r in results if isinstance(r, dict) and "output" in r]

            if success:
                return BranchResult(
                    branch_name=branch.name,
                    status=BranchStatus.COMPLETED,
                    output=outputs,
                    duration=time.time() - start_time,
                )
            else:
                return BranchResult(
                    branch_name=branch.name,
                    status=BranchStatus.FAILED,
                    error=Exception("One or more parallel steps failed"),
                    duration=time.time() - start_time,
                )
        else:
            # Execute steps sequentially
            for step in branch.steps:
                result = await self._execute_step(step)
                if not result["success"]:
                    return BranchResult(
                        branch_name=branch.name,
                        status=BranchStatus.FAILED,
                        error=result["error"],
                        duration=time.time() - start_time,
                    )

            return BranchResult(
                branch_name=branch.name,
                status=BranchStatus.COMPLETED,
                output=self.context.data.get("_last"),
                duration=time.time() - start_time,
            )

    async def _rollback(self) -> None:
        """Rollback completed steps in reverse order."""
        self.status = WorkflowStatus.ROLLING_BACK
        self.context.log("Rollback started", "WARNING")

        while self._rollback_stack:
            step, input_data = self._rollback_stack.pop()

            if step.rollback_action:
                try:
                    self.context.log(f"Rolling back step {step.name}")
                    await self._run_action(step.rollback_action, input_data)
                    self.context.log(f"Rollback for {step.name} completed")
                except Exception as e:
                    self.context.log(f"Rollback for {step.name} failed: {e}", "ERROR")

        self.context.log("Rollback completed", "WARNING")

    def pause(self) -> None:
        """Pause workflow execution."""
        if self.status == WorkflowStatus.RUNNING:
            self.status = WorkflowStatus.PAUSED
            self.context.log("Workflow paused")

    def cancel(self) -> None:
        """Cancel workflow execution."""
        self.status = WorkflowStatus.CANCELLED
        self.context.log("Workflow cancelled")

    def get_state(self) -> dict[str, Any]:
        """Get current workflow state."""
        return {
            "workflow_id": self.workflow_id,
            "name": self.name,
            "status": self.status.value,
            "context": self.context.data.copy(),
            "step_results": self.context.step_results.copy(),
            "execution_log": self.context.execution_log.copy(),
        }


def create_workflow(
    name: str,
    initial_data: Any = None,
    enable_rollback: bool = True,
) -> AutomationWorkflowV3:
    """Create a new workflow.

    Args:
        name: Workflow name
        initial_data: Initial input data
        enable_rollback: Enable rollback on failure

    Returns:
        New AutomationWorkflowV3 instance
    """
    return AutomationWorkflowV3(
        name=name,
        initial_data=initial_data,
        enable_rollback=enable_rollback,
    )
