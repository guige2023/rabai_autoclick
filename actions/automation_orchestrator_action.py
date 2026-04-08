"""
Automation Orchestrator Action Module.

Orchestrates multiple automation workflows with dependency management,
 parallel execution, and state coordination.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass, field
from enum import Enum
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class WorkflowState(Enum):
    """State of a workflow execution."""
    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExecutionStrategy(Enum):
    """Strategy for executing workflows."""
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    DAG = "dag"


@dataclass
class WorkflowDependency:
    """A dependency on another workflow."""
    workflow_id: str
    required_states: list[WorkflowState] = field(default_factory=lambda: [WorkflowState.SUCCESS])


@dataclass
class WorkflowDefinition:
    """Definition of a workflow in the orchestrator."""
    workflow_id: str
    name: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: dict = field(default_factory=dict)
    dependencies: list[WorkflowDependency] = field(default_factory=list)
    max_retries: int = 3
    timeout: Optional[float] = None
    continue_on_failure: bool = False


@dataclass
class WorkflowExecution:
    """Runtime state of a workflow execution."""
    definition: WorkflowDefinition
    state: WorkflowState = WorkflowState.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    retry_count: int = 0


@dataclass
class OrchestrationResult:
    """Result of orchestration execution."""
    success: bool
    total_workflows: int
    completed: int = 0
    failed: int = 0
    cancelled: int = 0
    executions: Dict[str, WorkflowExecution] = field(default_factory=dict)
    total_duration_ms: float = 0.0


class AutomationOrchestratorAction:
    """
    Workflow orchestration engine with DAG support.

    Manages multiple automation workflows with dependency resolution,
    parallel/sequential execution, and comprehensive state tracking.

    Example:
        orchestrator = AutomationOrchestratorAction(strategy=ExecutionStrategy.DAG)
        orchestrator.add_workflow("scrape", scrape_func, dependencies=[login_wf])
        orchestrator.add_workflow("process", process_func, dependencies=[scrape_wf])
        result = await orchestrator.execute_all()
    """

    def __init__(
        self,
        strategy: ExecutionStrategy = ExecutionStrategy.PARALLEL,
        max_concurrent: int = 10,
    ) -> None:
        self.strategy = strategy
        self.max_concurrent = max_concurrent
        self._workflows: Dict[str, WorkflowDefinition] = {}
        self._executions: Dict[str, WorkflowExecution] = {}
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._event_hooks: Dict[str, List[Callable]] = defaultdict(list)

    def add_workflow(
        self,
        workflow_id: str,
        func: Callable,
        name: Optional[str] = None,
        args: tuple = (),
        kwargs: Optional[dict[str, Any]] = None,
        dependencies: Optional[list[WorkflowDependency]] = None,
        max_retries: int = 3,
        timeout: Optional[float] = None,
        continue_on_failure: bool = False,
    ) -> "AutomationOrchestratorAction":
        """Add a workflow to the orchestrator."""
        definition = WorkflowDefinition(
            workflow_id=workflow_id,
            name=name or workflow_id,
            func=func,
            args=args,
            kwargs=kwargs or {},
            dependencies=dependencies or [],
            max_retries=max_retries,
            timeout=timeout,
            continue_on_failure=continue_on_failure,
        )
        self._workflows[workflow_id] = definition
        self._executions[workflow_id] = WorkflowExecution(definition=definition)
        return self

    def on_event(
        self,
        event: str,
        handler: Callable,
    ) -> "AutomationOrchestratorAction":
        """Register an event handler."""
        self._event_hooks[event].append(handler)
        return self

    def get_workflow(self, workflow_id: str) -> Optional[WorkflowDefinition]:
        """Get a workflow definition by ID."""
        return self._workflows.get(workflow_id)

    def get_execution_state(self, workflow_id: str) -> Optional[WorkflowState]:
        """Get current execution state of a workflow."""
        exec = self._executions.get(workflow_id)
        return exec.state if exec else None

    async def execute_all(self) -> OrchestrationResult:
        """Execute all registered workflows."""
        import time
        start_time = time.monotonic()

        if self.strategy == ExecutionStrategy.SEQUENTIAL:
            result = await self._execute_sequential()
        elif self.strategy == ExecutionStrategy.PARALLEL:
            result = await self._execute_parallel()
        elif self.strategy == ExecutionStrategy.DAG:
            result = await self._execute_dag()
        else:
            result = await self._execute_parallel()

        result.total_duration_ms = (time.monotonic() - start_time) * 1000
        return result

    async def _execute_sequential(self) -> OrchestrationResult:
        """Execute workflows sequentially."""
        for wf_id, definition in self._workflows.items():
            execution = self._executions[wf_id]
            if execution.state in (WorkflowState.SUCCESS, WorkflowState.FAILED):
                continue

            await self._execute_workflow(wf_id)

        return self._build_result()

    async def _execute_parallel(self) -> OrchestrationResult:
        """Execute workflows in parallel with concurrency limit."""
        self._semaphore = asyncio.Semaphore(self.max_concurrent)

        tasks = [
            self._execute_workflow_with_semaphore(wf_id)
            for wf_id in self._workflows.keys()
        ]

        await asyncio.gather(*tasks, return_exceptions=True)
        return self._build_result()

    async def _execute_dag(self) -> OrchestrationResult:
        """Execute workflows respecting DAG dependencies."""
        self._semaphore = asyncio.Semaphore(self.max_concurrent)

        while True:
            ready = self._get_ready_workflows()
            if not ready:
                break

            tasks = [
                self._execute_workflow_with_semaphore(wf_id)
                for wf_id in ready
            ]

            await asyncio.gather(*tasks, return_exceptions=True)

            await asyncio.sleep(0.1)

        return self._build_result()

    def _get_ready_workflows(self) -> List[str]:
        """Get workflows that are ready to execute."""
        ready: List[str] = []

        for wf_id, definition in self._workflows.items():
            execution = self._executions[wf_id]
            if execution.state != WorkflowState.PENDING:
                continue

            deps_satisfied = True
            for dep in definition.dependencies:
                dep_exec = self._executions.get(dep.workflow_id)
                if not dep_exec or dep_exec.state not in dep.required_states:
                    deps_satisfied = False
                    break

            if deps_satisfied:
                ready.append(wf_id)

        return ready

    async def _execute_workflow_with_semaphore(self, workflow_id: str) -> None:
        """Execute workflow with semaphore control."""
        if self._semaphore:
            async with self._semaphore:
                await self._execute_workflow(workflow_id)
        else:
            await self._execute_workflow(workflow_id)

    async def _execute_workflow(self, workflow_id: str) -> None:
        """Execute a single workflow."""
        import time
        execution = self._executions[workflow_id]
        definition = execution.definition

        execution.state = WorkflowState.RUNNING
        execution.started_at = time.monotonic()
        self._trigger_event("workflow_started", workflow_id)

        for retry in range(definition.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(definition.func):
                    result = await asyncio.wait_for(
                        definition.func(*definition.args, **definition.kwargs),
                        timeout=definition.timeout,
                    )
                else:
                    result = definition.func(*definition.args, **definition.kwargs)

                execution.state = WorkflowState.SUCCESS
                execution.result = result
                execution.completed_at = time.monotonic()
                self._trigger_event("workflow_completed", workflow_id, result)
                return

            except Exception as e:
                logger.warning(f"Workflow {workflow_id} attempt {retry + 1} failed: {e}")
                execution.retry_count = retry + 1
                execution.error = str(e)

                if retry < definition.max_retries:
                    execution.state = WorkflowState.WAITING
                    await asyncio.sleep(2 ** retry)

        execution.state = WorkflowState.FAILED
        execution.completed_at = time.monotonic()
        self._trigger_event("workflow_failed", workflow_id, execution.error)

    def _trigger_event(self, event: str, *args: Any) -> None:
        """Trigger event handlers."""
        for handler in self._event_hooks.get(event, []):
            try:
                handler(*args)
            except Exception as e:
                logger.error(f"Event handler error for {event}: {e}")

    def _build_result(self) -> OrchestrationResult:
        """Build orchestration result from executions."""
        completed = sum(
            1 for e in self._executions.values()
            if e.state in (WorkflowState.SUCCESS, WorkflowState.FAILED, WorkflowState.CANCELLED)
        )
        failed = sum(
            1 for e in self._executions.values()
            if e.state == WorkflowState.FAILED
        )
        cancelled = sum(
            1 for e in self._executions.values()
            if e.state == WorkflowState.CANCELLED
        )

        return OrchestrationResult(
            success=failed == 0,
            total_workflows=len(self._workflows),
            completed=completed,
            failed=failed,
            cancelled=cancelled,
            executions=self._executions.copy(),
        )

    def cancel_workflow(self, workflow_id: str) -> bool:
        """Cancel a running workflow."""
        execution = self._executions.get(workflow_id)
        if execution and execution.state == WorkflowState.RUNNING:
            execution.state = WorkflowState.CANCELLED
            return True
        return False

    def reset(self) -> None:
        """Reset all workflow executions to pending state."""
        for wf_id, execution in self._executions.items():
            execution.state = WorkflowState.PENDING
            execution.result = None
            execution.error = None
            execution.retry_count = 0
