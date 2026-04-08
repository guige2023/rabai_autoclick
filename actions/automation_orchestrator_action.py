"""
Automation Orchestrator Action - Coordinates multiple automation tasks.

This module provides task orchestration capabilities including dependency
management, parallel execution, failure handling, and result aggregation.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Awaitable
from enum import Enum
from collections import defaultdict


class TaskStatus(Enum):
    """Status of an orchestrated task."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class RetryPolicy(Enum):
    """Retry behavior for failed tasks."""
    NO_RETRY = "no_retry"
    IMMEDIATE = "immediate"
    EXPONENTIAL = "exponential"
    FIXED_INTERVAL = "fixed_interval"


@dataclass
class OrchestrationTask:
    """A single task within an orchestration workflow."""
    task_id: str
    name: str
    handler: Callable[[], Awaitable[Any]]
    dependencies: list[str] = field(default_factory=list)
    retry_policy: RetryPolicy = RetryPolicy.NO_RETRY
    max_retries: int = 3
    timeout: float = 60.0
    retry_delay: float = 1.0
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: str | None = None
    attempts: int = 0
    start_time: float = 0.0
    end_time: float = 0.0


@dataclass
class OrchestrationResult:
    """Result of an orchestrated workflow execution."""
    workflow_id: str
    success: bool
    tasks_completed: int = 0
    tasks_failed: int = 0
    total_duration_ms: float = 0.0
    results: dict[str, Any] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)


class Workflow:
    """
    A workflow containing multiple tasks with dependencies.
    
    Example:
        workflow = Workflow("data_pipeline")
        workflow.add_task(TaskA())
        workflow.add_task(TaskB(), dependencies=["TaskA"])
        result = await orchestrator.execute(workflow)
    """
    
    def __init__(self, workflow_id: str, name: str | None = None) -> None:
        self.workflow_id = workflow_id
        self.name = name or workflow_id
        self._tasks: dict[str, OrchestrationTask] = {}
        self._execution_order: list[str] = []
    
    def add_task(
        self,
        task: OrchestrationTask,
    ) -> None:
        """Add a task to the workflow."""
        self._tasks[task.task_id] = task
    
    def task(
        self,
        name: str,
        handler: Callable[[], Awaitable[Any]],
        dependencies: list[str] | None = None,
        **kwargs,
    ) -> Workflow:
        """Decorator-style task registration."""
        task = OrchestrationTask(
            task_id=str(uuid.uuid4()),
            name=name,
            handler=handler,
            dependencies=dependencies or [],
            **kwargs,
        )
        self.add_task(task)
        return self
    
    def _topological_sort(self) -> list[str]:
        """Sort tasks by dependency order."""
        in_degree: dict[str, int] = {tid: 0 for tid in self._tasks}
        adjacency: dict[str, list[str]] = defaultdict(list)
        
        for task in self._tasks.values():
            for dep in task.dependencies:
                if dep in self._tasks:
                    adjacency[dep].append(task.task_id)
                    in_degree[task.task_id] += 1
        
        queue = [tid for tid, degree in in_degree.items() if degree == 0]
        sorted_order = []
        
        while queue:
            current = queue.pop(0)
            sorted_order.append(current)
            for neighbor in adjacency[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        return sorted_order


class AutomationOrchestrator:
    """
    Orchestrates execution of multiple automation tasks.
    
    Handles parallel/sequential execution, dependency management,
    retry logic, and result aggregation.
    """
    
    def __init__(
        self,
        max_parallel: int = 5,
        default_timeout: float = 60.0,
        continue_on_failure: bool = True,
    ) -> None:
        self.max_parallel = max_parallel
        self.default_timeout = default_timeout
        self.continue_on_failure = continue_on_failure
        self._semaphore = asyncio.Semaphore(max_parallel)
        self._active_tasks: dict[str, asyncio.Task] = {}
    
    async def execute(
        self,
        workflow: Workflow,
        context: dict[str, Any] | None = None,
    ) -> OrchestrationResult:
        """
        Execute a workflow.
        
        Args:
            workflow: Workflow to execute
            context: Shared context passed to all tasks
            
        Returns:
            OrchestrationResult with execution summary
        """
        start_time = time.time()
        results: dict[str, Any] = {}
        errors: dict[str, str] = {}
        
        sorted_tasks = workflow._topological_sort()
        pending: dict[str, OrchestrationTask] = {
            tid: workflow._tasks[tid] for tid in sorted_tasks
        }
        running: list[str] = []
        completed: set[str] = set()
        
        while pending or running:
            available = [
                tid for tid in pending
                if all(dep in completed for dep in pending[tid].dependencies)
            ]
            
            for tid in available[:self.max_parallel - len(running)]:
                task = pending.pop(tid)
                running.append(tid)
                asyncio.create_task(
                    self._execute_task(task, results, errors, completed, running)
                )
            
            if running and not pending:
                await asyncio.sleep(0.01)
            
            completed_in_cycle = [tid for tid in running if tid not in pending and workflow._tasks[tid].status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.CANCELLED)]
            for tid in completed_in_cycle:
                running.remove(tid)
            
            if not running and pending:
                if not self.continue_on_failure:
                    break
        
        duration_ms = (time.time() - start_time) * 1000
        
        tasks_completed = sum(1 for t in workflow._tasks.values() if t.status == TaskStatus.COMPLETED)
        tasks_failed = sum(1 for t in workflow._tasks.values() if t.status == TaskStatus.FAILED)
        
        return OrchestrationResult(
            workflow_id=workflow.workflow_id,
            success=tasks_failed == 0,
            tasks_completed=tasks_completed,
            tasks_failed=tasks_failed,
            total_duration_ms=duration_ms,
            results=results,
            errors=errors,
        )
    
    async def _execute_task(
        self,
        task: OrchestrationTask,
        results: dict[str, Any],
        errors: dict[str, str],
        completed: set[str],
        running: list[str],
    ) -> None:
        """Execute a single task with retry handling."""
        async with self._semaphore:
            task.status = TaskStatus.RUNNING
            task.start_time = time.time()
            
            for attempt in range(task.max_retries + 1):
                task.attempts += 1
                try:
                    result = await asyncio.wait_for(
                        task.handler(),
                        timeout=task.timeout,
                    )
                    task.result = result
                    task.status = TaskStatus.COMPLETED
                    results[task.task_id] = result
                    break
                except asyncio.TimeoutError:
                    task.error = f"Task timed out after {task.timeout}s"
                    if attempt == task.max_retries:
                        task.status = TaskStatus.FAILED
                        errors[task.task_id] = task.error
                except Exception as e:
                    task.error = str(e)
                    if attempt < task.max_retries:
                        if task.retry_policy == RetryPolicy.EXPONENTIAL:
                            await asyncio.sleep(task.retry_delay * (2 ** attempt))
                        elif task.retry_policy == RetryPolicy.FIXED_INTERVAL:
                            await asyncio.sleep(task.retry_delay)
                        else:
                            await asyncio.sleep(task.retry_delay)
                    else:
                        task.status = TaskStatus.FAILED
                        errors[task.task_id] = task.error
            
            task.end_time = time.time()
            completed.add(task.task_id)


class AutomationOrchestratorAction:
    """
    High-level automation orchestrator action.
    
    Example:
        action = AutomationOrchestratorAction()
        
        @action.task(name="fetch_data")
        async def fetch():
            return {"data": [1, 2, 3]}
        
        @action.task(name="process", dependencies=["fetch_data"])
        async def process(result):
            return {"processed": result["data"]}
        
        result = await action.execute()
    """
    
    def __init__(
        self,
        max_parallel: int = 5,
        continue_on_failure: bool = True,
    ) -> None:
        self.orchestrator = AutomationOrchestrator(
            max_parallel=max_parallel,
            continue_on_failure=continue_on_failure,
        )
        self._workflows: dict[str, Workflow] = {}
    
    def create_workflow(
        self,
        workflow_id: str,
        name: str | None = None,
    ) -> Workflow:
        """Create a new workflow."""
        workflow = Workflow(workflow_id, name)
        self._workflows[workflow_id] = workflow
        return workflow
    
    async def execute(
        self,
        workflow_id: str | None = None,
        context: dict[str, Any] | None = None,
    ) -> OrchestrationResult:
        """Execute a workflow by ID or the first available."""
        if workflow_id:
            if workflow_id not in self._workflows:
                raise ValueError(f"Workflow {workflow_id} not found")
            workflow = self._workflows[workflow_id]
        elif self._workflows:
            workflow = next(iter(self._workflows.values()))
        else:
            raise ValueError("No workflow available to execute")
        
        return await self.orchestrator.execute(workflow, context)
    
    def get_task_status(self, workflow_id: str, task_id: str) -> TaskStatus | None:
        """Get status of a specific task."""
        if workflow_id in self._workflows:
            task = self._workflows[workflow_id]._tasks.get(task_id)
            return task.status if task else None
        return None


# Export public API
__all__ = [
    "TaskStatus",
    "RetryPolicy",
    "OrchestrationTask",
    "OrchestrationResult",
    "Workflow",
    "AutomationOrchestrator",
    "AutomationOrchestratorAction",
]
