"""
Automation Orchestrator Action Module.

Provides workflow orchestration with task dependencies, parallel
execution, and dynamic workflow composition.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


@dataclass
class TaskResult:
    """Result of task execution."""
    task_id: str
    status: TaskStatus
    output: Any = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def duration(self) -> Optional[float]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return None


@dataclass
class Task:
    """A task in the workflow."""
    task_id: str
    func: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    dependencies: Set[str] = field(default_factory=set)
    timeout: Optional[float] = None
    retry_count: int = 0
    max_retries: int = 0
    status: TaskStatus = TaskStatus.PENDING
    result: Optional[TaskResult] = None

    def __hash__(self) -> int:
        return hash(self.task_id)


class WorkflowDAG:
    """Directed Acyclic Graph for workflow task dependencies."""

    def __init__(self) -> None:
        self.tasks: Dict[str, Task] = {}
        self.dependents: Dict[str, Set[str]] = defaultdict(set)
        self.dependencies: Dict[str, Set[str]] = defaultdict(set)

    def add_task(self, task: Task) -> None:
        """Add a task to the DAG."""
        self.tasks[task.task_id] = task
        for dep in task.dependencies:
            self.dependents[dep].add(task.task_id)
            self.dependencies[task.task_id].add(dep)

    def get_ready_tasks(self, completed: Set[str]) -> List[Task]:
        """Get tasks that are ready to execute."""
        ready = []
        for task_id, task in self.tasks.items():
            if task.status != TaskStatus.PENDING:
                continue
            if task_id in completed:
                continue
            if task.dependencies.issubset(completed):
                ready.append(task)
        return ready

    def get_execution_order(self) -> List[List[Task]]:
        """Get tasks grouped by execution level (parallel tasks together)."""
        levels: List[List[Task]] = []
        completed: Set[str] = set()
        remaining = set(self.tasks.keys())

        while remaining:
            ready = [
                self.tasks[tid]
                for tid in remaining
                if self.dependencies[tid].issubset(completed)
            ]
            if not ready:
                raise ValueError("Workflow has circular dependencies")
            levels.append(ready)
            for task in ready:
                remaining.remove(task.task_id)
                completed.add(task.task_id)

        return levels


@dataclass
class WorkflowConfig:
    """Configuration for workflow execution."""
    max_parallel_tasks: int = 10
    default_timeout: float = 300.0
    continue_on_failure: bool = True
    enable_caching: bool = True


class WorkflowOrchestrator:
    """Main workflow orchestrator."""

    def __init__(self, config: Optional[WorkflowConfig] = None) -> None:
        self.config = config or WorkflowConfig()
        self.dag = WorkflowDAG()
        self.results: Dict[str, TaskResult] = {}
        self._cache: Dict[str, Any] = {}

    def add_task(
        self,
        task_id: str,
        func: Callable,
        dependencies: Optional[List[str]] = None,
        **kwargs,
    ) -> "WorkflowOrchestrator":
        """Add a task to the workflow."""
        task = Task(
            task_id=task_id,
            func=func,
            dependencies=set(dependencies or []),
            **kwargs,
        )
        self.dag.add_task(task)
        return self

    def add_parallel_tasks(
        self,
        tasks: List[Tuple[str, Callable, Optional[List[str]]]],
    ) -> "WorkflowOrchestrator":
        """Add multiple tasks that can run in parallel."""
        for task_id, func, dependencies in tasks:
            self.add_task(task_id, func, dependencies)
        return self

    async def _execute_task(self, task: Task) -> TaskResult:
        """Execute a single task."""
        import time
        start_time = time.time()

        # Check cache
        if self.config.enable_caching and task.task_id in self._cache:
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                output=self._cache[task.task_id],
                start_time=start_time,
                end_time=time.time(),
                metadata={"from_cache": True},
            )

        task.status = TaskStatus.RUNNING
        result = TaskResult(
            task_id=task.task_id,
            status=TaskStatus.RUNNING,
            start_time=start_time,
        )

        try:
            if asyncio.iscoroutinefunction(task.func):
                if task.timeout:
                    output = await asyncio.wait_for(
                        task.func(*task.args, **task.kwargs),
                        timeout=task.timeout,
                    )
                else:
                    output = await task.func(*task.args, **task.kwargs)
            else:
                loop = asyncio.get_event_loop()
                if task.timeout:
                    output = await asyncio.wait_for(
                        loop.run_in_executor(None, lambda: task.func(*task.args, **task.kwargs)),
                        timeout=task.timeout,
                    )
                else:
                    output = await loop.run_in_executor(
                        None, lambda: task.func(*task.args, **task.kwargs)
                    )

            result.status = TaskStatus.COMPLETED
            result.output = output
            result.end_time = time.time()
            task.status = TaskStatus.COMPLETED

            if self.config.enable_caching:
                self._cache[task.task_id] = output

        except asyncio.TimeoutError:
            result.status = TaskStatus.FAILED
            result.error = f"Task timeout after {task.timeout}s"
            result.end_time = time.time()
            task.status = TaskStatus.FAILED

        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)
            result.end_time = time.time()
            task.status = TaskStatus.FAILED

        task.result = result
        self.results[task.task_id] = result
        return result

    async def run(self) -> Dict[str, TaskResult]:
        """Execute the workflow."""
        execution_levels = self.dag.get_execution_order()
        failed_tasks: Set[str] = set()

        for level in execution_levels:
            # Filter out tasks with failed dependencies
            runnable = [
                task for task in level
                if not (task.dependencies & failed_tasks)
            ]

            if not runnable:
                continue

            # Execute tasks in parallel (respecting max limit)
            semaphore = asyncio.Semaphore(self.config.max_parallel_tasks)

            async def execute_with_semaphore(task: Task) -> TaskResult:
                async with semaphore:
                    return await self._execute_task(task)

            results = await asyncio.gather(
                *[execute_with_semaphore(task) for task in runnable],
                return_exceptions=True,
            )

            # Process results
            for task, result in zip(runnable, results):
                if isinstance(result, Exception):
                    self.results[task.task_id] = TaskResult(
                        task_id=task.task_id,
                        status=TaskStatus.FAILED,
                        error=str(result),
                    )
                    failed_tasks.add(task.task_id)
                elif result.status == TaskStatus.FAILED:
                    failed_tasks.add(task.task_id)
                    if not self.config.continue_on_failure:
                        break

        return self.results

    def get_result(self, task_id: str) -> Optional[TaskResult]:
        """Get result of a specific task."""
        return self.results.get(task_id)

    def get_workflow_summary(self) -> Dict[str, Any]:
        """Get summary of workflow execution."""
        status_counts = {status: 0 for status in TaskStatus}
        for result in self.results.values():
            status_counts[result.status] += 1

        total_duration = 0
        completed_count = 0
        for result in self.results.values():
            if result.duration:
                total_duration += result.duration
                completed_count += 1

        return {
            "total_tasks": len(self.dag.tasks),
            "status_counts": {s.value: c for s, c in status_counts.items()},
            "avg_duration": total_duration / completed_count if completed_count > 0 else 0,
            "failed_tasks": list(failed_tasks),
        }
