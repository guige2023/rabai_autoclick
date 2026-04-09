"""API Orchestration Action.

Orchestrates complex multi-step API workflows with dependency resolution,
parallel execution, fan-out/fan-in patterns, and result aggregation.
"""
from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class TaskStatus(Enum):
    """Status of an orchestration task."""
    PENDING = "pending"
    WAITING = "waiting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class APITask:
    """A single API task in an orchestration."""
    id: str
    name: str
    fn: Callable[..., Any]
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    depends_on: Set[str] = field(default_factory=set)
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class OrchestrationResult:
    """Result of an orchestration run."""
    orchestration_id: str
    status: str
    total_tasks: int
    completed_tasks: int
    failed_tasks: int
    skipped_tasks: int
    total_duration_ms: float
    task_results: Dict[str, Any] = field(default_factory=dict)
    errors: Dict[str, str] = field(default_factory=dict)


class APIOrchestrationAction:
    """Orchestrates complex API workflows with dependency management."""

    def __init__(self, max_concurrent: int = 10, orchestration_id: str = "default") -> None:
        self.max_concurrent = max_concurrent
        self.orchestration_id = orchestration_id
        self._tasks: Dict[str, APITask] = {}
        self._executor: Optional[asyncio.AbstractEventLoop] = None
        self._semaphore: Optional[asyncio.Semaphore] = None

    def add_task(
        self,
        task_id: str,
        name: str,
        fn: Callable[..., Any],
        args: Optional[tuple] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        depends_on: Optional[List[str]] = None,
        max_retries: int = 3,
    ) -> APITask:
        """Add a task to the orchestration."""
        task = APITask(
            id=task_id,
            name=name,
            fn=fn,
            args=args or (),
            kwargs=kwargs or {},
            depends_on=set(depends_on or []),
            max_retries=max_retries,
        )
        self._tasks[task_id] = task
        return task

    def get_task(self, task_id: str) -> Optional[APITask]:
        """Get a task by ID."""
        return self._tasks.get(task_id)

    def get_ready_tasks(self) -> List[APITask]:
        """Get tasks that are ready to execute (all dependencies met)."""
        ready = []
        for task in self._tasks.values():
            if task.status != TaskStatus.PENDING:
                continue
            deps_completed = all(
                self._tasks[dep_id].status == TaskStatus.COMPLETED
                for dep_id in task.depends_on
                if dep_id in self._tasks
            )
            deps_failed = any(
                self._tasks[dep_id].status == TaskStatus.FAILED
                for dep_id in task.depends_on
                if dep_id in self._tasks
            )
            if deps_failed:
                task.status = TaskStatus.SKIPPED
            elif deps_completed:
                ready.append(task)
        return ready

    async def _execute_task(self, task: APITask) -> Any:
        """Execute a single task."""
        task.status = TaskStatus.RUNNING
        task.start_time = time.time()

        try:
            result = task.fn(*task.args, **task.kwargs)
            if asyncio.iscoroutine(result):
                result = await result

            task.result = result
            task.status = TaskStatus.COMPLETED
            task.end_time = time.time()
            return result

        except Exception as e:
            task.error = str(e)
            task.retry_count += 1

            if task.retry_count <= task.max_retries:
                task.status = TaskStatus.PENDING
                await asyncio.sleep(0.5 * (2 ** task.retry_count))
            else:
                task.status = TaskStatus.FAILED
                task.end_time = time.time()

            raise

    async def run_async(
        self,
        timeout: Optional[float] = None,
    ) -> OrchestrationResult:
        """Run the orchestration asynchronously."""
        start_time = time.time()
        self._semaphore = asyncio.Semaphore(self.max_concurrent)

        pending = set(self._tasks.keys())
        running_tasks: Set[asyncio.Task] = set()

        while pending or running_tasks:
            ready = self.get_ready_tasks()
            for task in ready:
                if task.id in pending:
                    pending.remove(task.id)

            for task in ready:
                if self._semaphore:
                    coro = self._execute_task(task)
                    asyncio_task = asyncio.create_task(coro)
                    running_tasks.add(asyncio_task)

            if not running_tasks:
                break

            done, _ = await asyncio.wait(
                running_tasks,
                timeout=1.0,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for d in done:
                running_tasks.discard(d)

        total_duration = (time.time() - start_time) * 1000

        completed = sum(1 for t in self._tasks.values() if t.status == TaskStatus.COMPLETED)
        failed = sum(1 for t in self._tasks.values() if t.status == TaskStatus.FAILED)
        skipped = sum(1 for t in self._tasks.values() if t.status == TaskStatus.SKIPPED)

        task_results = {
            task_id: task.result
            for task_id, task in self._tasks.items()
            if task.status == TaskStatus.COMPLETED
        }

        errors = {
            task_id: task.error
            for task_id, task in self._tasks.items()
            if task.error
        }

        return OrchestrationResult(
            orchestration_id=self.orchestration_id,
            status="completed" if failed == 0 else "partial" if completed > 0 else "failed",
            total_tasks=len(self._tasks),
            completed_tasks=completed,
            failed_tasks=failed,
            skipped_tasks=skipped,
            total_duration_ms=total_duration,
            task_results=task_results,
            errors=errors,
        )

    def run(self, timeout: Optional[float] = None) -> OrchestrationResult:
        """Run the orchestration synchronously."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(self.run_async(timeout=timeout))

    def reset(self) -> None:
        """Reset all task states."""
        for task in self._tasks.values():
            task.status = TaskStatus.PENDING
            task.result = None
            task.error = None
            task.start_time = None
            task.end_time = None
            task.retry_count = 0

    def get_execution_order(self) -> List[str]:
        """Get topologically sorted execution order."""
        in_degree: Dict[str, int] = {tid: 0 for tid in self._tasks}
        graph: Dict[str, List[str]] = defaultdict(list)

        for task in self._tasks.values():
            for dep in task.depends_on:
                if dep in self._tasks:
                    graph[dep].append(task.id)
                    in_degree[task.id] += 1

        queue = [tid for tid, deg in in_degree.items() if deg == 0]
        order = []

        while queue:
            current = queue.pop(0)
            order.append(current)
            for neighbor in graph[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        return order

    def visualize_dag(self) -> Dict[str, Any]:
        """Get a representation of the task dependency graph."""
        return {
            "orchestration_id": self.orchestration_id,
            "tasks": {
                tid: {
                    "name": task.name,
                    "depends_on": list(task.depends_on),
                    "status": task.status.value,
                }
                for tid, task in self._tasks.items()
            },
            "execution_order": self.get_execution_order(),
        }
