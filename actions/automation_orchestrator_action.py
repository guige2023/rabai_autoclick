"""
Automation Orchestrator Module.

Orchestrates complex multi-step automation workflows with
dependency management, parallel execution, and failure recovery.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    PENDING = auto()
    READY = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()


@dataclass
class Task:
    task_id: str
    name: str
    handler: Callable
    dependencies: FrozenSet[str] = field(default_factory=frozenset)
    timeout: float = 300.0
    retry_count: int = 0
    retry_delay: float = 1.0
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Workflow:
    workflow_id: str
    name: str
    tasks: Dict[str, Task] = field(default_factory=dict)
    max_parallel: int = 5
    continue_on_failure: bool = True
    start_time: Optional[float] = None
    end_time: Optional[float] = None


class TaskGraph:
    """Manages task dependencies and execution order."""

    def __init__(self):
        self._graph: Dict[str, Set[str]] = defaultdict(set)
        self._reverse: Dict[str, Set[str]] = defaultdict(set)

    def add_dependency(self, task_id: str, depends_on: str) -> None:
        self._graph[task_id].add(depends_on)
        self._reverse[depends_on].add(task_id)

    def get_ready_tasks(self, completed: Set[str]) -> List[str]:
        ready = []
        for task_id, deps in self._graph.items():
            if deps <= completed and task_id not in completed:
                ready.append(task_id)
        return ready

    def get_execution_order(self) -> List[List[str]]:
        order: List[List[str]] = []
        completed: Set[str] = set()
        remaining = set(self._graph.keys())

        while remaining:
            batch = self.get_ready_tasks(completed)
            if not batch:
                break
            order.append(batch)
            completed.update(batch)
            remaining -= set(batch)

        return order

    def get_indegree(self, task_id: str) -> int:
        return len(self._graph.get(task_id, set()))


class AutomationOrchestrator:
    """
    Orchestrates complex automation workflows with parallel execution.
    """

    def __init__(self, max_parallel: int = 5):
        self.max_parallel = max_parallel
        self._workflows: Dict[str, Workflow] = {}
        self._task_graphs: Dict[str, TaskGraph] = {}
        self._running_workflows: Dict[str, asyncio.Task] = {}

    def create_workflow(
        self,
        workflow_id: str,
        name: str,
        tasks: List[Task],
        max_parallel: int = 5,
        continue_on_failure: bool = True,
    ) -> Workflow:
        workflow = Workflow(
            workflow_id=workflow_id,
            name=name,
            tasks={t.task_id: t for t in tasks},
            max_parallel=max_parallel,
            continue_on_failure=continue_on_failure,
        )
        self._workflows[workflow_id] = workflow

        graph = TaskGraph()
        for task in tasks:
            for dep in task.dependencies:
                graph.add_dependency(task.task_id, dep)
        self._task_graphs[workflow_id] = graph

        logger.info("Created workflow '%s' with %d tasks", name, len(tasks))
        return workflow

    async def execute_workflow(
        self, workflow_id: str, initial_context: Optional[Dict[str, Any]] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            return False, {"error": f"Workflow not found: {workflow_id}"}

        workflow.start_time = time.time()
        context = initial_context or {}

        for task in workflow.tasks.values():
            task.status = TaskStatus.PENDING
            task.result = None
            task.error = None

        completed: Set[str] = set()
        failed: Set[str] = set()
        graph = self._task_graphs[workflow_id]
        execution_order = graph.get_execution_order()

        logger.info("Executing workflow '%s' with %d batches", workflow.name, len(execution_order))

        for batch in execution_order:
            if not batch:
                continue

            batch_tasks = [(tid, workflow.tasks[tid]) for tid in batch if tid in workflow.tasks]

            if not workflow.continue_on_failure and failed:
                for task_id in batch:
                    if task_id in workflow.tasks:
                        workflow.tasks[task_id].status = TaskStatus.SKIPPED
                continue

            batch_results = await asyncio.gather(
                *[self._execute_task(t, context) for _, t in batch_tasks],
                return_exceptions=True,
            )

            for (task_id, task), result in zip(batch_tasks, batch_results):
                if isinstance(result, Exception):
                    task.status = TaskStatus.FAILED
                    task.error = str(result)
                    failed.add(task_id)
                    logger.error("Task %s failed: %s", task_id, result)
                elif isinstance(result, tuple) and len(result) == 2:
                    success, task_result = result
                    if success:
                        task.status = TaskStatus.COMPLETED
                        task.result = task_result
                        completed.add(task_id)
                    else:
                        task.status = TaskStatus.FAILED
                        task.error = str(task_result)
                        failed.add(task_id)
                else:
                    task.status = TaskStatus.COMPLETED
                    task.result = result
                    completed.add(task_id)

        workflow.end_time = time.time()
        duration = workflow.end_time - (workflow.start_time or 0)

        success = len(failed) == 0
        logger.info(
            "Workflow '%s' %s: %d/%d tasks completed in %.1fs",
            workflow.name,
            "SUCCESS" if success else "PARTIAL",
            len(completed),
            len(workflow.tasks),
            duration,
        )

        return success, {
            "workflow_id": workflow_id,
            "status": "success" if success else "partial",
            "completed": len(completed),
            "failed": len(failed),
            "duration_s": duration,
            "context": context,
        }

    async def _execute_task(
        self, task: Task, context: Dict[str, Any]
    ) -> Tuple[bool, Any]:
        task.status = TaskStatus.RUNNING
        task.started_at = time.time()

        for attempt in range(task.retry_count + 1):
            try:
                if asyncio.iscoroutinefunction(task.handler):
                    result = await asyncio.wait_for(
                        task.handler(context, task.metadata),
                        timeout=task.timeout,
                    )
                else:
                    result = task.handler(context, task.metadata)

                task.completed_at = time.time()
                return True, result

            except asyncio.TimeoutError:
                error = f"Task timed out after {task.timeout}s"
                if attempt < task.retry_count:
                    logger.warning("Task %s timeout, retrying %d", task.task_id, attempt + 1)
                    await asyncio.sleep(task.retry_delay * (attempt + 1))
                    continue
                task.error = error
                task.completed_at = time.time()
                return False, error

            except Exception as exc:
                error = str(exc)
                if attempt < task.retry_count:
                    logger.warning("Task %s error '%s', retrying %d", task.task_id, exc, attempt + 1)
                    await asyncio.sleep(task.retry_delay * (attempt + 1))
                    continue
                task.error = error
                task.completed_at = time.time()
                return False, error

    def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            return None

        task_statuses = {
            tid: t.status.name for tid, t in workflow.tasks.items()
        }
        completed = sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.COMPLETED)
        failed = sum(1 for t in workflow.tasks.values() if t.status == TaskStatus.FAILED)

        return {
            "workflow_id": workflow_id,
            "name": workflow.name,
            "status": "running" if workflow.start_time and not workflow.end_time else "completed",
            "completed_tasks": completed,
            "failed_tasks": failed,
            "total_tasks": len(workflow.tasks),
            "duration_s": (workflow.end_time or time.time()) - (workflow.start_time or 0),
            "task_statuses": task_statuses,
        }

    def list_workflows(self) -> List[str]:
        return list(self._workflows.keys())
