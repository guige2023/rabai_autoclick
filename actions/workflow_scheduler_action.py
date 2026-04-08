"""
Workflow scheduler module for orchestrating complex multi-step workflows.

Supports DAG-based scheduling, dependency resolution, and parallel execution.
"""
from __future__ import annotations

import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional


class WorkflowStatus(Enum):
    """Workflow status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class TaskStatus(Enum):
    """Task status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class WorkflowTask:
    """A task in a workflow."""
    id: str
    name: str
    action: Callable
    dependencies: list[str] = field(default_factory=list)
    timeout_seconds: int = 300
    retry_count: int = 0
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None


@dataclass
class Workflow:
    """A workflow definition."""
    id: str
    name: str
    tasks: list[WorkflowTask]
    description: str = ""
    parallel_execution: bool = True
    metadata: dict = field(default_factory=dict)


@dataclass
class WorkflowExecution:
    """A workflow execution instance."""
    id: str
    workflow: Workflow
    status: WorkflowStatus = WorkflowStatus.PENDING
    task_results: dict[str, Any] = field(default_factory=dict)
    task_errors: dict[str, str] = field(default_factory=dict)
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    current_tasks: list[str] = field(default_factory=list)


class WorkflowScheduler:
    """
    Workflow scheduler for DAG-based task orchestration.

    Supports dependency resolution, parallel execution,
    and workflow state management.
    """

    def __init__(self):
        self._workflows: dict[str, Workflow] = {}
        self._executions: dict[str, WorkflowExecution] = {}

    def create_workflow(
        self,
        name: str,
        tasks: list[WorkflowTask],
        parallel_execution: bool = True,
        description: str = "",
    ) -> Workflow:
        """Create a new workflow."""
        workflow = Workflow(
            id=str(uuid.uuid4())[:12],
            name=name,
            tasks=tasks,
            description=description,
            parallel_execution=parallel_execution,
        )

        self._workflows[workflow.id] = workflow
        return workflow

    def execute_workflow(
        self,
        workflow_id: str,
        execution_id: Optional[str] = None,
    ) -> WorkflowExecution:
        """Execute a workflow."""
        workflow = self._workflows.get(workflow_id)
        if not workflow:
            raise ValueError(f"Workflow not found: {workflow_id}")

        execution = WorkflowExecution(
            id=execution_id or str(uuid.uuid4())[:8],
            workflow=workflow,
        )

        self._executions[execution.id] = execution
        return self._execute_workflow(execution)

    def _execute_workflow(self, execution: WorkflowExecution) -> WorkflowExecution:
        """Execute a workflow's tasks."""
        execution.status = WorkflowStatus.RUNNING
        workflow = execution.workflow

        pending_tasks = {t.id: t for t in workflow.tasks}
        completed_tasks: set[str] = set()
        running_tasks: set[str] = set()

        while pending_tasks or running_tasks:
            if execution.status == WorkflowStatus.CANCELLED:
                break

            if workflow.parallel_execution:
                ready_tasks = self._get_ready_tasks(pending_tasks, completed_tasks)

                for task in ready_tasks[:self._max_parallel_tasks()]:
                    self._execute_task(task, execution)
                    running_tasks.add(task.id)
                    pending_tasks.pop(task.id)
            else:
                ready_tasks = self._get_ready_tasks(pending_tasks, completed_tasks)
                if ready_tasks:
                    task = ready_tasks[0]
                    self._execute_task(task, execution)
                    completed_tasks.add(task.id)
                    pending_tasks.pop(task.id)
                    running_tasks.discard(task.id)
                else:
                    break

            completed_this_round = set()
            for task_id in running_tasks:
                task = next((t for t in execution.workflow.tasks if t.id == task_id), None)
                if task and task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED):
                    completed_this_round.add(task_id)
                    completed_tasks.add(task_id)
                    running_tasks.discard(task_id)

                    if task.status == TaskStatus.COMPLETED:
                        execution.task_results[task.id] = task.result
                    elif task.status == TaskStatus.FAILED:
                        execution.task_errors[task.id] = task.error

            if not completed_this_round and not ready_tasks:
                break

        if execution.task_errors:
            execution.status = WorkflowStatus.FAILED
        else:
            execution.status = WorkflowStatus.COMPLETED

        execution.end_time = time.time()
        return execution

    def _get_ready_tasks(
        self,
        pending_tasks: dict[str, WorkflowTask],
        completed_tasks: set[str],
    ) -> list[WorkflowTask]:
        """Get tasks that are ready to execute."""
        ready = []

        for task in pending_tasks.values():
            if all(dep in completed_tasks for dep in task.dependencies):
                ready.append(task)

        return ready

    def _max_parallel_tasks(self) -> int:
        """Get maximum parallel tasks."""
        return 4

    def _execute_task(
        self,
        task: WorkflowTask,
        execution: WorkflowExecution,
    ) -> None:
        """Execute a single task."""
        task.status = TaskStatus.RUNNING
        task.start_time = time.time()

        try:
            result = task.action(execution.task_results)
            task.result = result
            task.status = TaskStatus.COMPLETED
        except Exception as e:
            task.error = str(e)

            if task.retry_count > 0:
                task.retry_count -= 1
                task.status = TaskStatus.PENDING
                return

            task.status = TaskStatus.FAILED

        task.end_time = time.time()

    def get_execution(self, execution_id: str) -> Optional[WorkflowExecution]:
        """Get a workflow execution."""
        return self._executions.get(execution_id)

    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel a workflow execution."""
        execution = self._executions.get(execution_id)
        if execution and execution.status == WorkflowStatus.RUNNING:
            execution.status = WorkflowStatus.CANCELLED
            return True
        return False

    def list_executions(
        self,
        workflow_id: Optional[str] = None,
        status: Optional[WorkflowStatus] = None,
    ) -> list[dict]:
        """List workflow executions."""
        executions = list(self._executions.values())

        if workflow_id:
            executions = [e for e in executions if e.workflow.id == workflow_id]
        if status:
            executions = [e for e in executions if e.status == status]

        return [
            {
                "id": e.id,
                "workflow_id": e.workflow.id,
                "workflow_name": e.workflow.name,
                "status": e.status.value,
                "start_time": e.start_time,
                "end_time": e.end_time,
            }
            for e in sorted(executions, key=lambda x: x.start_time, reverse=True)
        ]

    def get_workflow(self, workflow_id: str) -> Optional[Workflow]:
        """Get a workflow by ID."""
        return self._workflows.get(workflow_id)

    def list_workflows(self) -> list[Workflow]:
        """List all workflows."""
        return list(self._workflows.values())
