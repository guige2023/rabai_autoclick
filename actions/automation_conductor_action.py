"""
Automation Conductor Action Module

Provides workflow orchestration and coordination for complex automation tasks
with parallel execution, conditional routing, and result aggregation.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status."""

    PENDING = "pending"
    READY = "ready"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class TaskType(Enum):
    """Types of conductor tasks."""

    ACTION = "action"
    CONDITION = "condition"
    PARALLEL = "parallel"
    AGGREGATE = "aggregate"
    WAIT = "wait"
    NOTIFY = "notify"


@dataclass
class ConductorTask:
    """A task in the conductor workflow."""

    task_id: str
    name: str
    task_type: TaskType
    handler: Optional[Callable[..., Any]] = None
    input_mapping: Dict[str, str] = field(default_factory=dict)
    output_key: str = ""
    depends_on: List[str] = field(default_factory=list)
    condition: Optional[Callable[[Dict], bool]] = None
    retry_count: int = 0
    timeout_seconds: float = 60.0
    parallel_tasks: List["ConductorTask"] = field(default_factory=list)


@dataclass
class TaskResult:
    """Result of a task execution."""

    task_id: str
    status: TaskStatus
    output: Any = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    start_time: Optional[float] = None
    end_time: Optional[float] = None


@dataclass
class ConductorWorkflow:
    """A conductor workflow definition."""

    workflow_id: str
    name: str
    tasks: List[ConductorTask]
    status: TaskStatus = TaskStatus.PENDING
    results: Dict[str, TaskResult] = field(default_factory=dict)
    start_time: Optional[float] = None
    end_time: Optional[float] = None


@dataclass
class ConductorConfig:
    """Configuration for conductor."""

    enable_parallel: bool = True
    max_parallel_tasks: int = 10
    continue_on_error: bool = True
    default_timeout: float = 300.0
    enable_notifications: bool = True


class AutomationConductorAction:
    """
    Workflow conductor action for task orchestration.

    Features:
    - Complex workflow orchestration
    - Parallel and sequential task execution
    - Conditional task routing
    - Task dependencies and ordering
    - Result aggregation from parallel tasks
    - Task timeout and retry handling
    - Notification on workflow completion

    Usage:
        conductor = AutomationConductorAction(config)
        
        conductor.add_task("step1", task_type=TaskType.ACTION, handler=func1)
        conductor.add_task("step2", task_type=TaskType.ACTION, handler=func2, depends_on=["step1"])
        conductor.add_task("notify", task_type=TaskType.NOTIFY, handler=notify_func, depends_on=["step2"])
        
        result = await conductor.execute(workflow_name="my-workflow")
    """

    def __init__(self, config: Optional[ConductorConfig] = None):
        self.config = config or ConductorConfig()
        self._workflows: Dict[str, ConductorWorkflow] = {}
        self._notification_handlers: List[Callable[[str, Dict], None]] = []
        self._stats = {
            "workflows_executed": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "workflows_completed": 0,
        }

    def create_workflow(
        self,
        name: str,
    ) -> ConductorWorkflow:
        """Create a new workflow."""
        workflow_id = f"wf_{uuid.uuid4().hex[:12]}"
        workflow = ConductorWorkflow(
            workflow_id=workflow_id,
            name=name,
            tasks=[],
        )
        self._workflows[workflow_id] = workflow
        return workflow

    def add_task(
        self,
        workflow: ConductorWorkflow,
        task_id: str,
        name: str,
        task_type: TaskType,
        handler: Optional[Callable[..., Any]] = None,
        depends_on: Optional[List[str]] = None,
        condition: Optional[Callable[[Dict], bool]] = None,
        retry_count: int = 0,
        timeout_seconds: Optional[float] = None,
    ) -> ConductorTask:
        """Add a task to a workflow."""
        task = ConductorTask(
            task_id=task_id,
            name=name,
            task_type=task_type,
            handler=handler,
            depends_on=depends_on or [],
            condition=condition,
            retry_count=retry_count,
            timeout_seconds=timeout_seconds or self.config.default_timeout,
        )
        workflow.tasks.append(task)
        return task

    def add_parallel_tasks(
        self,
        workflow: ConductorWorkflow,
        parent_task_id: str,
        tasks: List[ConductorTask],
    ) -> None:
        """Add parallel tasks under a parent task."""
        for task in workflow.tasks:
            if task.task_id == parent_task_id:
                task.parallel_tasks = tasks
                task.task_type = TaskType.PARALLEL
                break

    async def execute(
        self,
        workflow_id: str,
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> ConductorWorkflow:
        """Execute a workflow."""
        workflow = self._workflows.get(workflow_id)
        if workflow is None:
            raise ValueError(f"Workflow not found: {workflow_id}")

        logger.info(f"Executing workflow: {workflow_id}")
        workflow.status = TaskStatus.RUNNING
        workflow.start_time = time.time()
        self._stats["workflows_executed"] += 1

        context = initial_context or {}
        completed_tasks: Set[str] = set()
        failed_tasks: Set[str] = set()

        try:
            while len(completed_tasks) + len(failed_tasks) < len(workflow.tasks):
                ready_tasks = self._get_ready_tasks(
                    workflow.tasks, completed_tasks, failed_tasks, context
                )

                if not ready_tasks:
                    if failed_tasks and not self.config.continue_on_error:
                        break
                    break

                if self.config.enable_parallel:
                    parallel_tasks = [t for t in ready_tasks if t.task_type == TaskType.PARALLEL]
                    action_tasks = [t for t in ready_tasks if t.task_type != TaskType.PARALLEL]

                    for task in action_tasks:
                        result = await self._execute_task(task, context, workflow)
                        workflow.results[task.task_id] = result
                        if result.status == TaskStatus.COMPLETED:
                            completed_tasks.add(task.task_id)
                            self._stats["tasks_completed"] += 1
                        else:
                            failed_tasks.add(task.task_id)
                            self._stats["tasks_failed"] += 1

                    for task in parallel_tasks:
                        results = await self._execute_parallel(task, context, workflow)
                        for sub_task, result in zip(task.parallel_tasks, results):
                            workflow.results[sub_task.task_id] = result
                            if result.status == TaskStatus.COMPLETED:
                                completed_tasks.add(sub_task.task_id)
                            else:
                                failed_tasks.add(sub_task.task_id)
                else:
                    for task in ready_tasks:
                        result = await self._execute_task(task, context, workflow)
                        workflow.results[task.task_id] = result
                        if result.status == TaskStatus.COMPLETED:
                            completed_tasks.add(task.task_id)
                            self._stats["tasks_completed"] += 1
                        else:
                            failed_tasks.add(task.task_id)
                            self._stats["tasks_failed"] += 1
                            if not self.config.continue_on_error:
                                break

            workflow.status = TaskStatus.COMPLETED
            self._stats["workflows_completed"] += 1

            if self.config.enable_notifications:
                self._send_notifications(workflow, context)

        except Exception as e:
            logger.error(f"Workflow execution error: {e}")
            workflow.status = TaskStatus.FAILED

        workflow.end_time = time.time()
        return workflow

    def _get_ready_tasks(
        self,
        tasks: List[ConductorTask],
        completed: Set[str],
        failed: Set[str],
        context: Dict[str, Any],
    ) -> List[ConductorTask]:
        """Get tasks that are ready to execute."""
        ready = []
        for task in tasks:
            if task.task_id in completed or task.task_id in failed:
                continue

            if not all(dep in completed for dep in task.depends_on):
                continue

            if task.condition and not task.condition(context):
                completed.add(task.task_id)
                continue

            ready.append(task)

        return ready

    async def _execute_task(
        self,
        task: ConductorTask,
        context: Dict[str, Any],
        workflow: ConductorWorkflow,
    ) -> TaskResult:
        """Execute a single task."""
        result = TaskResult(task_id=task.task_id, status=TaskStatus.RUNNING)
        result.start_time = time.time()

        try:
            input_data = {
                key: context.get(value)
                for key, value in task.input_mapping.items()
            }

            if asyncio.iscoroutinefunction(task.handler):
                output = await asyncio.wait_for(
                    task.handler(input_data),
                    timeout=task.timeout_seconds,
                )
            else:
                output = task.handler(input_data)

            result.output = output
            result.status = TaskStatus.COMPLETED

            if task.output_key:
                context[task.output_key] = output

        except asyncio.TimeoutError:
            result.status = TaskStatus.FAILED
            result.error = f"Task timed out after {task.timeout_seconds}s"
        except Exception as e:
            result.status = TaskStatus.FAILED
            result.error = str(e)

        result.end_time = time.time()
        result.duration_ms = (result.end_time - result.start_time) * 1000
        return result

    async def _execute_parallel(
        self,
        task: ConductorTask,
        context: Dict[str, Any],
        workflow: ConductorWorkflow,
    ) -> List[TaskResult]:
        """Execute parallel tasks."""
        tasks = [
            self._execute_task(sub_task, context, workflow)
            for sub_task in task.parallel_tasks
        ]
        return await asyncio.gather(*tasks)

    def _send_notifications(
        self,
        workflow: ConductorWorkflow,
        context: Dict[str, Any],
    ) -> None:
        """Send workflow completion notifications."""
        for handler in self._notification_handlers:
            try:
                handler(workflow.name, context)
            except Exception as e:
                logger.error(f"Notification handler error: {e}")

    def register_notification(
        self,
        handler: Callable[[str, Dict], None],
    ) -> None:
        """Register a notification handler."""
        self._notification_handlers.append(handler)

    def get_workflow(self, workflow_id: str) -> Optional[ConductorWorkflow]:
        """Get a workflow by ID."""
        return self._workflows.get(workflow_id)

    def get_stats(self) -> Dict[str, Any]:
        """Get conductor statistics."""
        return {
            **self._stats.copy(),
            "total_workflows": len(self._workflows),
        }


async def demo_conductor():
    """Demonstrate conductor workflow."""
    config = ConductorConfig()
    conductor = AutomationConductorAction(config)

    workflow = conductor.create_workflow("demo-workflow")

    async def step1(ctx):
        await asyncio.sleep(0.05)
        return {"result": "step1 done"}

    async def step2(ctx):
        await asyncio.sleep(0.05)
        return {"result": "step2 done"}

    conductor.add_task(workflow, "step1", "First Step", TaskType.ACTION, handler=step1)
    conductor.add_task(workflow, "step2", "Second Step", TaskType.ACTION, handler=step2, depends_on=["step1"])

    result = await conductor.execute(workflow.workflow_id)

    print(f"Workflow status: {result.status.value}")
    print(f"Stats: {conductor.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_conductor())
