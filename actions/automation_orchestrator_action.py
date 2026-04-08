"""
Automation Orchestrator Action Module.

Provides workflow orchestration with dependency resolution,
parallel execution, error handling, and state management.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    CANCELLED = "cancelled"


class RetryPolicy(Enum):
    """Retry policy types."""
    NONE = "none"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


@dataclass
class TaskConfig:
    """Configuration for a task."""
    name: str
    handler: Callable
    dependencies: List[str] = field(default_factory=list)
    retry_policy: RetryPolicy = RetryPolicy.NONE
    max_retries: int = 3
    timeout: float = 300.0
    required: bool = True
    priority: int = 0
    retry_delay: float = 1.0


@dataclass
class TaskResult:
    """Result of a task execution."""
    task_name: str
    status: TaskStatus
    result: Any = None
    error: Optional[Exception] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    retry_count: int = 0

    @property
    def duration(self) -> Optional[float]:
        """Get task duration in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None


@dataclass
class WorkflowState:
    """State of the workflow execution."""
    tasks: Dict[str, TaskResult] = field(default_factory=dict)
    current_task: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    cancelled: bool = False

    @property
    def is_complete(self) -> bool:
        """Check if workflow is complete."""
        return any(
            t.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED)
            for t in self.tasks.values()
        ) and self._all_tasks_done()

    def _all_tasks_done(self) -> bool:
        """Check if all tasks have finished."""
        return all(
            t.status != TaskStatus.PENDING and t.status != TaskStatus.RUNNING
            for t in self.tasks.values()
        )


class DependencyGraph:
    """Manages task dependencies and execution order."""

    def __init__(self):
        self.tasks: Dict[str, TaskConfig] = {}
        self.graph: Dict[str, Set[str]] = defaultdict(set)
        self.reverse_graph: Dict[str, Set[str]] = defaultdict(set)

    def add_task(self, config: TaskConfig):
        """Add a task to the dependency graph."""
        self.tasks[config.name] = config
        self.graph[config.name] = set(config.dependencies)
        for dep in config.dependencies:
            self.reverse_graph[dep].add(config.name)

    def get_execution_order(self) -> List[List[str]]:
        """Get tasks grouped by execution level (parallelizable within each level)."""
        in_degree: Dict[str, int] = {
            name: len(deps) for name, deps in self.graph.items()
        }
        levels: List[List[str]] = []
        remaining = set(self.tasks.keys())
        completed = set()

        while remaining:
            current_level = [
                name for name in remaining
                if in_degree[name] == 0
            ]
            if not current_level:
                raise ValueError("Circular dependency detected")

            levels.append(current_level)
            for name in current_level:
                remaining.remove(name)
                completed.add(name)
                for dependent in self.reverse_graph[name]:
                    in_degree[dependent] -= 1

        return levels

    def get_ready_tasks(
        self,
        completed: Set[str],
        running: Set[str]
    ) -> List[str]:
        """Get tasks that are ready to execute."""
        ready = []
        for name, deps in self.graph.items():
            if name not in completed and name not in running:
                if deps.issubset(completed):
                    ready.append(name)
        return ready

    def validate(self) -> Tuple[bool, Optional[str]]:
        """Validate the dependency graph."""
        try:
            self.get_execution_order()
            return True, None
        except ValueError as e:
            return False, str(e)


class WorkflowOrchestrator:
    """Main orchestrator for workflow execution."""

    def __init__(self, max_parallel: int = 5):
        self.max_parallel = max_parallel
        self.dependency_graph = DependencyGraph()
        self.state = WorkflowState()
        self.task_handlers: Dict[str, Callable] = {}
        self.on_task_start: Optional[Callable] = None
        self.on_task_complete: Optional[Callable] = None
        self.on_workflow_complete: Optional[Callable] = None

    def register_task(self, config: TaskConfig):
        """Register a task with the orchestrator."""
        self.dependency_graph.add_task(config)
        self.task_handlers[config.name] = config.handler
        self.state.tasks[config.name] = TaskResult(
            task_name=config.name,
            status=TaskStatus.PENDING
        )

    def set_task_start_callback(self, callback: Callable):
        """Set callback for task start events."""
        self.on_task_start = callback

    def set_task_complete_callback(self, callback: Callable):
        """Set callback for task complete events."""
        self.on_task_complete = callback

    def set_workflow_complete_callback(self, callback: Callable):
        """Set callback for workflow completion."""
        self.on_workflow_complete = callback

    def cancel(self):
        """Cancel workflow execution."""
        self.state.cancelled = True

    async def _execute_task(
        self,
        task_name: str,
        task_config: TaskConfig,
        initial_context: Dict[str, Any]
    ) -> TaskResult:
        """Execute a single task with retry logic."""
        result = self.state.tasks[task_name]
        result.start_time = datetime.now()
        result.status = TaskStatus.RUNNING
        self.state.current_task = task_name

        if self.on_task_start:
            await asyncio.to_thread(self.on_task_start, task_name, initial_context)

        for attempt in range(task_config.max_retries + 1):
            try:
                if asyncio.iscoroutinefunction(task_config.handler):
                    task_result = await asyncio.wait_for(
                        task_config.handler(initial_context),
                        timeout=task_config.timeout
                    )
                else:
                    task_result = await asyncio.wait_for(
                        asyncio.to_thread(task_config.handler, initial_context),
                        timeout=task_config.timeout
                    )

                result.status = TaskStatus.COMPLETED
                result.result = task_result
                result.retry_count = attempt
                break

            except asyncio.TimeoutError:
                result.error = TimeoutError(f"Task {task_name} timed out")
                if attempt == task_config.max_retries:
                    result.status = TaskStatus.FAILED
                else:
                    await asyncio.sleep(task_config.retry_delay * (attempt + 1))

            except Exception as e:
                result.error = e
                if attempt == task_config.max_retries:
                    result.status = TaskStatus.FAILED
                else:
                    if task_config.retry_policy == RetryPolicy.EXPONENTIAL:
                        delay = task_config.retry_delay * (2 ** attempt)
                    elif task_config.retry_policy == RetryPolicy.LINEAR:
                        delay = task_config.retry_delay * (attempt + 1)
                    else:
                        delay = task_config.retry_delay
                    await asyncio.sleep(delay)

        result.end_time = datetime.now()

        if self.on_task_complete:
            await asyncio.to_thread(
                self.on_task_complete, task_name, result, initial_context
            )

        self.state.current_task = None
        return result

    async def execute(self, initial_context: Dict[str, Any] = None) -> WorkflowState:
        """Execute the workflow."""
        if initial_context is None:
            initial_context = {}

        is_valid, error = self.dependency_graph.validate()
        if not is_valid:
            raise ValueError(f"Invalid workflow: {error}")

        self.state.start_time = datetime.now()
        completed: Set[str] = set()
        running: Set[str] = set()

        try:
            while not self.state.is_complete and not self.state.cancelled:
                ready = self.dependency_graph.get_ready_tasks(completed, running)

                if not ready and len(running) == 0:
                    break

                tasks_to_start = ready[:self.max_parallel - len(running)]

                for task_name in tasks_to_start:
                    task_config = self.dependency_graph.tasks[task_name]
                    running.add(task_name)
                    asyncio.create_task(
                        self._execute_task(task_name, task_config, initial_context)
                    )

                await asyncio.sleep(0.01)

                for task_name in list(running):
                    if self.state.tasks[task_name].status in (
                        TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.SKIPPED
                    ):
                        if self.state.tasks[task_name].status == TaskStatus.COMPLETED:
                            completed.add(task_name)
                        running.remove(task_name)

        finally:
            self.state.end_time = datetime.now()

        if self.on_workflow_complete:
            await asyncio.to_thread(self.on_workflow_complete, self.state)

        return self.state

    def get_state(self) -> WorkflowState:
        """Get current workflow state."""
        return self.state

    def get_results(self) -> Dict[str, Any]:
        """Get results from completed tasks."""
        return {
            name: result.result
            for name, result in self.state.tasks.items()
            if result.status == TaskStatus.COMPLETED
        }


async def demo_task_1(context: Dict[str, Any]) -> Dict[str, Any]:
    """Demo task 1."""
    await asyncio.sleep(0.1)
    context["task1_done"] = True
    return {"task1": "completed"}

async def demo_task_2(context: Dict[str, Any]) -> Dict[str, Any]:
    """Demo task 2."""
    await asyncio.sleep(0.1)
    context["task2_done"] = True
    return {"task2": "completed"}

async def demo_task_3(context: Dict[str, Any]) -> Dict[str, Any]:
    """Demo task 3."""
    await asyncio.sleep(0.1)
    context["task3_done"] = True
    return {"task3": "completed"}


async def main():
    """Demonstrate workflow orchestration."""
    orchestrator = WorkflowOrchestrator(max_parallel=2)

    orchestrator.register_task(TaskConfig(
        name="task1",
        handler=demo_task_1
    ))

    orchestrator.register_task(TaskConfig(
        name="task2",
        handler=demo_task_2,
        dependencies=["task1"]
    ))

    orchestrator.register_task(TaskConfig(
        name="task3",
        handler=demo_task_3,
        dependencies=["task1"],
        retry_policy=RetryPolicy.EXPONENTIAL
    ))

    orchestrator.register_task(TaskConfig(
        name="task4",
        handler=demo_task_2,
        dependencies=["task2", "task3"]
    ))

    print("Execution order:", orchestrator.dependency_graph.get_execution_order())

    state = await orchestrator.execute({})
    print(f"Workflow complete: {state.end_time is not None}")
    print(f"Results: {orchestrator.get_results()}")


if __name__ == "__main__":
    asyncio.run(main())
