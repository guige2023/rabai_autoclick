"""
Workflow Orchestrator Action.

Provides workflow orchestration with support for:
- DAG-based workflow definition
- Parallel task execution
- Conditional branching
- Error handling and retry
- Workflow monitoring
"""

from typing import Dict, List, Optional, Any, Callable, Awaitable, Set
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import threading
import logging
import json
import uuid
from collections import defaultdict, deque

logger = logging.getLogger(__name__)


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class WorkflowStatus(Enum):
    """Workflow execution status."""
    CREATED = "created"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


@dataclass
class TaskResult:
    """Result of a task execution."""
    task_id: str
    status: TaskStatus
    output: Any = None
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration_ms(self) -> Optional[float]:
        """Get task duration in milliseconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds() * 1000
        return None


@dataclass
class WorkflowDAG:
    """Directed Acyclic Graph for workflow."""
    nodes: Dict[str, "TaskDefinition"] = field(default_factory=dict)
    edges: Dict[str, List[str]] = field(default_factory=dict)  # node -> [downstream nodes]
    in_degree: Dict[str, int] = field(default_factory=dict)
    
    def add_task(
        self,
        task_id: str,
        handler: Callable[["WorkflowContext"], Awaitable[Any]],
        deps: Optional[List[str]] = None,
        max_retries: int = 0,
        timeout: Optional[float] = None
    ) -> "WorkflowDAG":
        """Add a task to the DAG."""
        task_def = TaskDefinition(
            task_id=task_id,
            handler=handler,
            deps=deps or [],
            max_retries=max_retries,
            timeout=timeout
        )
        self.nodes[task_id] = task_def
        
        # Update edges and in-degree
        if task_id not in self.edges:
            self.edges[task_id] = []
        if task_id not in self.in_degree:
            self.in_degree[task_id] = 0
        
        for dep in (deps or []):
            if dep not in self.edges:
                self.edges[dep] = []
            self.edges[dep].append(task_id)
            self.in_degree[task_id] = self.in_degree.get(task_id, 0) + 1
        
        return self
    
    def topological_sort(self) -> List[str]:
        """Get topological order of tasks."""
        in_degree_copy = self.in_degree.copy()
        queue = deque([n for n in self.nodes if in_degree_copy.get(n, 0) == 0])
        result = []
        
        while queue:
            node = queue.popleft()
            result.append(node)
            
            for downstream in self.edges.get(node, []):
                in_degree_copy[downstream] -= 1
                if in_degree_copy[downstream] == 0:
                    queue.append(downstream)
        
        if len(result) != len(self.nodes):
            raise ValueError("Cycle detected in workflow DAG")
        
        return result
    
    def get_ready_tasks(self, completed: Set[str]) -> List[str]:
        """Get tasks that are ready to execute."""
        ready = []
        for task_id in self.nodes:
            if task_id in completed:
                continue
            if self.in_degree.get(task_id, 0) <= len([d for d in (self.nodes[task_id].deps or []) if d in completed]):
                ready.append(task_id)
        return ready
    
    def get_execution_levels(self) -> List[List[str]]:
        """Get tasks grouped by execution level (parallel batches)."""
        levels = []
        completed: Set[str] = set()
        remaining = set(self.nodes.keys())
        
        while remaining:
            level = []
            for task_id in remaining:
                deps = self.nodes[task_id].deps or []
                if all(d in completed for d in deps):
                    level.append(task_id)
            
            if not level:
                raise ValueError("Cycle detected in workflow DAG")
            
            levels.append(level)
            completed.update(level)
            remaining -= set(level)
        
        return levels


@dataclass
class TaskDefinition:
    """Definition of a task in the workflow."""
    task_id: str
    handler: Callable[["WorkflowContext"], Awaitable[Any]]
    deps: List[str] = field(default_factory=list)
    max_retries: int = 0
    timeout: Optional[float] = None
    condition: Optional[Callable[["WorkflowContext"], bool]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass 
class WorkflowContext:
    """Context passed through workflow execution."""
    workflow_id: str
    task_results: Dict[str, TaskResult] = field(default_factory=dict)
    shared_data: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_result(self, task_id: str) -> Optional[TaskResult]:
        """Get result of a specific task."""
        return self.task_results.get(task_id)
    
    def get_output(self, task_id: str) -> Any:
        """Get output of a specific task."""
        result = self.task_results.get(task_id)
        return result.output if result else None


class WorkflowOrchestratorAction:
    """
    Workflow Orchestrator Action.
    
    Provides workflow orchestration with support for:
    - DAG-based task dependencies
    - Parallel task execution
    - Conditional execution
    - Retry logic
    - Progress monitoring
    """
    
    def __init__(self, name: str):
        """
        Initialize the Workflow Orchestrator Action.
        
        Args:
            name: Workflow name
        """
        self.name = name
        self.dag = WorkflowDAG()
        self.status = WorkflowStatus.CREATED
        self.workflow_id = str(uuid.uuid4())
        self.context: Optional[WorkflowContext] = None
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self._cancel_event = asyncio.Event()
        self._pause_event = asyncio.Event()
        self._pause_event.set()
        self._tasks: Dict[str, asyncio.Task] = {}
        self._lock = threading.RLock()
    
    def add_task(
        self,
        task_id: str,
        handler: Callable[[WorkflowContext], Awaitable[Any]],
        deps: Optional[List[str]] = None,
        max_retries: int = 0,
        timeout: Optional[float] = None,
        condition: Optional[Callable[[WorkflowContext], bool]] = None
    ) -> "WorkflowOrchestratorAction":
        """
        Add a task to the workflow.
        
        Args:
            task_id: Unique task identifier
            handler: Async function to execute
            deps: List of task IDs that must complete first
            max_retries: Maximum retry attempts
            timeout: Task timeout in seconds
            condition: Optional condition function
        
        Returns:
            Self for chaining
        """
        self.dag.add_task(
            task_id=task_id,
            handler=handler,
            deps=deps,
            max_retries=max_retries,
            timeout=timeout
        )
        return self
    
    async def execute(
        self,
        initial_data: Optional[Dict[str, Any]] = None
    ) -> WorkflowContext:
        """
        Execute the workflow.
        
        Args:
            initial_data: Initial data to pass to workflow
        
        Returns:
            WorkflowContext with all task results
        """
        if self.status == WorkflowStatus.RUNNING:
            raise RuntimeError("Workflow is already running")
        
        self.status = WorkflowStatus.RUNNING
        self.start_time = datetime.utcnow()
        self.workflow_id = str(uuid.uuid4())
        self.context = WorkflowContext(
            workflow_id=self.workflow_id,
            shared_data=initial_data or {}
        )
        self._cancel_event.clear()
        self._pause_event.set()
        
        logger.info(f"Starting workflow: {self.name} ({self.workflow_id})")
        
        try:
            await self._execute_dag()
            
            if self._cancel_event.is_set():
                self.status = WorkflowStatus.CANCELLED
            else:
                self.status = WorkflowStatus.COMPLETED
            
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            self.status = WorkflowStatus.FAILED
            self.context.metadata["error"] = str(e)
        
        finally:
            self.end_time = datetime.utcnow()
            logger.info(f"Workflow finished: {self.name} ({self.status.value})")
        
        return self.context
    
    async def _execute_dag(self) -> None:
        """Execute DAG with parallel task execution."""
        execution_levels = self.dag.get_execution_levels()
        completed: Set[str] = set()
        
        for level in execution_levels:
            if self._cancel_event.is_set():
                break
            
            await self._pause_event.wait()
            
            # Execute tasks in this level in parallel
            level_tasks = []
            for task_id in level:
                task_def = self.dag.nodes[task_id]
                
                # Check condition
                if task_def.condition and not task_def.condition(self.context):
                    self.context.task_results[task_id] = TaskResult(
                        task_id=task_id,
                        status=TaskStatus.SKIPPED,
                        output=None,
                        metadata={"reason": "condition_not_met"}
                    )
                    completed.add(task_id)
                    continue
                
                # Execute task
                level_tasks.append(
                    self._execute_task(task_id, task_def)
                )
            
            # Wait for all level tasks to complete
            if level_tasks:
                results = await asyncio.gather(*level_tasks, return_exceptions=True)
                for task_id, result in zip(level, results):
                    if isinstance(result, Exception):
                        logger.error(f"Task {task_id} raised exception: {result}")
                    completed.add(task_id)
    
    async def _execute_task(
        self,
        task_id: str,
        task_def: TaskDefinition
    ) -> TaskResult:
        """Execute a single task with retry logic."""
        result = TaskResult(
            task_id=task_id,
            status=TaskStatus.RUNNING,
            start_time=datetime.utcnow()
        )
        
        for attempt in range(task_def.max_retries + 1):
            if self._cancel_event.is_set():
                result.status = TaskStatus.CANCELLED
                break
            
            try:
                if task_def.timeout:
                    output = await asyncio.wait_for(
                        task_def.handler(self.context),
                        timeout=task_def.timeout
                    )
                else:
                    output = await task_def.handler(self.context)
                
                result.status = TaskStatus.COMPLETED
                result.output = output
                break
            
            except asyncio.TimeoutError:
                result.error = f"Task timed out after {task_def.timeout}s"
                result.status = TaskStatus.FAILED
            
            except Exception as e:
                result.error = str(e)
                
                if attempt < task_def.max_retries:
                    result.status = TaskStatus.RETRYING
                    result.retry_count = attempt + 1
                    logger.warning(
                        f"Task {task_id} failed, retrying "
                        f"({attempt + 1}/{task_def.max_retries}): {e}"
                    )
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    result.status = TaskStatus.FAILED
        
        result.end_time = datetime.utcnow()
        self.context.task_results[task_id] = result
        
        logger.debug(
            f"Task {task_id} {result.status.value} "
            f"({result.duration_ms:.0f}ms)"
        )
        
        return result
    
    def cancel(self) -> None:
        """Cancel workflow execution."""
        self._cancel_event.set()
        logger.info(f"Workflow '{self.name}' cancellation requested")
    
    def pause(self) -> None:
        """Pause workflow execution."""
        self._pause_event.clear()
        self.status = WorkflowStatus.PAUSED
        logger.info(f"Workflow '{self.name}' paused")
    
    def resume(self) -> None:
        """Resume a paused workflow."""
        self._pause_event.set()
        self.status = WorkflowStatus.RUNNING
        logger.info(f"Workflow '{self.name}' resumed")
    
    def get_progress(self) -> Dict[str, Any]:
        """Get workflow progress."""
        if not self.context:
            return {"status": "not_started"}
        
        total = len(self.dag.nodes)
        completed = sum(
            1 for r in self.context.task_results.values()
            if r.status in (TaskStatus.COMPLETED, TaskStatus.SKIPPED)
        )
        failed = sum(
            1 for r in self.context.task_results.values()
            if r.status == TaskStatus.FAILED
        )
        
        return {
            "workflow_id": self.workflow_id,
            "status": self.status.value,
            "total_tasks": total,
            "completed_tasks": completed,
            "failed_tasks": failed,
            "progress_percent": (completed / total * 100) if total > 0 else 0,
            "task_statuses": {
                task_id: r.status.value
                for task_id, r in self.context.task_results.items()
            }
        }
    
    def get_summary(self) -> Dict[str, Any]:
        """Get workflow execution summary."""
        progress = self.get_progress()
        
        duration_ms = None
        if self.start_time:
            end = self.end_time or datetime.utcnow()
            duration_ms = (end - self.start_time).total_seconds() * 1000
        
        return {
            "name": self.name,
            "workflow_id": self.workflow_id,
            "status": self.status.value,
            "duration_ms": duration_ms,
            "progress_percent": progress.get("progress_percent", 0),
            "task_count": progress.get("total_tasks", 0),
            "completed_count": progress.get("completed_tasks", 0),
            "failed_count": progress.get("failed_tasks", 0)
        }


# Example task handlers
async def fetch_data(ctx: WorkflowContext) -> Dict[str, Any]:
    """Fetch data task."""
    await asyncio.sleep(0.1)
    return {"users": [{"id": 1}, {"id": 2}], "count": 2}


async def process_data(ctx: WorkflowContext) -> int:
    """Process data task."""
    users = ctx.get_output("fetch_data")
    await asyncio.sleep(0.1)
    return len(users.get("users", []))


async def save_results(ctx: WorkflowContext) -> bool:
    """Save results task."""
    count = ctx.get_output("process_data")
    await asyncio.sleep(0.1)
    return True


# Standalone execution
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    async def main():
        # Create workflow
        workflow = WorkflowOrchestratorAction("data-processing")
        
        # Add tasks
        workflow.add_task("fetch_data", fetch_data)
        workflow.add_task("process_data", process_data, deps=["fetch_data"])
        workflow.add_task("save_results", save_results, deps=["process_data"])
        
        # Execute
        result = await workflow.execute({"source": "database"})
        
        print(f"Workflow status: {workflow.status.value}")
        print(f"Summary: {json.dumps(workflow.get_summary(), indent=2, default=str)}")
        print(f"Progress: {json.dumps(workflow.get_progress(), indent=2)}")
        
        for task_id, task_result in result.task_results.items():
            print(f"  {task_id}: {task_result.status.value} ({task_result.duration_ms:.0f}ms)")
    
    asyncio.run(main())
