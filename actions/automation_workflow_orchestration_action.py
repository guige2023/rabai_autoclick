"""
Automation Workflow Orchestration Action Module.

Provides workflow orchestration capabilities including task scheduling,
dependency management, parallel execution, and workflow state persistence.

Author: RabAI Team
"""

from typing import Any, Callable, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum
import threading
import time
import uuid
from datetime import datetime, timedelta
from collections import defaultdict, deque


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class TaskType(Enum):
    """Task types."""
    ACTION = "action"
    CONDITIONAL = "conditional"
    PARALLEL = "parallel"
    LOOP = "loop"
    SUBWORKFLOW = "subworkflow"
    DELAY = "delay"
    NOTIFY = "notify"


@dataclass
class Task:
    """Represents a workflow task."""
    id: str
    name: str
    task_type: TaskType
    func: Optional[Callable] = None
    params: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    retry_count: int = 0
    max_retries: int = 3
    timeout: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # Runtime state
    status: TaskStatus = TaskStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


@dataclass
class Workflow:
    """Represents a workflow definition."""
    id: str
    name: str
    tasks: Dict[str, Task] = field(default_factory=dict)
    entry_task_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExecutionContext:
    """Context passed to tasks during execution."""
    workflow_id: str
    execution_id: str
    variables: Dict[str, Any] = field(default_factory=dict)
    shared_state: Dict[str, Any] = field(default_factory=dict)
    execution_history: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class WorkflowResult:
    """Result of workflow execution."""
    workflow_id: str
    execution_id: str
    status: TaskStatus
    completed_tasks: int
    failed_tasks: int
    duration_seconds: float
    results: Dict[str, Any]
    errors: Dict[str, str]
    started_at: datetime
    completed_at: datetime


class DAGScheduler:
    """
    Directed Acyclic Graph scheduler for task execution ordering.
    
    Example:
        scheduler = DAGScheduler()
        scheduler.add_task("A", dependencies=[])
        scheduler.add_task("B", dependencies=["A"])
        scheduler.add_task("C", dependencies=["A"])
        
        order = scheduler.get_execution_order()
    """
    
    def __init__(self):
        self.tasks: Dict[str, Task] = {}
        self.dependents: Dict[str, Set[str]] = defaultdict(set)
        self._lock = threading.RLock()
    
    def add_task(self, task: Task) -> "DAGScheduler":
        """Add a task to the scheduler."""
        with self._lock:
            self.tasks[task.id] = task
            for dep in task.dependencies:
                self.dependents[dep].add(task.id)
        return self
    
    def get_execution_order(self) -> List[List[str]]:
        """Get execution order as list of parallel batches."""
        with self._lock:
            in_degree = defaultdict(int)
            for task in self.tasks.values():
                in_degree[task.id] = len(task.dependencies)
            
            # Start with tasks that have no dependencies
            batches = []
            remaining = set(self.tasks.keys())
            
            while remaining:
                # Find tasks with zero in-degree
                batch = [t for t in remaining if in_degree[t] == 0]
                
                if not batch:
                    raise ValueError("Circular dependency detected")
                
                batches.append(batch)
                
                # Remove batch from remaining and update in-degrees
                for task_id in batch:
                    remaining.remove(task_id)
                    for dependent in self.dependents[task_id]:
                        in_degree[dependent] -= 1
            
            return batches
    
    def get_ready_tasks(self, completed: Set[str]) -> List[str]:
        """Get tasks that are ready to execute."""
        with self._lock:
            ready = []
            for task_id, task in self.tasks.items():
                if task.status != TaskStatus.PENDING:
                    continue
                if all(dep in completed for dep in task.dependencies):
                    ready.append(task_id)
            return ready
    
    def has_next(self, completed: Set[str]) -> bool:
        """Check if there are more tasks to execute."""
        return any(
            t.status == TaskStatus.PENDING
            for t in self.tasks.values()
            if all(dep in completed for dep in t.dependencies)
        )


class WorkflowEngine:
    """
    Workflow orchestration engine.
    
    Example:
        engine = WorkflowEngine()
        engine.load_workflow(workflow)
        
        result = engine.execute()
    """
    
    def __init__(self, max_parallel: int = 4):
        self.max_parallel = max_parallel
        self.workflow: Optional[Workflow] = None
        self.scheduler: Optional[DAGScheduler] = None
        self.execution_context: Optional[ExecutionContext] = None
        self._running = False
        self._lock = threading.RLock()
    
    def load_workflow(self, workflow: Workflow) -> "WorkflowEngine":
        """Load a workflow for execution."""
        with self._lock:
            self.workflow = workflow
            self.scheduler = DAGScheduler()
            for task in workflow.tasks.values():
                self.scheduler.add_task(task)
        return self
    
    def execute(
        self,
        variables: Optional[Dict[str, Any]] = None
    ) -> WorkflowResult:
        """Execute the loaded workflow."""
        if not self.workflow or not self.scheduler:
            raise ValueError("No workflow loaded")
        
        execution_id = str(uuid.uuid4())
        
        with self._lock:
            self._running = True
            self.execution_context = ExecutionContext(
                workflow_id=self.workflow.id,
                execution_id=execution_id,
                variables=variables or {}
            )
        
        started_at = datetime.now()
        completed_tasks = 0
        failed_tasks = 0
        results = {}
        errors = {}
        completed: Set[str] = set()
        
        try:
            # Get execution order
            execution_order = self.scheduler.get_execution_order()
            
            for batch in execution_order:
                if not self._running:
                    break
                
                # Execute batch in parallel
                batch_results = self._execute_batch(batch)
                
                for task_id, task_result in batch_results.items():
                    completed.add(task_id)
                    
                    if isinstance(task_result, Exception):
                        failed_tasks += 1
                        errors[task_id] = str(task_result)
                    else:
                        completed_tasks += 1
                        results[task_id] = task_result
                
                # Check for failures
                if failed_tasks > 0:
                    break
        
        finally:
            with self._lock:
                self._running = False
        
        completed_at = datetime.now()
        
        status = TaskStatus.COMPLETED
        if failed_tasks > 0:
            status = TaskStatus.FAILED
        elif not self._running:
            status = TaskStatus.CANCELLED
        
        return WorkflowResult(
            workflow_id=self.workflow.id,
            execution_id=execution_id,
            status=status,
            completed_tasks=completed_tasks,
            failed_tasks=failed_tasks,
            duration_seconds=(completed_at - started_at).total_seconds(),
            results=results,
            errors=errors,
            started_at=started_at,
            completed_at=completed_at
        )
    
    def _execute_batch(self, task_ids: List[str]) -> Dict[str, Any]:
        """Execute a batch of tasks."""
        results = {}
        threads = []
        results_dict: Dict[str, Any] = {}
        lock = threading.Lock()
        
        def execute_task(task_id: str):
            task = self.workflow.tasks[task_id]
            try:
                task.status = TaskStatus.RUNNING
                task.started_at = datetime.now()
                
                result = self._execute_task(task)
                
                task.status = TaskStatus.COMPLETED
                task.result = result
                task.completed_at = datetime.now()
                
                with lock:
                    results_dict[task_id] = result
                
                # Update context
                if self.execution_context:
                    self.execution_context.variables[task_id] = result
                    self.execution_context.execution_history.append({
                        "task_id": task_id,
                        "status": "completed",
                        "timestamp": datetime.now().isoformat()
                    })
                
            except Exception as e:
                task.status = TaskStatus.FAILED
                task.error = str(e)
                task.completed_at = datetime.now()
                
                with lock:
                    results_dict[task_id] = e
                
                if self.execution_context:
                    self.execution_context.execution_history.append({
                        "task_id": task_id,
                        "status": "failed",
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
        
        # Start threads
        for task_id in task_ids[:self.max_parallel]:
            t = threading.Thread(target=execute_task, args=(task_id,))
            t.start()
            threads.append(t)
        
        # Wait for completion
        for t in threads:
            t.join()
        
        return results_dict
    
    def _execute_task(self, task: Task) -> Any:
        """Execute a single task."""
        if task.task_type == TaskType.DELAY:
            delay = task.params.get("seconds", 0)
            time.sleep(delay)
            return {"delayed": delay}
        
        elif task.task_type == TaskType.CONDITIONAL:
            condition = task.params.get("condition")
            if callable(condition):
                if not condition(self.execution_context):
                    return {"skipped": True}
            return {"condition_met": True}
        
        elif task.task_type == TaskType.NOTIFY:
            message = task.params.get("message", "")
            return {"notified": message}
        
        elif task.func:
            return task.func(self.execution_context, task.params)
        
        return {"executed": task.name}
    
    def cancel(self):
        """Cancel workflow execution."""
        with self._lock:
            self._running = False


class BaseAction:
    """Base class for all actions."""
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Any:
        raise NotImplementedError


class AutomationWorkflowOrchestrationAction(BaseAction):
    """
    Workflow orchestration action for automation.
    
    Parameters:
        operation: Operation type (create/add_task/execute/cancel)
        workflow_id: Workflow identifier
        task_id: Task identifier
        tasks: List of task definitions
    
    Example:
        action = AutomationWorkflowOrchestrationAction()
        result = action.execute({}, {
            "operation": "execute",
            "workflow_id": "data_pipeline",
            "tasks": [{"id": "t1", "name": "Fetch"}]
        })
    """
    
    _workflows: Dict[str, Workflow] = {}
    _lock = threading.Lock()
    
    def execute(self, context: Dict[str, Any], params: Dict[str, Any]) -> Dict[str, Any]:
        """Execute workflow orchestration operation."""
        operation = params.get("operation", "create")
        workflow_id = params.get("workflow_id", "default")
        task_id = params.get("task_id")
        task_defs = params.get("tasks", [])
        max_parallel = params.get("max_parallel", 4)
        
        if operation == "create":
            workflow = Workflow(
                id=workflow_id,
                name=params.get("name", workflow_id)
            )
            
            for task_def in task_defs:
                task = Task(
                    id=task_def["id"],
                    name=task_def.get("name", task_def["id"]),
                    task_type=TaskType(task_def.get("type", "action")),
                    params=task_def.get("params", {}),
                    dependencies=task_def.get("dependencies", []),
                    max_retries=task_def.get("max_retries", 3)
                )
                workflow.tasks[task.id] = task
            
            if task_defs:
                workflow.entry_task_id = task_defs[0].get("id")
            
            with self._lock:
                self._workflows[workflow_id] = workflow
            
            return {
                "success": True,
                "operation": "create",
                "workflow_id": workflow_id,
                "task_count": len(task_defs),
                "created_at": workflow.created_at.isoformat()
            }
        
        elif operation == "execute":
            with self._lock:
                if workflow_id not in self._workflows:
                    return {"success": False, "error": f"Workflow '{workflow_id}' not found"}
                workflow = self._workflows[workflow_id]
            
            engine = WorkflowEngine(max_parallel=max_parallel)
            engine.load_workflow(workflow)
            
            result = engine.execute()
            
            return {
                "success": True,
                "operation": "execute",
                "workflow_id": workflow_id,
                "execution_id": result.execution_id,
                "status": result.status.value,
                "completed_tasks": result.completed_tasks,
                "failed_tasks": result.failed_tasks,
                "duration_seconds": result.duration_seconds,
                "started_at": result.started_at.isoformat(),
                "completed_at": result.completed_at.isoformat()
            }
        
        elif operation == "cancel":
            with self._lock:
                if workflow_id in self._workflows:
                    # Would need to store engine reference for cancellation
                    return {"success": True, "operation": "cancel", "workflow_id": workflow_id}
            return {"success": False, "error": "Workflow not found"}
        
        elif operation == "status":
            with self._lock:
                if workflow_id not in self._workflows:
                    return {"success": False, "error": "Workflow not found"}
                workflow = self._workflows[workflow_id]
            
            tasks_status = {
                task_id: task.status.value
                for task_id, task in workflow.tasks.items()
            }
            
            return {
                "success": True,
                "operation": "status",
                "workflow_id": workflow_id,
                "task_count": len(workflow.tasks),
                "tasks": tasks_status
            }
        
        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
