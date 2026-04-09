"""Workflow Orchestrator Action Module.

Orchestrates complex multi-step workflows with:
- Step dependency management
- Parallel execution
- Error handling and recovery
- State persistence
- Conditional branching

Author: rabai_autoclick team
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, Set

logger = logging.getLogger(__name__)


class StepStatus(Enum):
    """Workflow step execution status."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()
    CANCELLED = auto()


class StepType(Enum):
    """Types of workflow steps."""
    TASK = auto()
    PARALLEL = auto()
    BRANCH = auto()
    LOOP = auto()
    WAIT = auto()
    APPROVAL = auto()


@dataclass
class WorkflowStep:
    """A single step in a workflow."""
    id: str
    name: str
    step_type: StepType = StepType.TASK
    func: Optional[Callable] = None
    dependencies: List[str] = field(default_factory=list)
    condition: Optional[Callable[[Dict[str, Any]], bool]] = None
    timeout_seconds: float = 300.0
    retry_count: int = 0
    continue_on_failure: bool = False
    parallel_tasks: List["WorkflowStep"] = field(default_factory=list)
    branch_conditions: Dict[str, Callable] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    status: StepStatus = StepStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    attempts: int = 0


@dataclass
class WorkflowExecution:
    """Execution context for a workflow run."""
    id: str
    name: str
    status: StepStatus = StepStatus.PENDING
    steps: Dict[str, WorkflowStep] = field(default_factory=dict)
    state: Dict[str, Any] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    errors: List[Dict[str, Any]] = field(default_factory=list)
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class WorkflowMetrics:
    """Workflow execution metrics."""
    total_steps: int = 0
    completed_steps: int = 0
    failed_steps: int = 0
    skipped_steps: int = 0
    total_duration_ms: float = 0.0
    step_durations: Dict[str, float] = field(default_factory=dict)


class WorkflowOrchestrator:
    """Orchestrates complex workflows with dependencies and parallel execution.
    
    Features:
    - Dependency-based execution ordering
    - Parallel step execution
    - Conditional branching
    - Error handling with retry
    - State management
    - Execution tracking and metrics
    """
    
    def __init__(self, name: str, max_parallel: int = 10):
        self.name = name
        self.max_parallel = max_parallel
        self._steps: Dict[str, WorkflowStep] = {}
        self._execution_order: List[List[str]] = []
        self._running_tasks: Dict[str, asyncio.Task] = {}
        self._semaphore = asyncio.Semaphore(max_parallel)
        self._lock = asyncio.Lock()
    
    def add_step(self, step: WorkflowStep) -> "WorkflowOrchestrator":
        """Add a step to the workflow.
        
        Args:
            step: Workflow step to add
            
        Returns:
            Self for chaining
        """
        self._steps[step.id] = step
        return self
    
    def add_task(
        self,
        step_id: str,
        name: str,
        func: Callable,
        dependencies: Optional[List[str]] = None,
        **kwargs
    ) -> "WorkflowOrchestrator":
        """Add a task step.
        
        Args:
            step_id: Unique step identifier
            name: Step name
            func: Async function to execute
            dependencies: List of step IDs this depends on
            **kwargs: Additional step configuration
            
        Returns:
            Self for chaining
        """
        step = WorkflowStep(
            id=step_id,
            name=name,
            step_type=StepType.TASK,
            func=func,
            dependencies=dependencies or [],
            **kwargs
        )
        return self.add_step(step)
    
    def add_parallel(
        self,
        step_id: str,
        name: str,
        tasks: List[WorkflowStep],
        dependencies: Optional[List[str]] = None,
        **kwargs
    ) -> "WorkflowOrchestrator":
        """Add parallel execution step.
        
        Args:
            step_id: Unique step identifier
            name: Step name
            tasks: List of steps to run in parallel
            dependencies: List of step IDs this depends on
            **kwargs: Additional step configuration
            
        Returns:
            Self for chaining
        """
        step = WorkflowStep(
            id=step_id,
            name=name,
            step_type=StepType.PARALLEL,
            parallel_tasks=tasks,
            dependencies=dependencies or [],
            **kwargs
        )
        return self.add_step(step)
    
    def add_branch(
        self,
        step_id: str,
        name: str,
        conditions: Dict[str, Callable],
        default_branch: Optional[str] = None,
        dependencies: Optional[List[str]] = None,
        **kwargs
    ) -> "WorkflowOrchestrator":
        """Add conditional branch step.
        
        Args:
            step_id: Unique step identifier
            name: Step name
            conditions: Dict mapping branch name to condition function
            default_branch: Default branch if no conditions match
            dependencies: List of step IDs this depends on
            **kwargs: Additional step configuration
            
        Returns:
            Self for chaining
        """
        step = WorkflowStep(
            id=step_id,
            name=name,
            step_type=StepType.BRANCH,
            branch_conditions=conditions,
            dependencies=dependencies or [],
            **kwargs
        )
        return self.add_step(step)
    
    def build(self) -> None:
        """Build the execution plan by computing dependency order."""
        self._execution_order = self._compute_execution_order()
    
    def _compute_execution_order(self) -> List[List[str]]:
        """Compute the execution order based on dependencies.
        
        Returns:
            List of step ID batches (each batch can run in parallel)
        """
        in_degree = {sid: 0 for sid in self._steps}
        adjacency = defaultdict(list)
        
        for step_id, step in self._steps.items():
            for dep in step.dependencies:
                if dep not in self._steps:
                    raise ValueError(f"Step {step_id} depends on unknown step {dep}")
                adjacency[dep].append(step_id)
                in_degree[step_id] += 1
        
        order = []
        queue = deque([sid for sid, degree in in_degree.items() if degree == 0])
        
        while queue:
            batch = list(queue)
            queue.clear()
            
            order.append(batch)
            
            for step_id in batch:
                for next_step in adjacency[step_id]:
                    in_degree[next_step] -= 1
                    if in_degree[next_step] == 0:
                        queue.append(next_step)
        
        if sum(in_degree.values()) > 0:
            raise ValueError("Circular dependency detected in workflow")
        
        return order
    
    async def execute(
        self,
        initial_state: Optional[Dict[str, Any]] = None,
        context: Optional[Dict[str, Any]] = None
    ) -> WorkflowExecution:
        """Execute the workflow.
        
        Args:
            initial_state: Initial workflow state
            context: Execution context
            
        Returns:
            Workflow execution result
        """
        execution = WorkflowExecution(
            id=f"wf_{int(time.time() * 1000000)}",
            name=self.name,
            state=initial_state or {},
        )
        execution.steps = {sid: self._steps[sid] for sid in self._steps}
        
        execution.started_at = datetime.now().isoformat()
        
        if not self._execution_order:
            self.build()
        
        try:
            for batch in self._execution_order:
                await self._execute_batch(batch, execution, context)
                
                if execution.status == StepStatus.FAILED:
                    break
        
        except Exception as e:
            execution.status = StepStatus.FAILED
            execution.errors.append({
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })
        
        execution.completed_at = datetime.now().isoformat()
        
        return execution
    
    async def _execute_batch(
        self,
        step_ids: List[str],
        execution: WorkflowExecution,
        context: Optional[Dict[str, Any]]
    ) -> None:
        """Execute a batch of steps.
        
        Args:
            step_ids: Step IDs to execute
            execution: Execution context
            context: Execution context
        """
        tasks = []
        
        for step_id in step_ids:
            step = execution.steps[step_id]
            
            if step.condition and not step.condition(execution.state):
                step.status = StepStatus.SKIPPED
                continue
            
            if not self._all_dependencies_met(step, execution):
                step.status = StepStatus.PENDING
                continue
            
            if step.step_type == StepType.TASK:
                task = asyncio.create_task(self._execute_task(step, execution, context))
                tasks.append(task)
            elif step.step_type == StepType.PARALLEL:
                task = asyncio.create_task(self._execute_parallel(step, execution, context))
                tasks.append(task)
            elif step.step_type == StepType.BRANCH:
                task = asyncio.create_task(self._execute_branch(step, execution, context))
                tasks.append(task)
        
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    step_id = step_ids[i]
                    execution.errors.append({
                        "step_id": step_id,
                        "error": str(result),
                        "timestamp": datetime.now().isoformat()
                    })
    
    async def _execute_task(
        self,
        step: WorkflowStep,
        execution: WorkflowExecution,
        context: Optional[Dict[str, Any]]
    ) -> None:
        """Execute a single task step.
        
        Args:
            step: Step to execute
            execution: Execution context
            context: Execution context
        """
        step.status = StepStatus.RUNNING
        step.start_time = time.time()
        step.attempts += 1
        
        try:
            async with self._semaphore:
                if asyncio.iscoroutinefunction(step.func):
                    result = await asyncio.wait_for(
                        step.func(execution.state, execution.outputs, context),
                        timeout=step.timeout_seconds
                    )
                else:
                    result = step.func(execution.state, execution.outputs, context)
            
            step.result = result
            step.status = StepStatus.COMPLETED
            execution.outputs[step.id] = result
            
        except asyncio.TimeoutError:
            step.status = StepStatus.FAILED
            step.error = f"Timeout after {step.timeout_seconds}s"
            
            if step.continue_on_failure:
                step.status = StepStatus.SKIPPED
            else:
                execution.status = StepStatus.FAILED
        
        except Exception as e:
            step.error = str(e)
            
            if step.retry_count > 0 and step.attempts <= step.retry_count:
                step.status = StepStatus.PENDING
                await asyncio.sleep(1)
            else:
                step.status = StepStatus.FAILED
                
                if step.continue_on_failure:
                    step.status = StepStatus.SKIPPED
                else:
                    execution.status = StepStatus.FAILED
        
        step.end_time = time.time()
    
    async def _execute_parallel(
        self,
        step: WorkflowStep,
        execution: WorkflowExecution,
        context: Optional[Dict[str, Any]]
    ) -> None:
        """Execute parallel tasks.
        
        Args:
            step: Parallel step to execute
            execution: Execution context
            context: Execution context
        """
        step.status = StepStatus.RUNNING
        step.start_time = time.time()
        
        results = []
        
        for task in step.parallel_tasks:
            result = await task.func(execution.state, execution.outputs, context)
            results.append(result)
        
        step.result = results
        step.status = StepStatus.COMPLETED
        execution.outputs[step.id] = results
        step.end_time = time.time()
    
    async def _execute_branch(
        self,
        step: WorkflowStep,
        execution: WorkflowExecution,
        context: Optional[Dict[str, Any]]
    ) -> None:
        """Execute conditional branch.
        
        Args:
            step: Branch step to execute
            execution: Execution context
            context: Execution context
        """
        step.status = StepStatus.RUNNING
        step.start_time = time.time()
        
        selected_branch = None
        
        for branch_name, condition in step.branch_conditions.items():
            if condition(execution.state):
                selected_branch = branch_name
                break
        
        step.result = {"branch": selected_branch}
        step.status = StepStatus.COMPLETED
        execution.outputs[step.id] = step.result
        step.end_time = time.time()
    
    def _all_dependencies_met(self, step: WorkflowStep, execution: WorkflowExecution) -> bool:
        """Check if all dependencies are met.
        
        Args:
            step: Step to check
            execution: Execution context
            
        Returns:
            True if all dependencies are completed
        """
        for dep_id in step.dependencies:
            dep_step = execution.steps.get(dep_id)
            if not dep_step or dep_step.status != StepStatus.COMPLETED:
                return False
        return True
    
    def get_metrics(self, execution: WorkflowExecution) -> WorkflowMetrics:
        """Get workflow execution metrics.
        
        Args:
            execution: Execution to measure
            
        Returns:
            Workflow metrics
        """
        metrics = WorkflowMetrics(total_steps=len(execution.steps))
        
        for step in execution.steps.values():
            if step.end_time and step.start_time:
                duration = (step.end_time - step.start_time) * 1000
                metrics.step_durations[step.id] = duration
                metrics.total_duration_ms += duration
            
            if step.status == StepStatus.COMPLETED:
                metrics.completed_steps += 1
            elif step.status == StepStatus.FAILED:
                metrics.failed_steps += 1
            elif step.status == StepStatus.SKIPPED:
                metrics.skipped_steps += 1
        
        return metrics
