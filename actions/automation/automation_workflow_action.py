"""Automation Workflow Action Module.

Provides workflow orchestration capabilities for automation tasks,
including step management, conditional branching, and error handling.

Example:
    >>> from actions.automation.automation_workflow_action import AutomationWorkflowAction
    >>> workflow = AutomationWorkflowAction()
    >>> workflow.add_step("fetch", fetch_data)
    >>> workflow.add_step("process", process_data, depends_on=["fetch"])
    >>> result = await workflow.execute()
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple


class WorkflowStatus(Enum):
    """Status of a workflow execution."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class StepStatus(Enum):
    """Status of a workflow step."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class WorkflowStep:
    """A single step in a workflow.
    
    Attributes:
        name: Step name
        func: Step function
        depends_on: List of step names this step depends on
        condition: Optional condition for step execution
        retry_count: Number of retries on failure
        timeout: Step timeout in seconds
        on_failure: Action to take on failure (continue/abort/stop)
    """
    name: str
    func: Callable[..., Any]
    depends_on: List[str] = field(default_factory=list)
    condition: Optional[Callable[[Dict], bool]] = None
    retry_count: int = 0
    timeout: float = 60.0
    on_failure: str = "abort"  # continue, abort, stop


@dataclass
class StepResult:
    """Result of a step execution.
    
    Attributes:
        step_name: Name of the executed step
        status: Step status
        result: Step result if successful
        error: Error if failed
        duration: Execution duration in seconds
        attempts: Number of execution attempts
    """
    step_name: str
    status: StepStatus
    result: Any = None
    error: Optional[str] = None
    duration: float = 0.0
    attempts: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


@dataclass
class WorkflowResult:
    """Result of a workflow execution.
    
    Attributes:
        workflow_id: Unique workflow identifier
        status: Final workflow status
        step_results: Results of all steps
        outputs: Workflow outputs
        total_duration: Total execution duration
        error: Error message if failed
    """
    workflow_id: str
    status: WorkflowStatus
    step_results: Dict[str, StepResult] = field(default_factory=dict)
    outputs: Dict[str, Any] = field(default_factory=dict)
    total_duration: float = 0.0
    error: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None


class AutomationWorkflowAction:
    """Handles workflow orchestration for automation tasks.
    
    Provides step-based workflow execution with dependency management,
    conditional execution, and comprehensive error handling.
    
    Attributes:
        steps: Dictionary of workflow steps
        outputs: Workflow outputs storage
    
    Example:
        >>> workflow = AutomationWorkflowAction()
        >>> workflow.add_step("init", initialize)
        >>> workflow.add_step("process", process, depends_on=["init"])
        >>> result = await workflow.execute()
    """
    
    def __init__(self, name: str = "workflow"):
        """Initialize the workflow action.
        
        Args:
            name: Workflow name
        """
        self.name = name
        self._steps: Dict[str, WorkflowStep] = {}
        self._step_order: List[str] = []
        self._outputs: Dict[str, Any] = {}
        self._workflow_counter = 0
        self._lock = threading.RLock()
    
    def _generate_workflow_id(self) -> str:
        """Generate a unique workflow ID.
        
        Returns:
            Unique workflow identifier
        """
        with self._lock:
            self._workflow_counter += 1
            return f"workflow_{self._workflow_counter}_{int(time.time() * 1000)}"
    
    def add_step(
        self,
        name: str,
        func: Callable[..., Any],
        depends_on: Optional[List[str]] = None,
        condition: Optional[Callable[[Dict], bool]] = None,
        retry_count: int = 0,
        timeout: float = 60.0,
        on_failure: str = "abort"
    ) -> "AutomationWorkflowAction":
        """Add a step to the workflow.
        
        Args:
            name: Step name (must be unique)
            func: Step function
            depends_on: List of step names this step depends on
            condition: Optional condition function for conditional execution
            retry_count: Number of retries on failure
            timeout: Step timeout in seconds
            on_failure: Action on failure (continue/abort/stop)
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            if name in self._steps:
                raise ValueError(f"Step '{name}' already exists")
            
            self._steps[name] = WorkflowStep(
                name=name,
                func=func,
                depends_on=depends_on or [],
                condition=condition,
                retry_count=retry_count,
                timeout=timeout,
                on_failure=on_failure
            )
            self._step_order.append(name)
            return self
    
    def add_parallel_steps(
        self,
        steps: List[Tuple[str, Callable[..., Any]]],
        name_prefix: str = "parallel"
    ) -> "AutomationWorkflowAction":
        """Add multiple steps that can run in parallel.
        
        Args:
            steps: List of (name, function) tuples
            name_prefix: Prefix for parallel step names
        
        Returns:
            Self for method chaining
        """
        for i, (name, func) in enumerate(steps):
            self.add_step(f"{name_prefix}_{i}", func)
        return self
    
    def add_output(self, name: str, value: Any) -> "AutomationWorkflowAction":
        """Add a static output to the workflow.
        
        Args:
            name: Output name
            value: Output value
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._outputs[name] = value
            return self
    
    async def execute(
        self,
        initial_data: Optional[Dict[str, Any]] = None,
        workflow_id: Optional[str] = None
    ) -> WorkflowResult:
        """Execute the workflow.
        
        Args:
            initial_data: Initial data to pass to the workflow
            workflow_id: Optional workflow identifier
        
        Returns:
            WorkflowResult with execution results
        """
        workflow_id = workflow_id or self._generate_workflow_id()
        start_time = datetime.now()
        
        result = WorkflowResult(
            workflow_id=workflow_id,
            status=WorkflowStatus.RUNNING,
            start_time=start_time
        )
        
        context = initial_data or {}
        context["_workflow_id"] = workflow_id
        
        try:
            # Validate dependencies
            if not self._validate_dependencies():
                result.status = WorkflowStatus.FAILED
                result.error = "Invalid step dependencies (circular or missing)"
                return result
            
            # Execute steps in dependency order
            completed: Set[str] = set()
            pending = set(self._steps.keys())
            
            while pending:
                # Find steps ready to execute
                ready = self._get_ready_steps(pending, completed, context)
                
                if not ready:
                    if pending:
                        result.status = WorkflowStatus.FAILED
                        result.error = "No steps ready to execute (dependency issue)"
                        return result
                    break
                
                # Execute ready steps
                step_results = await self._execute_steps(list(ready), context)
                
                for step_name, step_result in step_results.items():
                    result.step_results[step_name] = step_result
                    completed.add(step_name)
                    pending.discard(step_name)
                    
                    if step_result.status == StepStatus.COMPLETED:
                        context[f"{step_name}_result"] = step_result.result
                    elif step_result.status == StepStatus.FAILED:
                        step = self._steps[step_name]
                        if step.on_failure == "stop":
                            result.status = WorkflowStatus.FAILED
                            result.error = f"Step '{step_name}' failed: {step_result.error}"
                            return result
                        elif step.on_failure == "abort":
                            pending = pending - completed
                            result.status = WorkflowStatus.FAILED
                            result.error = f"Step '{step_name}' failed: {step_result.error}"
                            return result
            
            result.status = WorkflowStatus.COMPLETED
            result.outputs = {k: v for k, v in context.items() if not k.startswith("_")}
            
        except Exception as e:
            result.status = WorkflowStatus.FAILED
            result.error = f"Workflow execution error: {str(e)}"
        
        result.end_time = datetime.now()
        result.total_duration = (result.end_time - start_time).total_seconds()
        
        return result
    
    def _validate_dependencies(self) -> bool:
        """Validate step dependencies.
        
        Returns:
            True if dependencies are valid
        """
        for name, step in self._steps.items():
            for dep in step.depends_on:
                if dep not in self._steps:
                    return False
                if dep == name:  # Self-dependency
                    return False
        
        # Check for circular dependencies
        try:
            self._topological_sort()
            return True
        except ValueError:
            return False
    
    def _topological_sort(self) -> List[str]:
        """Perform topological sort of steps.
        
        Returns:
            Sorted list of step names
        
        Raises:
            ValueError: If circular dependency detected
        """
        visited: Set[str] = set()
        temp: Set[str] = set()
        result: List[str] = []
        
        def visit(name: str):
            if name in temp:
                raise ValueError("Circular dependency detected")
            if name in visited:
                return
            
            temp.add(name)
            
            for dep in self._steps[name].depends_on:
                visit(dep)
            
            temp.remove(name)
            visited.add(name)
            result.append(name)
        
        for name in self._steps:
            if name not in visited:
                visit(name)
        
        return result
    
    def _get_ready_steps(
        self,
        pending: Set[str],
        completed: Set[str],
        context: Dict[str, Any]
    ) -> Set[str]:
        """Get steps that are ready to execute.
        
        Args:
            pending: Set of pending step names
            completed: Set of completed step names
            context: Workflow context
        
        Returns:
            Set of step names ready to execute
        """
        ready: Set[str] = set()
        
        for name in pending:
            step = self._steps[name]
            
            # Check if all dependencies are completed
            if not all(dep in completed for dep in step.depends_on):
                continue
            
            # Check condition if present
            if step.condition is not None:
                try:
                    if not step.condition(context):
                        # Condition not met, skip this step
                        continue
                except Exception:
                    continue
            
            ready.add(name)
        
        return ready
    
    async def _execute_steps(
        self,
        step_names: List[str],
        context: Dict[str, Any]
    ) -> Dict[str, StepResult]:
        """Execute multiple steps (potentially in parallel).
        
        Args:
            step_names: List of step names to execute
            context: Workflow context
        
        Returns:
            Dictionary of step results
        """
        results: Dict[str, StepResult] = {}
        
        # For now, execute sequentially
        for name in step_names:
            result = await self._execute_step(name, context)
            results[name] = result
            
            # Stop on failure if on_failure is stop
            if result.status == StepStatus.FAILED and self._steps[name].on_failure == "stop":
                break
        
        return results
    
    async def _execute_step(
        self,
        name: str,
        context: Dict[str, Any]
    ) -> StepResult:
        """Execute a single workflow step.
        
        Args:
            name: Step name
            context: Workflow context
        
        Returns:
            StepResult with execution result
        """
        step = self._steps[name]
        start_time = datetime.now()
        
        result = StepResult(
            step_name=name,
            status=StepStatus.RUNNING,
            start_time=start_time
        )
        
        for attempt in range(step.retry_count + 1):
            try:
                result.attempts = attempt + 1
                
                if asyncio.iscoroutinefunction(step.func):
                    step_result = await asyncio.wait_for(
                        step.func(context),
                        timeout=step.timeout
                    )
                else:
                    step_result = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: step.func(context)
                    )
                
                result.status = StepStatus.COMPLETED
                result.result = step_result
                result.end_time = datetime.now()
                result.duration = (result.end_time - start_time).total_seconds()
                return result
                
            except asyncio.TimeoutError:
                result.error = f"Step timed out after {step.timeout}s"
            except Exception as e:
                result.error = str(e)
            
            if attempt < step.retry_count:
                await asyncio.sleep(1.0 * (attempt + 1))
        
        result.status = StepStatus.FAILED
        result.end_time = datetime.now()
        result.duration = (result.end_time - start_time).total_seconds()
        
        return result
    
    def execute_sync(
        self,
        initial_data: Optional[Dict[str, Any]] = None
    ) -> WorkflowResult:
        """Synchronous version of execute.
        
        Args:
            initial_data: Initial data to pass to the workflow
        
        Returns:
            WorkflowResult with execution results
        """
        return asyncio.run(self.execute(initial_data))
