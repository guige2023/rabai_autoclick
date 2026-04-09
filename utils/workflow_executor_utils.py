"""
Workflow Executor Utilities for UI Automation.

This module provides utilities for executing and managing complex
automation workflows with steps, dependencies, and error handling.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class StepStatus(Enum):
    """Status of a workflow step."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()
    CANCELLED = auto()


class WorkflowStatus(Enum):
    """Status of a workflow execution."""
    CREATED = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    PAUSED = auto()


@dataclass
class WorkflowStep:
    """
    A step in a workflow.
    
    Attributes:
        step_id: Unique identifier
        name: Step name
        func: Function to execute
        depends_on: List of step IDs this depends on
        timeout: Step timeout in seconds
        retry_count: Number of retries on failure
        skip_on_failure: Skip if previous steps failed
    """
    step_id: str
    name: str
    func: Callable[[], Any]
    depends_on: list[str] = field(default_factory=list)
    timeout: float = 60.0
    retry_count: int = 0
    skip_on_failure: bool = False
    metadata: dict[str, Any] = field(default_factory=dict)
    
    # Runtime state
    status: StepStatus = StepStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    
    @property
    def duration_ms(self) -> Optional[float]:
        """Get step duration in milliseconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time) * 1000
        return None
    
    @property
    def is_complete(self) -> bool:
        """Check if step is complete."""
        return self.status in (StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.SKIPPED)


@dataclass
class WorkflowContext:
    """
    Shared context for workflow execution.
    
    Steps can store and retrieve data from this context.
    """
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from context."""
        return self.data.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a value in context."""
        self.data[key] = value
    
    def has(self, key: str) -> bool:
        """Check if key exists."""
        return key in self.data
    
    def update(self, **kwargs) -> None:
        """Update multiple values."""
        self.data.update(kwargs)


class Workflow:
    """
    A workflow consisting of ordered steps.
    
    Example:
        workflow = Workflow(name="login_flow")
        workflow.add_step(WorkflowStep(
            step_id="1",
            name="open_browser",
            func=open_browser
        ))
        workflow.add_step(WorkflowStep(
            step_id="2",
            name="enter_credentials",
            func=enter_credentials,
            depends_on=["1"]
        ))
        
        result = workflow.execute()
    """
    
    def __init__(self, name: str):
        self.name = name
        self.workflow_id = str(uuid.uuid4())
        self._steps: dict[str, WorkflowStep] = {}
        self._step_order: list[str] = []
        self._context = WorkflowContext()
        self._status = WorkflowStatus.CREATED
        self._results: dict[str, Any] = {}
    
    @property
    def status(self) -> WorkflowStatus:
        """Get workflow status."""
        return self._status
    
    @property
    def context(self) -> WorkflowContext:
        """Get workflow context."""
        return self._context
    
    def add_step(self, step: WorkflowStep) -> 'Workflow':
        """Add a step to the workflow."""
        self._steps[step.step_id] = step
        if step.step_id not in self._step_order:
            self._step_order.append(step.step_id)
        return self
    
    def remove_step(self, step_id: str) -> bool:
        """Remove a step from the workflow."""
        if step_id in self._steps:
            del self._steps[step_id]
            self._step_order.remove(step_id)
            return True
        return False
    
    def get_step(self, step_id: str) -> Optional[WorkflowStep]:
        """Get a step by ID."""
        return self._steps.get(step_id)
    
    def execute(
        self,
        context: Optional[WorkflowContext] = None
    ) -> 'WorkflowResult':
        """
        Execute the workflow.
        
        Args:
            context: Optional shared context
            
        Returns:
            WorkflowResult with execution details
        """
        start_time = time.time()
        self._status = WorkflowStatus.RUNNING
        self._context = context or WorkflowContext()
        
        # Reset step states
        for step in self._steps.values():
            step.status = StepStatus.PENDING
            step.result = None
            step.error = None
        
        # Execute steps in order respecting dependencies
        completed = set()
        
        while len(completed) < len(self._steps):
            # Find steps ready to execute
            ready = self._find_ready_steps(completed)
            
            if not ready:
                # Check if we're blocked or done
                pending = [s for s in self._steps.values() if s.status == StepStatus.PENDING]
                if pending:
                    self._status = WorkflowStatus.FAILED
                    return self._create_result(start_time, error="Circular dependency or missing dependency")
                break
            
            # Execute ready steps
            for step_id in ready:
                step = self._steps[step_id]
                
                if not self._can_execute(step, completed):
                    continue
                
                self._execute_step(step)
                completed.add(step_id)
                
                if step.status == StepStatus.FAILED:
                    if not step.skip_on_failure:
                        self._status = WorkflowStatus.FAILED
                        return self._create_result(start_time)
            
            # Small delay to prevent busy loop
            time.sleep(0.01)
        
        # Check if all completed successfully
        failed = [s for s in self._steps.values() if s.status == StepStatus.FAILED]
        
        if failed:
            self._status = WorkflowStatus.FAILED
        else:
            self._status = WorkflowStatus.COMPLETED
        
        return self._create_result(start_time)
    
    def _find_ready_steps(self, completed: set[str]) -> list[str]:
        """Find steps that are ready to execute."""
        ready = []
        
        for step_id, step in self._steps.items():
            if step.status != StepStatus.PENDING:
                continue
            
            if step_id in completed:
                continue
            
            # Check dependencies
            deps_met = all(dep in completed for dep in step.depends_on)
            
            if deps_met:
                ready.append(step_id)
        
        return ready
    
    def _can_execute(self, step: WorkflowStep, completed: set[str]) -> bool:
        """Check if a step can be executed."""
        if step.depends_on:
            return all(dep in completed for dep in step.depends_on)
        return True
    
    def _execute_step(self, step: WorkflowStep) -> None:
        """Execute a single step."""
        step.status = StepStatus.RUNNING
        step.start_time = time.time()
        
        for attempt in range(step.retry_count + 1):
            try:
                result = step.func()
                step.result = result
                self._results[step.step_id] = result
                self._context.set(step.step_id, result)
                step.status = StepStatus.COMPLETED
                step.end_time = time.time()
                return
            except Exception as e:
                if attempt < step.retry_count:
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    continue
                step.error = f"{type(e).__name__}: {str(e)}"
                step.status = StepStatus.FAILED
                step.end_time = time.time()
    
    def _create_result(
        self,
        start_time: float,
        error: Optional[str] = None
    ) -> 'WorkflowResult':
        """Create a workflow result."""
        return WorkflowResult(
            workflow_id=self.workflow_id,
            name=self.name,
            status=self._status,
            duration_ms=(time.time() - start_time) * 1000,
            context=self._context,
            results=self._results,
            error=error
        )
    
    def cancel(self) -> None:
        """Cancel workflow execution."""
        self._status = WorkflowStatus.CANCELLED
        for step in self._steps.values():
            if step.status == StepStatus.RUNNING:
                step.status = StepStatus.CANCELLED
                step.end_time = time.time()
    
    def pause(self) -> None:
        """Pause workflow execution."""
        self._status = WorkflowStatus.PAUSED


@dataclass
class WorkflowResult:
    """Result of workflow execution."""
    workflow_id: str
    name: str
    status: WorkflowStatus
    duration_ms: float
    context: WorkflowContext
    results: dict[str, Any]
    error: Optional[str] = None
    
    @property
    def is_success(self) -> bool:
        """Check if workflow completed successfully."""
        return self.status == WorkflowStatus.COMPLETED
