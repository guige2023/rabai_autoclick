"""
Automation sequence utilities for workflow orchestration.

Provides sequence management, step execution, and
workflow state tracking for automation.
"""

from __future__ import annotations

import time
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
from dataclasses_json import dataclass_json


class StepStatus(Enum):
    """Step execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class Step:
    """Automation workflow step."""
    id: str
    name: str
    action: Callable
    args: tuple = field(default_factory=tuple)
    kwargs: Dict[str, Any] = field(default_factory=dict)
    retry_count: int = 0
    retry_delay: float = 1.0
    timeout: float = 30.0
    enabled: bool = True
    continue_on_error: bool = False


@dataclass
class StepResult:
    """Step execution result."""
    step_id: str
    status: StepStatus
    duration: float
    result: Any = None
    error: Optional[str] = None
    retries: int = 0


@dataclass
class SequenceResult:
    """Sequence execution result."""
    sequence_id: str
    status: StepStatus
    total_steps: int
    completed_steps: int
    failed_steps: int
    total_duration: float
    step_results: List[StepResult] = field(default_factory=list)


class AutomationSequence:
    """Manages automation workflow sequences."""
    
    def __init__(self, sequence_id: str, name: str):
        """
        Initialize automation sequence.
        
        Args:
            sequence_id: Sequence identifier.
            name: Sequence name.
        """
        self.sequence_id = sequence_id
        self.name = name
        self.steps: List[Step] = []
        self._current_step: Optional[Step] = None
        self._step_results: List[StepResult] = []
        self._state: Dict[str, Any] = {}
    
    def add_step(self, step: Step) -> 'AutomationSequence':
        """
        Add step to sequence.
        
        Args:
            step: Step to add.
            
        Returns:
            Self for chaining.
        """
        self.steps.append(step)
        return self
    
    def step(self, name: str,
            retry_count: int = 0,
            retry_delay: float = 1.0,
            timeout: float = 30.0,
            continue_on_error: bool = False) -> Callable:
        """
        Decorator to add a step.
        
        Args:
            name: Step name.
            retry_count: Number of retries.
            retry_delay: Delay between retries.
            timeout: Step timeout.
            continue_on_error: Continue if step fails.
        """
        def decorator(func: Callable) -> Callable:
            step = Step(
                id=func.__name__,
                name=name,
                action=func,
                retry_count=retry_count,
                retry_delay=retry_delay,
                timeout=timeout,
                continue_on_error=continue_on_error
            )
            self.add_step(step)
            return func
        return decorator
    
    def execute(self) -> SequenceResult:
        """
        Execute the sequence.
        
        Returns:
            SequenceResult.
        """
        start_time = time.time()
        failed = 0
        completed = 0
        results = []
        
        for step in self.steps:
            if not step.enabled:
                results.append(StepResult(
                    step_id=step.id,
                    status=StepStatus.SKIPPED,
                    duration=0
                ))
                continue
            
            self._current_step = step
            result = self._execute_step(step)
            results.append(result)
            
            if result.status == StepStatus.SUCCESS:
                completed += 1
            else:
                failed += 1
                if not step.continue_on_error:
                    break
        
        duration = time.time() - start_time
        
        return SequenceResult(
            sequence_id=self.sequence_id,
            status=StepStatus.SUCCESS if failed == 0 else StepStatus.FAILED,
            total_steps=len(self.steps),
            completed_steps=completed,
            failed_steps=failed,
            total_duration=duration,
            step_results=results
        )
    
    def _execute_step(self, step: Step) -> StepResult:
        """Execute a single step."""
        start = time.time()
        
        for attempt in range(step.retry_count + 1):
            try:
                result = step.action(*step.args, **step.kwargs)
                
                return StepResult(
                    step_id=step.id,
                    status=StepStatus.SUCCESS,
                    duration=time.time() - start,
                    result=result,
                    retries=attempt
                )
            except Exception as e:
                if attempt < step.retry_count:
                    time.sleep(step.retry_delay)
                    continue
                
                return StepResult(
                    step_id=step.id,
                    status=StepStatus.FAILED,
                    duration=time.time() - start,
                    error=str(e),
                    retries=attempt + 1
                )
        
        return StepResult(
            step_id=step.id,
            status=StepStatus.FAILED,
            duration=time.time() - start,
            retries=step.retry_count + 1
        )
    
    def get_state(self) -> Dict[str, Any]:
        """Get sequence state."""
        return self._state.copy()
    
    def set_state(self, key: str, value: Any) -> None:
        """Set state value."""
        self._state[key] = value
    
    def get_step_result(self, step_id: str) -> Optional[StepResult]:
        """Get result for step."""
        for result in self._step_results:
            if result.step_id == step_id:
                return result
        return None


def create_sequence(sequence_id: str, name: str) -> AutomationSequence:
    """
    Create an automation sequence.
    
    Args:
        sequence_id: Sequence ID.
        name: Sequence name.
        
    Returns:
        New AutomationSequence.
    """
    return AutomationSequence(sequence_id, name)


def simple_sequence(name: str) -> Callable[[Callable], Callable]:
    """
    Create simple sequence decorator.
    
    Args:
        name: Sequence name.
    """
    def decorator(func: Callable) -> Callable:
        seq = AutomationSequence(func.__name__, name)
        
        def wrapper(*args, **kwargs):
            return seq.execute()
        
        wrapper.sequence = seq
        wrapper.__name__ = func.__name__
        return wrapper
    return decorator
