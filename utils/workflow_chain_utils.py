"""
Workflow Chain Utilities for Automating Sequential Task Execution.

This module provides utilities for building and executing chains of automation tasks
in sequence, with support for error handling, retry logic, and state management.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
import traceback
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional


class ChainStatus(Enum):
    """Status of a workflow chain execution."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    CANCELLED = auto()
    PAUSED = auto()


class StepStatus(Enum):
    """Status of an individual step in the chain."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()
    RETRYING = auto()


@dataclass
class ChainStepResult:
    """Result of executing a single step in the chain."""
    step_name: str
    status: StepStatus
    output: Optional[Any] = None
    error: Optional[str] = None
    duration_ms: float = 0.0
    retry_count: int = 0
    timestamp: float = field(default_factory=time.time)


@dataclass
class ChainContext:
    """Shared context passed between chain steps."""
    data: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the context."""
        return self.data.get(key, default)
    
    def set(self, key: str, value: Any) -> None:
        """Set a value in the context."""
        self.data[key] = value
    
    def update(self, **kwargs) -> None:
        """Update multiple values in the context."""
        self.data.update(kwargs)


@dataclass
class WorkflowChain:
    """
    A sequential workflow chain for automation tasks.
    
    Attributes:
        name: Name of the workflow chain
        steps: List of (name, callable) tuples defining the chain steps
        max_retries: Maximum retry attempts for failed steps
        retry_delay: Delay between retries in seconds
        stop_on_failure: Whether to stop the chain if a step fails
    """
    name: str
    steps: list[tuple[str, Callable[[ChainContext], Any]]] = field(default_factory=list)
    max_retries: int = 3
    retry_delay: float = 1.0
    stop_on_failure: bool = True
    _status: ChainStatus = field(default=ChainStatus.PENDING, init=False)
    
    def add_step(self, name: str, func: Callable[[ChainContext], Any]) -> None:
        """Add a step to the chain."""
        self.steps.append((name, func))
    
    def step(self, name: str):
        """Decorator to add a step to the chain."""
        def decorator(func: Callable[[ChainContext], Any]):
            self.add_step(name, func)
            return func
        return decorator
    
    def execute(self, initial_context: Optional[ChainContext] = None) -> tuple[ChainStatus, ChainContext, list[ChainStepResult]]:
        """
        Execute the workflow chain.
        
        Args:
            initial_context: Optional initial context to pass to steps
            
        Returns:
            Tuple of (final_status, context, step_results)
        """
        self._status = ChainStatus.RUNNING
        context = initial_context or ChainContext()
        results: list[ChainStepResult] = []
        
        for step_name, step_func in self.steps:
            result = self._execute_step(step_name, step_func, context)
            results.append(result)
            
            if result.status == StepStatus.FAILED:
                if self.stop_on_failure:
                    self._status = ChainStatus.FAILED
                    break
            elif result.status == StepStatus.SKIPPED:
                continue
                
        if self._status == ChainStatus.RUNNING:
            self._status = ChainStatus.COMPLETED
            
        return self._status, context, results
    
    def _execute_step(
        self, 
        name: str, 
        func: Callable[[ChainContext], Any],
        context: ChainContext
    ) -> ChainStepResult:
        """Execute a single step with retry logic."""
        result = ChainStepResult(step_name=name, status=StepStatus.RUNNING)
        start_time = time.time()
        retry_count = 0
        
        while retry_count <= self.max_retries:
            try:
                output = func(context)
                result.output = output
                result.status = StepStatus.COMPLETED
                result.duration_ms = (time.time() - start_time) * 1000
                result.retry_count = retry_count
                return result
            except Exception as e:
                retry_count += 1
                result.retry_count = retry_count
                if retry_count > self.max_retries:
                    result.status = StepStatus.FAILED
                    result.error = f"{type(e).__name__}: {str(e)}"
                    result.duration_ms = (time.time() - start_time) * 1000
                    return result
                time.sleep(self.retry_delay)
                
        result.status = StepStatus.SKIPPED
        return result
    
    def cancel(self) -> None:
        """Cancel the chain execution."""
        self._status = ChainStatus.CANCELLED
    
    def pause(self) -> None:
        """Pause the chain execution."""
        self._status = ChainStatus.PAUSED


class ConditionalStep:
    """A step that executes conditionally based on a predicate."""
    
    def __init__(
        self,
        condition: Callable[[ChainContext], bool],
        then_func: Callable[[ChainContext], Any],
        else_func: Optional[Callable[[ChainContext], Any]] = None
    ):
        self.condition = condition
        self.then_func = then_func
        self.else_func = else_func
    
    def execute(self, context: ChainContext) -> Any:
        """Execute the conditional step."""
        if self.condition(context):
            return self.then_func(context)
        elif self.else_func:
            return self.else_func(context)
        return None


class ParallelStep:
    """A step that executes multiple functions in parallel."""
    
    def __init__(
        self,
        funcs: list[Callable[[ChainContext], Any]],
        combine: Optional[Callable[[list[Any]], Any]] = None
    ):
        self.funcs = funcs
        self.combine = combine
    
    def execute(self, context: ChainContext) -> Any:
        """Execute all functions in parallel and combine results."""
        import concurrent.futures
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.funcs)) as executor:
            futures = [executor.submit(f, context) for f in self.funcs]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        if self.combine:
            return self.combine(results)
        return results


def create_retry_chain(
    func: Callable[[ChainContext], Any],
    max_retries: int = 3,
    retry_delay: float = 1.0,
    retry_predicate: Optional[Callable[[Exception], bool]] = None
) -> Callable[[ChainContext], Any]:
    """
    Wrap a function with retry logic.
    
    Args:
        func: Function to wrap
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds
        retry_predicate: Optional predicate to determine if an exception should trigger retry
        
    Returns:
        Wrapped function with retry logic
    """
    def wrapper(context: ChainContext) -> Any:
        last_exception = None
        for attempt in range(max_retries + 1):
            try:
                return func(context)
            except Exception as e:
                last_exception = e
                if retry_predicate and not retry_predicate(e):
                    raise
                if attempt < max_retries:
                    time.sleep(retry_delay)
        raise last_exception
    return wrapper


def create_timeout_chain(
    func: Callable[[ChainContext], Any],
    timeout_seconds: float
) -> Callable[[ChainContext], Any]:
    """
    Wrap a function with timeout logic.
    
    Args:
        func: Function to wrap
        timeout_seconds: Maximum execution time in seconds
        
    Returns:
        Wrapped function with timeout logic
    """
    import signal
    
    def handler(signum, frame):
        raise TimeoutError(f"Function execution exceeded {timeout_seconds} seconds")
    
    def wrapper(context: ChainContext) -> Any:
        signal.signal(signal.SIGALRM, handler)
        signal.alarm(int(timeout_seconds))
        try:
            return func(context)
        finally:
            signal.alarm(0)
    return wrapper
