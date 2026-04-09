"""Automation Timeout Action Module.

Provides timeout management for automation tasks with support
for configurable timeouts, timeout handlers, and task cancellation.

Example:
    >>> from actions.automation.automation_timeout_action import AutomationTimeoutAction
    >>> action = AutomationTimeoutAction(default_timeout=30.0)
    >>> result = action.execute_with_timeout(task, timeout=60.0)
"""

from __future__ import annotations

import asyncio
import signal
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
import functools


T = TypeVar('T')


class TimeoutStrategy(Enum):
    """Timeout handling strategies."""
    RAISE = "raise"
    DEFAULT = "default"
    CANCEL = "cancel"
    CONTINUE = "continue"


class TimeoutError(Exception):
    """Exception raised when an operation times out.
    
    Attributes:
        timeout: The timeout value that was exceeded
        operation: Name of the operation that timed out
        elapsed: Actual time elapsed before timeout
    """
    
    def __init__(self, message: str, timeout: float, operation: str = "", elapsed: float = 0.0):
        super().__init__(message)
        self.timeout = timeout
        self.operation = operation
        self.elapsed = elapsed


@dataclass
class TimeoutConfig:
    """Configuration for timeout behavior.
    
    Attributes:
        default_timeout: Default timeout in seconds
        min_timeout: Minimum allowed timeout
        max_timeout: Maximum allowed timeout
        strategy: How to handle timeouts
        default_value: Default value to return on timeout
        enable_cancellation: Whether to support task cancellation
    """
    default_timeout: float = 30.0
    min_timeout: float = 0.1
    max_timeout: float = 3600.0
    strategy: TimeoutStrategy = TimeoutStrategy.RAISE
    default_value: Any = None
    enable_cancellation: bool = True


@dataclass
class TaskContext:
    """Context for a running task.
    
    Attributes:
        task_id: Unique task identifier
        name: Task name
        start_time: When the task started
        timeout: Assigned timeout
        status: Current status
        result: Task result if completed
        error: Error if task failed
    """
    task_id: str
    name: str
    start_time: datetime
    timeout: float
    status: str = "running"
    result: Any = None
    error: Optional[Exception] = None
    cancelled: bool = False
    end_time: Optional[datetime] = None


class AutomationTimeoutAction:
    """Handles timeout management for automation tasks.
    
    Provides configurable timeouts with support for task cancellation,
    timeout handlers, and various timeout strategies.
    
    Attributes:
        config: Current timeout configuration
        active_tasks: Currently running tasks
    
    Example:
        >>> action = AutomationTimeoutAction()
        >>> action.execute_with_timeout(my_task, timeout=30.0)
    """
    
    def __init__(self, config: Optional[TimeoutConfig] = None):
        """Initialize the timeout action.
        
        Args:
            config: Timeout configuration. Uses defaults if not provided.
        """
        self.config = config or TimeoutConfig()
        self._active_tasks: Dict[str, TaskContext] = {}
        self._timeout_handlers: Dict[str, Callable[[TaskContext], None]] = {}
        self._lock = threading.RLock()
        self._task_counter = 0
    
    def set_timeout_handler(
        self,
        name: str,
        handler: Callable[[TaskContext], None]
    ) -> "AutomationTimeoutAction":
        """Register a timeout handler for a task type.
        
        Args:
            name: Task type name
            handler: Handler function to call on timeout
        
        Returns:
            Self for method chaining
        """
        with self._lock:
            self._timeout_handlers[name] = handler
            return self
    
    def execute_with_timeout(
        self,
        task: Callable[..., T],
        *args: Any,
        timeout: Optional[float] = None,
        task_name: str = "task",
        **kwargs: Any
    ) -> T:
        """Execute a task with a timeout.
        
        Args:
            task: The task to execute
            *args: Positional arguments for the task
            timeout: Timeout in seconds (uses default if not provided)
            task_name: Name for the task
            **kwargs: Keyword arguments for the task
        
        Returns:
            Task result
        
        Raises:
            TimeoutError: If the task times out and strategy is RAISE
        """
        timeout = self._validate_timeout(timeout)
        task_id = self._generate_task_id()
        
        context = TaskContext(
            task_id=task_id,
            name=task_name,
            start_time=datetime.now(),
            timeout=timeout
        )
        
        self._register_task(context)
        
        result_container: Dict[str, Any] = {}
        exception_container: Dict[str, Exception] = {}
        completed = threading.Event()
        
        def worker():
            try:
                if asyncio.iscoroutinefunction(task):
                    loop = asyncio.new_event_loop()
                    result_container["result"] = loop.run_until_complete(task(*args, **kwargs))
                else:
                    result_container["result"] = task(*args, **kwargs)
            except Exception as e:
                exception_container["exception"] = e
            finally:
                completed.set()
        
        thread = threading.Thread(target=worker, daemon=True)
        thread.start()
        
        if not completed.wait(timeout=timeout):
            self._handle_timeout(context)
            
            if self.config.strategy == TimeoutStrategy.RAISE:
                elapsed = (datetime.now() - context.start_time).total_seconds()
                raise TimeoutError(
                    f"Task '{task_name}' timed out after {timeout}s",
                    timeout=timeout,
                    operation=task_name,
                    elapsed=elapsed
                )
            elif self.config.strategy == TimeoutStrategy.DEFAULT:
                return self.config.default_value
            elif self.config.strategy == TimeoutStrategy.CANCEL:
                context.cancelled = True
                if self.config.enable_cancellation:
                    # Thread cannot be forcibly stopped, mark as cancelled
                    pass
                return self.config.default_value
            else:
                return self.config.default_value
        
        self._complete_task(context)
        
        if exception_container.get("exception"):
            raise exception_container["exception"]
        
        return result_container.get("result")
    
    async def execute_async_with_timeout(
        self,
        task: Callable[..., Any],
        *args: Any,
        timeout: Optional[float] = None,
        task_name: str = "task",
        **kwargs: Any
    ) -> Any:
        """Execute an async task with a timeout.
        
        Args:
            task: The async task to execute
            *args: Positional arguments for the task
            timeout: Timeout in seconds
            task_name: Name for the task
            **kwargs: Keyword arguments for the task
        
        Returns:
            Task result
        
        Raises:
            TimeoutError: If the task times out
        """
        timeout = self._validate_timeout(timeout)
        task_id = self._generate_task_id()
        
        context = TaskContext(
            task_id=task_id,
            name=task_name,
            start_time=datetime.now(),
            timeout=timeout
        )
        
        self._register_task(context)
        
        try:
            result = await asyncio.wait_for(
                task(*args, **kwargs),
                timeout=timeout
            )
            self._complete_task(context)
            return result
            
        except asyncio.TimeoutError:
            self._handle_timeout(context)
            elapsed = (datetime.now() - context.start_time).total_seconds()
            
            if self.config.strategy == TimeoutStrategy.RAISE:
                raise TimeoutError(
                    f"Task '{task_name}' timed out after {timeout}s",
                    timeout=timeout,
                    operation=task_name,
                    elapsed=elapsed
                )
            elif self.config.strategy == TimeoutStrategy.DEFAULT:
                return self.config.default_value
            else:
                return self.config.default_value
    
    def _validate_timeout(self, timeout: Optional[float]) -> float:
        """Validate and normalize timeout value.
        
        Args:
            timeout: Raw timeout value
        
        Returns:
            Validated timeout value
        """
        if timeout is None:
            return self.config.default_timeout
        
        return max(
            self.config.min_timeout,
            min(timeout, self.config.max_timeout)
        )
    
    def _generate_task_id(self) -> str:
        """Generate a unique task ID.
        
        Returns:
            Unique task identifier
        """
        with self._lock:
            self._task_counter += 1
            return f"task_{self._task_counter}_{int(time.time() * 1000)}"
    
    def _register_task(self, context: TaskContext) -> None:
        """Register a running task.
        
        Args:
            context: Task context to register
        """
        with self._lock:
            self._active_tasks[context.task_id] = context
    
    def _complete_task(self, context: TaskContext) -> None:
        """Mark a task as complete.
        
        Args:
            context: Task context to complete
        """
        with self._lock:
            context.status = "completed"
            context.end_time = datetime.now()
            if context.task_id in self._active_tasks:
                del self._active_tasks[context.task_id]
    
    def _handle_timeout(self, context: TaskContext) -> None:
        """Handle a task timeout.
        
        Args:
            context: Task context that timed out
        """
        with self._lock:
            context.status = "timeout"
            context.end_time = datetime.now()
            if context.task_id in self._active_tasks:
                del self._active_tasks[context.task_id]
        
        handler = self._timeout_handlers.get(context.name)
        if handler:
            try:
                handler(context)
            except Exception:
                pass  # Don't let handler errors propagate
    
    def get_active_tasks(self) -> List[TaskContext]:
        """Get list of currently active tasks.
        
        Returns:
            List of active task contexts
        """
        with self._lock:
            return list(self._active_tasks.values())
    
    def get_task(self, task_id: str) -> Optional[TaskContext]:
        """Get context for a specific task.
        
        Args:
            task_id: Task identifier
        
        Returns:
            Task context or None if not found
        """
        with self._lock:
            return self._active_tasks.get(task_id)
    
    def cancel_task(self, task_id: str) -> bool:
        """Cancel a running task.
        
        Args:
            task_id: Task identifier
        
        Returns:
            True if task was found and cancelled
        """
        with self._lock:
            if task_id in self._active_tasks:
                self._active_tasks[task_id].cancelled = True
                self._active_tasks[task_id].status = "cancelled"
                self._active_tasks[task_id].end_time = datetime.now()
                del self._active_tasks[task_id]
                return True
            return False
    
    def with_timeout(
        self,
        timeout: Optional[float] = None,
        task_name: str = "decorated"
    ) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Decorator to add timeout to a function.
        
        Args:
            timeout: Timeout in seconds
            task_name: Name for the decorated task
        
        Returns:
            Decorated function
        
        Example:
            >>> @action.with_timeout(timeout=30.0)
            ... def my_task():
            ...     pass
        """
        def decorator(func: Callable[..., T]) -> Callable[..., T]:
            @functools.wraps(func)
            def wrapper(*args: Any, **kwargs: Any) -> T:
                return self.execute_with_timeout(
                    func, *args, timeout=timeout,
                    task_name=task_name, **kwargs
                )
            
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await self.execute_async_with_timeout(
                    func, *args, timeout=timeout,
                    task_name=task_name, **kwargs
                )
            
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return wrapper
        
        return decorator
