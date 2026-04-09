"""Automation Circuit Breaker Action Module.

Provides circuit breaker pattern implementation for automation tasks
to prevent cascading failures and provide fault tolerance.

Example:
    >>> from actions.automation.automation_circuit_breaker_action import AutomationCircuitBreakerAction
    >>> cb = AutomationCircuitBreakerAction()
    >>> result = await cb.execute(task, fallback_fn)
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar
import functools


T = TypeVar('T')


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if service recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior.
    
    Attributes:
        failure_threshold: Number of failures before opening circuit
        success_threshold: Number of successes in half-open to close
        timeout: Time to wait before trying half-open
        expected_exceptions: Exception types to count as failures
    """
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    expected_exceptions: tuple = (Exception,)


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker.
    
    Attributes:
        total_calls: Total number of calls
        successful_calls: Number of successful calls
        failed_calls: Number of failed calls
        rejected_calls: Number of rejected calls (when open)
        state_changes: Number of state changes
    """
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state_changes: int = 0
    current_state: CircuitState = CircuitState.CLOSED


@dataclass
class CircuitBreakerEvent:
    """Event recorded by circuit breaker.
    
    Attributes:
        event_type: Type of event (call, success, failure, reject, state_change)
        timestamp: When the event occurred
        duration: Call duration if applicable
        error: Error message if applicable
    """
    event_type: str
    timestamp: datetime
    duration: Optional[float] = None
    error: Optional[str] = None
    from_state: Optional[CircuitState] = None
    to_state: Optional[CircuitState] = None


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit is open."""
    
    def __init__(self, message: str, circuit_state: CircuitState, retry_after: float):
        super().__init__(message)
        self.circuit_state = circuit_state
        self.retry_after = retry_after


class AutomationCircuitBreakerAction:
    """Implements the circuit breaker pattern.
    
    Prevents cascading failures by opening the circuit after
    a threshold of failures, and testing recovery periodically.
    
    Attributes:
        config: Circuit breaker configuration
        name: Name for this circuit breaker
    
    Example:
        >>> cb = AutomationCircuitBreakerAction(failure_threshold=3)
        >>> result = await cb.execute(task, fallback=fallback_fn)
    """
    
    def __init__(
        self,
        name: str = "default",
        config: Optional[CircuitBreakerConfig] = None
    ):
        """Initialize the circuit breaker.
        
        Args:
            name: Circuit breaker name
            config: Configuration object
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._lock = threading.RLock()
        
        # Statistics
        self._total_calls = 0
        self._successful_calls = 0
        self._failed_calls = 0
        self._rejected_calls = 0
        self._state_changes = 0
        
        # Event history
        self._events: List[CircuitBreakerEvent] = []
        self._max_events = 1000
    
    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        with self._lock:
            self._check_state_transition()
            return self._state
    
    def _check_state_transition(self) -> None:
        """Check if state should transition based on timeout."""
        if self._state == CircuitState.OPEN and self._last_failure_time:
            elapsed = time.time() - self._last_failure_time
            if elapsed >= self.config.timeout:
                self._transition_to(CircuitState.HALF_OPEN)
    
    def _transition_to(self, new_state: CircuitState) -> None:
        """Transition to a new state.
        
        Args:
            new_state: State to transition to
        """
        if self._state == new_state:
            return
        
        from_state = self._state
        self._state = new_state
        self._state_changes += 1
        
        if new_state == CircuitState.HALF_OPEN:
            self._success_count = 0
        elif new_state == CircuitState.CLOSED:
            self._failure_count = 0
            self._success_count = 0
        
        self._record_event(
            "state_change",
            from_state=from_state,
            to_state=new_state
        )
    
    def _record_event(
        self,
        event_type: str,
        duration: Optional[float] = None,
        error: Optional[str] = None,
        from_state: Optional[CircuitState] = None,
        to_state: Optional[CircuitState] = None
    ) -> None:
        """Record a circuit breaker event.
        
        Args:
            event_type: Type of event
            duration: Duration if applicable
            error: Error message if applicable
            from_state: Previous state for state changes
            to_state: New state for state changes
        """
        with self._lock:
            event = CircuitBreakerEvent(
                event_type=event_type,
                timestamp=datetime.now(),
                duration=duration,
                error=error,
                from_state=from_state,
                to_state=to_state
            )
            
            self._events.append(event)
            
            # Trim old events
            if len(self._events) > self._max_events:
                self._events = self._events[-self._max_events:]
    
    def _record_success(self) -> None:
        """Record a successful call."""
        with self._lock:
            self._successful_calls += 1
            self._failure_count = 0
            
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self.config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
    
    def _record_failure(self) -> None:
        """Record a failed call."""
        with self._lock:
            self._failed_calls += 1
            self._failure_count += 1
            self._last_failure_time = time.time()
            self._success_count = 0
            
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self._failure_count >= self.config.failure_threshold:
                self._transition_to(CircuitState.OPEN)
    
    async def execute(
        self,
        task: Callable[..., Any],
        *args: Any,
        fallback: Optional[Callable[..., Any]] = None,
        **kwargs: Any
    ) -> Any:
        """Execute a task with circuit breaker protection.
        
        Args:
            task: Task to execute
            *args: Positional arguments for task
            fallback: Optional fallback function
            **kwargs: Keyword arguments for task
        
        Returns:
            Task result or fallback result
        
        Raises:
            CircuitBreakerOpenError: If circuit is open and no fallback
        """
        self._total_calls += 1
        
        # Check circuit state
        current_state = self.state
        
        if current_state == CircuitState.OPEN:
            self._rejected_calls += 1
            self._record_event("reject")
            
            if fallback:
                return await self._execute_fallback(fallback, *args, **kwargs)
            
            retry_after = self.config.timeout
            if self._last_failure_time:
                retry_after = max(0, self.config.timeout - (time.time() - self._last_failure_time))
            
            raise CircuitBreakerOpenError(
                f"Circuit breaker '{self.name}' is OPEN",
                circuit_state=CircuitState.OPEN,
                retry_after=retry_after
            )
        
        # Execute task
        start_time = time.time()
        self._record_event("call")
        
        try:
            if asyncio.iscoroutinefunction(task):
                result = await task(*args, **kwargs)
            else:
                result = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: task(*args, **kwargs)
                )
            
            duration = time.time() - start_time
            self._record_success()
            self._record_event("success", duration=duration)
            
            return result
            
        except self.config.expected_exceptions as e:
            duration = time.time() - start_time
            self._record_failure()
            self._record_event("failure", duration=duration, error=str(e))
            
            if fallback:
                return await self._execute_fallback(fallback, *args, **kwargs)
            
            raise
    
    async def _execute_fallback(
        self,
        fallback: Callable[..., Any],
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Execute a fallback function.
        
        Args:
            fallback: Fallback function
            *args: Positional arguments
            **kwargs: Keyword arguments
        
        Returns:
            Fallback result
        """
        try:
            if asyncio.iscoroutinefunction(fallback):
                return await fallback(*args, **kwargs)
            else:
                return await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: fallback(*args, **kwargs)
                )
        except Exception as e:
            raise Exception(f"Fallback also failed: {str(e)}") from e
    
    def execute_sync(
        self,
        task: Callable[..., T],
        *args: Any,
        fallback: Optional[Callable[..., T]] = None,
        **kwargs: Any
    ) -> T:
        """Synchronous version of execute.
        
        Args:
            task: Task to execute
            *args: Positional arguments
            fallback: Optional fallback function
            **kwargs: Keyword arguments
        
        Returns:
            Task result or fallback result
        """
        return asyncio.run(self.execute(task, *args, fallback=fallback, **kwargs))
    
    def reset(self) -> None:
        """Reset the circuit breaker to closed state."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._success_count = 0
            self._last_failure_time = None
            self._total_calls = 0
            self._successful_calls = 0
            self._failed_calls = 0
            self._rejected_calls = 0
            self._state_changes = 0
            self._events.clear()
    
    def get_stats(self) -> CircuitBreakerStats:
        """Get circuit breaker statistics.
        
        Returns:
            CircuitBreakerStats object
        """
        with self._lock:
            return CircuitBreakerStats(
                total_calls=self._total_calls,
                successful_calls=self._successful_calls,
                failed_calls=self._failed_calls,
                rejected_calls=self._rejected_calls,
                state_changes=self._state_changes,
                current_state=self._state
            )
    
    def get_events(self, limit: int = 100) -> List[CircuitBreakerEvent]:
        """Get recent circuit breaker events.
        
        Args:
            limit: Maximum number of events to return
        
        Returns:
            List of recent events
        """
        with self._lock:
            return self._events[-limit:]
    
    def is_callable(self) -> bool:
        """Check if a call would be allowed.
        
        Returns:
            True if circuit is closed or half-open
        """
        return self.state != CircuitState.OPEN
    
    def get_retry_after(self) -> float:
        """Get seconds until circuit might close.
        
        Returns:
            Seconds until next retry attempt
        """
        if self._last_failure_time is None:
            return 0.0
        
        elapsed = time.time() - self._last_failure_time
        return max(0, self.config.timeout - elapsed)
    
    def decorator(
        self,
        fallback: Optional[Callable[..., Any]] = None
    ) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """Create a decorator for circuit breaker protection.
        
        Args:
            fallback: Optional fallback function
        
        Returns:
            Decorated function
        
        Example:
            >>> @cb.decorator(fallback=my_fallback)
            ... async def my_function():
            ...     pass
        """
        def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
            @functools.wraps(func)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                return await self.execute(func, *args, fallback=fallback, **kwargs)
            
            @functools.wraps(func)
            def sync_wrapper(*args: Any, **kwargs: Any) -> Any:
                return self.execute_sync(func, *args, fallback=fallback, **kwargs)
            
            if asyncio.iscoroutinefunction(func):
                return async_wrapper
            return sync_wrapper
        
        return decorator
