"""Automation Retry Action Module.

Provides configurable retry mechanisms for automation tasks with support
for exponential backoff, circuit breakers, and various retry policies.

Example:
    >>> from actions.automation.automation_retry_action import AutomationRetryAction
    >>> action = AutomationRetryAction(max_retries=3)
    >>> result = await action.execute_with_retry(task)
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
import threading


T = TypeVar('T')


class RetryStrategy(Enum):
    """Retry strategy types."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"


class RetryError(Exception):
    """Exception raised when all retry attempts are exhausted."""
    
    def __init__(self, message: str, attempts: int, last_error: Optional[Exception] = None):
        super().__init__(message)
        self.attempts = attempts
        self.last_error = last_error


@dataclass
class RetryConfig:
    """Configuration for retry behavior.
    
    Attributes:
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries in seconds
        strategy: Retry strategy to use
        jitter: Whether to add random jitter to delays
        jitter_factor: Maximum jitter as a fraction of delay
        recoverable_types: Exception types that should trigger retries
        non_recoverable_types: Exception types that should not trigger retries
    """
    max_retries: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_factor: float = 0.1
    recoverable_types: tuple = (Exception,)
    non_recoverable_types: tuple = (KeyboardInterrupt, SystemExit)
    timeout: Optional[float] = None


@dataclass
class RetryAttempt:
    """Record of a single retry attempt.
    
    Attributes:
        attempt_number: Which attempt this was (1-indexed)
        start_time: When the attempt started
        end_time: When the attempt ended
        success: Whether the attempt succeeded
        error: Error that occurred if unsuccessful
        duration: How long the attempt took
    """
    attempt_number: int
    start_time: datetime
    end_time: Optional[datetime] = None
    success: bool = False
    error: Optional[Exception] = None
    duration: Optional[float] = None


class AutomationRetryAction:
    """Handles retry logic for automation tasks.
    
    Supports multiple retry strategies including fixed, linear, exponential,
    and exponential with jitter. Thread-safe for concurrent operations.
    
    Attributes:
        config: Current retry configuration
        attempts: History of all retry attempts
    
    Example:
        >>> config = RetryConfig(max_retries=5, strategy=RetryStrategy.EXPONENTIAL)
        >>> action = AutomationRetryAction(config)
        >>> result = await action.execute_with_retry(async_task)
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        """Initialize the retry action.
        
        Args:
            config: Retry configuration. Uses defaults if not provided.
        """
        self.config = config or RetryConfig()
        self._attempts: List[RetryAttempt] = []
        self._lock = threading.RLock()
        self._circuit_open = False
        self._circuit_open_time: Optional[datetime] = None
        self._failure_count = 0
    
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """Determine if an error should trigger a retry.
        
        Args:
            error: The exception that occurred
            attempt: Current attempt number
        
        Returns:
            True if the operation should be retried
        """
        # Check non-recoverable types
        if isinstance(error, self.config.non_recoverable_types):
            return False
        
        # Check max retries
        if attempt >= self.config.max_retries:
            return False
        
        # Check circuit breaker
        if self._circuit_open:
            if self._circuit_open_time:
                elapsed = (datetime.now() - self._circuit_open_time).total_seconds()
                if elapsed < self.config.max_delay:
                    return False
                self._circuit_open = False
            else:
                return False
        
        # Check recoverable types
        if isinstance(error, self.config.recoverable_types):
            return True
        
        return False
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate the delay before the next retry.
        
        Args:
            attempt: Current attempt number
        
        Returns:
            Delay in seconds before next retry
        """
        base_delay = min(
            self.config.initial_delay * (2 ** attempt),
            self.config.max_delay
        )
        
        if self.config.strategy == RetryStrategy.FIXED:
            delay = self.config.initial_delay
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self.config.initial_delay * attempt
        elif self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = base_delay
        elif self.config.strategy == RetryStrategy.FIBONACCI:
            fib = self._fibonacci(attempt + 1)
            delay = min(self.config.initial_delay * fib, self.config.max_delay)
        elif self.config.strategy == RetryStrategy.EXPONENTIAL_WITH_JITTER:
            delay = self._jitter(base_delay)
        else:
            delay = base_delay
        
        return min(delay, self.config.max_delay)
    
    def _fibonacci(self, n: int) -> int:
        """Calculate the nth Fibonacci number.
        
        Args:
            n: Position in Fibonacci sequence
        
        Returns:
            The nth Fibonacci number
        """
        if n <= 1:
            return n
        a, b = 0, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b
    
    def _jitter(self, base_delay: float) -> float:
        """Add random jitter to a delay.
        
        Args:
            base_delay: Base delay value
        
        Returns:
            Delay with random jitter applied
        """
        if not self.config.jitter:
            return base_delay
        
        jitter_range = base_delay * self.config.jitter_factor
        return base_delay + random.uniform(-jitter_range, jitter_range)
    
    async def execute_with_retry(
        self,
        task: Callable[..., Any],
        *args: Any,
        **kwargs: Any
    ) -> Any:
        """Execute a task with retry logic.
        
        Args:
            task: The task to execute (can be sync or async)
            *args: Positional arguments for the task
            **kwargs: Keyword arguments for the task
        
        Returns:
            Result of the successful task execution
        
        Raises:
            RetryError: If all retry attempts are exhausted
        """
        last_error: Optional[Exception] = None
        
        for attempt in range(self.config.max_retries + 1):
            retry_attempt = RetryAttempt(
                attempt_number=attempt + 1,
                start_time=datetime.now()
            )
            
            try:
                start = time.time()
                
                if asyncio.iscoroutinefunction(task):
                    result = await task(*args, **kwargs)
                else:
                    result = task(*args, **kwargs)
                
                retry_attempt.end_time = datetime.now()
                retry_attempt.success = True
                retry_attempt.duration = time.time() - start
                
                self._record_attempt(retry_attempt)
                self._circuit_close()
                return result
                
            except Exception as e:
                last_error = e
                retry_attempt.end_time = datetime.now()
                retry_attempt.success = False
                retry_attempt.error = e
                retry_attempt.duration = time.time() - start
                
                self._record_attempt(retry_attempt)
                
                if not self.should_retry(e, attempt):
                    break
                
                delay = self.calculate_delay(attempt)
                
                if asyncio.iscoroutinefunction(task):
                    await asyncio.sleep(delay)
                else:
                    time.sleep(delay)
        
        raise RetryError(
            f"All {self.config.max_retries + 1} attempts failed",
            attempts=self.config.max_retries + 1,
            last_error=last_error
        )
    
    def execute_sync_with_retry(
        self,
        task: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """Synchronous version of execute_with_retry.
        
        Args:
            task: The synchronous task to execute
            *args: Positional arguments for the task
            **kwargs: Keyword arguments for the task
        
        Returns:
            Result of the successful task execution
        
        Raises:
            RetryError: If all retry attempts are exhausted
        """
        last_error: Optional[Exception] = None
        
        for attempt in range(self.config.max_retries + 1):
            retry_attempt = RetryAttempt(
                attempt_number=attempt + 1,
                start_time=datetime.now()
            )
            
            try:
                start = time.time()
                result = task(*args, **kwargs)
                
                retry_attempt.end_time = datetime.now()
                retry_attempt.success = True
                retry_attempt.duration = time.time() - start
                
                self._record_attempt(retry_attempt)
                self._circuit_close()
                return result
                
            except Exception as e:
                last_error = e
                retry_attempt.end_time = datetime.now()
                retry_attempt.success = False
                retry_attempt.error = e
                retry_attempt.duration = time.time() - start
                
                self._record_attempt(retry_attempt)
                
                if not self.should_retry(e, attempt):
                    break
                
                delay = self.calculate_delay(attempt)
                time.sleep(delay)
        
        raise RetryError(
            f"All {self.config.max_retries + 1} attempts failed",
            attempts=self.config.max_retries + 1,
            last_error=last_error
        )
    
    def _record_attempt(self, attempt: RetryAttempt) -> None:
        """Record a retry attempt.
        
        Args:
            attempt: The attempt to record
        """
        with self._lock:
            self._attempts.append(attempt)
            
            if not attempt.success:
                self._failure_count += 1
                
                if self._failure_count >= self.config.max_retries:
                    self._circuit_open = True
                    self._circuit_open_time = datetime.now()
            else:
                self._failure_count = 0
    
    def _circuit_close(self) -> None:
        """Close the circuit breaker after success."""
        with self._lock:
            self._circuit_open = False
            self._circuit_open_time = None
            self._failure_count = 0
    
    def get_attempt_history(self) -> List[RetryAttempt]:
        """Get history of all retry attempts.
        
        Returns:
            List of all retry attempts
        """
        with self._lock:
            return self._attempts.copy()
    
    def get_success_rate(self) -> float:
        """Calculate the overall success rate.
        
        Returns:
            Success rate as a percentage (0-100)
        """
        with self._lock:
            if not self._attempts:
                return 0.0
            
            successful = sum(1 for a in self._attempts if a.success)
            return (successful / len(self._attempts)) * 100
    
    def get_average_duration(self) -> float:
        """Calculate average attempt duration.
        
        Returns:
            Average duration in seconds
        """
        with self._lock:
            durations = [a.duration for a in self._attempts if a.duration is not None]
            
            if not durations:
                return 0.0
            
            return sum(durations) / len(durations)
    
    def reset(self) -> None:
        """Reset all attempt history and circuit breaker."""
        with self._lock:
            self._attempts.clear()
            self._circuit_open = False
            self._circuit_open_time = None
            self._failure_count = 0
