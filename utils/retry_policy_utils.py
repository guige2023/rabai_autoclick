"""
Retry Policy Utilities for UI Automation.

This module provides configurable retry policies and backoff strategies
for handling transient failures in automation workflows.

Author: AI Assistant
License: MIT
"""

from __future__ import annotations

import time
import random
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Callable, Optional, TypeVar


T = TypeVar('T')


class BackoffStrategy(Enum):
    """Backoff strategies for retries."""
    FIXED = auto()
    LINEAR = auto()
    EXPONENTIAL = auto()
    EXPONENTIAL_WITH_JITTER = auto()
    FIBONACCI = auto()


@dataclass
class RetryConfig:
    """
    Configuration for retry behavior.
    
    Attributes:
        max_attempts: Maximum number of attempts
        initial_delay: Initial delay between retries in seconds
        max_delay: Maximum delay between retries
        backoff: Backoff strategy to use
        jitter: Whether to add random jitter
        retry_on_exceptions: List of exception types to retry
        skip_on_exceptions: List of exception types to skip
    """
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_factor: float = 0.1
    retry_on_exceptions: list[type] = field(default_factory=lambda: [Exception])
    skip_on_exceptions: list[type] = field(default_factory=list)
    
    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay for a specific attempt.
        
        Args:
            attempt: Attempt number (0-indexed)
            
        Returns:
            Delay in seconds
        """
        if self.backoff == BackoffStrategy.FIXED:
            delay = self.initial_delay
        elif self.backoff == BackoffStrategy.LINEAR:
            delay = self.initial_delay * (attempt + 1)
        elif self.backoff == BackoffStrategy.EXPONENTIAL:
            delay = self.initial_delay * (2 ** attempt)
        elif self.backoff == BackoffStrategy.FIBONACCI:
            delay = self.initial_delay * _fibonacci(attempt + 1)
        else:
            delay = self.initial_delay
        
        # Cap at max_delay
        delay = min(delay, self.max_delay)
        
        # Add jitter if enabled
        if self.jitter:
            jitter_amount = delay * self.jitter_factor
            delay += random.uniform(-jitter_amount, jitter_amount)
            delay = max(0.1, delay)  # Ensure minimum delay
        
        return delay


def _fibonacci(n: int) -> int:
    """Calculate the nth Fibonacci number."""
    if n <= 1:
        return 1
    a, b = 1, 1
    for _ in range(n - 1):
        a, b = b, a + b
    return b


@dataclass
class RetryResult:
    """
    Result of a retry operation.
    
    Attributes:
        success: Whether the operation succeeded
        result: The result if successful
        attempts: Number of attempts made
        total_duration_ms: Total time spent
        last_error: Last error if failed
        errors: List of all errors encountered
    """
    success: bool
    result: Any = None
    attempts: int = 0
    total_duration_ms: float = 0.0
    last_error: Optional[str] = None
    errors: list[str] = field(default_factory=list)


class RetryPolicy:
    """
    Retry policy executor.
    
    Example:
        policy = RetryPolicy(RetryConfig(max_attempts=5, backoff=BackoffStrategy.EXPONENTIAL))
        result = policy.execute(lambda: some_flakey_operation())
    """
    
    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()
    
    def execute(
        self,
        func: Callable[[], T],
        before_retry: Optional[Callable[[int, Exception], None]] = None
    ) -> RetryResult:
        """
        Execute a function with retry logic.
        
        Args:
            func: Function to execute
            before_retry: Optional callback called before each retry
            
        Returns:
            RetryResult with execution details
        """
        start_time = time.time()
        errors = []
        
        for attempt in range(self.config.max_attempts):
            try:
                result = func()
                duration_ms = (time.time() - start_time) * 1000
                
                return RetryResult(
                    success=True,
                    result=result,
                    attempts=attempt + 1,
                    total_duration_ms=duration_ms
                )
                
            except Exception as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                errors.append(error_msg)
                
                # Check if we should skip this exception
                if self._should_skip(e):
                    duration_ms = (time.time() - start_time) * 1000
                    return RetryResult(
                        success=False,
                        attempts=attempt + 1,
                        total_duration_ms=duration_ms,
                        last_error=error_msg,
                        errors=errors
                    )
                
                # Check if we have more attempts
                if attempt < self.config.max_attempts - 1:
                    # Call before_retry callback
                    if before_retry:
                        before_retry(attempt + 1, e)
                    
                    # Calculate and apply delay
                    delay = self.config.get_delay(attempt)
                    time.sleep(delay)
        
        duration_ms = (time.time() - start_time) * 1000
        return RetryResult(
            success=False,
            attempts=self.config.max_attempts,
            total_duration_ms=duration_ms,
            last_error=errors[-1] if errors else None,
            errors=errors
        )
    
    def _should_skip(self, exception: Exception) -> bool:
        """Check if exception should be skipped (not retried)."""
        for exc_type in self.config.skip_on_exceptions:
            if isinstance(exception, exc_type):
                return True
        return False


def retry(
    max_attempts: int = 3,
    initial_delay: float = 1.0,
    backoff: BackoffStrategy = BackoffStrategy.EXPONENTIAL
) -> Callable[[Callable[[], T]], Callable[[], T]]:
    """
    Decorator for adding retry logic to a function.
    
    Example:
        @retry(max_attempts=5, backoff=BackoffStrategy.EXPONENTIAL)
        def flaky_operation():
            return might_fail()
    """
    def decorator(func: Callable[[], T]) -> Callable[[], T]:
        def wrapper() -> T:
            config = RetryConfig(
                max_attempts=max_attempts,
                initial_delay=initial_delay,
                backoff=backoff
            )
            policy = RetryPolicy(config)
            result = policy.execute(func)
            
            if result.success:
                return result.result
            else:
                raise RuntimeError(f"Failed after {result.attempts} attempts: {result.last_error}")
        
        return wrapper
    return decorator


class CircuitBreakerRetry:
    """
    Combines retry logic with circuit breaker pattern.
    
    Example:
        breaker = CircuitBreakerRetry(
            failure_threshold=5,
            timeout=60,
            retry_config=RetryConfig(max_attempts=3)
        )
        result = breaker.execute(flaky_operation)
    """
    
    def __init__(
        self,
        failure_threshold: int = 5,
        timeout: float = 60.0,
        retry_config: Optional[RetryConfig] = None
    ):
        self.retry_config = retry_config or RetryConfig()
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._is_open = False
    
    def execute(self, func: Callable[[], T]) -> RetryResult:
        """
        Execute with circuit breaker and retry.
        
        Returns:
            RetryResult
        """
        # Check if circuit is open
        if self._is_open:
            if self._last_failure_time:
                elapsed = time.time() - self._last_failure_time
                if elapsed >= self.timeout:
                    # Transition to half-open
                    self._failure_count = 0
                    self._is_open = False
                else:
                    return RetryResult(
                        success=False,
                        last_error="Circuit breaker is open",
                        attempts=0
                    )
        
        # Execute with retry
        policy = RetryPolicy(self.retry_config)
        result = policy.execute(func)
        
        if result.success:
            self._failure_count = 0
            self._is_open = False
        else:
            self._failure_count += 1
            self._last_failure_time = time.time()
            
            if self._failure_count >= self.failure_threshold:
                self._is_open = True
        
        return result
    
    @property
    def is_open(self) -> bool:
        """Check if circuit breaker is open."""
        return self._is_open
