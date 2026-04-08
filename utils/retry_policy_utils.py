"""Retry policy and backoff utilities for resilient operations."""

from typing import Callable, TypeVar, Optional, Tuple, List
import time
import random


T = TypeVar("T")


class RetryPolicy:
    """Configurable retry policy with backoff strategies."""

    def __init__(
        self,
        max_attempts: int = 3,
        initial_delay: float = 0.1,
        max_delay: float = 10.0,
        backoff_multiplier: float = 2.0,
        jitter: bool = True,
        exceptions: Tuple[type, ...] = (Exception,)
    ):
        """Initialize retry policy.
        
        Args:
            max_attempts: Maximum number of retry attempts.
            initial_delay: Initial delay in seconds.
            max_delay: Maximum delay cap in seconds.
            backoff_multiplier: Exponential backoff multiplier.
            jitter: If True, add random jitter to delays.
            exceptions: Tuple of exception types to retry on.
        """
        self.max_attempts = max_attempts
        self.initial_delay = initial_delay
        self.max_delay = max_delay
        self.backoff_multiplier = backoff_multiplier
        self.jitter = jitter
        self.exceptions = exceptions

    def delay(self, attempt: int) -> float:
        """Calculate delay for given attempt number."""
        delay = min(self.initial_delay * (self.backoff_multiplier ** attempt), self.max_delay)
        if self.jitter:
            delay *= (0.5 + random.random())
        return delay

    def execute(self, func: Callable[[], T]) -> T:
        """Execute function with retry policy.
        
        Args:
            func: Function to execute.
        
        Returns:
            Function result.
        
        Raises:
            Last exception if all retries exhausted.
        """
        last_exception = None
        for attempt in range(self.max_attempts):
            try:
                return func()
            except self.exceptions as e:
                last_exception = e
                if attempt < self.max_attempts - 1:
                    time.sleep(self.delay(attempt))
        raise last_exception


def retry_with_linear_backoff(
    func: Callable[[], T],
    max_attempts: int = 3,
    delay: float = 0.5,
    exceptions: Tuple[type, ...] = (Exception,)
) -> T:
    """Simple linear backoff retry.
    
    Args:
        func: Function to execute.
        max_attempts: Maximum attempts.
        delay: Fixed delay between attempts.
        exceptions: Exceptions to retry on.
    
    Returns:
        Function result.
    """
    last_exception = None
    for _ in range(max_attempts):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            time.sleep(delay)
    raise last_exception


def retry_with_exponential_backoff(
    func: Callable[[], T],
    max_attempts: int = 5,
    initial_delay: float = 0.1,
    max_delay: float = 30.0,
    exceptions: Tuple[type, ...] = (Exception,)
) -> T:
    """Exponential backoff retry with jitter.
    
    Args:
        func: Function to execute.
        max_attempts: Maximum attempts.
        initial_delay: Starting delay.
        max_delay: Maximum delay cap.
        exceptions: Exceptions to retry on.
    
    Returns:
        Function result.
    """
    delay = initial_delay
    last_exception = None
    for _ in range(max_attempts):
        try:
            return func()
        except exceptions as e:
            last_exception = e
            jitter = random.uniform(0, delay)
            time.sleep(min(delay + jitter, max_delay))
            delay *= 2
    raise last_exception
