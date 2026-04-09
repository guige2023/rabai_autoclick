"""
Automation Retry Action Module.

Provides configurable retry strategies with exponential backoff,
jitter, and circuit breaker patterns.

Author: RabAi Team
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, Type, TypeVar

T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry strategy types."""
    FIXED = "fixed"
    EXPONENTIAL = "exponential"
    LINEAR = "linear"


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"           # Failing, reject requests
    HALF_OPEN = "half_open"  # Testing if recovery possible


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_factor: float = 0.2
    retryable_exceptions: tuple = (Exception,)


@dataclass
class CircuitBreaker:
    """Circuit breaker for failing fast on persistent errors."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_attempts: int = 3
    state: CircuitState = field(default=CircuitState.CLOSED)
    failure_count: int = field(default=0)
    last_failure_time: float = field(default_factory=time.time)
    half_open_successes: int = field(default=0)

    def record_success(self) -> None:
        """Record a successful call."""
        self.failure_count = 0
        self.state = CircuitState.CLOSED
        self.half_open_successes = 0

    def record_failure(self) -> None:
        """Record a failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.failure_count >= self.failure_threshold:
            self.state = CircuitState.OPEN

    def can_execute(self) -> bool:
        """Check if execution is allowed."""
        if self.state == CircuitState.CLOSED:
            return True

        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time >= self.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                return True
            return False

        return True

    def on_half_open_success(self) -> None:
        """Record success in half-open state."""
        self.half_open_successes += 1
        if self.half_open_successes >= self.half_open_attempts:
            self.record_success()

    def on_half_open_failure(self) -> None:
        """Record failure in half-open state."""
        self.state = CircuitState.OPEN
        self.failure_count = 1


class RetryExecutor:
    """Executes operations with retry logic."""

    def __init__(self, config: Optional[RetryConfig] = None) -> None:
        self.config = config or RetryConfig()

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt."""
        if self.config.strategy == RetryStrategy.FIXED:
            delay = self.config.base_delay
        elif self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.config.base_delay * (2 ** attempt)
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self.config.base_delay * (attempt + 1)
        else:
            delay = self.config.base_delay

        delay = min(delay, self.config.max_delay)

        if self.config.jitter:
            jitter_range = delay * self.config.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)

        return max(0, delay)

    async def execute(
        self,
        func: Callable[..., T],
        *args,
        **kwargs
    ) -> T:
        """Execute function with retry logic."""
        last_exception = None

        for attempt in range(self.config.max_attempts):
            try:
                return await func(*args, **kwargs)
            except self.config.retryable_exceptions as e:
                last_exception = e
                if attempt < self.config.max_attempts - 1:
                    delay = self.calculate_delay(attempt)
                    await asyncio.sleep(delay)
                else:
                    raise last_exception

        raise last_exception

    def execute_sync(
        self,
        func: Callable[..., T],
        *args,
        **kwargs
    ) -> T:
        """Synchronous execute with retry."""
        last_exception = None

        for attempt in range(self.config.max_attempts):
            try:
                return func(*args, **kwargs)
            except self.config.retryable_exceptions as e:
                last_exception = e
                if attempt < self.config.max_attempts - 1:
                    delay = self.calculate_delay(attempt)
                    time.sleep(delay)
                else:
                    raise last_exception

        raise last_exception


async def retry_with_circuit_breaker(
    func: Callable[..., T],
    circuit_breaker: CircuitBreaker,
    retry_config: Optional[RetryConfig] = None,
    *args,
    **kwargs
) -> T:
    """Execute with both retry and circuit breaker."""
    if not circuit_breaker.can_execute():
        raise Exception("Circuit breaker is open")

    executor = RetryExecutor(retry_config)

    try:
        result = await executor.execute(func, *args, **kwargs)
        circuit_breaker.record_success()
        return result
    except Exception as e:
        if circuit_breaker.state == CircuitState.HALF_OPEN:
            circuit_breaker.on_half_open_failure()
        else:
            circuit_breaker.record_failure()
        raise e


def retry_decorator(config: Optional[RetryConfig] = None) -> Callable:
    """Decorator for adding retry logic to functions."""
    def decorator(func: Callable) -> Callable:
        async def wrapper(*args, **kwargs):
            executor = RetryExecutor(config)
            return await executor.execute(func, *args, **kwargs)
        return wrapper
    return decorator
