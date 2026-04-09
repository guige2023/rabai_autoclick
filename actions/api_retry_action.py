"""
API Retry Action Module

Provides configurable retry logic for API calls with exponential backoff,
jitter, circuit breaker pattern, and comprehensive error handling.

MIT License - Copyright (c) 2025 RabAi Research
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Union

import httpx

logger = logging.getLogger(__name__)

T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry strategy types."""

    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"
    FIBONACCI = "fibonacci"


class CircuitState(Enum):
    """Circuit breaker states."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True
    jitter_range: float = 0.3
    retry_on_status: Set[int] = field(
        default_factory=lambda: {408, 429, 500, 502, 503, 504}
    )
    retry_on_exceptions: tuple = field(
        default_factory=lambda: (
            httpx.TimeoutException,
            httpx.NetworkError,
            httpx.HTTPStatusError,
            ConnectionError,
            TimeoutError,
        )
    )
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker pattern."""

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_requests: int = 3


class CircuitBreaker:
    """
    Circuit breaker implementation to prevent cascading failures.

    States:
    - CLOSED: Normal operation, requests pass through
    - OPEN: Failures exceeded threshold, requests are rejected
    - HALF_OPEN: Testing if service recovered
    """

    def __init__(self, config: Optional[CircuitBreakerConfig] = None):
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count: int = 0
        self._success_count: int = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_requests_sent: int = 0

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._state = CircuitState.HALF_OPEN
                self._half_open_requests_sent = 0
        return self._state

    def _should_attempt_reset(self) -> bool:
        """Check if enough time has passed to attempt reset."""
        if self._last_failure_time is None:
            return True
        return time.time() - self._last_failure_time >= self.config.timeout_seconds

    def record_success(self) -> None:
        """Record a successful request."""
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._success_count = 0
                logger.info("Circuit breaker closed after successful recovery")
        elif self._state == CircuitState.CLOSED:
            self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed request."""
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.OPEN
            logger.warning("Circuit breaker reopened after half-open failure")
        elif self._failure_count >= self.config.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(f"Circuit breaker opened after {self._failure_count} failures")

    def can_execute(self) -> bool:
        """Check if a request can be executed."""
        return self.state != CircuitState.OPEN

    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return {
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure_time": self._last_failure_time,
        }


class RetryHandler:
    """Handles retry logic with configurable backoff strategies."""

    def __init__(self, config: Optional[RetryConfig] = None):
        self.config = config or RetryConfig()

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number."""
        if self.config.strategy == RetryStrategy.EXPONENTIAL:
            delay = self.config.base_delay * (self.config.exponential_base ** attempt)
        elif self.config.strategy == RetryStrategy.LINEAR:
            delay = self.config.base_delay * (attempt + 1)
        elif self.config.strategy == RetryStrategy.FIBONACCI:
            delay = self.config.base_delay * self._fibonacci(attempt + 2)
        else:  # FIXED
            delay = self.config.base_delay

        delay = min(delay, self.config.max_delay)

        if self.config.jitter:
            jitter_amount = delay * self.config.jitter_range
            delay += random.uniform(-jitter_amount, jitter_amount)

        return max(0, delay)

    @staticmethod
    def _fibonacci(n: int) -> int:
        """Calculate nth Fibonacci number."""
        if n <= 1:
            return n
        a, b = 0, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b

    def should_retry(self, attempt: int, exception: Exception) -> bool:
        """Determine if the operation should be retried."""
        if attempt >= self.config.max_attempts:
            return False

        if isinstance(exception, httpx.HTTPStatusError):
            return exception.response.status_code in self.config.retry_on_status

        return isinstance(exception, self.config.retry_on_exceptions)


class APIRetryAction:
    """
    Main action class for retryable API calls.

    Features:
    - Configurable retry strategies (exponential, linear, fixed, fibonacci)
    - Jitter support to prevent thundering herd
    - Circuit breaker pattern integration
    - Comprehensive error handling
    - Request/response logging
    """

    def __init__(
        self,
        retry_config: Optional[RetryConfig] = None,
        circuit_config: Optional[CircuitBreakerConfig] = None,
    ):
        self.retry_config = retry_config or RetryConfig()
        self.circuit_config = circuit_config or CircuitBreakerConfig()
        self._retry_handler = RetryHandler(self.retry_config)
        self._circuit_breaker = CircuitBreaker(self.circuit_config)
        self._stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "retried_requests": 0,
            "circuit_trips": 0,
        }

    async def execute(
        self,
        func: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """
        Execute a function with retry logic.

        Args:
            func: Async function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function

        Raises:
            Last exception if all retries are exhausted
        """
        self._stats["total_requests"] += 1

        if not self._circuit_breaker.can_execute():
            self._stats["failed_requests"] += 1
            raise CircuitBreakerOpenError("Circuit breaker is OPEN")

        last_exception: Optional[Exception] = None

        for attempt in range(self.retry_config.max_attempts):
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                self._circuit_breaker.record_success()
                self._stats["successful_requests"] += 1
                return result

            except Exception as e:
                last_exception = e

                if not self._retry_handler.should_retry(attempt, e):
                    logger.error(f"Non-retryable error: {e}")
                    self._circuit_breaker.record_failure()
                    self._stats["failed_requests"] += 1
                    raise

                if attempt < self.retry_config.max_attempts - 1:
                    delay = self._retry_handler.calculate_delay(attempt)
                    self._stats["retried_requests"] += 1
                    logger.warning(
                        f"Retry {attempt + 1}/{self.retry_config.max_attempts} "
                        f"after {delay:.2f}s delay: {e}"
                    )
                    await asyncio.sleep(delay)

        self._circuit_breaker.record_failure()
        self._stats["failed_requests"] += 1
        raise last_exception

    def with_retry(
        self,
        func: Callable[..., Any],
    ) -> Callable[..., Any]:
        """
        Decorator to add retry logic to a function.

        Usage:
            @action.with_retry
            async def fetch_data():
                ...
        """
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await self.execute(func, *args, **kwargs)

        return wrapper

    def get_circuit_state(self) -> CircuitState:
        """Get current circuit breaker state."""
        return self._circuit_breaker.state

    def get_circuit_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        return self._circuit_breaker.get_stats()

    def get_stats(self) -> Dict[str, Any]:
        """Get retry action statistics."""
        return self._stats.copy()

    def reset_stats(self) -> None:
        """Reset all statistics."""
        self._stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "retried_requests": 0,
            "circuit_trips": 0,
        }


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open and request is rejected."""

    pass


async def demo_retry():
    """Demonstrate retry action usage."""
    action = APIRetryAction()

    call_count = 0

    async def unreliable_api_call():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise httpx.TimeoutException("Simulated timeout")
        return {"data": "success"}

    result = await action.execute(unreliable_api_call)
    print(f"Result: {result}")
    print(f"Stats: {action.get_stats()}")


if __name__ == "__main__":
    asyncio.run(demo_retry())
