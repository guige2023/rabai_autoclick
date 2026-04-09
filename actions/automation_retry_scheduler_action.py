"""
Automation Retry Scheduler Action Module.

Intelligent retry scheduling with exponential backoff, jitter,
circuit breaker pattern, and priority-based task queuing.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Awaitable, Callable, Optional, TypeVar


T = TypeVar("T")


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
    jitter_range: float = 0.5
    retry_on: tuple[type[Exception], ...] = (Exception,)


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""

    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    half_open_attempts: int = 1


@dataclass
class TaskResult:
    """Result of a scheduled task execution."""

    success: bool
    result: Optional[Any] = None
    error: Optional[Exception] = None
    attempts: int = 0
    total_time: float = 0.0


@dataclass
class CircuitBreaker:
    """Circuit breaker for preventing cascade failures."""

    state: CircuitState = CircuitState.CLOSED
    failure_count: int = 0
    last_failure_time: float = 0.0
    half_open_successes: int = 0

    def record_success(self) -> None:
        """Record a successful call."""
        if self.state == CircuitState.HALF_OPEN:
            self.half_open_successes += 1
            self.state = CircuitState.CLOSED
            self.failure_count = 0
        elif self.state == CircuitState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self, config: CircuitBreakerConfig) -> None:
        """Record a failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
        elif self.failure_count >= config.failure_threshold:
            self.state = CircuitState.OPEN

    def can_execute(self, config: CircuitBreakerConfig) -> bool:
        """Check if execution is allowed."""
        if self.state == CircuitState.CLOSED:
            return True
        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time > config.recovery_timeout:
                self.state = CircuitState.HALF_OPEN
                self.half_open_successes = 0
                return True
            return False
        return True


class RetryScheduler:
    """
    Intelligent retry scheduler with circuit breaker support.

    Supports exponential backoff, jitter, priority queuing,
    and automatic circuit breaking.
    """

    def __init__(
        self,
        retry_config: Optional[RetryConfig] = None,
        circuit_config: Optional[CircuitBreakerConfig] = None,
        enable_circuit_breaker: bool = True,
    ) -> None:
        """
        Initialize the retry scheduler.

        Args:
            retry_config: Retry behavior configuration.
            circuit_config: Circuit breaker configuration.
            enable_circuit_breaker: Whether to enable circuit breaker.
        """
        self._retry_config = retry_config or RetryConfig()
        self._circuit_config = circuit_config or CircuitBreakerConfig()
        self._enable_circuit_breaker = enable_circuit_breaker
        self._circuit = CircuitBreaker()
        self._task_queue: asyncio.PriorityQueue[tuple[int, int, Any]] = (
            asyncio.PriorityQueue()
        )
        self._task_counter = 0
        self._running = False
        self._worker_task: Optional[asyncio.Task] = None

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt number."""
        delay = self._retry_config.base_delay * (
            self._retry_config.exponential_base ** attempt
        )
        delay = min(delay, self._retry_config.max_delay)
        if self._retry_config.jitter:
            jitter = delay * self._retry_config.jitter_range
            delay += random.uniform(-jitter, jitter)
        return max(0, delay)

    async def execute_with_retry(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        priority: int = 0,
        **kwargs: Any,
    ) -> TaskResult:
        """
        Execute a function with automatic retry and circuit breaker.

        Args:
            func: Async function to execute.
            *args: Positional arguments for func.
            priority: Lower number = higher priority.
            **kwargs: Keyword arguments for func.

        Returns:
            TaskResult with success status and data.
        """
        start_time = time.time()
        last_error: Optional[Exception] = None

        for attempt in range(self._retry_config.max_attempts):
            if self._enable_circuit_breaker:
                if not self._circuit.can_execute(self._circuit_config):
                    return TaskResult(
                        success=False,
                        error=Exception("Circuit breaker is OPEN"),
                        attempts=attempt + 1,
                        total_time=time.time() - start_time,
                    )

            try:
                result = await func(*args, **kwargs)
                if self._enable_circuit_breaker:
                    self._circuit.record_success()
                return TaskResult(
                    success=True,
                    result=result,
                    attempts=attempt + 1,
                    total_time=time.time() - start_time,
                )
            except self._retry_config.retry_on as e:
                last_error = e
                if self._enable_circuit_breaker:
                    self._circuit.record_failure(self._circuit_config)
                if attempt < self._retry_config.max_attempts - 1:
                    delay = self._calculate_delay(attempt)
                    await asyncio.sleep(delay)
            except Exception as e:
                last_error = e
                break

        return TaskResult(
            success=False,
            error=last_error,
            attempts=self._retry_config.max_attempts,
            total_time=time.time() - start_time,
        )

    async def schedule_task(
        self,
        func: Callable[..., Awaitable[T]],
        *args: Any,
        priority: int = 0,
        **kwargs: Any,
    ) -> None:
        """
        Schedule a task in the priority queue.

        Args:
            func: Async function to execute.
            *args: Positional arguments.
            priority: Lower number = higher priority.
            **kwargs: Keyword arguments.
        """
        self._task_counter += 1
        entry = (priority, self._task_counter, (func, args, kwargs))
        await self._task_queue.put(entry)

    async def _worker(self) -> None:
        """Background worker that processes queued tasks."""
        while self._running:
            try:
                priority, counter, task_data = await asyncio.wait_for(
                    self._task_queue.get(), timeout=1.0
                )
                func, args, kwargs = task_data
                await self.execute_with_retry(func, *args, priority=priority, **kwargs)
                self._task_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception:
                continue

    async def start(self) -> None:
        """Start the background worker."""
        if self._running:
            return
        self._running = True
        self._worker_task = asyncio.create_task(self._worker())

    async def stop(self) -> None:
        """Stop the background worker and wait for queue to drain."""
        self._running = False
        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
        await self._task_queue.join()

    def get_circuit_state(self) -> CircuitState:
        """Return current circuit breaker state."""
        return self._circuit.state

    def get_queue_size(self) -> int:
        """Return current queue size."""
        return self._task_queue.qsize()


def create_retry_scheduler(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    enable_circuit_breaker: bool = True,
) -> RetryScheduler:
    """
    Factory function to create a configured retry scheduler.

    Args:
        max_attempts: Maximum retry attempts.
        base_delay: Initial delay between retries.
        enable_circuit_breaker: Enable circuit breaker pattern.

    Returns:
        Configured RetryScheduler instance.
    """
    retry_config = RetryConfig(max_attempts=max_attempts, base_delay=base_delay)
    circuit_config = CircuitBreakerConfig()
    return RetryScheduler(
        retry_config=retry_config,
        circuit_config=circuit_config,
        enable_circuit_breaker=enable_circuit_breaker,
    )
