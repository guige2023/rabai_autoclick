"""
Retry scheduler with configurable backoff strategies.

Provides utilities for scheduling retries with exponential backoff,
jitter, and deadline-aware cancellation.
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Any, Awaitable, Callable, TypeVar

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Supported backoff strategies."""

    EXPONENTIAL = auto()
    LINEAR = auto()
    FIXED = auto()
    FIBONACCI = auto()


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    initial_delay: float = 0.1
    max_delay: float = 30.0
    multiplier: float = 2.0
    jitter: bool = True
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    deadline: float | None = None


@dataclass
class RetryState:
    """Tracks retry attempt state."""

    attempt: int = 0
    total_delay: float = 0.0
    last_error: Exception | None = None


async def retry_with_backoff(
    coro_func: Callable[..., Awaitable[T]],
    config: RetryConfig | None = None,
    *args: Any,
    **kwargs: Any,
) -> T:
    """
    Execute an async callable with retry and backoff.

    Args:
        coro_func: Async callable to retry
        config: Retry configuration
        *args: Positional arguments to coro_func
        **kwargs: Keyword arguments to coro_func

    Returns:
        Result from coro_func

    Raises:
        Last exception if all retries are exhausted
    """
    config = config or RetryConfig()
    state = RetryState()

    while state.attempt < config.max_attempts:
        if config.deadline and time.monotonic() >= config.deadline:
            raise asyncio.TimeoutError(
                f"Retry deadline exceeded after {state.attempt} attempts"
            )

        try:
            return await coro_func(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            state.last_error = exc
            state.attempt += 1

            if state.attempt >= config.max_attempts:
                break

            delay = _calculate_delay(state.attempt, config)
            state.total_delay += delay

            await asyncio.sleep(delay)

    raise state.last_error


def _calculate_delay(attempt: int, config: RetryConfig) -> float:
    """Calculate backoff delay for the given attempt."""
    if config.strategy == BackoffStrategy.FIXED:
        delay = config.initial_delay
    elif config.strategy == BackoffStrategy.LINEAR:
        delay = config.initial_delay * attempt
    elif config.strategy == BackoffStrategy.FIBONACCI:
        delay = config.initial_delay * _fibonacci(attempt)
    else:
        delay = config.initial_delay * (config.multiplier ** (attempt - 1))

    delay = min(delay, config.max_delay)

    if config.jitter:
        delay *= 0.5 + random.random()

    return delay


def _fibonacci(n: int) -> int:
    """Calculate nth Fibonacci number."""
    if n <= 1:
        return 1
    a, b = 1, 1
    for _ in range(n - 1):
        a, b = b, a + b
    return b


class RetryScheduler:
    """
    Manages scheduled retries with backoff for multiple tasks.

    Example:
        scheduler = RetryScheduler()
        scheduler.schedule("task1", coro_func, RetryConfig(max_attempts=5))
        results = await scheduler.run_all()
    """

    def __init__(self) -> None:
        self._tasks: dict[str, tuple[Callable[..., Awaitable[Any]], RetryConfig]] = {}
        self._results: dict[str, Any] = {}
        self._errors: dict[str, Exception] = {}

    def schedule(
        self,
        task_id: str,
        coro_func: Callable[..., Awaitable[T]],
        config: RetryConfig | None = None,
    ) -> None:
        """Register a task for retry-aware execution."""
        self._tasks[task_id] = (coro_func, config or RetryConfig())

    async def run_all(self) -> dict[str, Any]:
        """Execute all scheduled tasks concurrently."""
        coros = {
            task_id: retry_with_backoff(coro_func, config)
            for task_id, (coro_func, config) in self._tasks.items()
        }

        results = await asyncio.gather(*coros.values(), return_exceptions=True)

        for task_id, result in zip(self._tasks.keys(), results):
            if isinstance(result, Exception):
                self._errors[task_id] = result
            else:
                self._results[task_id] = result

        return self._results

    @property
    def results(self) -> dict[str, Any]:
        """Return successful results."""
        return self._results.copy()

    @property
    def errors(self) -> dict[str, Exception]:
        """Return errors from failed tasks."""
        return self._errors.copy()
