"""
API Retry Strategy Action Module.

Provides configurable retry with exponential backoff,
decorator support, and circuit breaker integration.
"""

from __future__ import annotations

from typing import Any, Callable, Optional, TypeVar, Union
from dataclasses import dataclass, field
from enum import Enum
import logging
import random
import time
import asyncio
from functools import wraps

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Backoff strategy types."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    EXPONENTIAL_WITH_JITTER = "exponential_with_jitter"
    FULL_JITTER = "full_jitter"


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    initial_delay: float = 0.5
    max_delay: float = 60.0
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL_WITH_JITTER
    retryable_exceptions: tuple = (Exception,)
    jitter_factor: float = 0.1


@dataclass
class RetryState:
    """Tracks retry attempt state and history."""
    attempt: int = 0
    total_delay: float = 0.0
    last_error: Optional[Exception] = None
    start_time: float = field(default_factory=time.time)


class RetryAction:
    """
    Configurable retry with backoff strategies.

    Supports fixed, linear, exponential, and jittered backoff.
    Can be used as a decorator or called directly.

    Example:
        retry = RetryAction(max_attempts=5, initial_delay=1.0)
        result = retry.execute(some_function, arg1, kwarg1="value")

        @retry.decorator
        def fragile_api_call():
            ...
    """

    def __init__(self, config: Optional[RetryConfig] = None, **kwargs: Any) -> None:
        self.config = config or RetryConfig(**kwargs)
        self._state: Optional[RetryState] = None

    def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute a function with retry logic."""
        self._state = RetryState()
        last_exception: Optional[Exception] = None

        while self._state.attempt < self.config.max_attempts:
            try:
                return func(*args, **kwargs)
            except self.config.retryable_exceptions as e:
                last_exception = e
                self._state.last_error = e
                self._state.attempt += 1

                if self._state.attempt >= self.config.max_attempts:
                    break

                delay = self.calculate_delay(self._state.attempt)
                self._state.total_delay += delay

                logger.warning(
                    "Retry attempt %d/%d after %.2fs: %s",
                    self._state.attempt,
                    self.config.max_attempts,
                    delay,
                    str(e),
                )
                time.sleep(delay)

        raise last_exception or Exception("Retry exhausted")

    async def execute_async(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute an async function with retry logic."""
        self._state = RetryState()
        last_exception: Optional[Exception] = None

        while self._state.attempt < self.config.max_attempts:
            try:
                return await func(*args, **kwargs)
            except self.config.retryable_exceptions as e:
                last_exception = e
                self._state.last_error = e
                self._state.attempt += 1

                if self._state.attempt >= self.config.max_attempts:
                    break

                delay = self.calculate_delay(self._state.attempt)
                self._state.total_delay += delay

                logger.warning(
                    "Async retry attempt %d/%d after %.2fs: %s",
                    self._state.attempt,
                    self.config.max_attempts,
                    delay,
                    str(e),
                )
                await asyncio.sleep(delay)

        raise last_exception or Exception("Async retry exhausted")

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt."""
        base_delay = self.config.initial_delay * (2 ** (attempt - 1))
        base_delay = min(base_delay, self.config.max_delay)

        strategy = self.config.backoff_strategy

        if strategy == BackoffStrategy.FIXED:
            return self.config.initial_delay

        elif strategy == BackoffStrategy.LINEAR:
            return base_delay

        elif strategy == BackoffStrategy.EXPONENTIAL:
            return base_delay

        elif strategy == BackoffStrategy.EXPONENTIAL_WITH_JITTER:
            jitter = base_delay * self.config.jitter_factor
            return base_delay + random.uniform(-jitter, jitter)

        elif strategy == BackoffStrategy.FULL_JITTER:
            return random.uniform(0, base_delay)

        return base_delay

    def decorator(
        self,
        func: Optional[Callable[..., T]] = None,
        config: Optional[RetryConfig] = None,
        **kwargs: Any,
    ) -> Union[Callable[[Callable[..., T]], Callable[..., T]], Callable[..., T]]:
        """Decorator to add retry to any function."""
        _config = config or RetryConfig(**kwargs)

        def decorator_inner(f: Callable[..., T]) -> Callable[..., T]:
            @wraps(f)
            def wrapper(*args: Any, **kwkwargs: Any) -> T:
                retry = RetryAction(config=_config)
                return retry.execute(f, *args, **kwkwargs)

            @wraps(f)
            async def async_wrapper(*args: Any, **kwkwargs: Any) -> T:
                retry = RetryAction(config=_config)
                return await retry.execute_async(f, *args, **kwkwargs)

            if asyncio.iscoroutinefunction(f):
                return async_wrapper  # type: ignore
            return wrapper  # type: ignore

        if func is not None:
            return decorator_inner(func)
        return decorator_inner

    @property
    def state(self) -> Optional[RetryState]:
        """Get current retry state."""
        return self._state
