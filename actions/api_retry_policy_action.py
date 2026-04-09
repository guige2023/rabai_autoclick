"""
API Retry Policy Action Module.

Configurable retry policies with exponential backoff, jitter,
circuit breaker integration, and timeout management.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Optional, TypeVar
import logging

logger = logging.getLogger(__name__)
T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry strategy types."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


@dataclass
class RetryPolicy:
    """
    Configurable retry policy for API calls.

    Attributes:
        max_attempts: Maximum number of retry attempts.
        base_delay: Base delay between retries in seconds.
        max_delay: Maximum delay cap in seconds.
        strategy: Retry strategy (exponential/linear/fixed).
        jitter: Whether to add random jitter to delays.
        retryable_exceptions: Tuple of exception types that trigger retry.
        timeout: Per-attempt timeout in seconds (0 = no timeout).
    """
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter: bool = True
    retryable_exceptions: tuple = (Exception,)
    timeout: float = 0.0
    _attempts: int = field(default=0, init=False)

    def reset(self) -> None:
        """Reset attempt counter."""
        self._attempts = 0

    def calculate_delay(self) -> float:
        """Calculate delay for current attempt with optional jitter."""
        if self.strategy == RetryStrategy.FIXED:
            delay = self.base_delay
        elif self.strategy == RetryStrategy.LINEAR:
            delay = self.base_delay * self._attempts
        else:
            delay = self.base_delay * (2 ** (self._attempts - 1))

        delay = min(delay, self.max_delay)

        if self.jitter:
            delay *= 0.5 + random.random()

        return delay

    def is_retryable(self, exception: Exception) -> bool:
        """Check if exception qualifies for retry."""
        return isinstance(exception, self.retryable_exceptions)


class RetryPolicyAction:
    """
    Executes API calls with configurable retry policies.

    Example:
        policy = RetryPolicy(max_attempts=3, base_delay=1.0, strategy=RetryStrategy.EXPONENTIAL)
        action = RetryPolicyAction(policy)
        result = await action.execute(some_api_call, *args, **kwargs)
    """

    def __init__(self, policy: Optional[RetryPolicy] = None):
        """
        Initialize retry policy action.

        Args:
            policy: RetryPolicy instance. Uses default if None.
        """
        self.policy = policy or RetryPolicy()

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """
        Execute function with retry policy.

        Args:
            func: Async function to execute.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Result from successful func execution.

        Raises:
            Exception: Last exception if all retries exhausted.
        """
        self.policy.reset()
        last_exception: Optional[Exception] = None

        while self.policy._attempts < self.policy.max_attempts:
            self.policy._attempts += 1

            try:
                if asyncio.iscoroutinefunction(func):
                    if self.policy.timeout > 0:
                        return await asyncio.wait_for(
                            func(*args, **kwargs),
                            timeout=self.policy.timeout
                        )
                    return await func(*args, **kwargs)
                else:
                    if self.policy.timeout > 0:
                        return await asyncio.wait_for(
                            asyncio.to_thread(func, *args, **kwargs),
                            timeout=self.policy.timeout
                        )
                    return await asyncio.to_thread(func, *args, **kwargs)

            except Exception as e:
                last_exception = e
                logger.warning(
                    f"Attempt {self.policy._attempts}/{self.policy.max_attempts} failed: {e}"
                )

                if self.policy._attempts >= self.policy.max_attempts:
                    break

                if not self.policy.is_retryable(e):
                    raise

                delay = self.policy.calculate_delay()
                logger.info(f"Retrying in {delay:.2f}s...")
                await asyncio.sleep(delay)

        raise last_exception

    def execute_sync(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any
    ) -> T:
        """
        Synchronous version of execute.

        Args:
            func: Sync function to execute.
            *args: Positional arguments for func.
            **kwargs: Keyword arguments for func.

        Returns:
            Result from successful func execution.

        Raises:
            Exception: Last exception if all retries exhausted.
        """
        self.policy.reset()
        last_exception: Optional[Exception] = None

        while self.policy._attempts < self.policy.max_attempts:
            self.policy._attempts += 1

            try:
                if self.policy.timeout > 0:
                    import signal

                    def timeout_handler(signum: int, frame: Any) -> None:
                        raise TimeoutError(f"Function timed out after {self.policy.timeout}s")

                    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                    signal.alarm(int(self.policy.timeout))

                result = func(*args, **kwargs)

                if self.policy.timeout > 0:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)

                return result

            except Exception as e:
                last_exception = e
                logger.warning(
                    f"Attempt {self.policy._attempts}/{self.policy.max_attempts} failed: {e}"
                )

                if self.policy._attempts >= self.policy.max_attempts:
                    break

                if not self.policy.is_retryable(e):
                    raise

                delay = self.policy.calculate_delay()
                logger.info(f"Retrying in {delay:.2f}s...")
                time.sleep(delay)

        raise last_exception
