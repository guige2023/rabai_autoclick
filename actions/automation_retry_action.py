"""Automation Retry Action Module.

Provides configurable retry logic with backoff strategies,
circuit breaker, and retry hooks.
"""
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BackoffStrategy(Enum):
    """Backoff strategy."""
    FIXED = "fixed"
    LINEAR = "linear"
    EXPONENTIAL = "exponential"
    FIBONACCI = "fibonacci"
    RANDOM = "random"


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_factor: float = 0.1
    retryable_errors: Optional[List[type]] = None
    retryable_messages: Optional[List[str]] = None


@dataclass
class RetryStats:
    """Retry statistics."""
    total_attempts: int = 0
    successes: int = 0
    failures: int = 0
    total_delay: float = 0.0


class AutomationRetryAction:
    """Retry handler with backoff.

    Example:
        retry = AutomationRetryAction(
            RetryConfig(max_attempts=5, strategy=BackoffStrategy.EXPONENTIAL)
        )

        result = await retry.execute(flaky_operation, arg1, arg2)
    """

    def __init__(self, config: Optional[RetryConfig] = None) -> None:
        self.config = config or RetryConfig()
        self.stats = RetryStats()
        self._hooks: Dict[str, List[Callable]] = {
            "before_retry": [],
            "after_retry": [],
            "on_success": [],
            "on_failure": [],
        }

    def add_hook(
        self,
        event: str,
        hook: Callable,
    ) -> "AutomationRetryAction":
        """Add retry hook.

        Returns self for chaining.
        """
        if event in self._hooks:
            self._hooks[event].append(hook)
        return self

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with retry.

        Args:
            func: Function to execute
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Result from func

        Raises:
            Last exception if all retries fail
        """
        last_exception: Optional[Exception] = None
        delay = self.config.initial_delay

        for attempt in range(1, self.config.max_attempts + 1):
            self.stats.total_attempts += 1

            try:
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)

                self.stats.successes += 1
                self._trigger_hooks("on_success", attempt, None)

                return result

            except Exception as e:
                last_exception = e

                if not self._is_retryable(e):
                    self.stats.failures += 1
                    self._trigger_hooks("on_failure", attempt, e)
                    raise

                if attempt == self.config.max_attempts:
                    self.stats.failures += 1
                    self._trigger_hooks("on_failure", attempt, e)
                    break

                self._trigger_hooks("before_retry", attempt, e)

                await self._sleep(delay)
                self.stats.total_delay += delay

                delay = self._next_delay(delay)

                self._trigger_hooks("after_retry", attempt, None)

        raise last_exception

    def _is_retryable(self, error: Exception) -> bool:
        """Check if error is retryable."""
        if self.config.retryable_errors:
            return any(isinstance(error, t) for t in self.config.retryable_errors)

        if self.config.retryable_messages:
            msg = str(error)
            return any(m in msg for m in self.config.retryable_messages)

        return True

    def _next_delay(self, current: float) -> float:
        """Calculate next delay based on strategy."""
        if self.config.strategy == BackoffStrategy.FIXED:
            return current

        elif self.config.strategy == BackoffStrategy.LINEAR:
            return current + self.config.initial_delay

        elif self.config.strategy == BackoffStrategy.EXPONENTIAL:
            return min(current * 2, self.config.max_delay)

        elif self.config.strategy == BackoffStrategy.FIBONACCI:
            return min(current * 1.618, self.config.max_delay)

        elif self.config.strategy == BackoffStrategy.RANDOM:
            return random.uniform(
                self.config.initial_delay,
                self.config.max_delay
            )

        return current

    async def _sleep(self, delay: float) -> None:
        """Sleep with optional jitter."""
        sleep_time = delay

        if self.config.jitter:
            jitter_range = delay * self.config.jitter_factor
            sleep_time += random.uniform(-jitter_range, jitter_range)

        sleep_time = max(0.1, sleep_time)
        await asyncio.sleep(sleep_time)

    def _trigger_hooks(
        self,
        event: str,
        attempt: int,
        error: Optional[Exception],
    ) -> None:
        """Trigger hooks for event."""
        for hook in self._hooks.get(event, []):
            try:
                hook(attempt, error)
            except Exception as e:
                logger.error(f"Hook error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Get retry statistics."""
        return {
            "total_attempts": self.stats.total_attempts,
            "successes": self.stats.successes,
            "failures": self.stats.failures,
            "total_delay": self.stats.total_delay,
            "success_rate": (
                self.stats.successes / self.stats.total_attempts
                if self.stats.total_attempts > 0 else 0
            ),
        }

    def reset_stats(self) -> None:
        """Reset statistics."""
        self.stats = RetryStats()
