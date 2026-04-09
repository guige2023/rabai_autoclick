"""API Resilience Action module.

Provides comprehensive resilience patterns for API calls including
retry with backoff, timeout handling, bulkhead isolation, and
fallback responses.
"""

import asyncio
import random
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


@dataclass
class ResilienceConfig:
    """Configuration for resilience patterns."""

    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    exponential_base: float = 2.0
    jitter: bool = True
    timeout: float = 30.0
    retry_on: tuple = (Exception,)
    fallback_value: Optional[Any] = None


@dataclass
class ResilienceMetrics:
    """Metrics for resilience operations."""

    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    retried_calls: int = 0
    timed_out_calls: int = 0
    fallback_calls: int = 0
    total_retry_delay: float = 0.0
    _last_call_times: list = field(default_factory=list)

    def record_success(self, retry_delay: float = 0.0) -> None:
        """Record successful call."""
        self.total_calls += 1
        self.successful_calls += 1
        self.total_retry_delay += retry_delay

    def record_failure(self) -> None:
        """Record failed call."""
        self.total_calls += 1
        self.failed_calls += 1

    def record_timeout(self) -> None:
        """Record timeout."""
        self.total_calls += 1
        self.timed_out_calls += 1

    def record_fallback(self) -> None:
        """Record fallback used."""
        self.total_calls += 1
        self.fallback_calls += 1

    def record_retry(self) -> None:
        """Record retry."""
        self.retried_calls += 1

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        if self.total_calls == 0:
            return 0.0
        return self.successful_calls / self.total_calls

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_calls": self.total_calls,
            "successful_calls": self.successful_calls,
            "failed_calls": self.failed_calls,
            "retried_calls": self.retried_calls,
            "timed_out_calls": self.timed_out_calls,
            "fallback_calls": self.fallback_calls,
            "total_retry_delay": self.total_retry_delay,
            "success_rate": self.success_rate,
        }


def calculate_backoff(
    attempt: int,
    base_delay: float,
    max_delay: float,
    exponential_base: float,
    jitter: bool,
) -> float:
    """Calculate delay for given retry attempt.

    Args:
        attempt: Retry attempt number (0-indexed)
        base_delay: Base delay in seconds
        max_delay: Maximum delay cap
        exponential_base: Exponential multiplier
        jitter: Whether to add randomness

    Returns:
        Calculated delay in seconds
    """
    delay = base_delay * (exponential_base**attempt)
    delay = min(delay, max_delay)

    if jitter:
        delay = delay * (0.5 + random.random() * 0.5)

    return delay


async def call_with_resilience(
    func: Callable[..., Any],
    config: ResilienceConfig,
    metrics: Optional[ResilienceMetrics] = None,
    *args: Any,
    **kwargs: Any,
) -> Any:
    """Execute API call with resilience patterns.

    Args:
        func: Async function to call
        config: Resilience configuration
        metrics: Optional metrics tracker
        *args: Positional arguments
        **kwargs: Keyword arguments

    Returns:
        Function result or fallback value
    """
    total_retry_delay = 0.0
    last_exception: Optional[Exception] = None

    for attempt in range(config.max_retries + 1):
        try:
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=config.timeout,
            )
            if attempt > 0 and metrics:
                metrics.record_success(total_retry_delay)
            return result

        except asyncio.TimeoutError:
            last_exception = TimeoutError(f"Call timed out after {config.timeout}s")
            if attempt < config.max_retries:
                delay = calculate_backoff(
                    attempt,
                    config.base_delay,
                    config.max_delay,
                    config.exponential_base,
                    config.jitter,
                )
                total_retry_delay += delay
                if metrics:
                    metrics.record_retry()
                await asyncio.sleep(delay)
            else:
                if metrics:
                    metrics.record_timeout()

        except config.retry_on as e:
            last_exception = e
            if attempt < config.max_retries:
                delay = calculate_backoff(
                    attempt,
                    config.base_delay,
                    config.max_delay,
                    config.exponential_base,
                    config.jitter,
                )
                total_retry_delay += delay
                if metrics:
                    metrics.record_retry()
                await asyncio.sleep(delay)
            else:
                if metrics:
                    metrics.record_failure()
        except Exception as e:
            if metrics:
                metrics.record_failure()
            raise e

    if config.fallback_value is not None:
        if metrics:
            metrics.record_fallback()
        return config.fallback_value

    if last_exception:
        raise last_exception

    raise RuntimeError("Resilience call failed with no exception")


class Bulkhead:
    """Bulkhead pattern for resource isolation."""

    def __init__(self, max_concurrent: int = 10, max_queue_size: int = 100):
        self.max_concurrent = max_concurrent
        self.max_queue_size = max_queue_size
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._active = 0
        self._queue_full = False

    async def __aenter__(self) -> "Bulkhead":
        """Acquire bulkhead slot."""
        if self._semaphore.locked():
            if self._active >= self.max_concurrent:
                if self._queue_full:
                    raise RuntimeError("Bulkhead queue full")
                self._queue_full = True
        await self._semaphore.acquire()
        self._active += 1
        self._queue_full = False
        return self

    async def __aexit__(self, *args: Any) -> None:
        """Release bulkhead slot."""
        self._active -= 1
        self._semaphore.release()

    @property
    def available(self) -> int:
        """Available slots."""
        return max(0, self.max_concurrent - self._active)
