"""API Resilience Action with bulkhead, retries, and fault tolerance.

This module provides resilience patterns for API clients:
- Bulkhead isolation (thread/connection pool separation)
- Automatic timeout management
- Retry with exponential backoff
- Fallback mechanisms
- Health tracking and circuit breaker integration
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Awaitable, TypeVar
import httpx

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FailureMode(Enum):
    """How to handle operation failures."""

    FAIL_FAST = "fail_fast"  # Return immediately
    RETRY = "retry"  # Retry with backoff
    FALLBACK = "fallback"  # Use fallback value
    CIRCUIT_BREAKER = "circuit_breaker"  # Use circuit breaker


@dataclass
class BulkheadConfig:
    """Configuration for bulkhead isolation."""

    max_concurrent_calls: int = 10
    max_queue_size: int = 100
    timeout: float = 30.0


@dataclass
class RetryConfig:
    """Configuration for retry behavior."""

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    retryable_status_codes: list[int] = field(
        default_factory=lambda: [408, 429, 500, 502, 503, 504]
    )
    retryable_exceptions: list[type[Exception]] = field(
        default_factory=lambda: [httpx.TimeoutException, httpx.NetworkError]
    )


@dataclass
class CircuitBreakerState:
    """Circuit breaker state tracking."""

    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 30.0
    state: str = "closed"  # closed, open, half_open
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: float = 0.0


@dataclass
class FallbackConfig:
    """Configuration for fallback behavior."""

    fallback_value: Any = None
    fallback_func: Callable[[], Awaitable[T]] | None = None
    cache_fallback: bool = True
    cache_ttl: float = 300.0
    _cached_value: Any = field(default=None, repr=False)
    _cache_time: float = field(default=0.0, repr=False)


@dataclass
class ResilienceMetrics:
    """Metrics for resilience patterns."""

    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    retried_calls: int = 0
    fallback_calls: int = 0
    circuit_open: int = 0
    bulkhead_rejected: int = 0
    timeout_calls: int = 0
    total_retry_attempts: int = 0


class Bulkhead:
    """Semaphore-based bulkhead for concurrency control."""

    def __init__(self, config: BulkheadConfig):
        """Initialize bulkhead.

        Args:
            config: Bulkhead configuration
        """
        self.config = config
        self._semaphore = asyncio.Semaphore(config.max_concurrent_calls)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=config.max_queue_size)
        self._active_calls = 0

    async def enter(self) -> bool:
        """Attempt to enter the bulkhead.

        Returns:
            True if entry granted, False if rejected
        """
        if self._semaphore.locked():
            # Try to queue
            try:
                self._queue.put_nowait(True)
                return True
            except asyncio.QueueFull:
                return False

        await self._semaphore.acquire()
        self._active_calls += 1
        return True

    async def exit(self) -> None:
        """Exit the bulkhead."""
        if not self._queue.empty():
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                pass
        else:
            self._semaphore.release()
            self._active_calls -= 1

    def get_stats(self) -> dict[str, int]:
        """Get bulkhead statistics."""
        return {
            "active_calls": self._active_calls,
            "queued_calls": self._queue.qsize(),
            "available": self.config.max_concurrent_calls - self._active_calls,
        }


class CircuitBreaker:
    """Circuit breaker for fault tolerance."""

    def __init__(self, config: CircuitBreakerState):
        """Initialize circuit breaker.

        Args:
            config: Circuit breaker configuration
        """
        self.config = config
        self.state = config.state

    def is_allowed(self) -> bool:
        """Check if operation is allowed."""
        if self.state == "closed":
            return True

        if self.state == "open":
            if time.time() - self.config.last_failure_time >= self.config.timeout:
                self.state = "half_open"
                self.config.success_count = 0
                logger.info("Circuit breaker entering half-open state")
                return True
            return False

        # half_open - allow limited calls
        return True

    def record_success(self) -> None:
        """Record a successful call."""
        if self.state == "half_open":
            self.config.success_count += 1
            if self.config.success_count >= self.config.success_threshold:
                self.state = "closed"
                self.config.failure_count = 0
                logger.info("Circuit breaker closed")
        elif self.state == "closed":
            self.config.failure_count = max(0, self.config.failure_count - 1)

    def record_failure(self) -> None:
        """Record a failed call."""
        self.config.failure_count += 1
        self.config.last_failure_time = time.time()

        if self.state == "half_open":
            self.state = "open"
            logger.warning("Circuit breaker reopened")

        elif self.config.failure_count >= self.config.failure_threshold:
            self.state = "open"
            logger.warning(f"Circuit breaker opened after {self.config.failure_count} failures")


class APIResilienceAction:
    """API resilience wrapper with bulkhead, retry, and circuit breaker."""

    def __init__(
        self,
        bulkhead: BulkheadConfig | None = None,
        retry: RetryConfig | None = None,
        circuit_breaker: CircuitBreakerState | None = None,
        fallback: FallbackConfig | None = None,
    ):
        """Initialize API resilience action.

        Args:
            bulkhead: Bulkhead configuration
            retry: Retry configuration
            circuit_breaker: Circuit breaker configuration
            fallback: Fallback configuration
        """
        self.bulkhead = Bulkhead(bulkhead or BulkheadConfig())
        self.retry_config = retry or RetryConfig()
        self.circuit_breaker = CircuitBreaker(circuit_breaker or CircuitBreakerState())
        self.fallback_config = fallback or FallbackConfig()
        self.metrics = ResilienceMetrics()

    async def call(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        failure_mode: FailureMode = FailureMode.CIRCUIT_BREAKER,
        **kwargs,
    ) -> T | None:
        """Call a function with resilience patterns.

        Args:
            func: Async function to call
            *args: Positional arguments for the function
            failure_mode: How to handle failures
            **kwargs: Keyword arguments for the function

        Returns:
            Result of the function call or fallback value
        """
        self.metrics.total_calls += 1

        # Check circuit breaker
        if failure_mode == FailureMode.CIRCUIT_BREAKER and not self.circuit_breaker.is_allowed():
            self.metrics.circuit_open += 1
            return await self._handle_failure(failure_mode)

        # Check bulkhead
        if not await self.bulkhead.enter():
            self.metrics.bulkhead_rejected += 1
            logger.warning("Bulkhead rejected call")
            return await self._handle_failure(failure_mode)

        try:
            # Execute with retry
            result = await self._execute_with_retry(func, *args, **kwargs)
            self.circuit_breaker.record_success()
            self.metrics.successful_calls += 1
            return result

        except Exception as e:
            self.circuit_breaker.record_failure()
            self.metrics.failed_calls += 1
            return await self._handle_failure(failure_mode, e)

        finally:
            await self.bulkhead.exit()

    async def _execute_with_retry(
        self,
        func: Callable[..., Awaitable[T]],
        *args,
        **kwargs,
    ) -> T:
        """Execute function with retry logic."""
        last_exception: Exception | None = None

        for attempt in range(self.retry_config.max_attempts):
            try:
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=self.bulkhead.config.timeout,
                )
                if attempt > 0:
                    self.metrics.retried_calls += 1
                    self.metrics.total_retry_attempts += attempt
                return result

            except asyncio.TimeoutError:
                self.metrics.timeout_calls += 1
                last_exception = TimeoutError("Operation timed out")
                logger.warning(f"Timeout on attempt {attempt + 1}")

            except httpx.HTTPStatusError as e:
                if e.response.status_code in self.retry_config.retryable_status_codes:
                    last_exception = e
                    logger.warning(f"Retryable status {e.response.status_code} on attempt {attempt + 1}")
                else:
                    raise

            except Exception as e:
                # Check if exception is retryable
                is_retryable = any(isinstance(e, exc_type) for exc_type in self.retry_config.retryable_exceptions)
                if is_retryable:
                    last_exception = e
                    logger.warning(f"Retryable exception {type(e).__name__} on attempt {attempt + 1}")
                else:
                    raise

            # Wait before retry
            if attempt < self.retry_config.max_attempts - 1:
                delay = min(
                    self.retry_config.base_delay * (self.retry_config.exponential_base ** attempt),
                    self.retry_config.max_delay,
                )
                await asyncio.sleep(delay)

        # All retries exhausted
        if last_exception:
            raise last_exception
        raise Exception("Max retries exceeded")

    async def _handle_failure(
        self,
        failure_mode: FailureMode,
        exception: Exception | None = None,
    ) -> T | None:
        """Handle a failed call based on failure mode."""
        if failure_mode == FailureMode.FAIL_FAST:
            if exception:
                raise exception
            raise Exception("Operation failed")

        elif failure_mode == FailureMode.FALLBACK:
            self.metrics.fallback_calls += 1
            return await self._get_fallback()

        elif failure_mode == FailureMode.CIRCUIT_BREAKER:
            self.metrics.fallback_calls += 1
            return await self._get_fallback()

        return None

    async def _get_fallback(self) -> T | None:
        """Get fallback value."""
        if self.fallback_config.fallback_func:
            # Check cache
            if self.fallback_config.cache_fallback:
                cache_age = time.time() - self.fallback_config._cache_time
                if cache_age < self.fallback_config.cache_ttl:
                    return self.fallback_config._cached_value

            # Execute fallback function
            result = await self.fallback_config.fallback_func()

            # Cache result
            if self.fallback_config.cache_fallback:
                self.fallback_config._cached_value = result
                self.fallback_config._cache_time = time.time()

            return result

        return self.fallback_config.fallback_value

    def get_metrics(self) -> dict[str, Any]:
        """Get resilience metrics."""
        return {
            **self.metrics.__dict__,
            "bulkhead_stats": self.bulkhead.get_stats(),
            "circuit_breaker_state": self.circuit_breaker.state,
            "success_rate": (
                self.metrics.successful_calls / self.metrics.total_calls
                if self.metrics.total_calls > 0 else 0
            ),
        }


async def with_resilience(
    func: Callable[..., Awaitable[T]],
    *args,
    max_retries: int = 3,
    timeout: float = 30.0,
    fallback: T | None = None,
    fallback_func: Callable[[], Awaitable[T]] | None = None,
    **kwargs,
) -> T | None:
    """Convenience function for adding resilience to an API call.

    Args:
        func: Async function to wrap
        *args: Positional arguments
        max_retries: Maximum retry attempts
        timeout: Request timeout
        fallback: Static fallback value
        fallback_func: Async function to call for fallback
        **kwargs: Keyword arguments

    Returns:
        Result or fallback value
    """
    bulkhead = BulkheadConfig(timeout=timeout)
    retry = RetryConfig(max_attempts=max_retries)
    fallback_config = FallbackConfig(
        fallback_value=fallback,
        fallback_func=fallback_func,
    )

    action = APIResilienceAction(
        bulkhead=bulkhead,
        retry=retry,
        fallback=fallback_config,
    )

    return await action.call(func, *args, failure_mode=FailureMode.FALLBACK, **kwargs)
