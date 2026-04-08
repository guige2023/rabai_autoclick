"""API Retry Action Module.

Provides configurable retry logic with exponential backoff,
circuit breaker pattern, and request deduplication.
"""
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union

T = TypeVar("T")


class RetryStrategy(Enum):
    """Retry strategy type."""
    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    CONSTANT = "constant"
    FIBONACCI = "fibonacci"


class CircuitState(Enum):
    """Circuit breaker state."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    initial_delay: float = 0.5
    max_delay: float = 30.0
    strategy: RetryStrategy = RetryStrategy.EXPONENTIAL
    jitter: bool = True
    jitter_factor: float = 0.1
    retry_on: Optional[List[type]] = None
    retry_on_messages: Optional[List[str]] = None


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_attempts: int = 3
    success_threshold: int = 2


@dataclass
class RetryStats:
    """Retry statistics."""
    total_attempts: int = 0
    successful_retries: int = 0
    failed_retries: int = 0
    circuit_trips: int = 0


class CircuitBreaker:
    """Circuit breaker for API calls.

    Example:
        cb = CircuitBreaker(CircuitBreakerConfig(failure_threshold=5))
        await cb.call(api_function, arg1, arg2)
    """

    def __init__(self, config: CircuitBreakerConfig) -> None:
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.half_open_attempts = 0

    async def call(
        self,
        func: Callable[..., T],
        *args: Any,
        **kwargs: Any,
    ) -> T:
        """Execute function with circuit breaker."""
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self.state = CircuitState.HALF_OPEN
                self.half_open_attempts = 0
            else:
                raise CircuitOpenError("Circuit breaker is OPEN")

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            self._on_success()
            return result

        except Exception as e:
            self._on_failure()
            raise

    def _should_attempt_reset(self) -> bool:
        """Check if should attempt reset."""
        if self.last_failure_time is None:
            return True
        return time.time() - self.last_failure_time >= self.config.recovery_timeout

    def _on_success(self) -> None:
        """Handle successful call."""
        self.failure_count = 0
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitState.CLOSED
                self.success_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        if self.state == CircuitState.HALF_OPEN:
            self.state = CircuitState.OPEN
        elif self.failure_count >= self.config.failure_threshold:
            self.state = CircuitState.OPEN


class CircuitOpenError(Exception):
    """Raised when circuit is open."""
    pass


class APIRetryAction:
    """API Retry Handler with circuit breaker.

    Example:
        retry = APIRetryAction(
            retry_config=RetryConfig(max_attempts=3),
            circuit_config=CircuitBreakerConfig(failure_threshold=5)
        )
        result = await retry.execute(api_call_function, arg1, arg2)
    """

    def __init__(
        self,
        retry_config: Optional[RetryConfig] = None,
        circuit_config: Optional[CircuitBreakerConfig] = None,
    ) -> None:
        self.retry_config = retry_config or RetryConfig()
        self.circuit_config = circuit_config or CircuitBreakerConfig()
        self.circuit_breaker = CircuitBreaker(self.circuit_config)
        self.stats = RetryStats()
        self._dedup_cache: Dict[str, float] = {}
        self._dedup_ttl = 60.0

    async def execute(
        self,
        func: Callable[..., T],
        *args: Any,
        dedup_key: Optional[str] = None,
        **kwargs: Any,
    ) -> T:
        """Execute function with retry and circuit breaker.

        Args:
            func: Async or sync function to call
            *args: Positional arguments for func
            dedup_key: Optional deduplication key
            **kwargs: Keyword arguments for func

        Returns:
            Result from func

        Raises:
            The last exception if all retries fail
        """
        if dedup_key and self._is_duplicate(dedup_key):
            raise DuplicateRequestError(f"Duplicate request: {dedup_key}")

        last_exception: Optional[Exception] = None
        delay = self.retry_config.initial_delay

        for attempt in range(1, self.retry_config.max_attempts + 1):
            self.stats.total_attempts += 1

            try:
                result = await self.circuit_breaker.call(func, *args, **kwargs)
                if attempt > 1:
                    self.stats.successful_retries += 1
                if dedup_key:
                    self._mark_deduplicated(dedup_key)
                return result

            except Exception as e:
                last_exception = e

                if not self._should_retry(e):
                    self.stats.failed_retries += 1
                    raise

                if attempt == self.retry_config.max_attempts:
                    self.stats.failed_retries += 1
                    break

                await self._sleep(delay, attempt)
                delay = self._next_delay(delay)

        raise last_exception

    def _should_retry(self, error: Exception) -> bool:
        """Check if error is retryable."""
        if self.retry_config.retry_on:
            return any(isinstance(error, t) for t in self.retry_config.retry_on)

        if self.retry_config.retry_on_messages:
            msg = str(error)
            return any(m in msg for m in self.retry_config.retry_on_messages)

        return True

    def _next_delay(self, current_delay: float) -> float:
        """Calculate next delay based on strategy."""
        strategy = self.retry_config.strategy
        max_delay = self.retry_config.max_delay

        if strategy == RetryStrategy.EXPONENTIAL:
            delay = current_delay * 2
        elif strategy == RetryStrategy.LINEAR:
            delay = current_delay + self.retry_config.initial_delay
        elif strategy == RetryStrategy.FIBONACCI:
            delay = current_delay * 1.618
        else:
            delay = self.retry_config.initial_delay

        if self.retry_config.jitter:
            jitter_range = current_delay * self.retry_config.jitter_factor
            delay += random.uniform(-jitter_range, jitter_range)

        return min(delay, max_delay)

    async def _sleep(self, delay: float, attempt: int) -> None:
        """Sleep with jitter."""
        if self.retry_config.jitter:
            jitter = delay * self.retry_config.jitter_factor
            delay += random.uniform(-jitter, jitter)

        await asyncio.sleep(max(0.1, delay))

    def _is_duplicate(self, key: str) -> bool:
        """Check for duplicate request."""
        if key not in self._dedup_cache:
            return False
        if time.time() > self._dedup_cache[key] + self._dedup_ttl:
            del self._dedup_cache[key]
            return False
        return True

    def _mark_deduplicated(self, key: str) -> None:
        """Mark request as deduplicated."""
        self._dedup_cache[key] = time.time()

    def get_stats(self) -> Dict[str, Any]:
        """Get retry statistics."""
        return {
            "total_attempts": self.stats.total_attempts,
            "successful_retries": self.stats.successful_retries,
            "failed_retries": self.stats.failed_retries,
            "circuit_state": self.circuit_breaker.state.value,
            "circuit_failures": self.circuit_breaker.failure_count,
        }


class DuplicateRequestError(Exception):
    """Raised for duplicate requests."""
    pass
