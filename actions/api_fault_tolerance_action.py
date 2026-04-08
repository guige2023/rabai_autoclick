"""
API Fault Tolerance Action Module.

Provides fault tolerance patterns including
timeout handling, bulkhead isolation, and fallback strategies.
"""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TypeVar
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import asyncio
import logging
import time

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FaultToleranceStrategy(Enum):
    """Fault tolerance strategies."""
    TIMEOUT = "timeout"
    RETRY = "retry"
    CIRCUIT_BREAKER = "circuit_breaker"
    BULKHEAD = "bulkhead"
    FALLBACK = "fallback"
    RATE_LIMIT = "rate_limit"


@dataclass
class TimeoutConfig:
    """Timeout configuration."""
    timeout_seconds: float
    timeout_strategy: str = "cancel"


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    retryable_exceptions: Tuple[type, ...] = (Exception,)


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3


@dataclass
class BulkheadConfig:
    """Bulkhead configuration."""
    max_concurrent_calls: int = 10
    max_queue_size: int = 100


@dataclass
class FaultToleranceMetrics:
    """Fault tolerance metrics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    timeout_calls: int = 0
    rejected_calls: int = 0
    circuit_breaker_trips: int = 0


class TimeoutHandler:
    """Handles timeout for operations."""

    def __init__(self, config: TimeoutConfig):
        self.config = config
        self._timed_out = False

    async def execute(
        self,
        func: Callable[..., T],
        *args,
        **kwargs
    ) -> T:
        """Execute with timeout."""
        self._timed_out = False

        try:
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.timeout_seconds
            )
            return result

        except asyncio.TimeoutError:
            self._timed_out = True
            logger.warning(f"Operation timed out after {self.config.timeout_seconds}s")
            raise TimeoutError(f"Operation timed out after {self.config.timeout_seconds}s")


class RetryHandler:
    """Handles retry with backoff."""

    def __init__(self, config: RetryConfig):
        self.config = config

    async def execute(
        self,
        func: Callable[..., T],
        *args,
        **kwargs
    ) -> T:
        """Execute with retry."""
        last_exception = None

        for attempt in range(self.config.max_attempts):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return await asyncio.to_thread(func, *args, **kwargs)

            except self.config.retryable_exceptions as e:
                last_exception = e
                if attempt < self.config.max_attempts - 1:
                    delay = min(
                        self.config.base_delay * (self.config.exponential_base ** attempt),
                        self.config.max_delay
                    )
                    logger.warning(f"Retry attempt {attempt + 1} after {delay:.2f}s: {e}")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"All {self.config.max_attempts} attempts failed")

        if last_exception:
            raise last_exception


class CircuitBreakerState:
    """Circuit breaker state."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerHandler:
    """Handles circuit breaker pattern."""

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[datetime] = None
        self.half_open_calls = 0

    def can_execute(self) -> bool:
        """Check if can execute."""
        if self.state == CircuitBreakerState.CLOSED:
            return True

        if self.state == CircuitBreakerState.OPEN:
            elapsed = (datetime.now() - self.last_failure_time).total_seconds()
            if elapsed >= self.config.timeout_seconds:
                self.state = CircuitBreakerState.HALF_OPEN
                self.half_open_calls = 0
                return True
            return False

        if self.state == CircuitBreakerState.HALF_OPEN:
            return self.half_open_calls < self.config.half_open_max_calls

        return True

    def record_success(self):
        """Record successful call."""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
                self.success_count = 0
        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)

    def record_failure(self):
        """Record failed call."""
        self.failure_count += 1
        self.last_failure_time = datetime.now()

        if self.state == CircuitBreakerState.HALF_OPEN:
            self.state = CircuitBreakerState.OPEN

        elif self.state == CircuitBreakerState.CLOSED:
            if self.failure_count >= self.config.failure_threshold:
                self.state = CircuitBreakerState.OPEN


class BulkheadHandler:
    """Handles bulkhead isolation."""

    def __init__(self, config: BulkheadConfig):
        self.config = config
        self.semaphore = asyncio.Semaphore(config.max_concurrent_calls)
        self.active_calls = 0
        self.queued_calls = 0
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=config.max_queue_size)

    async def execute(
        self,
        func: Callable[..., T],
        *args,
        **kwargs
    ) -> T:
        """Execute within bulkhead."""
        async with self.semaphore:
            self.active_calls += 1
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return await asyncio.to_thread(func, *args, **kwargs)
            finally:
                self.active_calls -= 1


class FaultToleranceManager:
    """Manages all fault tolerance strategies."""

    def __init__(self):
        self.strategies: Dict[FaultToleranceStrategy, Any] = {}
        self.metrics = FaultToleranceMetrics()
        self.circuit_breakers: Dict[str, CircuitBreakerHandler] = {}
        self.bulkheads: Dict[str, BulkheadHandler] = {}

    def set_timeout(self, config: TimeoutConfig):
        """Set timeout strategy."""
        self.strategies[FaultToleranceStrategy.TIMEOUT] = TimeoutHandler(config)

    def set_retry(self, config: RetryConfig):
        """Set retry strategy."""
        self.strategies[FaultToleranceStrategy.RETRY] = RetryHandler(config)

    def add_circuit_breaker(self, name: str, config: CircuitBreakerConfig):
        """Add circuit breaker."""
        self.circuit_breakers[name] = CircuitBreakerHandler(name, config)

    def add_bulkhead(self, name: str, config: BulkheadConfig):
        """Add bulkhead."""
        self.bulkheads[name] = BulkheadHandler(config)

    async def execute(
        self,
        func: Callable,
        *args,
        timeout: Optional[TimeoutConfig] = None,
        retry: Optional[RetryConfig] = None,
        circuit_breaker: Optional[str] = None,
        bulkhead: Optional[str] = None,
        fallback: Optional[Callable] = None,
        **kwargs
    ) -> Any:
        """Execute with fault tolerance."""
        self.metrics.total_calls += 1

        if circuit_breaker and circuit_breaker in self.circuit_breakers:
            cb = self.circuit_breakers[circuit_breaker]
            if not cb.can_execute():
                self.metrics.rejected_calls += 1
                if fallback:
                    return fallback()
                raise Exception(f"Circuit breaker {circuit_breaker} is open")

            try:
                result = await self._execute_internal(
                    func, timeout, retry, *args, **kwargs
                )
                cb.record_success()
                self.metrics.successful_calls += 1
                return result

            except Exception as e:
                cb.record_failure()
                self.metrics.failed_calls += 1
                if fallback:
                    return fallback()
                raise

        if bulkhead and bulkhead in self.bulkheads:
            bh = self.bulkheads[bulkhead]
            result = await bh.execute(
                self._execute_internal, func, timeout, retry, *args, **kwargs
            )
            self.metrics.successful_calls += 1
            return result

        return await self._execute_internal(func, timeout, retry, *args, **kwargs)

    async def _execute_internal(
        self,
        func: Callable,
        timeout: Optional[TimeoutConfig],
        retry: Optional[RetryConfig],
        *args,
        **kwargs
    ) -> Any:
        """Internal execution with timeout and retry."""
        if timeout:
            handler = TimeoutHandler(timeout)
            func = handler.execute

        if retry:
            retry_handler = RetryHandler(retry)
            original_func = func

            async def wrapped():
                return await original_func(*args, **kwargs)

            return await retry_handler.execute(wrapped)

        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        return await asyncio.to_thread(func, *args, **kwargs)


async def main():
    """Demonstrate fault tolerance."""
    manager = FaultToleranceManager()

    manager.add_circuit_breaker("api", CircuitBreakerConfig(failure_threshold=3))

    async def demo_func():
        await asyncio.sleep(0.1)
        return "success"

    try:
        result = await manager.execute(
            demo_func,
            circuit_breaker="api"
        )
        print(f"Result: {result}")
        print(f"Metrics: {manager.metrics.total_calls}")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    asyncio.run(main())
