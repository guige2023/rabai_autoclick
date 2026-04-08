"""
API Circuit Breaker Action Module.

Provides fault tolerance patterns including circuit breaker,
bulkhead isolation, retry with backoff, and fallback strategies.
"""

from typing import Any, Callable, Dict, List, Optional, Set, TypeVar, Generic
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging
import time
import uuid
from collections import deque

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class FailureReason(Enum):
    """Reasons for circuit breaker trip."""
    TIMEOUT = "timeout"
    ERROR = "error"
    REJECTED = "rejected"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout: float = 60.0
    half_open_max_calls: int = 3
    exclude_errors: Set[type] = field(default_factory=set)


@dataclass
class CircuitBreakerStats:
    """Statistics for circuit breaker."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    timeout_calls: int = 0
    state_changes: int = 0
    last_state_change: Optional[datetime] = None

    @property
    def failure_rate(self) -> float:
        """Get failure rate."""
        if self.total_calls == 0:
            return 0.0
        return self.failed_calls / self.total_calls

    @property
    def success_rate(self) -> float:
        """Get success rate."""
        if self.total_calls == 0:
            return 0.0
        return self.successful_calls / self.total_calls


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    def __init__(self, name: str, timeout: float):
        self.name = name
        self.timeout = timeout
        super().__init__(f"Circuit breaker '{name}' is open. Retry after {timeout} seconds.")


class CircuitBreaker:
    """Circuit breaker implementation."""

    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
        fallback: Optional[Callable] = None
    ):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.fallback = fallback
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._last_failure_reason: Optional[FailureReason] = None
        self._half_open_calls = 0
        self.stats = CircuitBreakerStats()

    @property
    def state(self) -> CircuitState:
        """Get current circuit state."""
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
        return self._state

    @property
    def is_closed(self) -> bool:
        """Check if circuit is closed."""
        return self.state == CircuitState.CLOSED

    @property
    def is_open(self) -> bool:
        """Check if circuit is open."""
        return self.state == CircuitState.OPEN

    @property
    def is_half_open(self) -> bool:
        """Check if circuit is half-open."""
        return self.state == CircuitState.HALF_OPEN

    def _should_attempt_reset(self) -> bool:
        """Check if should attempt to reset circuit."""
        if not self._last_failure_time:
            return False
        elapsed = (datetime.now() - self._last_failure_time).total_seconds()
        return elapsed >= self.config.timeout

    def _transition_to_half_open(self):
        """Transition to half-open state."""
        self._state = CircuitState.HALF_OPEN
        self._half_open_calls = 0
        self._record_state_change()

    def _transition_to_open(self, reason: FailureReason):
        """Transition to open state."""
        if self._state != CircuitState.OPEN:
            self._state = CircuitState.OPEN
            self._last_failure_time = datetime.now()
            self._last_failure_reason = reason
            self._failure_count = 0
            self._record_state_change()

    def _transition_to_closed(self):
        """Transition to closed state."""
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._record_state_change()

    def _record_state_change(self):
        """Record state change."""
        self.stats.state_changes += 1
        self.stats.last_state_change = datetime.now()

    def _on_success(self):
        """Handle successful call."""
        self.stats.successful_calls += 1

        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                self._transition_to_closed()

        elif self._state == CircuitState.CLOSED:
            self._failure_count = max(0, self._failure_count - 1)

    def _on_failure(self, reason: FailureReason):
        """Handle failed call."""
        self.stats.failed_calls += 1
        self._failure_count += 1
        self._last_failure_reason = reason

        if reason == FailureReason.TIMEOUT:
            self.stats.timeout_calls += 1

        if self._state == CircuitState.HALF_OPEN:
            self._transition_to_open(reason)

        elif self._state == CircuitState.CLOSED:
            if self._failure_count >= self.config.failure_threshold:
                self._transition_to_open(reason)

    def _on_rejected(self):
        """Handle rejected call."""
        self.stats.rejected_calls += 1

    async def call(
        self,
        func: Callable[..., T],
        *args,
        **kwargs
    ) -> T:
        """Execute function with circuit breaker protection."""
        self.stats.total_calls += 1

        if self.state == CircuitState.OPEN:
            self._on_rejected()
            if self.fallback:
                return self.fallback()
            raise CircuitBreakerOpenError(self.name, self.config.timeout)

        if self._state == CircuitState.HALF_OPEN:
            if self._half_open_calls >= self.config.half_open_max_calls:
                self._on_rejected()
                if self.fallback:
                    return self.fallback()
                raise CircuitBreakerOpenError(self.name, self.config.timeout)
            self._half_open_calls += 1

        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = await asyncio.to_thread(func, *args, **kwargs)
            self._on_success()
            return result

        except tuple(self.config.exclude_errors):
            self._on_success()
            raise

        except asyncio.TimeoutError:
            self._on_failure(FailureReason.TIMEOUT)
            if self.fallback:
                return self.fallback()
            raise

        except Exception as e:
            self._on_failure(FailureReason.ERROR)
            if self.fallback:
                return self.fallback()
            raise

    def reset(self):
        """Manually reset circuit breaker."""
        self._transition_to_closed()


class Bulkhead:
    """Bulkhead isolation pattern."""

    def __init__(
        self,
        max_concurrent_calls: int = 10,
        max_queue_size: int = 100
    ):
        self.max_concurrent_calls = max_concurrent_calls
        self.max_queue_size = max_queue_size
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._active_calls = 0
        self._queued_calls = 0
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        """Acquire bulkhead slot."""
        async with self._lock:
            if self._semaphore is None:
                self._semaphore = asyncio.Semaphore(self.max_concurrent_calls)

        if self._active_calls >= self.max_concurrent_calls:
            if self._queued_calls >= self.max_queue_size:
                raise Exception("Bulkhead queue full - rejected")
            self._queued_calls += 1

        await self._semaphore.acquire()
        self._active_calls += 1
        self._queued_calls = max(0, self._queued_calls - 1)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Release bulkhead slot."""
        self._active_calls = max(0, self._active_calls - 1)
        self._semaphore.release()


class RetryStrategy:
    """Retry strategy with backoff."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter

    def get_delay(self, attempt: int) -> float:
        """Calculate delay for given attempt."""
        delay = min(
            self.base_delay * (self.exponential_base ** attempt),
            self.max_delay
        )

        if self.jitter:
            import random
            delay = delay * (0.5 + random.random())

        return delay


async def retry_with_backoff(
    func: Callable,
    strategy: RetryStrategy,
    retryable_exceptions: Tuple[type, ...] = (Exception,)
) -> Any:
    """Execute function with retry and backoff."""
    last_exception = None

    for attempt in range(strategy.max_attempts):
        try:
            if asyncio.iscoroutinefunction(func):
                return await func()
            else:
                return await asyncio.to_thread(func)

        except retryable_exceptions as e:
            last_exception = e
            if attempt < strategy.max_attempts - 1:
                delay = strategy.get_delay(attempt)
                logger.warning(f"Retry attempt {attempt + 1} after {delay:.2f}s: {e}")
                await asyncio.sleep(delay)
            else:
                logger.error(f"All {strategy.max_attempts} attempts failed")

    if last_exception:
        raise last_exception


@dataclass
class FallbackResult:
    """Result from fallback."""
    used_fallback: bool
    result: Any
    error: Optional[str] = None


class FallbackManager:
    """Manages fallback strategies."""

    def __init__(self):
        self._fallbacks: Dict[str, Callable] = {}

    def register(self, name: str, fallback: Callable):
        """Register fallback handler."""
        self._fallbacks[name] = fallback

    def get(self, name: str) -> Optional[Callable]:
        """Get fallback handler."""
        return self._fallbacks.get(name)

    async def execute_with_fallback(
        self,
        name: str,
        primary: Callable,
        *args,
        **kwargs
    ) -> FallbackResult:
        """Execute with fallback on failure."""
        try:
            if asyncio.iscoroutinefunction(primary):
                result = await primary(*args, **kwargs)
            else:
                result = await asyncio.to_thread(primary, *args, **kwargs)

            return FallbackResult(used_fallback=False, result=result)

        except Exception as e:
            fallback = self._fallbacks.get(name)
            if fallback:
                try:
                    if asyncio.iscoroutinefunction(fallback):
                        result = await fallback(*args, **kwargs)
                    else:
                        result = await asyncio.to_thread(fallback, *args, **kwargs)
                    return FallbackResult(used_fallback=True, result=result, error=str(e))
                except Exception as fallback_error:
                    return FallbackResult(
                        used_fallback=True,
                        result=None,
                        error=f"Primary: {e}, Fallback: {fallback_error}"
                    )

            return FallbackResult(used_fallback=False, result=None, error=str(e))


class ResilientClient:
    """Client with all resilience patterns."""

    def __init__(
        self,
        circuit_breaker: Optional[CircuitBreaker] = None,
        bulkhead: Optional[Bulkhead] = None,
        retry_strategy: Optional[RetryStrategy] = None,
        fallback_manager: Optional[FallbackManager] = None
    ):
        self.circuit_breaker = circuit_breaker
        self.bulkhead = bulkhead
        self.retry_strategy = retry_strategy
        self.fallback_manager = fallback_manager

    async def request(
        self,
        func: Callable,
        *args,
        use_bulkhead: bool = True,
        use_retry: bool = True,
        **kwargs
    ) -> Any:
        """Make resilient request."""
        async def execute():
            if use_bulkhead and self.bulkhead:
                async with self.bulkhead:
                    return await func(*args, **kwargs)
            return await func(*args, **kwargs)

        if use_retry and self.retry_strategy:
            async def with_retry():
                return await retry_with_backoff(
                    execute,
                    self.retry_strategy
                )
            execute = with_retry

        if self.circuit_breaker:
            return await self.circuit_breaker.call(execute)
        return await execute()


async def demo_function() -> str:
    """Demo function for testing."""
    await asyncio.sleep(0.1)
    return "success"


def demo_fallback() -> str:
    """Demo fallback function."""
    return "fallback result"


async def main():
    """Demonstrate resilience patterns."""
    cb_config = CircuitBreakerConfig(failure_threshold=3, timeout=5)
    circuit_breaker = CircuitBreaker("demo", config=cb_config, fallback=demo_fallback)

    print(f"Initial state: {circuit_breaker.state}")

    for i in range(5):
        try:
            result = await circuit_breaker.call(demo_function)
            print(f"Call {i + 1}: {result}")
        except CircuitBreakerOpenError as e:
            print(f"Circuit open: {e}")
        except Exception as e:
            print(f"Error: {e}")

    print(f"Final state: {circuit_breaker.state}")
    print(f"Stats: {circuit_breaker.stats}")

    retry_strategy = RetryStrategy(max_attempts=3, base_delay=0.1)
    result = await retry_with_backoff(demo_function, retry_strategy)
    print(f"Retry result: {result}")


if __name__ == "__main__":
    asyncio.run(main())
