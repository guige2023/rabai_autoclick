"""API Resilience Action Module.

Provides resilience patterns for API interactions including circuit breaker,
bulkhead isolation, retry policies, and fallback mechanisms.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class BulkheadState(Enum):
    """Bulkhead isolation states."""
    NORMAL = "normal"
    LIMITED = "limited"
    REJECTED = "rejected"


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3


@dataclass
class CircuitBreakerStats:
    """Circuit breaker statistics."""
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state: CircuitState = CircuitState.CLOSED
    last_failure: Optional[float] = None
    last_success: Optional[float] = None


class CircuitBreaker:
    """Circuit breaker pattern implementation."""

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self._name = name
        self._config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._half_open_calls = 0
        self._stats = CircuitBreakerStats()

    @property
    def name(self) -> str:
        return self._name

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def stats(self) -> CircuitBreakerStats:
        self._stats.state = self._state
        self._stats.last_failure = self._last_failure_time
        return self._stats

    def is_available(self) -> bool:
        """Check if the circuit allows calls."""
        if self._state == CircuitState.CLOSED:
            return True

        if self._state == CircuitState.OPEN:
            if time.time() - self._last_failure_time >= self._config.timeout_seconds:
                self._transition_to_half_open()
                return True
            return False

        # HALF_OPEN
        return self._half_open_calls < self._config.half_open_max_calls

    async def call(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute a function through the circuit breaker."""
        if not self.is_available():
            self._stats.rejected_calls += 1
            raise CircuitBreakerOpenError(f"Circuit {self._name} is open")

        try:
            if self._state == CircuitState.HALF_OPEN:
                self._half_open_calls += 1

            self._stats.total_calls += 1
            result = await func(*args, **kwargs)
            self._on_success()
            return result

        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self) -> None:
        """Handle successful call."""
        self._stats.successful_calls += 1
        self._stats.last_success = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self._config.success_threshold:
                self._transition_to_closed()
        else:
            self._failure_count = 0

    def _on_failure(self) -> None:
        """Handle failed call."""
        self._stats.failed_calls += 1
        self._last_failure_time = time.time()

        if self._state == CircuitState.HALF_OPEN:
            self._transition_to_open()
        else:
            self._failure_count += 1
            if self._failure_count >= self._config.failure_threshold:
                self._transition_to_open()

    def _transition_to_open(self) -> None:
        """Transition to OPEN state."""
        logger.warning(f"Circuit {self._name} transitioning to OPEN")
        self._state = CircuitState.OPEN
        self._success_count = 0
        self._half_open_calls = 0

    def _transition_to_half_open(self) -> None:
        """Transition to HALF_OPEN state."""
        logger.info(f"Circuit {self._name} transitioning to HALF_OPEN")
        self._state = CircuitState.HALF_OPEN
        self._half_open_calls = 0
        self._success_count = 0

    def _transition_to_closed(self) -> None:
        """Transition to CLOSED state."""
        logger.info(f"Circuit {self._name} transitioning to CLOSED")
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0

    def reset(self) -> None:
        """Reset the circuit breaker."""
        self._transition_to_closed()
        self._stats = CircuitBreakerStats()


class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open."""
    pass


@dataclass
class BulkheadConfig:
    """Configuration for bulkhead isolation."""
    max_concurrent_calls: int = 10
    max_queue_size: int = 5
    timeout_seconds: float = 30.0


@dataclass
class BulkheadStats:
    """Bulkhead statistics."""
    current_calls: int = 0
    queued_calls: int = 0
    total_calls: int = 0
    rejected_calls: int = 0
    timed_out_calls: int = 0


class Bulkhead:
    """Bulkhead isolation pattern implementation."""

    def __init__(self, name: str, config: Optional[BulkheadConfig] = None):
        self._name = name
        self._config = config or BulkheadConfig()
        self._semaphore = asyncio.Semaphore(self._config.max_concurrent_calls)
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=self._config.max_queue_size)
        self._stats = BulkheadStats()

    @property
    def stats(self) -> BulkheadStats:
        self._stats.current_calls = self._config.max_concurrent_calls - self._semaphore._value
        self._stats.queued_calls = self._queue.qsize()
        return self._stats

    async def execute(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute a function through the bulkhead."""
        self._stats.total_calls += 1

        # Try to acquire semaphore
        try:
            async asyncio.wait_for(
                self._semaphore.acquire(),
                timeout=self._config.timeout_seconds
            )
        except asyncio.TimeoutError:
            self._stats.timed_out_calls += 1
            raise BulkheadRejectedError(f"Bulkhead {self._name} timed out")

        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        finally:
            self._semaphore.release()

    def reset(self) -> None:
        """Reset the bulkhead."""
        self._stats = BulkheadStats()


class BulkheadRejectedError(Exception):
    """Raised when bulkhead rejects a call."""
    pass


class RetryPolicy:
    """Retry policy with various backoff strategies."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        backoff_multiplier: float = 2.0,
        jitter: bool = True
    ):
        self._max_attempts = max_attempts
        self._base_delay = base_delay
        self._max_delay = max_delay
        self._multiplier = backoff_multiplier
        self._jitter = jitter

    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt."""
        delay = min(self._base_delay * (self._multiplier ** (attempt - 1)), self._max_delay)

        if self._jitter:
            import random
            delay = delay * (0.5 + random.random() * 0.5)

        return delay

    async def execute(
        self,
        func: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute a function with retries."""
        last_error = None

        for attempt in range(1, self._max_attempts + 1):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)

            except Exception as e:
                last_error = e
                logger.warning(f"Attempt {attempt} failed: {e}")

                if attempt < self._max_attempts:
                    delay = self.calculate_delay(attempt)
                    logger.info(f"Retrying in {delay:.2f}s...")
                    await asyncio.sleep(delay)

        raise last_error


class FallbackManager:
    """Manages fallback handlers for failed operations."""

    def __init__(self):
        self._fallbacks: Dict[str, Callable] = {}

    def register(self, operation: str, fallback: Callable) -> None:
        """Register a fallback handler for an operation."""
        self._fallbacks[operation] = fallback

    def unregister(self, operation: str) -> bool:
        """Unregister a fallback handler."""
        return self._fallbacks.pop(operation, None) is not None

    async def execute(
        self,
        operation: str,
        primary: Callable,
        *args,
        **kwargs
    ) -> Any:
        """Execute primary or fallback."""
        try:
            if asyncio.iscoroutinefunction(primary):
                return await primary(*args, **kwargs)
            else:
                return primary(*args, **kwargs)
        except Exception as e:
            logger.warning(f"Primary failed for {operation}: {e}")
            fallback = self._fallbacks.get(operation)
            if fallback:
                if asyncio.iscoroutinefunction(fallback):
                    return await fallback(*args, **kwargs)
                else:
                    return fallback(*args, **kwargs)
            raise


class ResilienceOrchestrator:
    """Orchestrates multiple resilience patterns."""

    def __init__(self):
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._bulkheads: Dict[str, Bulkhead] = {}
        self._retry_policies: Dict[str, RetryPolicy] = {}
        self._fallback_manager = FallbackManager()

    def create_circuit_breaker(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None
    ) -> CircuitBreaker:
        """Create a circuit breaker."""
        cb = CircuitBreaker(name, config)
        self._circuit_breakers[name] = cb
        return cb

    def create_bulkhead(
        self,
        name: str,
        config: Optional[BulkheadConfig] = None
    ) -> Bulkhead:
        """Create a bulkhead."""
        bh = Bulkhead(name, config)
        self._bulkheads[name] = bh
        return bh

    def create_retry_policy(
        self,
        name: str,
        max_attempts: int = 3,
        base_delay: float = 1.0
    ) -> RetryPolicy:
        """Create a retry policy."""
        policy = RetryPolicy(max_attempts=max_attempts, base_delay=base_delay)
        self._retry_policies[name] = policy
        return policy

    def get_circuit_breaker(self, name: str) -> Optional[CircuitBreaker]:
        """Get a circuit breaker by name."""
        return self._circuit_breakers.get(name)

    def get_bulkhead(self, name: str) -> Optional[Bulkhead]:
        """Get a bulkhead by name."""
        return self._bulkheads.get(name)

    def get_stats(self) -> Dict[str, Any]:
        """Get statistics for all resilience components."""
        return {
            "circuit_breakers": {
                name: {
                    "state": cb.state.value,
                    "total_calls": cb.stats.total_calls,
                    "failed_calls": cb.stats.failed_calls,
                    "rejected_calls": cb.stats.rejected_calls
                }
                for name, cb in self._circuit_breakers.items()
            },
            "bulkheads": {
                name: {
                    "current_calls": bh.stats.current_calls,
                    "total_calls": bh.stats.total_calls,
                    "rejected_calls": bh.stats.rejected_calls
                }
                for name, bh in self._bulkheads.items()
            }
        }


class APIResilienceAction:
    """Main action class for API resilience patterns."""

    def __init__(self):
        self._orchestrator = ResilienceOrchestrator()

    def create_circuit_breaker(
        self,
        name: str,
        failure_threshold: int = 5,
        timeout_seconds: float = 60.0
    ) -> Dict[str, Any]:
        """Create a circuit breaker."""
        config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            timeout_seconds=timeout_seconds
        )
        cb = self._orchestrator.create_circuit_breaker(name, config)
        return {"success": True, "name": name, "state": cb.state.value}

    def create_bulkhead(
        self,
        name: str,
        max_concurrent: int = 10,
        max_queue: int = 5
    ) -> Dict[str, Any]:
        """Create a bulkhead."""
        config = BulkheadConfig(
            max_concurrent_calls=max_concurrent,
            max_queue_size=max_queue
        )
        bh = self._orchestrator.create_bulkhead(name, config)
        return {"success": True, "name": name}

    def get_circuit_state(self, name: str) -> Dict[str, Any]:
        """Get circuit breaker state."""
        cb = self._orchestrator.get_circuit_breaker(name)
        if cb:
            stats = cb.stats
            return {
                "success": True,
                "name": name,
                "state": cb.state.value,
                "stats": {
                    "total_calls": stats.total_calls,
                    "successful_calls": stats.successful_calls,
                    "failed_calls": stats.failed_calls,
                    "rejected_calls": stats.rejected_calls
                }
            }
        return {"success": False, "error": "Circuit breaker not found"}

    def get_stats(self) -> Dict[str, Any]:
        """Get resilience statistics."""
        return {"success": True, "stats": self._orchestrator.get_stats()}

    async def execute(
        self,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute the API resilience action.

        Args:
            context: Dictionary containing:
                - operation: Operation to perform
                - Other operation-specific fields

        Returns:
            Dictionary with operation results.
        """
        operation = context.get("operation", "get_stats")

        if operation == "create_circuit_breaker":
            return self.create_circuit_breaker(
                name=context.get("name", "default"),
                failure_threshold=context.get("failure_threshold", 5),
                timeout_seconds=context.get("timeout_seconds", 60.0)
            )

        elif operation == "create_bulkhead":
            return self.create_bulkhead(
                name=context.get("name", "default"),
                max_concurrent=context.get("max_concurrent", 10),
                max_queue=context.get("max_queue", 5)
            )

        elif operation == "get_circuit_state":
            return self.get_circuit_state(context.get("name", "default"))

        elif operation == "get_stats":
            return self.get_stats()

        elif operation == "circuit_state":
            return self.get_circuit_state(context.get("name", "default"))

        else:
            return {"success": False, "error": f"Unknown operation: {operation}"}
