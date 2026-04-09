"""
API Resilience and Fault Tolerance Module.

Implements retry with backoff, circuit breakers, bulkheads,
and fallback strategies for robust API client implementations.

Author: AutoGen
"""
from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Set, Tuple, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class BackoffStrategy(Enum):
    EXPONENTIAL = auto()
    LINEAR = auto()
    FIBONACCI = auto()
    CONSTANT = auto()
    JITTER = auto()


@dataclass
class RetryConfig:
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    backoff_strategy: BackoffStrategy = BackoffStrategy.EXPONENTIAL
    retryable_exceptions: Tuple[type, ...] = (Exception,)
    timeout: float = 30.0


@dataclass
class CircuitState:
    name: str
    state: str = "closed"
    failure_count: int = 0
    success_count: int = 0
    last_failure_time: Optional[float] = None
    last_success_time: Optional[float] = None


class CircuitBreakerState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout: float = 60.0
    half_open_max_calls: int = 1


class CircuitBreaker:
    """
    Circuit breaker pattern implementation.
    """

    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitBreakerState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[float] = None
        self._last_state_change: float = time.time()
        self._half_open_calls: int = 0

    @property
    def state(self) -> CircuitBreakerState:
        if self._state == CircuitBreakerState.OPEN:
            if time.time() - self._last_state_change >= self.config.timeout:
                self._transition_to(CircuitBreakerState.HALF_OPEN)
        return self._state

    def _transition_to(self, new_state: CircuitBreakerState) -> None:
        self._state = new_state
        self._last_state_change = time.time()
        logger.info("Circuit breaker '%s' transitioned to %s", self.name, new_state.value)
        if new_state == CircuitBreakerState.HALF_OPEN:
            self._half_open_calls = 0

    def record_success(self) -> None:
        self._success_count += 1
        self._last_success_time = time.time()

        if self._state == CircuitBreakerState.HALF_OPEN:
            self._half_open_calls += 1
            if self._half_open_calls >= self.config.half_open_max_calls:
                self._transition_to(CircuitBreakerState.CLOSED)
                self._failure_count = 0
        elif self._state == CircuitBreakerState.CLOSED:
            self._failure_count = max(0, self._failure_count - 1)

    def record_failure(self) -> None:
        self._failure_count += 1
        self._last_failure_time = time.time()

        if self._state == CircuitBreakerState.HALF_OPEN:
            self._transition_to(CircuitBreakerState.OPEN)
        elif (
            self._state == CircuitBreakerState.CLOSED
            and self._failure_count >= self.config.failure_threshold
        ):
            self._transition_to(CircuitBreakerState.OPEN)

    def is_available(self) -> bool:
        return self.state != CircuitBreakerState.OPEN

    def get_stats(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "state": self.state.value,
            "failure_count": self._failure_count,
            "success_count": self._success_count,
            "last_failure": self._last_failure_time,
            "last_success": self._last_success_time,
        }


class Bulkhead:
    """Semaphore-based bulkhead for limiting concurrent calls."""

    def __init__(self, max_concurrent: int = 10, max_queue: int = 0):
        self.max_concurrent = max_concurrent
        self.max_queue = max_queue
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._current: int = 0
        self._rejected: int = 0
        self._executed: int = 0

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        if self._semaphore.locked() and self.max_queue > 0:
            self._rejected += 1
            raise RuntimeError("Bulkhead capacity exceeded")

        async with self._semaphore:
            self._current += 1
            self._executed += 1
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)
            finally:
                self._current -= 1

    def get_stats(self) -> Dict[str, int]:
        return {
            "current": self._current,
            "max_concurrent": self.max_concurrent,
            "executed": self._executed,
            "rejected": self._rejected,
        }


class ResilientAPIClient:
    """
    API client with built-in resilience patterns.
    """

    def __init__(self):
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._bulkheads: Dict[str, Bulkhead] = {}
        self._default_retry_config = RetryConfig()

    def get_circuit_breaker(self, name: str) -> CircuitBreaker:
        if name not in self._circuit_breakers:
            self._circuit_breakers[name] = CircuitBreaker(name)
        return self._circuit_breakers[name]

    def get_bulkhead(self, name: str, max_concurrent: int = 10) -> Bulkhead:
        if name not in self._bulkheads:
            self._bulkheads[name] = Bulkhead(max_concurrent)
        return self._bulkheads[name]

    async def call_with_retry(
        self,
        func: Callable,
        config: Optional[RetryConfig] = None,
        *args,
        **kwargs,
    ) -> Any:
        cfg = config or self._default_retry_config
        last_exception: Optional[Exception] = None

        for attempt in range(cfg.max_attempts):
            try:
                if asyncio.iscoroutinefunction(func):
                    result = await asyncio.wait_for(func(*args, **kwargs), timeout=cfg.timeout)
                else:
                    result = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: func(*args, **kwargs)
                    )
                return result

            except cfg.retryable_exceptions as exc:
                last_exception = exc
                if attempt < cfg.max_attempts - 1:
                    delay = self._calculate_delay(attempt, cfg)
                    logger.warning(
                        "Retry attempt %d/%d after %.1fs: %s",
                        attempt + 1, cfg.max_attempts, delay, exc
                    )
                    await asyncio.sleep(delay)
                else:
                    raise

            except asyncio.TimeoutError:
                last_exception = asyncio.TimeoutError(f"Call timed out after {cfg.timeout}s")
                if attempt < cfg.max_attempts - 1:
                    delay = self._calculate_delay(attempt, cfg)
                    await asyncio.sleep(delay)
                else:
                    raise

        raise last_exception or Exception("Max retries exceeded")

    def _calculate_delay(self, attempt: int, config: RetryConfig) -> float:
        if config.backoff_strategy == BackoffStrategy.EXPONENTIAL:
            delay = config.initial_delay * (config.multiplier ** attempt)
        elif config.backoff_strategy == BackoffStrategy.LINEAR:
            delay = config.initial_delay * (attempt + 1)
        elif config.backoff_strategy == BackoffStrategy.FIBONACCI:
            delay = config.initial_delay * self._fibonacci(attempt + 1)
        elif config.backoff_strategy == BackoffStrategy.CONSTANT:
            delay = config.initial_delay
        else:
            delay = config.initial_delay

        delay = min(delay, config.max_delay)

        if config.jitter:
            delay = delay * (0.5 + random.random())

        return delay

    @staticmethod
    def _fibonacci(n: int) -> int:
        if n <= 1:
            return 1
        a, b = 1, 1
        for _ in range(n - 1):
            a, b = b, a + b
        return b

    async def call_with_circuit_breaker(
        self,
        circuit_name: str,
        func: Callable,
        *args,
        **kwargs,
    ) -> Any:
        cb = self.get_circuit_breaker(circuit_name)

        if not cb.is_available():
            raise RuntimeError(f"Circuit breaker '{circuit_name}' is open")

        try:
            result = await self.call_with_retry(func, *args, **kwargs)
            cb.record_success()
            return result
        except Exception as exc:
            cb.record_failure()
            raise

    def with_resilience(self, name: str = "default"):
        """Decorator to add resilience patterns to a function."""
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                return await self.call_with_circuit_breaker(name, func, *args, **kwargs)
            return wrapper
        return decorator
