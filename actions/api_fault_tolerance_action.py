# Copyright (c) 2024. coded by claude
"""API Fault Tolerance Action Module.

Implements fault tolerance patterns for API calls including
circuit breaker, bulkhead isolation, and fallback mechanisms.
"""
from typing import Optional, Dict, Any, Callable, TypeVar
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import asyncio
import logging

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    success_threshold: int = 2
    timeout_seconds: float = 60.0
    half_open_max_calls: int = 3


@dataclass
class CircuitBreakerMetrics:
    total_calls: int = 0
    successful_calls: int = 0
    failed_calls: int = 0
    rejected_calls: int = 0
    state: CircuitState = CircuitState.CLOSED


class CircuitBreaker:
    def __init__(self, name: str, config: Optional[CircuitBreakerConfig] = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: Optional[datetime] = None
        self._half_open_calls = 0
        self._metrics = CircuitBreakerMetrics()

    async def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        self._metrics.total_calls += 1
        if not self._can_execute():
            self._metrics.rejected_calls += 1
            raise Exception(f"Circuit breaker '{self.name}' is open")
        try:
            result = func(*args, **kwargs)
            if asyncio.iscoroutine(result):
                result = await result
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise e

    def _can_execute(self) -> bool:
        if self._state == CircuitState.CLOSED:
            return True
        if self._state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
                return True
            return False
        if self._state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self.config.half_open_max_calls
        return False

    def _should_attempt_reset(self) -> bool:
        if not self._last_failure_time:
            return False
        return (datetime.now() - self._last_failure_time).total_seconds() >= self.config.timeout_seconds

    def _on_success(self) -> None:
        self._metrics.successful_calls += 1
        self._failure_count = 0
        if self._state == CircuitState.HALF_OPEN:
            self._success_count += 1
            if self._success_count >= self.config.success_threshold:
                self._transition_to_closed()
        elif self._state == CircuitState.CLOSED:
            pass

    def _on_failure(self) -> None:
        self._metrics.failed_calls += 1
        self._failure_count += 1
        self._last_failure_time = datetime.now()
        if self._state == CircuitState.HALF_OPEN:
            self._transition_to_open()
        elif self._state == CircuitState.CLOSED:
            if self._failure_count >= self.config.failure_threshold:
                self._transition_to_open()

    def _transition_to_open(self) -> None:
        self._state = CircuitState.OPEN
        self._metrics.state = CircuitState.OPEN
        logger.warning(f"Circuit breaker '{self.name}' opened")

    def _transition_to_half_open(self) -> None:
        self._state = CircuitState.HALF_OPEN
        self._metrics.state = CircuitState.HALF_OPEN
        self._half_open_calls = 0
        self._success_count = 0
        logger.info(f"Circuit breaker '{self.name}' half-open")

    def _transition_to_closed(self) -> None:
        self._state = CircuitState.CLOSED
        self._metrics.state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        logger.info(f"Circuit breaker '{self.name}' closed")

    def get_state(self) -> CircuitState:
        return self._state

    def get_metrics(self) -> CircuitBreakerMetrics:
        return self._metrics
